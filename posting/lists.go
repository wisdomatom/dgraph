/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dgryski/go-farm"
	ostats "go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"google.golang.org/protobuf/proto"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/dgraph-io/ristretto/v2/z"
)

const (
	mb        = 1 << 20
	numShards = 16
)

var (
	pstore                x.KVDB
	closer                *z.Closer
	EnableDetailedMetrics bool
)

// Init initializes the posting lists package, the in memory and dirty list hash.
func Init(ps x.KVDB, cacheSize int64, removeOnUpdate bool) {
	pstore = ps
	closer = z.NewCloser(1)
	go x.MonitorMemoryMetrics(closer)

	MemLayerInstance = initMemoryLayer(cacheSize, removeOnUpdate)
}

func SetEnabledDetailedMetrics(enableMetrics bool) {
	EnableDetailedMetrics = enableMetrics
}

// Cleanup waits until the closer has finished processing.
func Cleanup() {
	closer.SignalAndWait()
}

// GetNoStore returns the list stored in the key or creates a new one if it doesn't exist.
// It does not store the list in any cache.
func GetNoStore(key []byte, readTs uint64) (rlist *List, err error) {
	return getNew(key, pstore, readTs, false)
}

type cacheShard struct {
	sync.RWMutex
	// The keys for these maps is a string representation of the Badger key for the posting list.
	// deltas keep track of the updates made by txn. These must be kept around until written to disk
	// during commit.
	deltas map[string][]byte

	// max committed timestamp of the read posting lists.
	maxVersions map[string]uint64

	// plists are posting lists in memory. They can be discarded to reclaim space.
	plists map[string]*List
}

// LocalCache stores a cache of posting lists and deltas.
// This doesn't sync, so call this only when you don't care about dirty posting lists in
// memory(for example before populating snapshot) or after calling syncAllMarks
type LocalCache struct {
	startTs  uint64
	commitTs uint64
	shards   [numShards]cacheShard
}

func (lc *LocalCache) getShard(key string) *cacheShard {
	return &lc.shards[farm.Fingerprint64([]byte(key))%numShards]
}

func (lc *LocalCache) Lock()    {}
func (lc *LocalCache) Unlock()  {}
func (lc *LocalCache) RLock()   {}
func (lc *LocalCache) RUnlock() {}

// struct to implement LocalCache interface from vector-indexer
// acts as wrapper for dgraph *LocalCache
type viLocalCache struct {
	delegate *LocalCache
}

func (vc *viLocalCache) Find(prefix []byte, filter func([]byte) bool) (uint64, error) {
	return vc.delegate.Find(prefix, filter)
}

func (vc *viLocalCache) Get(key []byte) ([]byte, error) {
	pl, err := vc.delegate.Get(key)
	if err != nil {
		return nil, err
	}
	pl.Lock()
	defer pl.Unlock()
	return vc.GetValueFromPostingList(pl)
}

func (vc *viLocalCache) GetWithLockHeld(key []byte) ([]byte, error) {
	pl, err := vc.delegate.Get(key)
	if err != nil {
		return nil, err
	}
	return vc.GetValueFromPostingList(pl)
}

func (vc *viLocalCache) GetValueFromPostingList(pl *List) ([]byte, error) {
	if pl.cache != nil {
		return pl.cache, nil
	}
	value := pl.findStaticValue(vc.delegate.startTs)

	if value == nil || len(value.Postings) == 0 {
		return nil, ErrNoValue
	}

	if value.Postings[0].Op == Del {
		return nil, ErrNoValue
	}

	pl.cache = value.Postings[0].Value
	return pl.cache, nil
}

func NewViLocalCache(delegate *LocalCache) *viLocalCache {
	return &viLocalCache{delegate: delegate}
}

// NewLocalCache returns a new LocalCache instance.
func NewLocalCache(startTs uint64) *LocalCache {
	lc := &LocalCache{
		startTs: startTs,
	}
	for i := 0; i < numShards; i++ {
		lc.shards[i].deltas = make(map[string][]byte)
		lc.shards[i].plists = make(map[string]*List)
		lc.shards[i].maxVersions = make(map[string]uint64)
	}
	return lc
}

// NoCache returns a new LocalCache instance, which won't cache anything. Useful to pass startTs
// around.
func NoCache(startTs uint64) *LocalCache {
	return &LocalCache{startTs: startTs}
}

func (lc *LocalCache) UpdateCommitTs(commitTs uint64) {
	lc.commitTs = commitTs
}

func (lc *LocalCache) Find(pred []byte, filter func([]byte) bool) (uint64, error) {
	txn := pstore.NewTransaction(lc.startTs, false)
	defer txn.Discard()

	attr := string(pred)

	initKey := x.ParsedKey{
		Attr: attr,
	}
	startKey := x.DataKey(attr, 0)
	prefix := initKey.DataPrefix()

	var prevKey []byte
	it := txn.NewIterator(x.KVIterOpts{
		PrefetchValues: false,
		AllVersions:    true,
		Prefix:         prefix,
	})
	defer it.Close()

	for it.Seek(startKey); it.Valid(); {
		item := it.Item()
		if bytes.Equal(item.Key(), prevKey) {
			it.Next()
			continue
		}
		prevKey = append(prevKey[:0], item.Key()...)

		// Parse the key upfront, otherwise ReadPostingList would advance the
		// iterator.
		pk, err := x.Parse(item.Key())
		if err != nil {
			return 0, err
		}

		// If we have moved to the next attribute, break
		if pk.Attr != attr {
			break
		}

		if pk.HasStartUid {
			// The keys holding parts of a split key should not be accessed here because
			// they have a different prefix. However, the check is being added to guard
			// against future bugs.
			continue
		}

		// This bit would only be set if there are valid uids in UidPack.
		key := x.DataKey(attr, pk.Uid)
		pl, err := lc.Get(key)
		if err != nil {
			return 0, err
		}
		vals, err := pl.Value(lc.startTs)
		switch {
		case err == ErrNoValue:
			it.Next()
			continue
		case err != nil:
			return 0, err
		}

		if filter(vals.Value.([]byte)) {
			return pk.Uid, nil
		}

		it.Next()
	}

	return 0, badger.ErrKeyNotFound
}

func (lc *LocalCache) getNoStore(key string) *List {
	shard := lc.getShard(key)
	shard.RLock()
	defer shard.RUnlock()
	if l, ok := shard.plists[key]; ok {
		return l
	}
	return nil
}

// SetIfAbsent adds the list for the specified key to the cache. If a list for the same
// key already exists, the cache will not be modified and the existing list
// will be returned instead. This behavior is meant to prevent the goroutines
// using the cache from ending up with an orphaned version of a list.
func (lc *LocalCache) SetIfAbsent(key string, updated *List) *List {
	shard := lc.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	if pl, ok := shard.plists[key]; ok {
		return pl
	}
	shard.plists[key] = updated
	return updated
}

func (lc *LocalCache) getInternal(key []byte, readFromDisk, readUids bool) (*List, error) {
	skey := string(key)
	getNewPlistNil := func() (*List, error) {
		shard := lc.getShard(skey)
		shard.RLock()
		defer shard.RUnlock()
		if shard.plists == nil {
			return getNew(key, pstore, lc.startTs, readUids)
		}
		if l, ok := shard.plists[skey]; ok {
			return l, nil
		}
		return nil, nil
	}

	if l, err := getNewPlistNil(); l != nil || err != nil {
		return l, err
	}

	var pl *List
	if readFromDisk {
		var err error
		pl, err = getNew(key, pstore, lc.startTs, readUids)
		if err != nil {
			return nil, err
		}
	} else {
		pl = &List{
			key:         key,
			plist:       new(pb.PostingList),
			mutationMap: newMutableLayer(),
		}
	}

	// If we just brought this posting list into memory and we already have a delta for it, let's
	// apply it before returning the list.
	shard := lc.getShard(skey)
	shard.RLock()
	if delta, ok := shard.deltas[skey]; ok && len(delta) > 0 {
		pl.setMutation(lc.startTs, delta)
	}
	shard.RUnlock()
	return lc.SetIfAbsent(skey, pl), nil
}

func (lc *LocalCache) readPostingListAt(key []byte) (*pb.PostingList, error) {
	if EnableDetailedMetrics {
		start := time.Now()
		defer func() {
			ms := x.SinceMs(start)
			pk, _ := x.Parse(key)
			var tags []tag.Mutator
			tags = append(tags, tag.Upsert(x.KeyMethod, "get"))
			tags = append(tags, tag.Upsert(x.KeyStatus, pk.Attr))
			_ = ostats.RecordWithTags(context.Background(), tags, x.BadgerReadLatencyMs.M(ms))
		}()
	}

	pl := &pb.PostingList{}
	txn := pstore.NewTransaction(lc.startTs, false)
	defer txn.Discard()

	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}

	err = item.Value(func(val []byte) error {
		return proto.Unmarshal(val, pl)
	})

	return pl, err
}

// GetSinglePosting retrieves the cached version of the first item in the list associated with the
// given key. This is used for retrieving the value of a scalar predicats.
func (lc *LocalCache) GetSinglePosting(key []byte) (*pb.PostingList, error) {
	// This would return an error if there is some data in the local cache, but we couldn't read it.
	getListFromLocalCache := func() (*pb.PostingList, error) {
		skey := string(key)
		shard := lc.getShard(skey)
		shard.RLock()

		pl := &pb.PostingList{}
		if delta, ok := shard.deltas[skey]; ok && len(delta) > 0 {
			err := proto.Unmarshal(delta, pl)
			shard.RUnlock()
			return pl, err
		}

		l := shard.plists[skey]
		shard.RUnlock()

		if l != nil {
			return l.StaticValue(lc.startTs)
		}

		return nil, nil
	}

	getPostings := func() (*pb.PostingList, error) {
		pl, err := getListFromLocalCache()
		// If both pl and err are empty, that means that there was no data in local cache, hence we should
		// read the data from badger.
		if pl != nil || err != nil {
			return pl, err
		}

		return lc.readPostingListAt(key)
	}

	pl, err := getPostings()
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// Filter and remove STAR_ALL and OP_DELETE Postings
	idx := 0
	for _, postings := range pl.Postings {
		if hasDeleteAll(postings) {
			return nil, nil
		}
		if postings.Op != Del {
			pl.Postings[idx] = postings
			idx++
		}
	}
	pl.Postings = pl.Postings[:idx]
	return pl, nil
}

// Get retrieves the cached version of the list associated with the given key.
func (lc *LocalCache) Get(key []byte) (*List, error) {
	return lc.getInternal(key, true, false)
}

func (lc *LocalCache) GetUids(key []byte) (*List, error) {
	return lc.getInternal(key, true, true)
}

// GetFromDelta gets the cached version of the list without reading from disk
// and only applies the existing deltas. This is used in situations where the
// posting list will only be modified and not read (e.g adding index mutations).
func (lc *LocalCache) GetFromDelta(key []byte) (*List, error) {
	return lc.getInternal(key, false, false)
}

// UpdateDeltasAndDiscardLists updates the delta cache before removing the stored posting lists.
func (lc *LocalCache) UpdateDeltasAndDiscardLists() {
	for i := 0; i < numShards; i++ {
		shard := &lc.shards[i]
		shard.Lock()
		if len(shard.plists) == 0 {
			shard.Unlock()
			continue
		}

		for key, pl := range shard.plists {
			data := pl.getMutation(lc.startTs)
			if len(data) > 0 {
				shard.deltas[key] = data
			}
			shard.maxVersions[key] = pl.maxVersion()
			// We can't run pl.release() here because LocalCache is still being used by other callers
			// for the same transaction, who might be holding references to posting lists.
			// TODO: Find another way to reuse postings via postingPool.
		}
		shard.plists = make(map[string]*List)
		shard.Unlock()
	}
}

func (lc *LocalCache) IterateDeltas(f func(key string, delta []byte)) {
	for i := 0; i < numShards; i++ {
		shard := &lc.shards[i]
		shard.RLock()
		for k, v := range shard.deltas {
			f(k, v)
		}
		shard.RUnlock()
	}
}

func (lc *LocalCache) IteratePlists(f func(key string, pl *List)) {
	for i := 0; i < numShards; i++ {
		shard := &lc.shards[i]
		shard.RLock()
		for k, v := range shard.plists {
			f(k, v)
		}
		shard.RUnlock()
	}
}

func (lc *LocalCache) IterateMaxVersions(f func(key string, maxVersion uint64)) {
	for i := 0; i < numShards; i++ {
		shard := &lc.shards[i]
		shard.RLock()
		for k, v := range shard.maxVersions {
			f(k, v)
		}
		shard.RUnlock()
	}
}

func (lc *LocalCache) DeltasLen() int {
	var count int
	for i := 0; i < numShards; i++ {
		shard := &lc.shards[i]
		shard.RLock()
		count += len(shard.deltas)
		shard.RUnlock()
	}
	return count
}

func (lc *LocalCache) PlistsLen() int {
	var count int
	for i := 0; i < numShards; i++ {
		shard := &lc.shards[i]
		shard.RLock()
		count += len(shard.plists)
		shard.RUnlock()
	}
	return count
}

func (lc *LocalCache) HasDelta(key string) bool {
	shard := lc.getShard(key)
	shard.RLock()
	defer shard.RUnlock()
	_, ok := shard.deltas[key]
	return ok
}

func (lc *LocalCache) AnyDelta(f func(key string, delta []byte) bool) bool {
	for i := 0; i < numShards; i++ {
		shard := &lc.shards[i]
		shard.RLock()
		for k, v := range shard.deltas {
			if f(k, v) {
				shard.RUnlock()
				return true
			}
		}
		shard.RUnlock()
	}
	return false
}

func (lc *LocalCache) GetDelta(key string) []byte {
	shard := lc.getShard(key)
	shard.RLock()
	defer shard.RUnlock()
	return shard.deltas[key]
}

func (lc *LocalCache) SetDelta(key string, delta []byte) {
	shard := lc.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	shard.deltas[key] = delta
}

func (lc *LocalCache) GetMaxVersion(key string) uint64 {
	shard := lc.getShard(key)
	shard.RLock()
	defer shard.RUnlock()
	return shard.maxVersions[key]
}

func (lc *LocalCache) fillPreds(ctx *api.TxnContext, gid uint32) {
	for i := 0; i < numShards; i++ {
		shard := &lc.shards[i]
		shard.RLock()
		for key := range shard.deltas {
			pk, err := x.Parse([]byte(key))
			x.Check(err)
			if len(pk.Attr) == 0 {
				continue
			}
			// Also send the group id that the predicate was being served by. This is useful when
			// checking if Zero should allow a commit during a predicate move.
			predKey := fmt.Sprintf("%d-%s", gid, pk.Attr)
			ctx.Preds = append(ctx.Preds, predKey)
		}
		shard.RUnlock()
	}
	ctx.Preds = x.Unique(ctx.Preds)
}
