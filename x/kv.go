/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package x

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/golang/glog"
)

var (
	tsCounter  uint64 = 100
	tsMax      uint64 = 0
	tsMu       sync.Mutex
	tsLeaser   func() (uint64, uint64, error)
	uidCounter uint64 = 100
	uidMax     uint64 = 0
	uidMu      sync.Mutex
	uidLeaser  func() (uint64, uint64, error)
)

func SetUidLeaser(f func() (uint64, uint64, error)) {
	uidMu.Lock()
	defer uidMu.Unlock()
	uidLeaser = f
}

func SetTsLeaser(f func() (uint64, uint64, error)) {
	tsMu.Lock()
	defer tsMu.Unlock()
	tsLeaser = f
}

func GetNextTs() uint64 {
	for {
		cur := atomic.LoadUint64(&tsCounter)
		max := atomic.LoadUint64(&tsMax)
		if cur < max {
			if atomic.CompareAndSwapUint64(&tsCounter, cur, cur+1) {
				return cur + 1
			}
			continue
		}

		if tsLeaser == nil {
			return atomic.AddUint64(&tsCounter, 1)
		}

		tsMu.Lock()
		cur = atomic.LoadUint64(&tsCounter)
		max = atomic.LoadUint64(&tsMax)
		if cur >= max {
			start, end, err := tsLeaser()
			if err == nil {
				atomic.StoreUint64(&tsCounter, start)
				atomic.StoreUint64(&tsMax, end)
			} else {
				glog.Errorf("Failed to renew TS lease: %v", err)
				val := atomic.AddUint64(&tsCounter, 1)
				tsMu.Unlock()
				return val
			}
		}
		tsMu.Unlock()
	}
}

func SetNextTsRange(start, end uint64) {
	tsMu.Lock()
	defer tsMu.Unlock()
	atomic.StoreUint64(&tsCounter, start)
	atomic.StoreUint64(&tsMax, end)
	glog.Infof("TS range leased: [%d, %d]", start, end)
}

func GetNextUid() uint64 {
	for {
		cur := atomic.LoadUint64(&uidCounter)
		max := atomic.LoadUint64(&uidMax)
		if cur < max {
			if atomic.CompareAndSwapUint64(&uidCounter, cur, cur+1) {
				return cur + 1
			}
			continue
		}

		if uidLeaser == nil {
			return atomic.AddUint64(&uidCounter, 1)
		}

		uidMu.Lock()
		cur = atomic.LoadUint64(&uidCounter)
		max = atomic.LoadUint64(&uidMax)
		if cur >= max {
			start, end, err := uidLeaser()
			if err == nil {
				atomic.StoreUint64(&uidCounter, start)
				atomic.StoreUint64(&uidMax, end)
			} else {
				glog.Errorf("Failed to renew UID lease: %v", err)
				val := atomic.AddUint64(&uidCounter, 1)
				uidMu.Unlock()
				return val
			}
		}
		uidMu.Unlock()
	}
}

func SetNextUid(val uint64) {
	uidMu.Lock()
	defer uidMu.Unlock()
	atomic.StoreUint64(&uidCounter, val)
	atomic.StoreUint64(&uidMax, val) // Just a base
}

func SetNextUidRange(start, end uint64) {
	uidMu.Lock()
	defer uidMu.Unlock()
	atomic.StoreUint64(&uidCounter, start)
	atomic.StoreUint64(&uidMax, end)
	glog.Infof("UID range leased: [%d, %d]", start, end)
}

var ErrKeyNotFound = badger.ErrKeyNotFound

const (
	// BitSchemaPosting signals that the value stores a schema or type.
	BitSchemaPosting byte = 0x01
	// BitDeltaPosting signals that the value stores the delta of a posting list.
	BitDeltaPosting byte = 0x04
	// BitCompletePosting signals that the values stores a complete posting list.
	BitCompletePosting byte = 0x08
	// BitEmptyPosting signals that the value stores an empty posting list.
	BitEmptyPosting byte = 0x10
	// BitDeletePosting is used for Badger deletion.
	BitDeletePosting byte = 0x20
)

// KVDB is the interface for the underlying KV store.
type KVDB interface {
	// NewTransactionAt creates a new transaction at the given read timestamp.
	NewTransactionAt(readTs uint64, update bool) KVTxn
	// Close closes the database.
	Close() error
	// Sync syncs the database to disk.
	Sync() error
	// DropPrefix drops all keys with the given prefix.
	DropPrefix(prefixes ...[]byte) error
	// DropAll drops all keys in the database.
	DropAll() error
	// NewStreamAt creates a new stream for scanning the database at the given read timestamp.
	NewStreamAt(readTs uint64) KVStream
	// NewWriteBatch creates a new write batch.
	NewWriteBatch() KVWriteBatch
	// NewManagedWriteBatch creates a new write batch for managed mode.
	NewManagedWriteBatch() KVWriteBatch
	// RunValueLogGC runs value log garbage collection.
	RunValueLogGC(discardRatio float64) error
	// Size returns the size of the database.
	Size() (int64, int64)
	// BanNamespace bans the namespace.
	BanNamespace(ns uint64) error
	SetDiscardTs(ts uint64)
	Tables() []badger.TableInfo
	NewStreamWriter() KVStreamWriter
	Subscribe(ctx context.Context, cb func(*pb.KVList) error, matches []pb.Match) error
	CacheMaxCost(badger.CacheType, int64) (int64, error)
	GetTimestamp(ctx context.Context) (uint64, error)
	AllocateStartTs() uint64
}

// KVStreamWriter is the interface for bulk loading data.
type KVStreamWriter interface {
	Prepare() error
	PrepareIncremental() error
	Write(buf *z.Buffer) error
	Flush() error
	Cancel()
}

// KVTxn is the interface for a KV transaction.
type KVTxn interface {
	// Get retrieves the value for the given key.
	Get(key []byte) (KVItem, error)
	// Set sets the value for the given key.
	Set(key, val []byte) error
	// SetWithMeta sets the value for the given key with metadata.
	SetWithMeta(key, val []byte, meta byte) error
	// SetWithDiscard sets the value and indicates earlier versions can be discarded.
	SetWithDiscard(key, val []byte, meta byte) error
	// SetEntry sets an entry.
	SetEntry(*badger.Entry) error
	// Delete deletes the key.
	Delete(key []byte) error
	// CommitAt commits the transaction at the given timestamp.
	CommitAt(commitTs uint64, callback func(error)) error
	// Discard discards the transaction.
	Discard()
	// NewIterator returns a new iterator for the transaction.
	NewIterator(opt KVIterOpts) KVIterator
	// LockKeys acquires pessimistic locks on the given keys.
	LockKeys(ctx context.Context, keys ...[]byte) error
}

// KVItem is the interface for a single KV pair returned by Get or Iterator.
type KVItem interface {
	Key() []byte
	KeyCopy([]byte) []byte
	Value(func(val []byte) error) error
	ValueCopy([]byte) ([]byte, error)
	UserMeta() byte
	Version() uint64
	IsDeletedOrExpired() bool
	ExpiresAt() uint64
}

// KVIterator is the interface for iterating over KV pairs.
type KVIterator interface {
	Rewind()
	Seek(key []byte)
	Valid() bool
	Next()
	Item() KVItem
	Close()
}

// KVStream is the interface for streaming KV pairs.
type KVStream interface {
	SetSend(func(buf *z.Buffer) error)
	SetKeyToList(func(key []byte, itr KVStreamIterator) (*pb.KVList, error))
	SetKeyToListWithThreadId(func(key []byte, itr KVStreamIterator, threadId int) (*pb.KVList, error))
	SetFinishThread(func(threadId int) (*pb.KVList, error))
	SetPrefix(prefix []byte)
	SetLogPrefix(prefix string)
	SetUseKeyToListWithThreadId(use bool)
	SetSinceTs(ts uint64)
	SetChooseKey(func(item KVItem) bool)
	Orchestrate(ctx context.Context) error
}

// KVStreamIterator is used within Stream.SetKeyToList to iterate over versions of a key.
type KVStreamIterator interface {
	Rewind()
	Seek(key []byte)
	Valid() bool
	Next()
	Item() KVItem
	Close()
}

// KVWriteBatch is the interface for bulk writes.
type KVWriteBatch interface {
	SetAt(key, val []byte, meta byte, ts uint64) error
	DeleteAt(key []byte, ts uint64) error
	Flush() error
	Write(buf *z.Buffer) error
}

// KVIterOpts contains options for the iterator.
type KVIterOpts struct {
	Prefix         []byte
	IsReverse      bool
	AllVersions    bool
	PrefetchValues bool
	PrefetchSize   int
}
