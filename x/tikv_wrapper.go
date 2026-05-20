/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package x

import (
	"bytes"
	"context"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"google.golang.org/protobuf/proto"
)

type tikvDB struct {
	client  *txnkv.Client
	pdAddrs []string
}

var logOnce sync.Once
var tikvTxns sync.Map

func NewTiKVKV(pdAddrs []string) (KVDB, error) {
	logOnce.Do(func() {
		l, p, _ := log.InitLogger(&log.Config{Level: "warn"})
		log.ReplaceGlobals(l, p)
	})
	client, err := txnkv.NewClient(pdAddrs)
	if err != nil {
		return nil, err
	}
	return &tikvDB{
		client:  client,
		pdAddrs: pdAddrs,
	}, nil
}

func (t *tikvDB) AllocateStartTs() uint64 {
	txn, err := t.client.Begin()
	if err != nil {
		panic(errors.Wrap(err, "failed to begin tikv transaction in AllocateStartTs"))
	}
	ts := txn.StartTS()
	tikvTxns.Store(ts, txn)
	return ts
}

func (t *tikvDB) NewTransactionAt(readTs uint64, update bool) KVTxn {
	if update {
		if v, ok := tikvTxns.LoadAndDelete(readTs); ok {
			return &tikvTxn{txn: v.(*transaction.KVTxn)}
		}
	}
	txn, err := t.client.Begin()
	if err != nil {
		panic(errors.Wrap(err, "failed to begin tikv transaction in NewTransactionAt"))
	}
	return &tikvTxn{txn: txn}
}

func (t *tikvDB) Close() error {
	return t.client.Close()
}

func (t *tikvDB) Sync() error {
	// TiKV is distributed and syncs on commit.
	return nil
}

func (t *tikvDB) DropPrefix(prefixes ...[]byte) error {
	// TiKV can delete ranges.
	ctx := context.Background()
	for _, prefix := range prefixes {
		end := make([]byte, len(prefix))
		copy(end, prefix)
		// Increment the last byte to get the end of the range.
		for i := len(end) - 1; i >= 0; i-- {
			end[i]++
			if end[i] != 0 {
				break
			}
		}
		_, err := t.client.DeleteRange(ctx, prefix, end, 1000)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *tikvDB) DropAll() error {
	// Dangerous operation, but required by interface.
	return t.DropPrefix([]byte{})
}

func (t *tikvDB) NewStreamAt(readTs uint64) KVStream {
	return &tikvStream{
		db:     t,
		readTs: readTs,
	}
}

type tikvStream struct {
	db        *tikvDB
	readTs    uint64
	prefix    []byte
	logPrefix string
	keyToList func(key []byte, itr KVStreamIterator) (*pb.KVList, error)
	send      func(buf *z.Buffer) error
	chooseKey func(item KVItem) bool
}

func (s *tikvStream) SetPrefix(prefix []byte)    { s.prefix = prefix }
func (s *tikvStream) SetLogPrefix(prefix string) { s.logPrefix = prefix }
func (s *tikvStream) SetKeyToList(f func(key []byte, itr KVStreamIterator) (*pb.KVList, error)) {
	s.keyToList = f
}
func (s *tikvStream) SetSend(f func(buf *z.Buffer) error) { s.send = f }
func (s *tikvStream) SetChooseKey(f func(item KVItem) bool) {
	s.chooseKey = f
}
func (s *tikvStream) SetKeyToListWithThreadId(f func(key []byte, itr KVStreamIterator, threadId int) (*pb.KVList, error)) {
}
func (s *tikvStream) SetFinishThread(f func(threadId int) (*pb.KVList, error)) {
}
func (s *tikvStream) SetUseKeyToListWithThreadId(use bool) {
}
func (s *tikvStream) SetSinceTs(ts uint64) {
}

func (s *tikvStream) Orchestrate(ctx context.Context) error {
	txn, err := s.db.client.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	iter, err := txn.Iter(s.prefix, nil)
	if err != nil {
		return err
	}
	defer iter.Close()

	// Simple implementation: iterate and call keyToList/send
	for iter.Valid() {
		key := iter.Key()
		if len(s.prefix) > 0 && !bytes.HasPrefix(key, s.prefix) {
			break
		}

		item := &tikvItem{key: key, value: iter.Value()}
		if s.chooseKey != nil && !s.chooseKey(item) {
			if err := iter.Next(); err != nil {
				return err
			}
			continue
		}

		if s.keyToList != nil {
			// This is a simplified version. Badger's Stream calls keyToList with an iterator
			// that can see multiple versions. Here we see only one.
			kvList, err := s.keyToList(key, &tikvStreamIterator{
				itr: &tikvInternalIteratorStub{item: item},
			})
			if err != nil {
				return err
			}
			if kvList != nil && s.send != nil {
				buf := z.NewBuffer(1024, "tikvStream")
				// We need a way to convert KVList to buffer.
				// In Badger, Orchestrate sends lists to Send.
				data, _ := proto.Marshal(kvList)
				buf.Write(data)
				if err := s.send(buf); err != nil {
					return err
				}
				buf.Release()
			}
		}

		if err := iter.Next(); err != nil {
			return err
		}
	}
	return nil
}

type tikvStreamIterator struct {
	itr tikvInternalIterator
}

func (it *tikvStreamIterator) Seek(key []byte) {
	// Not supported by stub
}

func (it *tikvStreamIterator) Valid() bool {
	return it.itr.Valid()
}

func (it *tikvStreamIterator) Next() {
	_ = it.itr.Next()
}

func (it *tikvStreamIterator) Item() KVItem {
	return &tikvItem{key: it.itr.Key(), value: it.itr.Value()}
}

func (it *tikvStreamIterator) Close() {
	it.itr.Close()
}

func (it *tikvStreamIterator) Rewind() {
	// Not supported by stub
}

type tikvInternalIteratorStub struct {
	item  *tikvItem
	valid bool
}

func (s *tikvInternalIteratorStub) Valid() bool     { return !s.valid }
func (s *tikvInternalIteratorStub) Key() []byte     { return s.item.key }
func (s *tikvInternalIteratorStub) Value() []byte   { return s.item.value }
func (s *tikvInternalIteratorStub) Next() error     { s.valid = true; return nil }
func (s *tikvInternalIteratorStub) Close()          {}
func (s *tikvInternalIteratorStub) Rewind()         { s.valid = false }
func (s *tikvInternalIteratorStub) Seek(key []byte) {}
func (s *tikvInternalIteratorStub) Item() KVItem    { return s.item }

func (t *tikvDB) NewWriteBatch() KVWriteBatch {
	// TiKV transactions can act as write batches.
	txn, _ := t.client.Begin()
	return &tikvWriteBatch{txn: txn}
}

func (t *tikvDB) NewManagedWriteBatch() KVWriteBatch {
	return t.NewWriteBatch()
}

func (t *tikvDB) RunValueLogGC(discardRatio float64) error {
	return nil // TiKV handles GC internally.
}

func (t *tikvDB) Size() (int64, int64) {
	return 0, 0 // Need to query PD for cluster stats if needed.
}

func (t *tikvDB) BanNamespace(ns uint64) error {
	return nil
}

func (t *tikvDB) SetDiscardTs(ts uint64) {}

func (t *tikvDB) Tables() []badger.TableInfo {
	return nil
}

func (t *tikvDB) NewStreamWriter() KVStreamWriter {
	// TiKV has IngestSST for bulk loading, but for now we can stub it.
	return nil
}

func (t *tikvDB) Subscribe(ctx context.Context, cb func(*pb.KVList) error, matches []pb.Match) error {
	return nil
}

func (t *tikvDB) CacheMaxCost(badger.CacheType, int64) (int64, error) {
	return 0, nil
}

func (t *tikvDB) GetTimestamp(ctx context.Context) (uint64, error) {
	return t.client.GetTimestamp(ctx)
}

type tikvInternalIterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next() error
	Close()
}

type tikvTxn struct {
	txn *transaction.KVTxn
}

func (tx *tikvTxn) Get(key []byte) (KVItem, error) {
	val, err := tx.txn.Get(context.Background(), key)
	if err != nil {
		if err.Error() == "not found" { // Need to verify TiKV not found error string or type
			return nil, ErrKeyNotFound
		}
		if err.Error() == "not exist" {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	return &tikvItem{key: key, value: val}, nil
}

func (tx *tikvTxn) Set(key, val []byte) error {
	// Use 0 as default meta
	return tx.SetWithMeta(key, val, 0)
}

func (tx *tikvTxn) SetWithMeta(key, val []byte, meta byte) error {
	// Protocol: [1-byte Meta] + [1-byte Reserved] + [Payload]
	buf := make([]byte, 2+len(val))
	buf[0] = meta
	buf[1] = 0 // Reserved
	copy(buf[2:], val)

	// if tx.txn.IsPessimistic() {
	// 	// Lock the key first in pessimistic mode to ensure queuing
	// 	if err := tx.txn.LockKeys(context.Background(), &kv.LockCtx{}, key); err != nil {
	// 		return err
	// 	}
	// }

	return tx.txn.Set(key, buf)
}

func (tx *tikvTxn) SetWithDiscard(key, val []byte, meta byte) error {
	return tx.SetWithMeta(key, val, meta)
}

func (tx *tikvTxn) SetEntry(e *badger.Entry) error {
	return tx.SetWithMeta(e.Key, e.Value, e.UserMeta)
}

func (tx *tikvTxn) Delete(key []byte) error {
	return tx.txn.Delete(key)
}

func (tx *tikvTxn) CommitAt(commitTs uint64, callback func(error)) error {
	// TiKV manages its own commitTS.
	err := tx.txn.Commit(context.Background())
	if callback != nil {
		callback(err)
	}
	return err
}

func (tx *tikvTxn) Discard() {
	tx.txn.Rollback()
}

func (tx *tikvTxn) NewIterator(opt KVIterOpts) KVIterator {
	// TiKV Iter takes start and end keys.
	var iter interface{}
	var err error
	if opt.IsReverse {
		// Handle reverse scan: TiKV's IterReverse(k) returns keys < k.
		// To match Badger's Seek(k) returning <= k, we append a null byte to the prefix/key.
		startKey := make([]byte, len(opt.Prefix), len(opt.Prefix)+1)
		copy(startKey, opt.Prefix)
		startKey = append(startKey, 0x00)
		iter, err = tx.txn.IterReverse(startKey)
	} else {
		iter, err = tx.txn.Iter(opt.Prefix, nil)
	}
	if err != nil {
		return nil
	}
	return &tikvIterator{
		txn:       tx.txn,
		iter:      iter.(tikvInternalIterator),
		prefix:    opt.Prefix,
		isReverse: opt.IsReverse,
	}
}

func (tx *tikvTxn) LockKeys(ctx context.Context, keys ...[]byte) error {
	// return tx.txn.LockKeys(ctx, &kv.LockCtx{}, keys...)
	return nil
}

type tikvItem struct {
	key   []byte
	value []byte
}

func (i *tikvItem) Key() []byte { return i.key }
func (i *tikvItem) KeyCopy(dst []byte) []byte {
	return append(dst[:0], i.key...)
}
func (i *tikvItem) Value(f func(val []byte) error) error {
	if len(i.value) < 2 {
		return f(i.value)
	}
	return f(i.value[2:])
}
func (i *tikvItem) ValueCopy(dst []byte) ([]byte, error) {
	if len(i.value) < 2 {
		return append(dst[:0], i.value...), nil
	}
	return append(dst[:0], i.value[2:]...), nil
}
func (i *tikvItem) UserMeta() byte {
	if len(i.value) < 1 {
		return 0
	}
	return i.value[0]
}
func (i *tikvItem) Version() uint64 {
	// This should be the commitTS of the KV. TiKV client-go doesn't expose it directly on Item.
	return 0
}
func (i *tikvItem) IsDeletedOrExpired() bool { return false }
func (i *tikvItem) ExpiresAt() uint64        { return 0 }

type tikvIterator struct {
	txn       *transaction.KVTxn
	iter      tikvInternalIterator
	prefix    []byte
	isReverse bool
}

func (it *tikvIterator) Rewind() {
	if it.iter != nil {
		it.iter.Close()
	}
	var err error
	if it.isReverse {
		startKey := make([]byte, len(it.prefix), len(it.prefix)+1)
		copy(startKey, it.prefix)
		startKey = append(startKey, 0x00)
		it.iter, err = it.txn.IterReverse(startKey)
	} else {
		it.iter, err = it.txn.Iter(it.prefix, nil)
	}
	if err != nil {
		it.iter = nil
	}
}

func (it *tikvIterator) Seek(key []byte) {
	if it.iter != nil {
		it.iter.Close()
	}
	var err error
	if it.isReverse {
		startKey := make([]byte, len(key), len(key)+1)
		copy(startKey, key)
		startKey = append(startKey, 0x00)
		it.iter, err = it.txn.IterReverse(startKey)
	} else {
		it.iter, err = it.txn.Iter(key, nil)
	}
	if err != nil {
		it.iter = nil
	}
}

func (it *tikvIterator) Valid() bool {
	if !it.iter.Valid() {
		return false
	}
	if len(it.prefix) > 0 {
		return bytes.HasPrefix(it.iter.Key(), it.prefix)
	}
	return true
}

func (it *tikvIterator) Next() {
	if err := it.iter.Next(); err != nil {
		// Log error or handle it. KVIterator.Next() doesn't return error.
	}
}

func (it *tikvIterator) Item() KVItem {
	return &tikvItem{key: it.iter.Key(), value: it.iter.Value()}
}

func (it *tikvIterator) Close() {
	it.iter.Close()
}

type tikvWriteBatch struct {
	txn *transaction.KVTxn
}

func (w *tikvWriteBatch) SetAt(key, val []byte, meta byte, ts uint64) error {
	return w.txn.Set(key, val) // ts is ignored as TiKV handles it
}

func (w *tikvWriteBatch) DeleteAt(key []byte, ts uint64) error {
	return w.txn.Delete(key)
}

func (w *tikvWriteBatch) Flush() error {
	return w.txn.Commit(context.Background())
}

func (w *tikvWriteBatch) Write(buf *z.Buffer) error {
	// Parse buffer and write to txn.
	return nil
}
