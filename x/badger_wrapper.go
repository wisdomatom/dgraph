/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package x

import (
	"context"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
)

type badgerDB struct {
	db *badger.DB
}

func NewBadgerKV(db *badger.DB) KVDB {
	return &badgerDB{db: db}
}

func NewBadgerIterator(it *badger.Iterator) KVIterator {
	return &badgerIterator{it: it}
}

func NewBadgerItem(item *badger.Item) KVItem {
	return &badgerItem{item: item}
}

func (b *badgerDB) NewTransactionAt(readTs uint64, update bool) KVTxn {
	return &badgerTxn{txn: b.db.NewTransactionAt(readTs, update)}
}

func (b *badgerDB) Close() error {
	return b.db.Close()
}

func (b *badgerDB) Sync() error {
	return b.db.Sync()
}

func (b *badgerDB) DropPrefix(prefixes ...[]byte) error {
	for _, prefix := range prefixes {
		if err := b.db.DropPrefix(prefix); err != nil {
			return err
		}
	}
	return nil
}

func (b *badgerDB) DropAll() error {
	return b.db.DropAll()
}

func (b *badgerDB) NewStreamAt(readTs uint64) KVStream {
	return &badgerStream{stream: b.db.NewStreamAt(readTs)}
}

func (b *badgerDB) NewWriteBatch() KVWriteBatch {
	return &badgerWriteBatch{wb: b.db.NewWriteBatch()}
}

func (b *badgerDB) NewManagedWriteBatch() KVWriteBatch {
	return &badgerWriteBatch{wb: b.db.NewManagedWriteBatch()}
}

func (b *badgerDB) RunValueLogGC(discardRatio float64) error {
	return b.db.RunValueLogGC(discardRatio)
}

func (b *badgerDB) Size() (int64, int64) {
	return b.db.Size()
}

func (b *badgerDB) BanNamespace(ns uint64) error {
	return b.db.BanNamespace(ns)
}

func (b *badgerDB) SetDiscardTs(ts uint64) {
	b.db.SetDiscardTs(ts)
}

func (b *badgerDB) Tables() []badger.TableInfo {
	return b.db.Tables()
}

func (b *badgerDB) NewStreamWriter() KVStreamWriter {
	return &badgerStreamWriter{sw: b.db.NewStreamWriter()}
}

func (b *badgerDB) Subscribe(ctx context.Context, cb func(*pb.KVList) error, matches []pb.Match) error {
	return b.db.Subscribe(ctx, cb, matches)
}

func (b *badgerDB) CacheMaxCost(t badger.CacheType, cost int64) (int64, error) {
	return b.db.CacheMaxCost(t, cost)
}

type badgerStreamWriter struct {
	sw *badger.StreamWriter
}

func (s *badgerStreamWriter) Prepare() error {
	return s.sw.Prepare()
}

func (s *badgerStreamWriter) PrepareIncremental() error {
	return s.sw.PrepareIncremental()
}

func (s *badgerStreamWriter) Write(buf *z.Buffer) error {
	return s.sw.Write(buf)
}

func (s *badgerStreamWriter) Flush() error {
	return s.sw.Flush()
}

func (s *badgerStreamWriter) Cancel() {
	s.sw.Cancel()
}

type badgerTxn struct {
	txn *badger.Txn
}

func (t *badgerTxn) Get(key []byte) (KVItem, error) {
	item, err := t.txn.Get(key)
	if err != nil {
		return nil, err
	}
	return &badgerItem{item: item}, nil
}

func (t *badgerTxn) Set(key, val []byte) error {
	return t.txn.Set(key, val)
}

func (t *badgerTxn) SetWithMeta(key, val []byte, meta byte) error {
	return t.txn.SetEntry(badger.NewEntry(key, val).WithMeta(meta))
}

func (t *badgerTxn) SetWithDiscard(key, val []byte, meta byte) error {
	return t.txn.SetEntry(badger.NewEntry(key, val).WithMeta(meta).WithDiscard())
}

func (t *badgerTxn) SetEntry(e *badger.Entry) error {
	return t.txn.SetEntry(e)
}

func (t *badgerTxn) Delete(key []byte) error {
	return t.txn.Delete(key)
}

func (t *badgerTxn) CommitAt(commitTs uint64, callback func(error)) error {
	return t.txn.CommitAt(commitTs, callback)
}

func (t *badgerTxn) Discard() {
	t.txn.Discard()
}

func (t *badgerTxn) NewIterator(opt KVIterOpts) KVIterator {
	bopt := badger.DefaultIteratorOptions
	bopt.Prefix = opt.Prefix
	bopt.Reverse = opt.IsReverse
	bopt.AllVersions = opt.AllVersions
	bopt.PrefetchValues = opt.PrefetchValues
	bopt.PrefetchSize = opt.PrefetchSize
	return &badgerIterator{it: t.txn.NewIterator(bopt)}
}

type badgerItem struct {
	item *badger.Item
}

func (i *badgerItem) Key() []byte {
	return i.item.Key()
}

func (i *badgerItem) KeyCopy(dst []byte) []byte {
	return i.item.KeyCopy(dst)
}

func (i *badgerItem) Value(f func(val []byte) error) error {
	return i.item.Value(f)
}

func (i *badgerItem) ValueCopy(dst []byte) ([]byte, error) {
	return i.item.ValueCopy(dst)
}

func (i *badgerItem) UserMeta() byte {

	return i.item.UserMeta()
}

func (i *badgerItem) Version() uint64 {
	return i.item.Version()
}

func (i *badgerItem) IsDeletedOrExpired() bool {
	return i.item.IsDeletedOrExpired()
}

func (i *badgerItem) ExpiresAt() uint64 {
	return i.item.ExpiresAt()
}

type badgerIterator struct {
	it *badger.Iterator
}

func (i *badgerIterator) Rewind() {
	i.it.Rewind()
}

func (i *badgerIterator) Seek(key []byte) {
	i.it.Seek(key)
}

func (i *badgerIterator) Valid() bool {
	return i.it.Valid()
}

func (i *badgerIterator) Next() {
	i.it.Next()
}

func (i *badgerIterator) Item() KVItem {
	return &badgerItem{item: i.it.Item()}
}

func (i *badgerIterator) Close() {
	i.it.Close()
}

type badgerStream struct {
	stream *badger.Stream
}

func (s *badgerStream) Send(f func(buf *z.Buffer) error) {
	s.stream.Send = f
}

func (s *badgerStream) SetKeyToList(f func(key []byte, itr KVStreamIterator) (*pb.KVList, error)) {
	s.stream.KeyToList = func(key []byte, itr *badger.Iterator) (*pb.KVList, error) {
		return f(key, &badgerStreamIterator{itr: itr})
	}
}

func (s *badgerStream) SetKeyToListWithThreadId(f func(key []byte, itr KVStreamIterator, threadId int) (*pb.KVList, error)) {
	s.stream.KeyToListWithThreadId = func(key []byte, itr *badger.Iterator, threadId int) (*pb.KVList, error) {
		return f(key, &badgerStreamIterator{itr: itr}, threadId)
	}
}

func (s *badgerStream) SetFinishThread(f func(threadId int) (*pb.KVList, error)) {
	s.stream.FinishThread = f
}

func (s *badgerStream) SetPrefix(prefix []byte) {
	s.stream.Prefix = prefix
}

func (s *badgerStream) SetLogPrefix(prefix string) {
	s.stream.LogPrefix = prefix
}

func (s *badgerStream) SetUseKeyToListWithThreadId(use bool) {
	s.stream.UseKeyToListWithThreadId = use
}

func (s *badgerStream) Orchestrate(ctx context.Context) error {
	return s.stream.Orchestrate(ctx)
}

func (s *badgerStream) SetSend(f func(buf *z.Buffer) error) {
	s.stream.Send = f
}

func (s *badgerStream) SetSinceTs(ts uint64) {
	s.stream.SinceTs = ts
}

func (s *badgerStream) SetChooseKey(f func(item KVItem) bool) {
	s.stream.ChooseKey = func(item *badger.Item) bool {
		return f(&badgerItem{item: item})
	}
}

type badgerStreamIterator struct {
	itr *badger.Iterator
}

func (i *badgerStreamIterator) Rewind() {
	i.itr.Rewind()
}

func (i *badgerStreamIterator) Seek(key []byte) {
	i.itr.Seek(key)
}

func (i *badgerStreamIterator) Valid() bool {
	return i.itr.Valid()
}

func (i *badgerStreamIterator) Next() {
	i.itr.Next()
}

func (i *badgerStreamIterator) Item() KVItem {
	return &badgerItem{item: i.itr.Item()}
}

func (i *badgerStreamIterator) Close() {
	i.itr.Close()
}

type badgerWriteBatch struct {
	wb *badger.WriteBatch
}

func (w *badgerWriteBatch) SetAt(key, val []byte, meta byte, ts uint64) error {
	return w.wb.SetEntryAt(&badger.Entry{
		Key:      key,
		Value:    val,
		UserMeta: meta,
	}, ts)
}

func (w *badgerWriteBatch) DeleteAt(key []byte, ts uint64) error {
	return w.wb.DeleteAt(key, ts)
}

func (w *badgerWriteBatch) Flush() error {
	return w.wb.Flush()
}

func (w *badgerWriteBatch) Write(buf *z.Buffer) error {
	return w.wb.Write(buf)
}
