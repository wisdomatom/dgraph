/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package x

import (
	"context"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
)

var (
	tsCounter  uint64 = 100
	uidCounter uint64 = 100
)

func GetNextTs() uint64 {
	return atomic.AddUint64(&tsCounter, 1)
}

func GetNextUid() uint64 {
	return atomic.AddUint64(&uidCounter, 1)
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
