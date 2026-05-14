/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"math"
	"sync"

	"github.com/golang/glog"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/dgraph/v25/x"
)

// TxnWriter is in charge or writing transactions to badger.
type TxnWriter struct {
	db  x.KVDB
	wg  sync.WaitGroup
	che chan error
}

// NewTxnWriter returns a new TxnWriter instance.
func NewTxnWriter(db x.KVDB) *TxnWriter {
	return &TxnWriter{
		db:  db,
		che: make(chan error, 1),
	}
}

func (w *TxnWriter) cb(err error) {
	defer w.wg.Done()
	if err == nil {
		return
	}

	glog.Errorf("TxnWriter got error during callback: %v", err)
	select {
	case w.che <- err:
	default:
	}
}

// Write stores the given key-value pairs in badger.
func (w *TxnWriter) Write(kvs *pb.KVList) error {
	for _, kv := range kvs.Kv {
		var meta byte
		if len(kv.UserMeta) > 0 {
			meta = kv.UserMeta[0]
		}
		if err := w.SetAt(kv.Key, kv.Value, meta, kv.Version); err != nil {
			return err
		}
	}
	return nil
}

func (w *TxnWriter) update(commitTs uint64, f func(txn x.KVTxn) error) error {
	if commitTs == 0 {
		return nil
	}
	txn := w.db.NewTransaction(math.MaxUint64, true)
	defer txn.Discard()

	err := f(txn)
	// We don't have ErrTxnTooBig in the interface yet, but let's assume it's handled or ignore for now.
	if err != nil {
		return err
	}
	w.wg.Add(1)
	return txn.CommitAt(commitTs, w.cb)
}

// SetAt writes a key-value pair at the given timestamp.
func (w *TxnWriter) SetAt(key, val []byte, meta byte, ts uint64) error {
	return w.update(ts, func(txn x.KVTxn) error {
		switch meta {
		case x.BitCompletePosting, x.BitEmptyPosting:
			return txn.SetWithDiscard(key, val, meta)
		default:
			return txn.SetWithMeta(key, val, meta)
		}
	})
}

// Flush waits until all operations are done and all data is written to disk.
func (w *TxnWriter) Flush() error {
	// No need to call Sync here.
	return w.Wait()
}

func (w *TxnWriter) Wait() error {
	w.wg.Wait()
	select {
	case err := <-w.che:
		return err
	default:
		return nil
	}
}
