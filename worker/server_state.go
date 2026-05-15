/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"math"
	"os"
	"sync/atomic"

	"github.com/golang/glog"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/dgraph-io/ristretto/v2/z"
)

const (
	// NOTE: SuperFlag defaults must include every possible option that can be used. This way, if a
	//       user makes a typo while defining a SuperFlag we can catch it and fail right away rather
	//       than fail during runtime while trying to retrieve an option that isn't there.
	//
	//       For easy readability, keep the options without default values (if any) at the end of
	//       the *Defaults string. Also, since these strings are printed in --help text, avoid line
	//       breaks.
	AuditDefaults  = `compress=false; days=10; size=100; dir=; output=; encrypt-file=;`
	BadgerDefaults = `compression=snappy; numgoroutines=8;`
	RaftDefaults   = `learner=false; snapshot-after-entries=10000; ` +
		`snapshot-after-duration=30m; pending-proposals=256; idx=; group=;`
	SecurityDefaults = `token=; whitelist=;`
	CDCDefaults      = `file=; kafka=; sasl_user=; sasl_password=; ca_cert=; client_cert=; ` +
		`client_key=; sasl-mechanism=PLAIN; tls=false;`
	LimitDefaults = `mutations=allow; query-edge=1000000; normalize-node=10000; ` +
		`mutations-nquad=1000000; disallow-drop=false; query-timeout=0ms; txn-abort-after=5m; ` +
		`max-retries=10; max-pending-queries=10000; shared-instance=false; type-filter-uid-limit=10`
	ZeroLimitsDefaults = `uid-lease=0; refill-interval=30s; disable-admin-http=false;`
	GraphQLDefaults    = `introspection=true; debug=false; extensions=true; poll-interval=1s; ` +
		`lambda-url=;`
	CacheDefaults        = `size-mb=4096; percentage=40,40,20; remove-on-update=false`
	FeatureFlagsDefaults = `normalize-compatibility-mode=; enable-detailed-metrics=false; log-slow-query-threshold=0`
)

// ServerState holds the state of the Dgraph server.
type ServerState struct {
	FinishCh chan struct{} // channel to wait for all pending reqs to finish.

	Pstore   x.KVDB
	WALstore *raftwal.DiskStorage
	gcCloser *z.Closer // closer for valueLogGC

	needTs chan tsReq
}

// State is the instance of ServerState used by the current server.
var State ServerState

// InitServerState initializes this server's state.
func InitServerState() {
	Config.validate()

	State.FinishCh = make(chan struct{})
	State.needTs = make(chan tsReq, 100)

	State.InitStorage()
	go State.fillTimestampRequests()

	groupId, err := x.ReadGroupIdFile(Config.PostingDir)
	if err != nil {
		glog.Warningf("Could not read %s file inside posting directory %s.", x.GroupIdFileName,
			Config.PostingDir)
	}
	x.WorkerConfig.ProposedGroupId = groupId
}

func setBadgerOptions(opt badger.Options) badger.Options {
	opt = opt.WithSyncWrites(false).
		WithLogger(&x.ToGlog{}).
		WithEncryptionKey(x.WorkerConfig.EncryptionKey)

	// Disable conflict detection in badger. Alpha runs in managed mode and
	// perform its own conflict detection so we don't need badger's conflict
	// detection. Using badger's conflict detection uses memory which can be
	// saved by disabling it.
	opt.DetectConflicts = false

	// Settings for the data directory.
	return opt
}

func (s *ServerState) InitStorage() {
	var err error

	if x.WorkerConfig.EncryptionKey != nil {
		glog.Infof("Encryption feature enabled.")
	}

	{
		// Write Ahead Log directory
		x.Checkf(os.MkdirAll(Config.WALDir, 0700), "Error while creating WAL dir.")
		s.WALstore, err = raftwal.InitEncrypted(Config.WALDir, x.WorkerConfig.EncryptionKey)
		x.Check(err)
	}
	{
		// Postings directory
		// All the writes to posting store should be synchronous. We use batched writers
		// for posting lists, so the cost of sync writes is amortized.
		x.Check(os.MkdirAll(Config.PostingDir, 0700))
		opt := x.WorkerConfig.Badger.
			WithDir(Config.PostingDir).WithValueDir(Config.PostingDir).
			WithNumVersionsToKeep(math.MaxInt32).
			WithNamespaceOffset(x.NamespaceOffset)
		opt = setBadgerOptions(opt)

		// Print the options w/o exposing key.
		// TODO: Build a stringify interface in Badger options, which is used to print nicely here.
		key := opt.EncryptionKey
		opt.EncryptionKey = nil
		glog.Infof("Opening postings BadgerDB with options: %+v\n", opt)
		opt.EncryptionKey = key

		db, err := badger.OpenManaged(opt)
		x.Checkf(err, "Error while creating badger KV posting store")
		s.Pstore = x.NewBadgerKV(db)

		// zero out from memory
		opt.EncryptionKey = nil
	}
	// Temp directory
	x.Check(os.MkdirAll(x.WorkerConfig.TmpDir, 0700))

	s.gcCloser = z.NewCloser(3)
	go x.RunVlogGC(s.Pstore, s.gcCloser)
	// Commenting this out because Badger is doing its own cache checks.
	// go x.MonitorCacheHealth(s.Pstore, s.gcCloser)
	go x.MonitorDiskMetrics("postings_fs", Config.PostingDir, s.gcCloser)
}

// Dispose stops and closes all the resources inside the server state.
func (s *ServerState) Dispose() {
	s.gcCloser.SignalAndWait()
	if err := s.Pstore.Close(); err != nil {
		glog.Errorf("Error while closing postings store: %v", err)
	}
	if err := s.WALstore.Close(); err != nil {
		glog.Errorf("Error while closing WAL store: %v", err)
	}
}

var (
	tsCounter uint64 = 100 // Start from a reasonable number
)

func (s *ServerState) GetTimestamp(readOnly bool) uint64 {
	ts := atomic.AddUint64(&tsCounter, 1)
	posting.Oracle().SetMaxAssigned(ts)
	return ts
}

func (s *ServerState) fillTimestampRequests() {
	// No-op for local mode
}

type tsReq struct {
	readOnly bool
	// A one-shot chan which we can send a txn timestamp upon.
	ch chan uint64
}
