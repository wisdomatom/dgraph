package embeding

import (
	"context"
	"net"
	"os"
	"path"
	"reflect"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/google/uuid"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgraph-io/dgraph/v25/conn"
	"github.com/dgraph-io/dgraph/v25/dgraph/cmd/zero"
	"github.com/dgraph-io/dgraph/v25/edgraph"
	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/worker"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	hapi "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/test/bufconn"
)

const (
	embedBufNetZero  = "buf-net-zero"
	embedBufNetAlpha = "buf-net-alpha"
)

type EmbedDgraph struct {
	listenerZero  *bufconn.Listener
	listenerAlpha *bufconn.Listener
	lock          *sync.Mutex
	client        map[string]*dgo.Dgraph
	closer        *z.Closer
}

type DgraphServer struct {
	Url          string `toml:"url" mapstructure:"url"`
	Host         string `toml:"host" mapstructure:"host"`
	Grpc         string `toml:"grpc" mapstructure:"grpc"`
	AuthToken    string `toml:"auth_token" mapstructure:"auth_token"`
	User         string `toml:"user" mapstructure:"user"`
	Password     string `toml:"password" mapstructure:"password"`
	Embed        bool   `toml:"embed" mapstructure:"embed"`
	EmbedDataDir string `toml:"embed_data_dir" mapstructure:"embed_data_dir"`
}

func NewDgraphEmbed(conf DgraphServer) (*EmbedDgraph, error) {
	err := os.Mkdir(conf.EmbedDataDir, 0750)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	err = os.Mkdir(path.Join(conf.EmbedDataDir, "zw"), 0750)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	f, err := os.OpenFile(path.Join(conf.EmbedDataDir, "zw", "wal.meta"), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	err = f.Close()
	if err != nil {
		return nil, err
	}

	x.WorkerConfig.Raft = z.NewSuperFlag(worker.RaftDefaults)
	// For embedded server, disable hard sync to improve performance.
	// This is less safe in case of a crash, but acceptable for local/testing.
	// x.WorkerConfig.HardSync = false

	ed := &EmbedDgraph{
		lock:   &sync.Mutex{},
		client: map[string]*dgo.Dgraph{},
	}
	listenerZero := bufconn.Listen(1024 * 1024 * 4)
	listenerAlpha := bufconn.Listen(1024 * 1024 * 8)

	ed.listenerZero = listenerZero
	ed.listenerAlpha = listenerAlpha

	pls := conn.GetPools()
	v := reflect.ValueOf(pls).Elem().FieldByName("all")
	ptr := unsafe.Pointer(v.UnsafeAddr()) //nolint:gosec // audit
	mapV := reflect.NewAt(v.Type(), ptr).Elem()

	npZ, err := embedConnPool(embedBufNetZero, listenerZero)
	if err != nil {
		return ed, err
	}
	mapV.SetMapIndex(reflect.ValueOf(embedBufNetZero), reflect.ValueOf(npZ))

	npA, err := embedConnPool(embedBufNetAlpha, listenerAlpha)
	if err != nil {
		return ed, err
	}
	mapV.SetMapIndex(reflect.ValueOf(embedBufNetAlpha), reflect.ValueOf(npA))

	ed.dZero(conf, listenerZero)

	ed.dAlpha(conf, listenerAlpha)

	return ed, nil
}

func embedConnPool(addr string, listener *bufconn.Listener) (*conn.Pool, error) {
	np := &conn.Pool{
		Addr: addr,
	}

	cnn, err := embedGrpcConn(listener, addr)
	if err != nil {
		return nil, err
	}

	v := reflect.ValueOf(np).Elem() // Get Pool struct value

	fn := func() {
		f := v.FieldByName("lastEcho")
		// 1. Get field address
		ptr := unsafe.Pointer(f.UnsafeAddr()) //nolint:gosec // audit
		// 2. Construct a 'writable reflect.Value'
		rw := reflect.NewAt(f.Type(), ptr).Elem()
		// 3. Set value
		rw.Set(reflect.ValueOf(time.Now()))
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			fn()
			<-ticker.C
		}
	}()

	// Modify conn
	fConn := v.FieldByName("conn")
	if fConn.IsValid() {
		// Key: use unsafe.Pointer + NewAt to create a 'writable' Value
		ptrConn := unsafe.Pointer(fConn.UnsafeAddr()) //nolint:gosec // audit
		writableConn := reflect.NewAt(fConn.Type(), ptrConn).Elem()

		// Perform assignment
		writableConn.Set(reflect.ValueOf(cnn))
	}

	return np, nil
}

func (r *EmbedDgraph) dAlpha(conf DgraphServer, lis *bufconn.Listener) {
	// setup data directories
	worker.Config.PostingDir = path.Join(conf.EmbedDataDir, "p")
	worker.Config.WALDir = path.Join(conf.EmbedDataDir, "w")
	worker.Config.TypeFilterUidLimit = 100000

	x.WorkerConfig.TmpDir = path.Join(conf.EmbedDataDir, "t")
	x.WorkerConfig.ZeroAddr = []string{embedBufNetZero}

	// TODO: optimize these and more options
	x.WorkerConfig.Badger = badger.DefaultOptions("").FromSuperFlag(worker.BadgerDefaults)
	if runtime.GOOS == "windows" {
		x.WorkerConfig.Badger.ValueLogFileSize = 1024 * 1024 * 64
	}
	x.Config.MaxRetries = 10
	x.Config.Limit = z.NewSuperFlag("max-pending-queries=100000")
	x.Config.LimitNormalizeNode = 1

	// initialize each package
	edgraph.Init()
	// worker.State.InitStorage()
	worker.InitServerState()
	worker.InitForLite(worker.State.Pstore)
	worker.InitTasks()
	worker.Init(worker.State.Pstore)

	schema.Init(worker.State.Pstore)
	cacheSizeBytes := 16 * 1024 * 1024
	posting.Init(worker.State.Pstore, int64(cacheSizeBytes), false)

	x.Config.LimitMutationsNquad = 2000000
	x.Config.LimitQueryEdge = 10000000

	server := grpc.NewServer(
		grpc.UnaryInterceptor(PeerRewriteInterceptor),
	)

	// Register our server wrapper that properly handles context and routing
	api.RegisterDgraphServer(server, &edgraph.Server{})
	hapi.RegisterHealthServer(server, health.NewServer())

	// Start the server in a goroutine
	go func() {
		if err := server.Serve(lis); err != nil {
			logrus.Errorf("dgraph server exited with error: %v", err)
		}
	}()

	worker.StartRaftNodes(worker.State.WALstore, false)
	// Store the engine as the active instance
	x.UpdateHealthStatus(true)
}

func (r *EmbedDgraph) dZero(conf DgraphServer, lis *bufconn.Listener) {
	nodeID := uint64(1)
	zwDir := path.Join(conf.EmbedDataDir, "zw")

	zero.Zero.Conf = &viper.Viper{}

	store := raftwal.Init(zwDir)
	store.SetUint(raftwal.RaftId, nodeID)
	store.SetUint(raftwal.GroupId, 0) // All zeros have group zero.

	server := grpc.NewServer(
		grpc.UnaryInterceptor(PeerRewriteInterceptor),
	)

	rc := pb.RaftContext{
		Id:        1,
		Addr:      embedBufNetZero,
		Group:     0,
		IsLearner: false,
	}

	m := conn.NewNode(&rc, store, nil)
	m.Cfg.DisableProposalForwarding = true
	// Speed up leader election for embedded mode.
	m.Cfg.HeartbeatTick = 1
	m.Cfg.ElectionTick = 3 // With tickDur=20ms, election timeout is ~60ms.
	rs := conn.NewRaftServer(m)

	closer := z.NewCloser(1)
	zr := zero.NewServer(m, closer)
	r.closer = closer

	zr.NumReplicas = 1
	zr.Init()

	pb.RegisterZeroServer(server, zr)
	pb.RegisterRaftServer(server, rs)

	// Start the server in a goroutine
	go func() {
		if err := server.Serve(lis); err != nil {
			logrus.Errorf("dgraph server exited with error: %v", err)
		}
	}()

	x.Check(zr.Node.InitAndStartNode())
}

func (r *EmbedDgraph) GetClient() (*dgo.Dgraph, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, cli := range r.client {
		return cli, nil
	}
	cnn, err := embedGrpcConn(r.listenerAlpha, embedBufNetAlpha)
	if err != nil {
		return nil, err
	}
	dgraphClient := api.NewDgraphClient(cnn)
	cli := dgo.NewDgraphClient(dgraphClient)
	r.client[uuid.NewString()] = cli
	return cli, nil
}

func embedGrpcConn(listener *bufconn.Listener, addr string) (*grpc.ClientConn, error) {
	cnn, err := grpc.DialContext(context.Background(), addr,
		grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return cnn, nil
}

func bufDialer(listener *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, url string) (net.Conn, error) {
		return listener.Dial()
	}
}

func PeerRewriteInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	// oldPeer, _ := peer.FromContext(ctx)
	ip := net.ParseIP("127.0.0.1")
	newPeer := &peer.Peer{
		Addr: &net.TCPAddr{
			IP: ip,
			//Port: 7080,
		},
	}

	ctx = peer.NewContext(ctx, newPeer)

	return handler(ctx, req)
}

func (r *EmbedDgraph) Close() {
	for _, cli := range r.client {
		cli.Close()
	}
	r.closer.Signal()
	_ = r.listenerAlpha.Close()
	_ = r.listenerZero.Close()
}
