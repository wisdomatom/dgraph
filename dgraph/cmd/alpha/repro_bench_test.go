package alpha

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/dgraph/v25/embeding"
	"github.com/tikv/client-go/v2/txnkv"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/dgraph-io/dgraph/v25/edgraph"
	//"github.com/dgraph-io/dgraph/v25/embeding"
	"github.com/dgraph-io/dgraph/v25/x"
)

func setupInternalDgraph(t testing.TB) (*dgo.Dgraph, func()) {
	x.WorkerConfig.TiKVAddrs = []string{"127.0.0.1:2379"}

	dir, err := os.MkdirTemp("", "dgraph-benchmarks")
	x.Check(err)

	ed, err := embeding.NewDgraphEmbed(embeding.DgraphServer{
		Embed:        true,
		EmbedDataDir: dir,
	})
	if err != nil {
		t.Fatalf("Failed to set up embedded Dgraph: %v", err)
	}
	dCli, err := ed.GetClient()
	if err != nil {
		t.Fatalf("Failed to get dgraph client: %v", err)
	}

	resp, err := dCli.NewReadOnlyTxn().Query(context.TODO(), `{q(func: uid(0x1)) {uid}}`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	fmt.Printf("Initial query response: %s\n", string(resp.Json))

	cleanup := func() {
		// ed.Close()
		os.RemoveAll(dir)
	}
	return nil, cleanup
}

func BenchmarkDgraphIssueRepro(b *testing.B) {
	// go test -v -bench BenchmarkDgraphIssueRepro ./dgraph/cmd/alpha --benchtime=1x
	runtime.GOMAXPROCS(12)
	_, cleanup := setupInternalDgraph(b)
	defer cleanup()

	// 开启 CPU Profile
	f, err := os.Create("cpu.prof")
	if err == nil {
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	s := &edgraph.Server{}

	// 初始化 Schema
	schemaDsl := `
		TestUser.hobby: [string] .
		_TestUser: bool .
		TestUser.name: string @index(hash) .
		TestUser.email: string @index(exact) .
		TestUser.birthday: datetime .
		TestUser.sex: bool .
		TestUser.avatar: string .
	`
	ctx := context.TODO()
	_, err = s.Alter(ctx, &api.Operation{Schema: schemaDsl})
	x.Check(err)

	concurrency := 10
	batchSize := 1000
	totalRows := 10000

	// 预生成所有批次的数据，避免 Marshal 开销影响计时
	type batchData struct {
		data []byte
	}
	batchesPerWorker := (totalRows / concurrency) / batchSize
	allBatches := make([][]batchData, concurrency)
	for w := 0; w < concurrency; w++ {
		allBatches[w] = make([]batchData, batchesPerWorker)
		for bt := 0; bt < batchesPerWorker; bt++ {
			userList := make([]map[string]interface{}, 0, batchSize)
			for j := 0; j < batchSize; j++ {
				uid := fmt.Sprintf("_:id%d_%d_%d_%d", w, bt, j, time.Now().UnixNano())
				email := fmt.Sprintf("raw_%d_%d_%d@bench.com", w, bt, j)

				userList = append(userList, map[string]interface{}{
					"uid":               uid,
					"TestUser.name":     fmt.Sprintf("tom_%d_%d_%d", w, bt, j),
					"TestUser.email":    email,
					"TestUser.birthday": "1990-01-01T00:00:00Z",
					"TestUser.sex":      true,
					"TestUser.avatar":   "http://avatar.png",
					// "TestUser.hobby":    []string{"ping-pong", "tennis", "basketball"},
					"TestUser.hobby": []string{"ping-pong"},
					"_TestUser":      true,
					// "dgraph.type":       "TestUser",
				})
			}
			data, _ := json.Marshal(userList)
			allBatches[w][bt] = batchData{data: data}
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		var successCount int64
		start := time.Now()

		fmt.Printf("Starting iteration %d...\n", i)

		for w := 0; w < concurrency; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for bt := 0; bt < batchesPerWorker; bt++ {
					req := &api.Request{
						CommitNow: true,
						Mutations: []*api.Mutation{{SetJson: allBatches[workerID][bt].data}},
					}

					_, err = s.QueryNoGrpc(ctx, req)
					if err == nil {
						atomic.AddInt64(&successCount, int64(batchSize))
					} else {
						fmt.Printf("Worker %d, batch %d: Mutation failed with error: %v\n", workerID, bt, err)
					}
				}
			}(w)
		}
		wg.Wait()
		duration := time.Since(start)
		fmt.Printf("Iteration %d: TPS=%.2f (Total: %d rows in %v)\n", i, float64(successCount)/duration.Seconds(), successCount, duration)
	}

	resp, err := s.QueryNoGrpc(context.TODO(), &api.Request{
		Query: `{query(func: has(TestUser.name)) {
			count(uid)
		}}`,
	})
	if err != nil {
		b.Fatalf("Failed to query count: %v", err)
	}
	fmt.Printf("Final count query response: %s\n", string(resp.Json))

	// 打印内存 Profile
	mf, err := os.Create("mem.prof")
	if err == nil {
		pprof.WriteHeapProfile(mf)
		mf.Close()
	}
}

func BenchmarkQueryTikv(b *testing.B) {
	// go test -v -bench BenchmarkQuery ./dgraph/cmd/alpha --benchtime=1x
	_, cleanup := setupInternalDgraph(b)
	defer cleanup()
	s := &edgraph.Server{}
	q := `
{query(func: eq(_TestUser, true)) {
			count(uid)
		}}
`
	//	q := `
	//{query(func: eq(_TestUser, true), first: 10) {
	//			count(uid)
	//			uid
	//			TestUser.name
	//			TestUser.email
	//			TestUser.birthday
	//			TestUser.sex
	//			TestUser.avatar
	//			TestUser.hobby
	//		}}
	//`

	resp, err := s.QueryNoGrpc(context.TODO(), &api.Request{
		Query: q,
	})
	if err != nil {
		b.Fatalf("Failed to query count: %v", err)
	}
	fmt.Printf("Final count query response: %s\n", string(resp.Json))
}

func Iterate(client *txnkv.Client) {
	txn, err := client.Begin()
	if err != nil {
		panic(err)
	}
	iter, err := txn.Iter(nil, nil)
	if err != nil {
		panic(err)
	}
	for iter.Valid() {
		//fmt.Printf("===key(%s)===value(%s)\n", iter.Key(), iter.Value())
		txn.Delete(iter.Key())
		iter.Next()
	}
	iter.Close()
	err =
		txn.Commit(context.TODO())
	if err != nil {
		panic(err)
	}
}

func BenchmarkIter(b *testing.B) {
	client, err := txnkv.NewClient([]string{"127.0.0.1:2379"})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	Iterate(client)
}
