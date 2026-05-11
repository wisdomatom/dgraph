package alpha

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/dgraph-io/dgraph/v25/embeding"
	"github.com/dgraph-io/dgraph/v25/x"
)

func setupInternalDgraph(t testing.TB) (*dgo.Dgraph, func()) {

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
		os.RemoveAll(dir)
	}
	return dCli, cleanup
}

func BenchmarkDgraphIssueRepro(b *testing.B) {
	runtime.GOMAXPROCS(16)
	server, cleanup := setupInternalDgraph(b)
	defer cleanup()

	// 开启 CPU Profile
	f, err := os.Create("cpu.prof")
	if err == nil {
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// 初始化 Schema
	schemaDsl := `
		TestUser.hobby: [string] .
		_TestUser: bool @index(bool) .
		TestUser.name: string @index(hash) .
		TestUser.email: string @index(exact) .
		TestUser.birthday: datetime .
		TestUser.sex: bool .
		TestUser.avatar: string .
	`
	ctx := context.TODO()
	err = server.Alter(ctx, &api.Operation{Schema: schemaDsl})
	x.Check(err)

	concurrency := 50
	batchSize := 1000
	totalRows := 200000

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
					"TestUser.name":     "tom_batch",
					"TestUser.email":    email,
					"TestUser.birthday": "1990-01-01T00:00:00Z",
					"TestUser.sex":      true,
					"TestUser.avatar":   "http://avatar.png",
					"TestUser.hobby":    []string{"ping-pong", "tennis", "basketball"},
					"_TestUser":         true,
					"dgraph.type":       "TestUser",
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

					// 直接调用内部 Server 接口，绕过 gRPC 和网络
					_, err := server.NewTxn().Do(ctx, req)
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

	// 打印内存 Profile
	mf, err := os.Create("mem.prof")
	if err == nil {
		pprof.WriteHeapProfile(mf)
		mf.Close()
	}
}
