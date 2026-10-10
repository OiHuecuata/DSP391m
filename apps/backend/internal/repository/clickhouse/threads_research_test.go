package clickhouse

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/username/DSP391m/apps/backend/internal/config"
	"github.com/username/DSP391m/apps/backend/internal/domain"
)

func TestIsolatedThreadsResearchFilters(t *testing.T) {
	if os.Getenv("DSP_THREADS_HTTP_FIXTURE") != "1" { t.Skip("requires the declared isolated fixture publication") }
	repository, err := NewThreadsResearchRepository(config.ClickHouse{Host:"127.0.0.1",Port:18123,Database:"threads_fixture",User:"threads_fixture",Password:"isolated-fixture-only",Timeout:10*time.Second})
	if err != nil { t.Fatal(err) }
	manifest,err:=repository.Generation(context.Background(),"");if err!=nil {t.Fatal(err)}
	for _,metric:=range []string{"likes","comments","shares","views","reposts","quotes"} {
		t.Run(metric,func(t *testing.T){value,err:=repository.EDA(context.Background(),domain.ThreadsFilters{Generation:manifest.GenerationID,Metric:metric,Platform:"threads",Bucket:"day"});if err!=nil {t.Fatal(err)};if value.Overview.UniquePosts!=120 {t.Fatal(value.Overview)}})
	}
}
