package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"

	api "github.com/username/DSP391m/apps/backend/internal/api"
	"github.com/username/DSP391m/apps/backend/internal/auth"
	"github.com/username/DSP391m/apps/backend/internal/config"
	clickhouserepo "github.com/username/DSP391m/apps/backend/internal/repository/clickhouse"
	collectorrepo "github.com/username/DSP391m/apps/backend/internal/repository/collector"
	"github.com/username/DSP391m/apps/backend/internal/services/analytics"
	"github.com/username/DSP391m/apps/backend/internal/services/dataops"
	systemservice "github.com/username/DSP391m/apps/backend/internal/services/system"
)

func main() {
	settings, err := config.Load()
	if err != nil {
		panic(err)
	}

	// Dependencies
	collectorStatusRepo := collectorrepo.NewStatusRepository(settings.ThreadsStatusPath)
	systemStatusService := systemservice.NewStatusService(collectorStatusRepo)
	systemStatusHandler := api.NewStatusHandler(systemStatusService)
	marketSignalRepo, err := clickhouserepo.NewMarketSignalRepository(settings.ClickHouse)
	if err != nil {
		panic(err)
	}
	marketSignalHandler := api.NewMarketSignalHandler(analytics.NewMarketSignalService(marketSignalRepo))
	manager, err := dataops.NewManager(dataops.Options{Root: settings.RepoRoot, Directory: settings.CollectionState, Python: settings.CollectionPython, Config: settings.CollectionConfig, YouTubeConfigured: settings.YouTubeConfigured})
	if err != nil {
		panic(err)
	}
	defer manager.Close()
	operations := &api.DataOpsHandler{Manager: manager, Status: dataops.NewStatusService(settings.LakehouseURI, settings.ClickHouse, manager)}

	// Gin router
	router := gin.Default()

	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status": "ok",
		})
	})

	router.GET(
		"/api/v1/system/status",
		auth.Require(false),
		gin.WrapH(systemStatusHandler),
	)
	router.GET("/api/v1/analytics/location-market-signals", auth.Require(false), gin.WrapH(marketSignalHandler))
	for _, path := range []string{"data-ops/status", "data-ops/lakehouse", "data-ops/warehouse", "data-ops/runs", "collection/status"} {
		router.GET("/api/v1/"+path, auth.Require(true), gin.WrapH(operations))
	}
	for _, path := range []string{"collection/start", "collection/stop", "collection/run", "collection/retry"} {
		router.POST("/api/v1/"+path, auth.Require(true), gin.WrapH(operations))
	}
	router.PATCH("/api/v1/collection/config", auth.Require(true), gin.WrapH(operations))

	addr := fmt.Sprintf("127.0.0.1:%d", settings.ServerPort)
	server := &http.Server{Addr: addr, Handler: router}
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-done; _ = server.Shutdown(context.Background()) }()
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}
