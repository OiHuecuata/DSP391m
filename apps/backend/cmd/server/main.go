package main

import (
	"fmt"

	"github.com/gin-gonic/gin"

	api "github.com/username/DSP391m/apps/backend/internal/api"
	"github.com/username/DSP391m/apps/backend/internal/config"
	clickhouserepo "github.com/username/DSP391m/apps/backend/internal/repository/clickhouse"
	collectorrepo "github.com/username/DSP391m/apps/backend/internal/repository/collector"
	"github.com/username/DSP391m/apps/backend/internal/services/analytics"
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

	// Gin router
	router := gin.Default()

	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status": "ok",
		})
	})

	router.GET(
		"/api/v1/system/status",
		gin.WrapH(systemStatusHandler),
	)
	router.GET("/api/v1/analytics/location-market-signals", gin.WrapH(marketSignalHandler))

	addr := fmt.Sprintf(":%d", settings.ServerPort)
	if err := router.Run(addr); err != nil {
		panic(err)
	}
}
