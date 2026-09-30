package main

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"

	api "github.com/username/DSP391m/apps/backend/internal/api"
	collectorrepo "github.com/username/DSP391m/apps/backend/internal/repository/collector"
	systemservice "github.com/username/DSP391m/apps/backend/internal/services/system"
)

func main() {
	// Temporary config
	viper.SetDefault("server.port", 8080)
	viper.SetDefault(
		"collector.threads_status_path",
		"../collector/.local/threads/status.json",
	)

	port := viper.GetInt("server.port")
	threadsStatusPath := viper.GetString("collector.threads_status_path")

	// Dependencies
	collectorStatusRepo := collectorrepo.NewStatusRepository(threadsStatusPath)
	systemStatusService := systemservice.NewStatusService(collectorStatusRepo)
	systemStatusHandler := api.NewStatusHandler(systemStatusService)

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

	addr := fmt.Sprintf(":%d", port)
	if err := router.Run(addr); err != nil {
		panic(err)
	}
}
