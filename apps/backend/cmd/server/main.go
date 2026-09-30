package main

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
)

func main() {
	// Temporary config
	viper.SetDefault("server.port", 8080)
	port := viper.GetInt("server.port")

	// Gin router
	router := gin.Default()
	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status": "ok",
		})
	})

	addr := fmt.Sprintf(":%d", port)
	if err := router.Run(addr); err != nil {
		panic(err)
	}
}
