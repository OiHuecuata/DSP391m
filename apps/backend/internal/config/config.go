package config

import (
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/spf13/viper"
)

type ClickHouse struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
	Timeout  time.Duration
}

func (c ClickHouse) URL() string {
	return (&url.URL{Scheme: "http", Host: net.JoinHostPort(c.Host, strconv.Itoa(c.Port))}).String()
}

func (c ClickHouse) Validate() error {
	if c.Host == "" || c.Port < 1 || c.Port > 65535 || c.User == "" || c.Timeout <= 0 {
		return fmt.Errorf("invalid ClickHouse host, port, user, or timeout")
	}
	if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(c.Database) {
		return fmt.Errorf("CLICKHOUSE_DATABASE must be a simple SQL identifier")
	}
	return nil
}

type Settings struct {
	ServerPort        int
	ThreadsStatusPath string
	ClickHouse        ClickHouse
}

// Load retains existing Viper defaults and explicitly binds the serving env vars.
// Environment files are examples; export values before starting the backend.
func Load() (Settings, error) {
	v := viper.New()
	defaults := map[string]any{
		"server.port":                   8080,
		"collector.threads_status_path": "../collector/.local/threads/status.json",
		"clickhouse.host":               "localhost",
		"clickhouse.port":               8123,
		"clickhouse.database":           "dsp391m",
		"clickhouse.user":               "dsp391m",
		"clickhouse.password":           "",
	}
	bindings := map[string]string{
		"server.port":                   "SERVER_PORT",
		"collector.threads_status_path": "COLLECTOR_THREADS_STATUS_PATH",
		"clickhouse.host":               "CLICKHOUSE_HOST",
		"clickhouse.port":               "CLICKHOUSE_PORT",
		"clickhouse.database":           "CLICKHOUSE_DATABASE",
		"clickhouse.user":               "CLICKHOUSE_USER",
		"clickhouse.password":           "CLICKHOUSE_PASSWORD",
	}
	for key, value := range defaults {
		v.SetDefault(key, value)
		if err := v.BindEnv(key, bindings[key]); err != nil {
			return Settings{}, err
		}
	}
	settings := Settings{
		ServerPort:        v.GetInt("server.port"),
		ThreadsStatusPath: v.GetString("collector.threads_status_path"),
		ClickHouse: ClickHouse{
			Host: v.GetString("clickhouse.host"), Port: v.GetInt("clickhouse.port"),
			Database: v.GetString("clickhouse.database"), User: v.GetString("clickhouse.user"),
			Password: v.GetString("clickhouse.password"), Timeout: 10 * time.Second,
		},
	}
	if settings.ServerPort < 1 || settings.ServerPort > 65535 {
		return Settings{}, fmt.Errorf("invalid SERVER_PORT")
	}
	return settings, settings.ClickHouse.Validate()
}
