package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/spf13/viper"
	"github.com/subosito/gotenv"
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
	if net.ParseIP(c.Host) == nil && !regexp.MustCompile(`^[A-Za-z0-9._-]+$`).MatchString(c.Host) {
		return fmt.Errorf("CLICKHOUSE_HOST must be a hostname or IP address")
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
	RepoRoot          string
	CollectionState   string
	CollectionConfig  string
	CollectionPython  string
	LakehouseURI      string
	YouTubeConfigured bool
	ThreadsConfigured bool
}

// Load retains existing Viper defaults and explicitly binds the serving env vars.
// Environment files are examples; export values before starting the backend.
func Load() (Settings, error) {
	root := os.Getenv("DSP_REPO_ROOT")
	if root == "" {
		root, _ = os.Getwd()
		for root != "/" {
			if _, err := os.Stat(filepath.Join(root, "contracts/data/silver/social-post.v1.schema.json")); err == nil {
				break
			}
			root = filepath.Dir(root)
		}
	}
	if root == "/" {
		return Settings{}, fmt.Errorf("DSP_REPO_ROOT is required outside the repository")
	}
	root, _ = filepath.Abs(root)
	// Load server-side environment only; exported values take precedence.
	if values, err := gotenv.Read(filepath.Join(root, ".env")); err == nil {
		for name, value := range values {
			if _, set := os.LookupEnv(name); !set {
				_ = os.Setenv(name, value)
			}
		}
	}
	envDefault := func(name, value string) string {
		if supplied := os.Getenv(name); supplied != "" {
			return supplied
		}
		return value
	}
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
	localSetting := func(name, value string) string {
		selected := envDefault(name, value)
		if !filepath.IsAbs(selected) {
			selected = filepath.Join(root, selected)
		}
		return filepath.Clean(selected)
	}
	settings := Settings{
		RepoRoot:          root,
		CollectionState:   localSetting("COLLECTION_STATE_DIR", filepath.Join(root, ".local/data-ops")),
		CollectionConfig:  localSetting("COLLECTION_CONFIG", filepath.Join(root, "configs/collection/data_ops.v1.json")),
		CollectionPython:  envDefault("COLLECTION_PYTHON", filepath.Join(root, ".venv/bin/python")),
		LakehouseURI:      envDefault("DSP_LAKEHOUSE_BASE_URI", (&url.URL{Scheme: "file", Path: filepath.Join(root, "pipelines/spark/.local/lakehouse")}).String()),
		YouTubeConfigured: os.Getenv("YOUTUBE_API_KEY") != "",
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
	settings.ThreadsConfigured = threadsConfigured(settings.CollectionConfig)
	return settings, settings.ClickHouse.Validate()
}
