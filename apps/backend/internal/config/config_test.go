package config

import "testing"

func TestClickHouseHostRejectsCredentialURLs(t *testing.T) {
	for _, host := range []string{"user:fixture-secret@localhost", "http://localhost", "localhost/path"} {
		c := ClickHouse{Host: host, Port: 8123, User: "test", Database: "test", Timeout: 1}
		if c.Validate() == nil {
			t.Fatal("unsafe host accepted")
		}
	}
}

func TestClickHouseEnvironment(t *testing.T) {
	t.Setenv("CLICKHOUSE_HOST", "127.0.0.1")
	t.Setenv("CLICKHOUSE_PORT", "18123")
	t.Setenv("CLICKHOUSE_DATABASE", "serving_test")
	t.Setenv("CLICKHOUSE_USER", "reader")
	t.Setenv("CLICKHOUSE_PASSWORD", "fixture")
	t.Setenv("SERVER_PORT", "18080")
	settings, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if settings.ClickHouse.URL() != "http://127.0.0.1:18123" || settings.ClickHouse.Database != "serving_test" || settings.ClickHouse.User != "reader" || settings.ClickHouse.Password != "fixture" || settings.ServerPort != 18080 {
		t.Fatalf("wrong configuration: %+v", settings)
	}
	t.Setenv("CLICKHOUSE_DATABASE", "bad;SQL")
	if _, err := Load(); err == nil {
		t.Fatal("unsafe database accepted")
	}
	t.Setenv("CLICKHOUSE_DATABASE", "serving_test")
	t.Setenv("CLICKHOUSE_PORT", "invalid")
	if _, err := Load(); err == nil {
		t.Fatal("invalid port accepted")
	}
}
