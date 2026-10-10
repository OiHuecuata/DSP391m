package config

import (
	"encoding/json"
	"os"
	"time"
)

// Readiness is a UI/controller guard. Python performs the full fail-closed review
// again before source requests, native commits, handoff and downstream use.
func threadsConfigured(path string) bool {
	var document struct {
		Threads struct {
			Mode   string `json:"mode"`
			Policy struct {
				Status, Reference string
				CollectionEnabled bool   `json:"collection_enabled"`
				ValidUntil        string `json:"valid_until"`
			} `json:"source_policy"`
		} `json:"threads_vertical"`
	}
	bytes, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(bytes, &document) != nil {
		return false
	}
	v := document.Threads
	if v.Mode == "fixture" {
		return v.Policy.Status == "SYNTHETIC" && v.Policy.Reference == "threads-vertical-fixture-v1"
	}
	expires, err := time.Parse(time.RFC3339, v.Policy.ValidUntil)
	return err == nil && expires.After(time.Now()) && v.Policy.Status == "APPROVED" && v.Policy.CollectionEnabled
}
