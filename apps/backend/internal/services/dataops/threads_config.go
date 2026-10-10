package dataops

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type ThreadsAcquisitionConfig struct {
	Queries        []string `json:"queries"`
	UniqueTarget   int      `json:"unique_target"`
	RequestBudget  int      `json:"request_budget"`
	Concurrency    int      `json:"concurrency"`
	PacingSeconds  float64  `json:"pacing_seconds"`
	MaxPublicPages int      `json:"max_public_pages"`
	PublishedFrom  string   `json:"published_from,omitempty"`
	PublishedTo    string   `json:"published_to,omitempty"`
}

func (m *Manager) ThreadsConfig() (ThreadsAcquisitionConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var doc map[string]json.RawMessage
	bytes, err := os.ReadFile(m.options.Config)
	if err != nil {
		return ThreadsAcquisitionConfig{}, err
	}
	if err = json.Unmarshal(bytes, &doc); err != nil {
		return ThreadsAcquisitionConfig{}, err
	}
	var vertical struct {
		Acquisition ThreadsAcquisitionConfig `json:"acquisition"`
	}
	if err = json.Unmarshal(doc["threads_vertical"], &vertical); err != nil {
		return ThreadsAcquisitionConfig{}, err
	}
	return vertical.Acquisition, nil
}

func (m *Manager) ConfigureThreads(value ThreadsAcquisitionConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state.Active != "" {
		return ErrConflict
	}
	if value.UniqueTarget < 1 || value.UniqueTarget > 1000 || value.RequestBudget < 1 || value.RequestBudget > 10000 || value.Concurrency < 1 || value.Concurrency > 4 || value.PacingSeconds < 0.5 || value.PacingSeconds > 60 || value.MaxPublicPages < 1 || value.MaxPublicPages > 100 || len(value.Queries) < 1 || len(value.Queries) > 64 {
		return errors.New("invalid Threads acquisition bounds")
	}
	for _, query := range value.Queries {
		if len(strings.TrimSpace(query)) < 1 || len(query) > 256 {
			return errors.New("invalid Threads query")
		}
	}
	for _, date := range []string{value.PublishedFrom, value.PublishedTo} {
		if date != "" {
			if _, err := time.Parse(time.RFC3339, date); err != nil {
				return errors.New("Threads publication bounds require timezone timestamps")
			}
		}
	}
	if value.PublishedFrom != "" && value.PublishedTo != "" {
		from, _ := time.Parse(time.RFC3339, value.PublishedFrom)
		to, _ := time.Parse(time.RFC3339, value.PublishedTo)
		if from.After(to) {
			return errors.New("Threads publication window is reversed")
		}
	}
	var doc map[string]json.RawMessage
	bytes, err := os.ReadFile(m.options.Config)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(bytes, &doc); err != nil {
		return err
	}
	var vertical map[string]json.RawMessage
	if err = json.Unmarshal(doc["threads_vertical"], &vertical); err != nil {
		return err
	}
	var acquisition map[string]json.RawMessage
	if err = json.Unmarshal(vertical["acquisition"], &acquisition); err != nil {
		return err
	}
	// Policy reviews, credentials and target URIs cannot be changed by this API.
	encoded, _ := json.Marshal(value)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(encoded, &fields)
	delete(acquisition, "published_from")
	delete(acquisition, "published_to")
	for key, v := range fields {
		acquisition[key] = v
	}
	vertical["acquisition"], _ = json.Marshal(acquisition)
	doc["threads_vertical"], _ = json.Marshal(vertical)
	encoded, _ = json.MarshalIndent(doc, "", "  ")
	file, err := os.CreateTemp(filepath.Dir(m.options.Config), ".threads-config-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(encoded); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(file.Name(), m.options.Config); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(m.options.Config))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
