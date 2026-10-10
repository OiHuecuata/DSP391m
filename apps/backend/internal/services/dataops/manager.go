// Package dataops owns persistent local collection and serialized pipeline cycles.
package dataops

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

var ErrConflict = errors.New("source already running or queued")

type Source struct {
	Enabled            bool           `json:"enabled"`
	Configured         bool           `json:"configured"`
	Interval           int            `json:"interval_seconds"`
	Status             string         `json:"status"`
	LastRun            string         `json:"last_run_id,omitempty"`
	Started            *time.Time     `json:"last_started_at,omitempty"`
	Completed          *time.Time     `json:"last_completed_at,omitempty"`
	Next               *time.Time     `json:"next_run_at,omitempty"`
	Error              string         `json:"last_error,omitempty"`
	Pending            bool           `json:"queued"`
	RetryRun           string         `json:"retry_run_id,omitempty"`
	Metrics            map[string]any `json:"metrics,omitempty"`
	PendingAcquisition string         `json:"pending_acquisition_run_id,omitempty"`
	Operation          string         `json:"queued_operation,omitempty"`
}
type Run struct {
	ID        string         `json:"run_id"`
	Source    string         `json:"source"`
	Status    string         `json:"status"`
	Started   time.Time      `json:"started_at"`
	Completed *time.Time     `json:"completed_at,omitempty"`
	Duration  float64        `json:"duration_seconds"`
	Error     string         `json:"error,omitempty"`
	Metrics   map[string]any `json:"metrics,omitempty"`
	Stages    map[string]any `json:"stages,omitempty"`
	PID       int            `json:"pid,omitempty"`
	Attempt   int            `json:"attempt"`
	Operation string         `json:"operation,omitempty"`
}
type State struct {
	StopRequested bool               `json:"stop_requested"`
	Continuous    bool               `json:"continuous"`
	Status        string             `json:"status"`
	Sources       map[string]*Source `json:"sources"`
	Runs          []*Run             `json:"runs"`
	Active        string             `json:"active_run_id,omitempty"`
	Progress      map[string]any     `json:"progress,omitempty"`
}
type Options struct {
	Root, Directory, Python, Config string
	YouTubeConfigured               bool
	Runner                          func(string, string) (*exec.Cmd, error)
}
type Manager struct {
	mu               sync.Mutex
	state            State
	options          Options
	lock             *os.File
	stop             chan struct{}
	done             chan struct{}
	wake             chan struct{}
	closing          bool
	persistenceError string
}
type SourceConfig struct {
	Enabled  bool `json:"enabled"`
	Interval int  `json:"interval_seconds"`
}

func NewManager(o Options) (*Manager, error) {
	if err := os.MkdirAll(o.Directory, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(o.Directory, "controller.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("another collection controller is active")
	}
	m := &Manager{options: o, lock: lock, stop: make(chan struct{}), done: make(chan struct{}), wake: make(chan struct{}, 1)}
	m.state = State{Status: "IDLE", Sources: map[string]*Source{}, Runs: []*Run{}}
	bytes, err := os.ReadFile(filepath.Join(o.Directory, "control.json"))
	if err == nil {
		if err = json.Unmarshal(bytes, &m.state); err != nil {
			lock.Close()
			return nil, fmt.Errorf("invalid persisted control state")
		}
	} else if !os.IsNotExist(err) {
		lock.Close()
		return nil, err
	}
	for name, interval := range map[string]int{"threads": 43200, "youtube": 21600, "pbplus": 86400} {
		s := m.state.Sources[name]
		if s == nil {
			s = &Source{Enabled: name == "threads" || name == "youtube" && o.YouTubeConfigured, Interval: interval, Status: "IDLE"}
			m.state.Sources[name] = s
		}
		s.Configured = name != "youtube" || o.YouTubeConfigured
		if s.Status == "RUNNING" || s.Status == "STOPPING" || s.Status == "RECOVERING" {
			s.Status = "INTERRUPTED"
			s.Error = "Controller restarted; reconciling worker outcome"
		}
	}
	if m.state.Active != "" {
		r := m.findRun(m.state.Active)
		if r != nil && m.workerAlive(r) {
			r.Status = "RECOVERING"
			m.state.Sources[r.Source].Status = "RECOVERING"
		} else {
			if r != nil {
				m.finishLocked(r, "Worker interrupted during backend restart")
			}
			m.state.Active = ""
		}
	}
	for _, s := range m.state.Sources {
		if m.state.Continuous && s.Enabled && s.Configured && s.Next == nil && s.Status != "RECOVERING" {
			next := time.Now().UTC()
			if s.Completed != nil {
				next = s.Completed.Add(sourceCadence(s))
			}
			s.Next = &next
		}
	}
	if err = m.persistLocked(); err != nil {
		lock.Close()
		return nil, err
	}
	go m.loop()
	return m, nil
}

func (m *Manager) findRun(id string) *Run {
	for _, r := range m.state.Runs {
		if r.ID == id {
			return r
		}
	}
	return nil
}
func (m *Manager) workerAlive(r *Run) bool {
	if r.PID <= 0 {
		return false
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", r.PID))
	return err == nil && strings.Contains(string(b), "collection_worker.py") && strings.Contains(string(b), r.ID)
}
func (m *Manager) persistLocked() error {
	b, err := json.MarshalIndent(m.state, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(m.options.Directory, ".control-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	if err == nil {
		err = os.Rename(name, filepath.Join(m.options.Directory, "control.json"))
	}
	return err
}
func (m *Manager) saveLocked() {
	if err := m.persistLocked(); err != nil {
		m.persistenceError = "Control state persistence failed"
		m.state.Continuous = false
	} else {
		m.persistenceError = ""
	}
}
func (m *Manager) Snapshot() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshStatusLocked()
	if checkpoint, err := m.threadsCheckpoint(); err == nil && !checkpoint.Exported {
		m.state.Sources["threads"].PendingAcquisition = checkpoint.RunID
	} else {
		m.state.Sources["threads"].PendingAcquisition = ""
	}
	m.state.Progress = nil
	if m.state.Active != "" {
		var p map[string]any
		if b, err := os.ReadFile(filepath.Join(m.options.Directory, "runs", m.state.Active, "progress.json")); err == nil && json.Unmarshal(b, &p) == nil {
			m.state.Progress = p
		}
	}
	b, _ := json.Marshal(m.state)
	var result State
	_ = json.Unmarshal(b, &result)
	return result
}
func (m *Manager) refreshStatusLocked() {
	if m.persistenceError != "" {
		m.state.Status = "FAILED"
		return
	}
	if m.state.Active != "" {
		if m.closing || m.state.StopRequested {
			m.state.Status = "STOPPING"
		} else {
			m.state.Status = "RUNNING"
		}
		return
	}
	for _, s := range m.state.Sources {
		if s.Pending {
			m.state.Status = "RUNNING"
			return
		}
	}
	m.state.Status = "IDLE"
}
func (m *Manager) Start(config map[string]SourceConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing {
		return errors.New("controller shutting down")
	}
	for name, c := range config {
		if m.state.Sources[name] == nil {
			return errors.New("unknown source")
		}
		if c.Interval < 10 || c.Interval > 604800 {
			return errors.New("interval must be 10..604800 seconds")
		}
		if c.Enabled && !m.state.Sources[name].Configured {
			return errors.New("YouTube is not configured")
		}
	}
	enabled := false
	for name, source := range m.state.Sources {
		selected := source.Enabled
		if update, ok := config[name]; ok {
			selected = update.Enabled
		}
		enabled = enabled || selected
	}
	if !enabled {
		return errors.New("select at least one source")
	}
	now := time.Now().UTC()
	for name, c := range config {
		s := m.state.Sources[name]
		was := s.Enabled
		s.Enabled = c.Enabled
		s.Interval = c.Interval
		if !c.Enabled {
			s.Next = nil
			s.Pending = false
		} else if (!m.state.Continuous || !was) && s.Status != "RUNNING" && s.Status != "RECOVERING" {
			s.Next = &now
		} else if m.state.Continuous && s.Next != nil && s.Completed != nil {
			next := s.Completed.Add(sourceCadence(s))
			s.Next = &next
		}
	}
	m.state.StopRequested = false
	m.state.Continuous = true
	m.refreshStatusLocked()
	if err := m.persistLocked(); err != nil {
		return err
	}
	m.signal()
	return nil
}
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.Continuous = false
	m.state.StopRequested = m.state.Active != ""
	for _, s := range m.state.Sources {
		s.Next = nil
		s.Pending = false
		s.RetryRun = ""
		if s.Status == "RUNNING" {
			s.Status = "STOPPING"
		}
	}
	m.refreshStatusLocked()
	return m.persistLocked()
}

// Configure saves source selections and cadence without triggering an idle run.
func (m *Manager) Configure(config map[string]SourceConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, c := range config {
		if m.state.Sources[name] == nil || c.Interval < 10 || c.Interval > 604800 {
			return errors.New("invalid source or interval")
		}
		if c.Enabled && !m.state.Sources[name].Configured {
			return errors.New("YouTube is not configured")
		}
	}
	now := time.Now().UTC()
	for name, c := range config {
		s := m.state.Sources[name]
		was := s.Enabled
		s.Enabled = c.Enabled
		s.Interval = c.Interval
		if !c.Enabled {
			s.Pending = false
			s.RetryRun = ""
			s.Next = nil
		} else if m.state.Continuous {
			if !was {
				s.Next = &now
			} else if s.Next != nil && s.Completed != nil {
				next := s.Completed.Add(sourceCadence(s))
				s.Next = &next
			}
		}
	}
	return m.persistLocked()
}

// Retry reuses immutable artifacts and observation identity; no source re-fetch.
func (m *Manager) Retry(id string) error  { return m.Replay(id) }
func (m *Manager) Replay(id string) error { return m.queueSaved(id, false) }
func (m *Manager) queueSaved(id string, recovery bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.findRun(id)
	if r == nil {
		return errors.New("unknown run")
	}
	s := m.state.Sources[r.Source]
	if m.closing || s.Pending || s.Status == "RUNNING" || s.Status == "STOPPING" || s.Status == "RECOVERING" {
		return ErrConflict
	}
	if r.Status != "FAILED" && r.Status != "PARTIAL" && r.Status != "INTERRUPTED" {
		return errors.New("retry requires failed or partial run")
	}
	if _, err := os.Stat(filepath.Join(m.options.Directory, "runs", id, "collection.json")); err != nil {
		if _, rawErr := os.Stat(filepath.Join(m.options.Directory, "runs", id, "bronze", r.Source, id+".json")); rawErr != nil {
			checkpoint, checkpointErr := m.threadsCheckpoint()
			if !recovery || r.Source != "threads" || checkpointErr != nil || checkpoint.RunID != id || checkpoint.Exported {
				return errors.New("no saved acquisition checkpoint or collection available")
			}
		}
	}
	s.Pending = true
	s.RetryRun = id
	s.Operation = "processing_replay"
	if recovery {
		s.Operation = "interrupted_recovery"
	}
	m.refreshStatusLocked()
	if err := m.persistLocked(); err != nil {
		return err
	}
	m.signal()
	return nil
}

type discoveryCheckpoint struct {
	RunID     string `json:"run_id"`
	Exported  bool   `json:"exported"`
	Committed bool   `json:"artifact_committed"`
}

func (m *Manager) threadsCheckpoint() (discoveryCheckpoint, error) {
	path := filepath.Join(m.options.Directory, "discovery", "threads.json")
	var config struct {
		Threads struct {
			Path string `json:"checkpoint_path"`
		} `json:"threads"`
	}
	if b, err := os.ReadFile(m.options.Config); err == nil && json.Unmarshal(b, &config) == nil && config.Threads.Path != "" {
		path = config.Threads.Path
		if !filepath.IsAbs(path) {
			path = filepath.Join(m.options.Root, path)
		}
	}
	var checkpoint discoveryCheckpoint
	b, err := os.ReadFile(path)
	if err == nil {
		err = json.Unmarshal(b, &checkpoint)
		if err == nil && !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`).MatchString(checkpoint.RunID) {
			err = errors.New("invalid checkpoint acquisition identity")
		}
	}
	return checkpoint, err
}

// Recover resumes uncommitted acquisition; Replay/Retry retains committed evidence.
func (m *Manager) Recover(id string) error {
	m.mu.Lock()
	checkpoint, err := m.threadsCheckpoint()
	if err == nil && checkpoint.RunID == id && !checkpoint.Exported && m.findRun(id) == nil {
		m.state.Runs = append(m.state.Runs, &Run{ID: id, Source: "threads", Status: "INTERRUPTED", Started: time.Now().UTC()})
	}
	m.mu.Unlock()
	if err != nil || checkpoint.RunID != id || checkpoint.Exported {
		return errors.New("no interrupted acquisition for this run")
	}
	return m.queueSaved(id, true)
}

func (m *Manager) Continue(id string) error {
	m.mu.Lock()
	r := m.findRun(id)
	checkpoint, err := m.threadsCheckpoint()
	valid := r != nil && r.Source == "threads" && err == nil && checkpoint.RunID == id && checkpoint.Exported && checkpoint.Committed
	m.mu.Unlock()
	if !valid {
		return errors.New("discovery continuation requires committed source evidence")
	}
	return m.RunNow([]string{"threads"})
}
func (m *Manager) RunNow(names []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing {
		return errors.New("controller shutting down")
	}
	if len(names) == 0 {
		return errors.New("select sources")
	}
	for _, name := range names {
		s := m.state.Sources[name]
		if s == nil {
			return errors.New("unknown source")
		}
		if !s.Configured {
			return errors.New("YouTube is not configured")
		}
		if s.Pending || s.Status == "RUNNING" || s.Status == "STOPPING" || s.Status == "RECOVERING" {
			return ErrConflict
		}
	}
	for _, name := range names {
		m.state.Sources[name].Pending = true
		m.state.Sources[name].Operation = "discovery_continuation"
	}
	if m.state.Active == "" {
		m.state.StopRequested = false
	}
	m.refreshStatusLocked()
	if err := m.persistLocked(); err != nil {
		return err
	}
	m.signal()
	return nil
}
func (m *Manager) signal() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}
func (m *Manager) loop() {
	defer close(m.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-m.wake:
		case <-ticker.C:
		}
		m.tick()
	}
}
func (m *Manager) tick() {
	m.mu.Lock()
	if m.state.Active != "" {
		r := m.findRun(m.state.Active)
		if r != nil && r.Status == "RECOVERING" && !m.workerAlive(r) {
			m.finishLocked(r, "Recovered worker ended without summary")
			m.state.Active = ""
			m.saveLocked()
		}
		m.mu.Unlock()
		return
	}
	if m.closing || m.persistenceError != "" {
		m.mu.Unlock()
		return
	}
	now := time.Now().UTC()
	name := ""
	// Oldest due time wins, so a short interval cannot starve another source.
	var earliest *time.Time
	for n, s := range m.state.Sources {
		if s.Pending {
			name = n
			break
		}
		if m.state.Continuous && s.Enabled && s.Configured && s.Next != nil && !s.Next.After(now) && (earliest == nil || s.Next.Before(*earliest)) {
			name = n
			earliest = s.Next
		}
	}
	if name == "" {
		m.mu.Unlock()
		return
	}
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		m.mu.Unlock()
		return
	}
	id := hex.EncodeToString(bytes)
	s := m.state.Sources[name]
	previous := (*Run)(nil)
	if s.RetryRun != "" {
		id = s.RetryRun
		previous = m.findRun(id)
		s.RetryRun = ""
	}
	s.Pending = false
	s.Next = nil
	s.Status = "RUNNING"
	s.LastRun = id
	s.Started = &now
	s.Error = ""
	r := &Run{ID: id, Source: name, Status: "RUNNING", Started: now, Operation: s.Operation}
	s.Operation = ""
	r.Attempt = 1
	if previous != nil {
		r.Attempt = max(1, previous.Attempt) + 1
	}
	m.state.Runs = append([]*Run{r}, m.state.Runs...)
	if len(m.state.Runs) > 100 {
		m.state.Runs = m.state.Runs[:100]
	}
	m.state.Active = id
	m.refreshStatusLocked()
	if err := m.persistLocked(); err != nil {
		m.state.Active = ""
		s.Status = "FAILED"
		m.persistenceError = "Control state persistence failed"
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	go m.run(r)
}
func (m *Manager) run(r *Run) {
	var cmd *exec.Cmd
	var err error
	if m.options.Runner != nil {
		cmd, err = m.options.Runner(r.Source, r.ID)
	} else {
		cmd = exec.Command(m.options.Python, filepath.Join(m.options.Root, "orchestration/local/collection_worker.py"), "--source", r.Source, "--run-id", r.ID, "--state", m.options.Directory, "--config", m.options.Config)
		if r.Operation != "" {
			cmd.Args = append(cmd.Args, "--operation", r.Operation)
		}
		cmd.Dir = m.options.Root
		cmd.Env = append(os.Environ(), "PYTHONUNBUFFERED=1")
		// Terminal interrupts target the backend; its shutdown waits for this job.
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}
	if err == nil {
		err = cmd.Start()
	}
	if err == nil {
		m.mu.Lock()
		r.PID = cmd.Process.Pid
		m.saveLocked()
		m.mu.Unlock()
		err = cmd.Wait()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	message := ""
	if err != nil {
		message = "Source cycle failed; inspect structured run summary"
	}
	m.finishLocked(r, message)
	m.state.Active = ""
	m.state.StopRequested = false
	m.refreshStatusLocked()
	m.saveLocked()
	m.signal()
}
func (m *Manager) finishLocked(r *Run, failure string) {
	now := time.Now().UTC()
	r.Completed = &now
	r.Duration = now.Sub(r.Started).Seconds()
	r.Status = "FAILED"
	r.Error = failure
	var result struct {
		Status, Error   string
		Metrics, Stages map[string]any
	}
	if b, err := os.ReadFile(filepath.Join(m.options.Directory, "runs", r.ID, "summary.json")); err == nil && json.Unmarshal(b, &result) == nil {
		if result.Status == "SUCCESS" || result.Status == "PARTIAL" || result.Status == "FAILED" {
			r.Status = result.Status
			r.Error = result.Error
			r.Metrics = result.Metrics
			r.Stages = result.Stages
		}
	}
	s := m.state.Sources[r.Source]
	s.Status = r.Status
	s.Completed = &now
	s.Error = r.Error
	s.Metrics = r.Metrics
	if m.state.Continuous && s.Enabled && !m.closing {
		next := now.Add(sourceCadence(s))
		s.Next = &next
	} else {
		s.Next = nil
	}
}

// Close stops scheduling and waits for the active cycle's transactions to finish.
func (m *Manager) Close() {
	m.mu.Lock()
	m.closing = true
	m.mu.Unlock()
	for {
		m.mu.Lock()
		active := m.state.Active != ""
		m.mu.Unlock()
		if !active {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	close(m.stop)
	<-m.done
	_ = syscall.Flock(int(m.lock.Fd()), syscall.LOCK_UN)
	m.lock.Close()
}

func sourceCadence(s *Source) time.Duration {
	interval := s.Interval
	if reason, _ := s.Metrics["stopping_reason"].(string); reason == "quotaExceeded" || reason == "dailyLimitExceeded" {
		interval = max(interval, 86400)
	}
	return time.Duration(interval) * time.Second
}
