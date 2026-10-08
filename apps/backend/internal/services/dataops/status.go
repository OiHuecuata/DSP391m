package dataops

import (
	"bufio"
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/username/DSP391m/apps/backend/internal/config"
)

type TableStatus struct {
	Name    string     `json:"name"`
	Exists  bool       `json:"exists"`
	Rows    *int64     `json:"rows"`
	Version *int64     `json:"version"`
	Updated *time.Time `json:"updated_at"`
	Error   string     `json:"error,omitempty"`
}
type LakeStatus struct {
	Status     string        `json:"status"`
	Backend    string        `json:"backend"`
	URI        string        `json:"uri"`
	Readable   bool          `json:"readable"`
	Writable   bool          `json:"writable"`
	Size       int64         `json:"physical_bytes"`
	TableCount int           `json:"table_count"`
	Tables     []TableStatus `json:"tables"`
	Updated    *time.Time    `json:"updated_at"`
	Refreshed  time.Time     `json:"refreshed_at"`
}
type WarehouseStatus struct {
	Status     string     `json:"status"`
	Backend    string     `json:"backend"`
	Host       string     `json:"host"`
	Database   string     `json:"database"`
	Connected  bool       `json:"connected"`
	Table      string     `json:"table"`
	Exists     bool       `json:"exists"`
	Rows       *int64     `json:"rows"`
	SourceRows *int64     `json:"source_activity_rows"`
	Locations  *int64     `json:"distinct_locations"`
	Snapshot   *string    `json:"latest_snapshot_date"`
	Generated  *string    `json:"gold_generated_at"`
	LastLoad   *time.Time `json:"last_successful_load_at"`
	Refreshed  time.Time  `json:"refreshed_at"`
	Error      string     `json:"error,omitempty"`
}
type StatusService struct {
	mu         sync.Mutex
	URI        string
	Paths      map[string]string
	ClickHouse config.ClickHouse
	Manager    *Manager
	ttl        time.Duration
	cachedAt   time.Time
	lake       LakeStatus
	warehouse  WarehouseStatus
}

func NewStatusService(uri string, ch config.ClickHouse, m *Manager) *StatusService {
	paths := map[string]string{}
	for name, variable := range map[string]string{"bronze/threads": "DSP_DELTA_THREADS_BRONZE_PATH", "bronze/youtube": "DSP_DELTA_YOUTUBE_BRONZE_PATH", "bronze/pbplus": "DSP_DELTA_PBPLUS_BRONZE_PATH", "silver/social_posts": "DSP_DELTA_SOCIAL_POSTS_PATH", "silver/courts": "DSP_DELTA_COURTS_PATH", "rejected/social_posts": "DSP_DELTA_REJECTED_SOCIAL_PATH", "gold/social_activity_daily": "DSP_DELTA_SOCIAL_ACTIVITY_DAILY_PATH", "gold/social_source_activity_daily": "DSP_DELTA_SOCIAL_SOURCE_ACTIVITY_DAILY_PATH", "gold/court_supply_by_location": "DSP_DELTA_COURT_SUPPLY_BY_LOCATION_PATH", "gold/location_market_signal_snapshot": "DSP_DELTA_LOCATION_MARKET_SIGNAL_SNAPSHOT_PATH", "quality/unresolved_court_locations": "DSP_DELTA_UNRESOLVED_COURT_LOCATIONS_PATH"} {
		paths[name] = os.Getenv(variable)
		if paths[name] == "" {
			paths[name] = strings.TrimRight(uri, "/") + "/" + name
		}
	}
	return &StatusService{URI: uri, Paths: paths, ClickHouse: ch, Manager: m, ttl: 15 * time.Second}
}
func localPath(uri string) (string, bool) {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme != "file" || u.Host != "" || u.RawQuery != "" || u.Fragment != "" {
		return "", false
	}
	return u.Path, true
}
func inspectTable(name, path string) TableStatus {
	result := TableStatus{Name: name}
	dir, ok := localPath(path)
	if !ok {
		result.Error = "Unsupported local table URI"
		return result
	}
	files, err := filepath.Glob(filepath.Join(dir, "_delta_log", "*.json"))
	if err != nil || len(files) == 0 {
		return result
	}
	sort.Strings(files)
	active := map[string]*int64{}
	complete := false
	for _, path := range files {
		version, err := strconv.ParseInt(strings.TrimSuffix(filepath.Base(path), ".json"), 10, 64)
		if err != nil {
			continue
		}
		if version == 0 {
			complete = true
		}
		f, err := os.Open(path)
		if err != nil {
			result.Error = "Delta log unreadable"
			continue
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 65536), 16*1024*1024)
		for scanner.Scan() {
			var action struct {
				Add *struct {
					Path  string
					Stats string
				}
				Remove *struct{ Path string }
			}
			if json.Unmarshal(scanner.Bytes(), &action) != nil {
				complete = false
				continue
			}
			if action.Remove != nil {
				delete(active, action.Remove.Path)
			}
			if action.Add != nil {
				var stats struct {
					NumRecords *int64 `json:"numRecords"`
				}
				_ = json.Unmarshal([]byte(action.Add.Stats), &stats)
				active[action.Add.Path] = stats.NumRecords
			}
		}
		if scanner.Err() != nil {
			complete = false
			result.Error = "Delta log inspection incomplete"
		}
		f.Close()
		info, err := os.Stat(path)
		if err == nil {
			t := info.ModTime().UTC()
			result.Updated = &t
		}
		v := version
		result.Version = &v
		result.Exists = true
	}
	if complete {
		var count int64
		for _, n := range active {
			if n == nil {
				complete = false
				break
			}
			count += *n
		}
		if complete {
			result.Rows = &count
		}
	}
	if result.Exists && result.Rows == nil && result.Error == "" {
		result.Error = "Row statistics unavailable; checkpoint-aware refresh required"
	}
	return result
}
func (s *StatusService) inspectLake() LakeStatus {
	now := time.Now().UTC()
	l := LakeStatus{Status: "FAILED", Backend: "Local Delta Lake", URI: s.URI, Refreshed: now, Tables: []TableStatus{}}
	dir, ok := localPath(s.URI)
	if !ok {
		l.URI = "[invalid local lakehouse URI]"
		return l
	}
	f, err := os.Open(dir)
	if err == nil {
		_, err = f.Readdirnames(1)
		f.Close()
		l.Readable = err == nil || err.Error() == "EOF"
	}
	if f, err := os.CreateTemp(dir, ".data-ops-probe-"); err == nil {
		name := f.Name()
		f.Close()
		os.Remove(name)
		l.Writable = true
	}
	_ = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if entry.Type().IsRegular() {
			if info, err := entry.Info(); err == nil {
				l.Size += info.Size()
			}
		}
		return nil
	})
	names := make([]string, 0, len(s.Paths))
	for name := range s.Paths {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t := inspectTable(name, s.Paths[name])
		l.Tables = append(l.Tables, t)
		if t.Exists {
			l.TableCount++
		}
		if t.Updated != nil && (l.Updated == nil || t.Updated.After(*l.Updated)) {
			l.Updated = t.Updated
		}
	}
	if l.Readable && l.Writable {
		l.Status = "HEALTHY"
	}
	return l
}
func (s *StatusService) inspectWarehouse(ctx context.Context) WarehouseStatus {
	c := s.ClickHouse
	w := WarehouseStatus{Status: "FAILED", Backend: "ClickHouse", Host: c.Host + ":" + strconv.Itoa(c.Port), Database: c.Database, Table: "location_market_signal_snapshot", Refreshed: time.Now().UTC()}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	query := func(sql string, target any) error {
		req, err := http.NewRequestWithContext(ctx, "POST", c.URL()+"/?output_format_json_quote_64bit_integers=0", strings.NewReader(sql+" FORMAT JSONEachRow"))
		if err != nil {
			return err
		}
		req.SetBasicAuth(c.User, c.Password)
		resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return fs.ErrInvalid
		}
		return json.NewDecoder(resp.Body).Decode(target)
	}
	var exists struct {
		N int64 `json:"n"`
	}
	if err := query("SELECT count() AS n FROM system.tables WHERE database = '"+c.Database+"' AND name = 'location_market_signal_snapshot'", &exists); err != nil {
		w.Error = "ClickHouse connection/query failed"
		return w
	}
	w.Connected = true
	w.Exists = exists.N > 0
	if !w.Exists {
		w.Status = "PARTIAL"
		w.Error = "Serving table missing"
		return w
	}
	var result struct {
		Rows      int64   `json:"rows"`
		Locations int64   `json:"locations"`
		Snapshot  *string `json:"snapshot"`
		Generated *string `json:"generated"`
	}
	if err := query("SELECT count() AS rows, uniqExact(location_id) AS locations, if(count()=0,NULL,toString(max(snapshot_date))) AS snapshot, if(count()=0,NULL,toString(max(gold_generated_at))) AS generated FROM "+c.Database+".location_market_signal_snapshot", &result); err != nil {
		w.Error = "Serving metrics query failed"
		return w
	}
	w.Rows = &result.Rows
	w.Locations = &result.Locations
	w.Snapshot = result.Snapshot
	w.Generated = result.Generated
	w.Status = "HEALTHY"
	var sourceExists struct {
		N int64 `json:"n"`
	}
	if query("SELECT count() AS n FROM system.tables WHERE database = '"+c.Database+"' AND name = 'social_source_activity_daily'", &sourceExists) == nil && sourceExists.N > 0 {
		var sourceRows struct {
			N int64 `json:"n"`
		}
		if query("SELECT count() AS n FROM "+c.Database+".social_source_activity_daily", &sourceRows) == nil {
			w.SourceRows = &sourceRows.N
		}
	}
	return w
}
func (s *StatusService) Infrastructure(ctx context.Context) (LakeStatus, WarehouseStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.cachedAt) >= s.ttl {
		s.lake = s.inspectLake()
		s.warehouse = s.inspectWarehouse(ctx)
		s.cachedAt = time.Now()
	}
	w := s.warehouse
	for _, r := range s.Manager.Snapshot().Runs {
		if stage, ok := r.Stages["clickhouse"].(map[string]any); ok && stage["result"] != nil && r.Completed != nil {
			w.LastLoad = r.Completed
			break
		}
	}
	return s.lake, w
}
func (s *StatusService) Status(ctx context.Context) map[string]any {
	l, w := s.Infrastructure(ctx)
	control := s.Manager.Snapshot()
	status := control.Status
	if status == "IDLE" {
		if len(control.Runs) == 0 {
			status = "IDLE"
		} else {
			status = control.Runs[0].Status
			if status == "SUCCESS" {
				status = "HEALTHY"
			}
			if status == "HEALTHY" {
				for _, source := range control.Sources {
					if source.Enabled && (source.Status == "FAILED" || source.Status == "PARTIAL" || source.Status == "INTERRUPTED") {
						status = "PARTIAL"
						break
					}
				}
			}
		}
		if l.Status != "HEALTHY" || w.Status != "HEALTHY" {
			status = "FAILED"
		}
	}
	return map[string]any{"pipeline": map[string]any{"status": status, "basis": "Latest complete cycle: collection, Kafka acknowledgements, Spark/Silver payload verification, Gold, ClickHouse field comparison; cached current storage health", "refresh_interval_seconds": 15}, "collection": control, "lakehouse": l, "warehouse": w}
}
