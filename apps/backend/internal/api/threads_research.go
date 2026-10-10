package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/username/DSP391m/apps/backend/internal/domain"
	ch "github.com/username/DSP391m/apps/backend/internal/repository/clickhouse"
)

type ThreadsResearchReader interface {
	EDA(context.Context, domain.ThreadsFilters) (domain.ThreadsEDA, error)
	Generation(context.Context, string) (domain.ResearchManifest, error)
}
type ThreadsResearchHandler struct{ Reader ThreadsResearchReader }

var generationPattern = regexp.MustCompile("^sha256:[0-9a-f]{64}$")

func (h *ThreadsResearchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := domain.ThreadsFilters{Generation: q.Get("generation"), From: q.Get("from"), To: q.Get("to"), Platform: q.Get("platform"), Locality: q.Get("locality"), Resolution: q.Get("resolution"), Intent: q.Get("intent"), Topic: q.Get("topic"), Metric: q.Get("metric"), Origin: q.Get("origin"), CollectionStatus: q.Get("coverage"), Bucket: q.Get("bucket")}
	bad := f.Generation != "" && !generationPattern.MatchString(f.Generation) || len(f.Locality) > 120
	for _, date := range []string{f.From, f.To} {
		if date != "" {
			parsed, err := time.Parse("2006-01-02", date)
			bad = bad || err != nil || parsed.Format("2006-01-02") != date
		}
	}
	bad = bad || f.From != "" && f.To != "" && f.From > f.To
	valid := func(value string, choices ...string) bool {
		if value == "" {
			return true
		}
		for _, item := range choices {
			if item == value {
				return true
			}
		}
		return false
	}
	intents := []string{"SEEK_COURT", "BOOKING_INTENT", "SEEK_PLAYERS", "SEEK_COACH", "PRICE_INQUIRY", "CAPACITY_COMPLAINT", "VENUE_FEEDBACK", "SUPPLY_ADVERTISEMENT", "SUPPLY_OPENING", "SUPPLY_CLOSURE", "EVENT_TOURNAMENT", "GENERAL_DISCUSSION", "IRRELEVANT"}
	bad = bad || !valid(f.Platform, "threads") || !valid(f.Resolution, "Resolved", "Unresolved", "Mixed", "NoMention") || !valid(f.Intent, intents...) || !valid(f.Metric, "likes", "comments", "views", "shares", "reposts", "quotes") || !valid(f.Origin, "SYNTHETIC", "PERMITTED_COLLECTION") || !valid(f.CollectionStatus, "PARTIAL", "COMPLETE", "FAILED") || !valid(f.Bucket, "day", "week", "month")
	if f.Topic != "" {
		allowed := false
		for _, label := range intents {
			if f.Topic == strings.ToLower(label) {
				allowed = true
			}
		}
		bad = bad || !allowed
	}
	if bad {
		http.Error(w, "Invalid research filter", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	if r.URL.Path == "/api/v2/research/threads/generation" {
		value, err := h.Reader.Generation(ctx, f.Generation)
		h.respond(w, value, err)
		return
	}
	value, err := h.Reader.EDA(ctx, f)
	h.respond(w, value, err)
}

func (h *ThreadsResearchHandler) respond(w http.ResponseWriter, value any, err error) {
	if err != nil {
		code := http.StatusServiceUnavailable
		if errors.Is(err, ch.ErrResearchUnavailable) {
			code = http.StatusNotFound
		}
		http.Error(w, "Research generation unavailable", code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(value)
}
