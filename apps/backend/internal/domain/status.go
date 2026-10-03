package domain

import "time"

// ComponentState represents the runtime/health state of one system component.
type ComponentState string

const (
	StateIdle          ComponentState = "idle"
	StateRunning       ComponentState = "running"
	StateSuccess       ComponentState = "success"
	StateHealthy       ComponentState = "healthy"
	StateFailed        ComponentState = "failed"
	StateUnavailable   ComponentState = "unavailable"
	StateNotConfigured ComponentState = "not_configured"
)

// ComponentType identifies the role of a component in the data platform.
type ComponentType string

const (
	ComponentTypeCrawler       ComponentType = "crawler"
	ComponentTypeDataLake      ComponentType = "data_lake"
	ComponentTypeDataWarehouse ComponentType = "data_warehouse"
)

// ComponentStatus is the API-facing status model shared by all system
// components.
//
// Details is intentionally flexible because crawler, Data Lake, and Data
// Warehouse expose different runtime metrics while still sharing the same
// top-level status contract.
type ComponentStatus struct {
	ID        string         `json:"id"`
	Type      ComponentType  `json:"type"`
	Status    ComponentState `json:"status"`
	UpdatedAt *time.Time     `json:"updated_at,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
}

// SystemStatus is the aggregate status returned by the system status service.
type SystemStatus struct {
	Components []ComponentStatus `json:"components"`
}
