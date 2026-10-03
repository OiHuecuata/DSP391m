import type { ComponentState, KnownComponentState } from "../types";

const states: Record<KnownComponentState, { label: string; tone: string }> = {
  idle: { label: "Idle", tone: "neutral" },
  running: { label: "Running", tone: "blue" },
  success: { label: "Success", tone: "green" },
  healthy: { label: "Healthy", tone: "green" },
  failed: { label: "Failed", tone: "red" },
  unavailable: { label: "Unavailable", tone: "amber" },
  not_configured: { label: "Not configured", tone: "neutral" },
};

export function StatusBadge({ status }: { status: ComponentState }) {
  const state = Object.hasOwn(states, status)
    ? states[status as KnownComponentState]
    : { label: status.replaceAll("_", " ") || "Unknown", tone: "neutral" };
  return <span className={`status-badge status-${state.tone}`}>
    <span className={status === "running" ? "status-dot pulse" : "status-dot"} aria-hidden="true" />
    {state.label}
  </span>;
}
