/** @jsxImportSource react */
import type { KeyboardEvent } from "react";

interface TabOption { id: string; name: string }

export function TrackerTabs({ id, label, options, selected, onSelect, variant = "primary" }: {
  id: string;
  label: string;
  options: readonly TabOption[];
  selected: string;
  onSelect: (id: string) => void;
  variant?: "primary" | "secondary" | "source";
}) {
  function navigate(event: KeyboardEvent<HTMLButtonElement>, index: number) {
    let next: number;
    switch (event.key) {
      case "ArrowRight": next = (index + 1) % options.length; break;
      case "ArrowLeft": next = (index - 1 + options.length) % options.length; break;
      case "Home": next = 0; break;
      case "End": next = options.length - 1; break;
      default: return;
    }
    event.preventDefault();
    onSelect(options[next].id);
    document.getElementById(`${id}-tab-${options[next].id}`)?.focus();
  }
  return <div className={`tracker-tabs tabs-${variant}`} role="tablist" aria-label={label}>
    {options.map((option, index) => <button key={option.id} type="button" role="tab"
      id={`${id}-tab-${option.id}`} aria-controls={`${id}-panel-${option.id}`}
      aria-selected={selected === option.id} tabIndex={selected === option.id ? 0 : -1}
      onClick={() => onSelect(option.id)} onKeyDown={(event) => navigate(event, index)}>
      {option.name}
    </button>)}
  </div>;
}
