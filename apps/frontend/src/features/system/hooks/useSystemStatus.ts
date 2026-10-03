"use client";

import { useEffect, useState } from "react";
import { getSystemStatus } from "@/services/system";
import type { SystemStatus } from "../types";

export const STATUS_POLL_INTERVAL_MS = 5_000;

export function useSystemStatus() {
  const [data, setData] = useState<SystemStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    async function poll() {
      try {
        const status = await getSystemStatus(controller.signal);
        if (controller.signal.aborted) return;
        setData(status);
        setError(null);
      } catch (cause: unknown) {
        if (controller.signal.aborted) return;
        setError(cause instanceof Error ? cause.message : "Could not fetch system status.");
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
          // Slow requests never overlap. Keep polling completed runs to discover the next run.
          timer = setTimeout(poll, STATUS_POLL_INTERVAL_MS);
        }
      }
    }
    void poll();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, []);
  return { data, loading, error };
}
