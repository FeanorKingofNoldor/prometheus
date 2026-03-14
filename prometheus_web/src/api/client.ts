const BASE = "/api";

/* ── Logging helpers ────────────────────────────────────── */

const LOG_PREFIX = "[prometheus/api]";
const LOG_ENABLED = true; // flip to false to silence

function logReq(method: string, path: string, body?: unknown) {
  if (!LOG_ENABLED) return;
  const tag = `%c${LOG_PREFIX} ${method} ${path}`;
  if (body !== undefined) {
    console.groupCollapsed(tag, "color:#facc15");
    console.log("body:", body);
    console.groupEnd();
  } else {
    console.log(tag, "color:#facc15");
  }
}

function logRes(method: string, path: string, status: number, data: unknown) {
  if (!LOG_ENABLED) return;
  const isEmpty =
    data === null ||
    data === undefined ||
    (Array.isArray(data) && data.length === 0) ||
    (typeof data === "object" && data !== null && Object.keys(data).length === 0);

  const color = isEmpty ? "color:#ef4444" : "color:#22c55e";
  const label = isEmpty ? "(EMPTY)" : "";

  console.groupCollapsed(
    `%c${LOG_PREFIX} ${method} ${path} → ${status} ${label}`,
    color,
  );
  console.log("response:", data);
  console.groupEnd();
}

function logErr(method: string, path: string, err: unknown) {
  if (!LOG_ENABLED) return;
  console.error(`${LOG_PREFIX} ${method} ${path} FAILED`, err);
}

/* ── ApiError ───────────────────────────────────────────── */

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

/* ── Fetch wrapper ──────────────────────────────────────── */

export async function apiFetch<T>(
  path: string,
  options?: RequestInit,
): Promise<T> {
  const method = options?.method ?? "GET";
  logReq(method, path, options?.body ? JSON.parse(options.body as string) : undefined);

  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...options?.headers },
    ...options,
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    const err = new ApiError(res.status, body || `HTTP ${res.status}`);
    logErr(method, path, { status: res.status, body });
    throw err;
  }
  const data: T = await res.json();
  logRes(method, path, res.status, data);
  return data;
}

/* ── Convenience methods ────────────────────────────────── */

export const api = {
  get: <T>(path: string) => apiFetch<T>(path),
  post: <T>(path: string, body?: unknown) =>
    apiFetch<T>(path, {
      method: "POST",
      body: body ? JSON.stringify(body) : undefined,
    }),
};
