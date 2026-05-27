/**
 * Iris SSE chat client.
 *
 * Streams from prometheus's `/api/iris/chat/stream` endpoint, which is a
 * proxy to apatheon's `/api/chat/stream`. Events arrive as:
 *
 *   data: {"type": "thinking"}\n\n
 *   data: {"type": "tool_call_start", "name": ..., "args_keys": [...]}\n\n
 *   data: {"type": "tool_call_result", "name": ..., "size": ...}\n\n
 *   data: {"type": "done", "response": {answer, sources, proposals, ...}}\n\n
 *   data: {"type": "error", "message": "..."}\n\n
 *
 * Plus optional `: heartbeat\n\n` comment lines that keep proxies alive.
 */

export type StreamEvent =
  | { type: "thinking" }
  | { type: "tool_call_start"; name: string; args_keys?: string[] }
  | { type: "tool_call_result"; name: string; size?: number }
  | { type: "done"; response: Record<string, unknown> }
  | { type: "error"; message: string };

export interface StreamIrisOptions {
  question: string;
  context?: Record<string, unknown>;
  history?: Array<{ role: "user" | "assistant"; content: string }>;
  onEvent: (e: StreamEvent) => void;
  signal?: AbortSignal;
}

const BASE = "/api";

/**
 * Open a streaming chat session. Returns a promise that resolves when the
 * stream finishes (either with a `done` or `error` event), or rejects on
 * transport failure. Callers should handle their own UI state via onEvent.
 */
export async function streamIris(opts: StreamIrisOptions): Promise<void> {
  const body = JSON.stringify({
    question: opts.question,
    context: {
      ...(opts.context ?? {}),
      history: opts.history ?? [],
    },
  });

  const res = await fetch(`${BASE}/iris/chat/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "text/event-stream" },
    body,
    signal: opts.signal,
  });

  if (!res.ok || !res.body) {
    opts.onEvent({
      type: "error",
      message: `HTTP ${res.status} ${res.statusText}`,
    });
    return;
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = "";

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });

    // SSE frame boundary is \n\n. Process every complete frame in the buffer.
    let nlIdx: number;
    while ((nlIdx = buf.indexOf("\n\n")) !== -1) {
      const frame = buf.slice(0, nlIdx);
      buf = buf.slice(nlIdx + 2);

      // Ignore SSE comment frames (heartbeats, ":...\n").
      if (!frame.startsWith("data:")) continue;

      const json = frame.slice(5).trim();
      if (!json) continue;
      try {
        const evt = JSON.parse(json) as StreamEvent;
        opts.onEvent(evt);
        if (evt.type === "done" || evt.type === "error") {
          // No need to keep reading; upstream will close shortly.
          return;
        }
      } catch (parseErr) {
        // Malformed event — surface but keep reading.
        opts.onEvent({
          type: "error",
          message: `Malformed SSE frame: ${String(parseErr)}`,
        });
      }
    }
  }
}
