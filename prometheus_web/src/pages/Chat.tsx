import { useCallback, useEffect, useRef, useState } from "react";
import { Send, Trash2 } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { useIris } from "../context/IrisContext";
import { streamIris, type StreamEvent } from "../api/irisStream";

/**
 * Full-page Iris route — shares the same conversation as the slide-in
 * overlay (IrisOverlay) via IrisContext. Expanding the overlay into the
 * page (via the maximize button) preserves the message history. Closing
 * the overlay and coming here also preserves it.
 *
 * The overlay is the primary UX; this page exists as a deep-link target
 * (e.g. `prometheus.apatheon.ai/iris`) and for users who want a roomier
 * surface for long conversations.
 */

const SUGGESTED_PROMPTS = [
  "Summarize my portfolio risk right now",
  "Any drift between live and backtest?",
  "What are today's autopilot notifications?",
  "Which strategies got flagged in diagnostics this week?",
  "What's the current regime?",
  "Cross-check Apatheon's flagged conflicts against my open positions",
];

function formatToolName(name: string): string {
  return name
    .replace(/^get_/, "")
    .replace(/^prometheus_/, "")
    .replace(/^cassandra_/, "")
    .replace(/_/g, " ");
}

export default function Chat() {
  const iris = useIris();
  const [input, setInput] = useState("");
  const [streaming, setStreaming] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [iris.messages, iris.streamingStatus]);

  useEffect(() => () => abortRef.current?.abort(), []);

  const send = useCallback(async (text: string) => {
    if (!text.trim() || streaming) return;
    const question = text.trim();
    setInput("");
    setStreaming(true);
    iris.setStreamingStatus("thinking…");
    iris.appendMessage({ role: "user", content: question, timestamp: Date.now() });

    const history = iris.messages.slice(-10).map((m) => ({
      role: m.role, content: m.content,
    }));
    const tools: string[] = [];

    abortRef.current?.abort();
    abortRef.current = new AbortController();

    try {
      await streamIris({
        question,
        history,
        signal: abortRef.current.signal,
        onEvent: (e: StreamEvent) => {
          if (e.type === "thinking") {
            iris.setStreamingStatus("thinking…");
          } else if (e.type === "tool_call_start") {
            tools.push(e.name);
            iris.setStreamingStatus(`using ${formatToolName(e.name)}…`);
          } else if (e.type === "tool_call_result") {
            iris.setStreamingStatus(`got ${formatToolName(e.name)} result`);
          } else if (e.type === "done") {
            const r = e.response as Record<string, unknown>;
            const answer = String(
              r.answer ?? r.response ?? r.message ?? r.content ?? r.text ?? "(no response)",
            );
            iris.appendMessage({
              role: "assistant", content: answer, timestamp: Date.now(),
              tools: tools.length ? [...tools] : undefined,
            });
            iris.setStreamingStatus(null);
          } else if (e.type === "error") {
            iris.appendMessage({
              role: "assistant", content: `Iris error: ${e.message}`,
              timestamp: Date.now(),
            });
            iris.setStreamingStatus(null);
          }
        },
      });
    } catch (err) {
      const name = err instanceof Error ? err.name : "";
      if (name !== "AbortError") {
        iris.appendMessage({
          role: "assistant", content: `Transport error: ${String(err)}`,
          timestamp: Date.now(),
        });
      }
      iris.setStreamingStatus(null);
    } finally {
      setStreaming(false);
    }
  }, [iris, streaming]);

  const handleKey = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      send(input);
    }
  };

  return (
    <div className="flex h-full flex-col">
      <PageHeader
        title="Iris"
        subtitle="Cross-project assistant — reads portfolio, decisions, drift, notifications, geopolitical state."
        actions={
          iris.messages.length > 0 ? (
            <button
              onClick={iris.clearMessages}
              className="flex items-center gap-1.5 rounded border border-border-dim px-2 py-1 text-[10px] uppercase tracking-wider text-muted hover:border-accent hover:text-accent"
              title="Clear conversation"
            >
              <Trash2 size={11} /> Clear
            </button>
          ) : null
        }
      />

      <div className="flex-1 overflow-y-auto rounded-lg border border-border-dim bg-surface-raised p-4">
        {iris.messages.length === 0 && (
          <div className="flex h-full items-center justify-center">
            <div className="text-center max-w-md">
              <div className="text-2xl font-bold text-accent">Iris</div>
              <p className="mt-2 text-xs text-muted">
                Ask about your trading state, geopolitical risk, or any analytical question. Powered by Apatheon's full tool registry plus cross-project tools that read Prometheus + Cassandra data.
              </p>
              <div className="mt-4 flex flex-wrap justify-center gap-2">
                {SUGGESTED_PROMPTS.map((q) => (
                  <button
                    key={q}
                    className="rounded border border-border-dim bg-surface-overlay px-3 py-1.5 text-xs text-muted hover:border-accent hover:text-zinc-100"
                    onClick={() => setInput(q)}
                  >
                    {q}
                  </button>
                ))}
              </div>
              <div className="mt-4 text-[10px] text-muted/60">
                Tip: use the brain icon (bottom-right) or{" "}
                <kbd className="rounded border border-border-dim bg-surface px-1 py-px">Ctrl</kbd>+
                <kbd className="rounded border border-border-dim bg-surface px-1 py-px">I</kbd>{" "}
                to chat from any page.
              </div>
            </div>
          </div>
        )}

        <div className="space-y-4">
          {iris.messages.map((m, i) => (
            <div
              key={i}
              className={`flex ${m.role === "user" ? "justify-end" : "justify-start"}`}
            >
              <div
                className={`max-w-[78%] rounded-lg px-4 py-2.5 text-[13px] leading-relaxed ${
                  m.role === "user"
                    ? "bg-accent/20 text-zinc-100"
                    : "bg-surface-overlay text-zinc-300"
                }`}
              >
                {m.role === "assistant" && (
                  <div className="mb-1 flex items-center justify-between gap-3">
                    <span className="text-[10px] font-semibold uppercase tracking-wider text-accent">
                      Iris
                    </span>
                    {m.tools && m.tools.length > 0 && (
                      <span className="text-[9px] text-muted/80">
                        {m.tools.map(formatToolName).join(" · ")}
                      </span>
                    )}
                  </div>
                )}
                <div className="whitespace-pre-wrap break-words">{m.content}</div>
                <div className="mt-1 text-[10px] text-muted">
                  {new Date(m.timestamp).toLocaleTimeString()}
                </div>
              </div>
            </div>
          ))}

          {streaming && (
            <div className="flex justify-start">
              <div className="rounded-lg bg-surface-overlay px-4 py-2.5">
                <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-accent">
                  Iris
                </div>
                <div className="flex items-center gap-2">
                  <span className="h-2 w-2 animate-pulse rounded-full bg-accent" />
                  <span className="h-2 w-2 animate-pulse rounded-full bg-accent [animation-delay:150ms]" />
                  <span className="h-2 w-2 animate-pulse rounded-full bg-accent [animation-delay:300ms]" />
                  {iris.streamingStatus && (
                    <span className="ml-2 text-[10px] text-muted italic">
                      {iris.streamingStatus}
                    </span>
                  )}
                </div>
              </div>
            </div>
          )}
          <div ref={bottomRef} />
        </div>
      </div>

      <div className="mt-3 flex gap-2">
        <textarea
          className="flex-1 resize-none rounded-lg border border-border-dim bg-surface-raised px-4 py-2.5 text-[13px] text-zinc-100 placeholder:text-muted focus:border-accent focus:outline-none"
          rows={2}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKey}
          placeholder="Ask Iris…"
          disabled={streaming}
        />
        <button
          className="flex items-center justify-center rounded-lg bg-accent px-4 text-zinc-950 hover:bg-accent/80 disabled:opacity-50"
          onClick={() => send(input)}
          disabled={!input.trim() || streaming}
        >
          <Send size={16} />
        </button>
      </div>
    </div>
  );
}
