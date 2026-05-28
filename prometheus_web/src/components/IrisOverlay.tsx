import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { useNavigate } from "react-router-dom";
import { Brain, Maximize2, Send, Trash2, X } from "lucide-react";
import { useIris } from "../context/IrisContext";
import { streamIris, type StreamEvent } from "../api/irisStream";

/**
 * The Iris UX, ported from apatheon — slide-in right-side drawer that
 * shares state with the full-page `/iris` route via `IrisContext`. The
 * brain FAB at bottom-right opens the drawer; Ctrl/Cmd+I toggles; Esc
 * closes. Streaming uses the existing `streamIris` SSE client.
 *
 * Differences vs apatheon's IrisOverlay: no workflow integration, no
 * action chips / allocation cards / view directives / audit badges,
 * no export menu, no demo mode. Pure conversational Iris.
 */

const SUGGESTED_PROMPTS = [
  "Summarize my portfolio risk right now",
  "Any drift between live and backtest?",
  "What are today's autopilot notifications?",
  "Which strategies got flagged in diagnostics this week?",
  "What's the current regime?",
];

function formatToolName(name: string): string {
  return name
    .replace(/^get_/, "")
    .replace(/^prometheus_/, "")
    .replace(/^cassandra_/, "")
    .replace(/_/g, " ");
}

function relativeTime(ts: number): string {
  const diff = Date.now() - ts;
  if (diff < 60_000) return "just now";
  const m = Math.floor(diff / 60_000);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h`;
  const d = Math.floor(h / 24);
  return `${d}d`;
}

export default function IrisOverlay() {
  const iris = useIris();
  const navigate = useNavigate();
  const [input, setInput] = useState("");
  const [streaming, setStreaming] = useState(false);
  const abortRef = useRef<AbortController | null>(null);
  const bottomRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const inputContainerRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom on new message
  useEffect(() => {
    if (iris.isOpen) {
      bottomRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [iris.messages, iris.streamingStatus, iris.isOpen]);

  // Focus input when drawer opens
  useEffect(() => {
    if (iris.isOpen) {
      setTimeout(() => textareaRef.current?.focus(), 100);
    }
  }, [iris.isOpen]);

  // Auto-grow textarea
  useLayoutEffect(() => {
    const ta = textareaRef.current;
    if (!ta) return;
    ta.style.height = "auto";
    ta.style.height = `${Math.min(ta.scrollHeight, 240)}px`;
  }, [input]);

  // Consume pending "Ask about" payloads — prefill the input
  useEffect(() => {
    if (!iris.isOpen) return;
    const ask = iris.consumeAsk();
    if (ask) {
      setInput(ask.question ?? "");
    }
  }, [iris.isOpen, iris.consumeAsk]);

  // Keyboard shortcuts: Ctrl/Cmd+I toggle, Esc close
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      // Cmd/Ctrl+I — toggle drawer
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "i") {
        e.preventDefault();
        iris.toggle();
      }
      // Esc — close drawer (only when open and not focused on input)
      if (e.key === "Escape" && iris.isOpen) {
        iris.close();
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [iris]);

  // Cleanup any in-flight stream on unmount
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
          role: "assistant",
          content: `Transport error: ${String(err)}`,
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

  const handleFab = () => {
    if (window.innerWidth < 768) {
      // Mobile: navigate to full page
      navigate("/iris");
    } else {
      iris.open();
    }
  };

  const fab = !iris.isOpen ? (
    <button
      onClick={handleFab}
      className="fixed bottom-6 right-6 z-50 flex h-12 w-12 items-center justify-center rounded-full bg-accent text-zinc-950 shadow-lg shadow-accent/20 transition-all hover:scale-105 hover:shadow-xl hover:shadow-accent/30"
      title="Open Iris (Ctrl+I)"
      aria-label="Open Iris"
    >
      <Brain size={22} />
    </button>
  ) : null;

  return (
    <>
      {fab}

      {/* Mobile backdrop */}
      <div
        className={`fixed inset-0 z-[60] bg-black/40 transition-opacity duration-300 md:hidden ${
          iris.isOpen ? "opacity-100" : "pointer-events-none opacity-0"
        }`}
        onClick={() => iris.close()}
        aria-hidden="true"
      />

      {/* Drawer */}
      <div
        className={`fixed right-0 top-0 z-[70] flex h-full w-[380px] max-w-[90vw] flex-col border-l border-border-dim bg-surface-raised shadow-2xl shadow-black/60 transition-transform duration-300 ease-in-out ${
          iris.isOpen ? "translate-x-0" : "translate-x-full"
        }`}
        aria-hidden={!iris.isOpen}
      >
        {/* Header */}
        <div className="flex items-center justify-between border-b border-border-dim bg-surface-raised px-4 py-3">
          <div className="flex items-center gap-2">
            <Brain size={18} className="text-accent" />
            <span className="text-sm font-bold text-zinc-100">Iris</span>
            <span className="rounded bg-surface-overlay px-1.5 py-0.5 text-[10px] text-muted">
              Prometheus
            </span>
          </div>
          <div className="flex items-center gap-1">
            {iris.messages.length > 0 && (
              <button
                onClick={iris.clearMessages}
                className="rounded p-1.5 text-muted hover:bg-surface-overlay hover:text-zinc-100 transition-colors"
                title="Clear conversation"
              >
                <Trash2 size={13} />
              </button>
            )}
            <button
              onClick={() => { iris.close(); navigate("/iris"); }}
              className="rounded p-1.5 text-muted hover:bg-surface-overlay hover:text-zinc-100 transition-colors"
              title="Open full page"
            >
              <Maximize2 size={13} />
            </button>
            <button
              onClick={() => iris.close()}
              className="rounded p-1.5 text-muted hover:bg-surface-overlay hover:text-zinc-100 transition-colors"
              title="Close (Esc)"
            >
              <X size={14} />
            </button>
          </div>
        </div>

        {/* Messages */}
        <div className="flex-1 overflow-y-auto p-3">
          {iris.messages.length === 0 && (
            <div className="flex h-full flex-col items-center justify-center">
              <Brain size={32} className="text-accent mb-3" />
              <div className="text-sm font-bold text-accent">Iris</div>
              <p className="mt-1.5 text-center text-[11px] text-muted px-4">
                Ask about your portfolio, drift, notifications, geopolitical
                risk, or anything else. Cross-project tools available.
              </p>
              <div className="mt-4 flex flex-col gap-1.5 px-3 w-full">
                {SUGGESTED_PROMPTS.map((q) => (
                  <button
                    key={q}
                    onClick={() => setInput(q)}
                    className="rounded border border-border-dim bg-surface-overlay/40 px-3 py-1.5 text-left text-[11px] text-muted hover:border-accent hover:text-zinc-100 transition-colors"
                  >
                    {q}
                  </button>
                ))}
              </div>
              <div className="mt-4 text-[9px] text-muted/60">
                <kbd className="rounded border border-border-dim bg-surface px-1 py-px text-[9px]">Ctrl</kbd> +{" "}
                <kbd className="rounded border border-border-dim bg-surface px-1 py-px text-[9px]">I</kbd>
                {" "}to toggle ·{" "}
                <kbd className="rounded border border-border-dim bg-surface px-1 py-px text-[9px]">Esc</kbd>
                {" "}to close
              </div>
            </div>
          )}

          <div className="space-y-3">
            {iris.messages.map((m, i) => (
              <div
                key={i}
                className={`flex ${m.role === "user" ? "justify-end" : "justify-start"}`}
              >
                <div
                  className={`max-w-[88%] rounded-lg px-3 py-2 text-[13px] leading-relaxed ${
                    m.role === "user"
                      ? "bg-accent/20 text-zinc-100"
                      : "bg-surface-overlay text-zinc-300"
                  }`}
                >
                  {m.role === "assistant" && (
                    <div className="mb-1 flex items-center justify-between gap-2">
                      <span className="text-[9px] font-semibold uppercase tracking-wider text-accent">
                        Iris
                      </span>
                      {m.tools && m.tools.length > 0 && (
                        <span className="text-[9px] text-muted/80 truncate">
                          {m.tools.map(formatToolName).join(" · ")}
                        </span>
                      )}
                    </div>
                  )}
                  <div className="whitespace-pre-wrap break-words">{m.content}</div>
                  <div className="mt-1 text-[9px] text-muted/70">
                    {relativeTime(m.timestamp)}
                  </div>
                </div>
              </div>
            ))}

            {streaming && (
              <div className="flex justify-start">
                <div className="rounded-lg bg-surface-overlay px-3 py-2">
                  <div className="mb-1 text-[9px] font-semibold uppercase tracking-wider text-accent">
                    Iris
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-accent" />
                    <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-accent [animation-delay:150ms]" />
                    <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-accent [animation-delay:300ms]" />
                    {iris.streamingStatus && (
                      <span className="ml-1 text-[10px] text-muted italic">
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

        {/* Input */}
        <div ref={inputContainerRef} className="border-t border-border-dim bg-surface-raised p-2.5">
          <div className="flex items-end gap-2">
            <textarea
              ref={textareaRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKey}
              placeholder="Ask Iris…"
              rows={1}
              disabled={streaming}
              className="flex-1 resize-none rounded-lg border border-border-dim bg-surface px-3 py-2 text-[13px] text-zinc-100 placeholder:text-muted focus:border-accent focus:outline-none disabled:opacity-50"
              style={{ maxHeight: "240px" }}
            />
            <button
              onClick={() => send(input)}
              disabled={!input.trim() || streaming}
              className="flex h-9 w-9 items-center justify-center rounded-lg bg-accent text-zinc-950 hover:bg-accent/80 disabled:cursor-not-allowed disabled:opacity-50 transition-colors"
              title="Send (Enter)"
              aria-label="Send"
            >
              <Send size={14} />
            </button>
          </div>
        </div>
      </div>
    </>
  );
}
