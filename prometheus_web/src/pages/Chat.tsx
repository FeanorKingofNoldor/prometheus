import { useState, useRef, useEffect } from "react";
import { Send } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { streamIris, type StreamEvent } from "../api/irisStream";

interface Message {
  role: "user" | "assistant";
  content: string;
  timestamp: number;
  /** When streaming, this message is being assembled. */
  pending?: boolean;
  /** Tool calls Iris made while answering this message. */
  tools?: string[];
}

function formatToolName(name: string): string {
  // Strip noisy prefixes for display ("get_prometheus_drift" → "drift")
  return name
    .replace(/^get_/, "")
    .replace(/^prometheus_/, "")
    .replace(/^cassandra_/, "")
    .replace(/_/g, " ");
}

export default function Chat() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [streaming, setStreaming] = useState(false);
  const [statusLine, setStatusLine] = useState<string>("");
  const bottomRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, statusLine]);

  useEffect(() => () => abortRef.current?.abort(), []);

  const handleSend = async () => {
    const text = input.trim();
    if (!text || streaming) return;

    const now = Date.now();
    const userMsg: Message = { role: "user", content: text, timestamp: now };
    setMessages((prev) => [...prev, userMsg]);
    setInput("");
    setStreaming(true);
    setStatusLine("thinking…");

    const history = messages
      .slice(-10)
      .map((m) => ({ role: m.role, content: m.content }));

    const tools: string[] = [];
    abortRef.current?.abort();
    abortRef.current = new AbortController();

    try {
      await streamIris({
        question: text,
        history,
        signal: abortRef.current.signal,
        onEvent: (e: StreamEvent) => {
          if (e.type === "thinking") {
            setStatusLine("thinking…");
          } else if (e.type === "tool_call_start") {
            tools.push(e.name);
            setStatusLine(`using ${formatToolName(e.name)}…`);
          } else if (e.type === "tool_call_result") {
            setStatusLine(`got ${formatToolName(e.name)} result`);
          } else if (e.type === "done") {
            const r = e.response as Record<string, unknown>;
            const answer = String(
              r.answer ?? r.response ?? r.message ?? r.content ?? r.text ?? "(no response)",
            );
            setMessages((prev) => [
              ...prev,
              {
                role: "assistant",
                content: answer,
                timestamp: Date.now(),
                tools: tools.length ? [...tools] : undefined,
              },
            ]);
            setStatusLine("");
          } else if (e.type === "error") {
            setMessages((prev) => [
              ...prev,
              {
                role: "assistant",
                content: `Iris error: ${e.message}`,
                timestamp: Date.now(),
              },
            ]);
            setStatusLine("");
          }
        },
      });
    } catch (err) {
      // AbortError on unmount/replace — silent
      const name = err instanceof Error ? err.name : "";
      if (name !== "AbortError") {
        setMessages((prev) => [
          ...prev,
          {
            role: "assistant",
            content: `Transport error: ${String(err)}`,
            timestamp: Date.now(),
          },
        ]);
      }
      setStatusLine("");
    } finally {
      setStreaming(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const suggestedPrompts = [
    "Summarize my portfolio risk right now",
    "Any drift between live and backtest?",
    "What are today's autopilot notifications?",
    "Which strategies got flagged in diagnostics this week?",
    "What's the current regime?",
  ];

  return (
    <div className="flex h-full flex-col">
      <PageHeader
        title="Iris"
        subtitle="Cross-project assistant — reads your portfolio, decisions, drift, notifications, geopolitical state."
      />

      <div className="flex-1 overflow-y-auto rounded-lg border border-border-dim bg-surface-raised p-4">
        {messages.length === 0 && (
          <div className="flex h-full items-center justify-center">
            <div className="text-center">
              <div className="text-2xl font-bold text-accent">Iris</div>
              <p className="mt-2 text-xs text-muted">
                Ask about your trading state, geopolitical risk, or any analytical question.
              </p>
              <div className="mt-4 flex flex-wrap justify-center gap-2">
                {suggestedPrompts.map((q) => (
                  <button
                    key={q}
                    className="rounded border border-border-dim bg-surface-overlay px-3 py-1.5 text-xs text-muted hover:border-accent hover:text-zinc-100"
                    onClick={() => setInput(q)}
                  >
                    {q}
                  </button>
                ))}
              </div>
            </div>
          </div>
        )}

        <div className="space-y-4">
          {messages.map((msg, i) => (
            <div
              key={i}
              className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
            >
              <div
                className={`max-w-[75%] rounded-lg px-4 py-2.5 text-xs leading-relaxed ${
                  msg.role === "user"
                    ? "bg-accent/20 text-zinc-100"
                    : "bg-surface-overlay text-zinc-300"
                }`}
              >
                {msg.role === "assistant" && (
                  <div className="mb-1 flex items-center justify-between gap-3">
                    <span className="text-[10px] font-semibold uppercase tracking-wider text-accent">
                      Iris
                    </span>
                    {msg.tools && msg.tools.length > 0 && (
                      <span className="text-[9px] text-muted/80">
                        used: {msg.tools.map(formatToolName).join(" · ")}
                      </span>
                    )}
                  </div>
                )}
                <div className="whitespace-pre-wrap">{msg.content}</div>
                <div className="mt-1 text-[10px] text-muted">
                  {new Date(msg.timestamp).toLocaleTimeString()}
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
                  {statusLine && (
                    <span className="ml-2 text-[10px] text-muted italic">{statusLine}</span>
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
          className="flex-1 resize-none rounded-lg border border-border-dim bg-surface-raised px-4 py-2.5 text-xs text-zinc-100 placeholder:text-muted focus:border-accent focus:outline-none"
          rows={2}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask Iris…"
          disabled={streaming}
        />
        <button
          className="flex items-center justify-center rounded-lg bg-accent px-4 text-zinc-950 hover:bg-accent/80 disabled:opacity-50"
          onClick={handleSend}
          disabled={!input.trim() || streaming}
        >
          <Send size={16} />
        </button>
      </div>
    </div>
  );
}
