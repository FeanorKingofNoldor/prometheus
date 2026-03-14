import { useState, useRef, useEffect } from "react";
import { Send } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { useKronosChat } from "../api/hooks";

interface Message {
  role: "user" | "assistant";
  content: string;
  timestamp: number;
}

export default function Chat() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const chat = useKronosChat();
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const handleSend = () => {
    const text = input.trim();
    if (!text || chat.isPending) return;

    const userMsg: Message = { role: "user", content: text, timestamp: Date.now() };
    setMessages((prev) => [...prev, userMsg]);
    setInput("");

    chat.mutate(
      { question: text, context: { history: messages.slice(-10).map((m) => ({ role: m.role, content: m.content })) } },
      {
        onSuccess: (data) => {
          const d = data as Record<string, unknown>;
          const reply = String(d.answer ?? d.response ?? d.message ?? d.content ?? d.text ?? "No response");
          setMessages((prev) => [...prev, { role: "assistant", content: reply, timestamp: Date.now() }]);
        },
        onError: (err) => {
          setMessages((prev) => [...prev, { role: "assistant", content: `Error: ${String(err)}`, timestamp: Date.now() }]);
        },
      },
    );
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div className="flex h-full flex-col">
      <PageHeader title="Kronos" subtitle="AI assistant" />

      {/* Messages */}
      <div className="flex-1 overflow-y-auto rounded-lg border border-border-dim bg-surface-raised p-4">
        {messages.length === 0 && (
          <div className="flex h-full items-center justify-center">
            <div className="text-center">
              <div className="text-2xl font-bold text-accent">Kronos</div>
              <p className="mt-2 text-xs text-muted">
                Ask about portfolio risk, strategy performance, market regimes, or anything else.
              </p>
              <div className="mt-4 flex flex-wrap justify-center gap-2">
                {[
                  "Summarize today's portfolio risk",
                  "What regime are we in?",
                  "Explain recent execution decisions",
                  "Diagnose strategy momentum_alpha",
                ].map((q) => (
                  <button
                    key={q}
                    className="rounded border border-border-dim bg-surface-overlay px-3 py-1.5 text-xs text-muted hover:border-accent hover:text-zinc-100"
                    onClick={() => { setInput(q); }}
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
                  <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-accent">
                    Kronos
                  </div>
                )}
                <div className="whitespace-pre-wrap">{msg.content}</div>
                <div className="mt-1 text-[10px] text-muted">
                  {new Date(msg.timestamp).toLocaleTimeString()}
                </div>
              </div>
            </div>
          ))}

          {chat.isPending && (
            <div className="flex justify-start">
              <div className="rounded-lg bg-surface-overlay px-4 py-2.5">
                <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-accent">Kronos</div>
                <div className="flex gap-1">
                  <span className="h-2 w-2 animate-pulse rounded-full bg-accent" />
                  <span className="h-2 w-2 animate-pulse rounded-full bg-accent [animation-delay:150ms]" />
                  <span className="h-2 w-2 animate-pulse rounded-full bg-accent [animation-delay:300ms]" />
                </div>
              </div>
            </div>
          )}
          <div ref={bottomRef} />
        </div>
      </div>

      {/* Input */}
      <div className="mt-3 flex gap-2">
        <textarea
          className="flex-1 resize-none rounded-lg border border-border-dim bg-surface-raised px-4 py-2.5 text-xs text-zinc-100 placeholder:text-muted focus:border-accent focus:outline-none"
          rows={2}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask Kronos..."
          disabled={chat.isPending}
        />
        <button
          className="flex items-center justify-center rounded-lg bg-accent px-4 text-zinc-950 hover:bg-accent/80 disabled:opacity-50"
          onClick={handleSend}
          disabled={!input.trim() || chat.isPending}
        >
          <Send size={16} />
        </button>
      </div>
    </div>
  );
}
