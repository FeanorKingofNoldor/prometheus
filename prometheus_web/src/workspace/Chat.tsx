import { useEffect, useRef, useState } from "react";
import { Send, Square, Trash2, Sparkles } from "lucide-react";
import { streamIris } from "../api/irisStream";
import { Heading, words } from "./ui";
type Message = { role: "user" | "assistant"; content: string };
export default function Chat() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState("");
  const [error, setError] = useState("");
  const abort = useRef<AbortController | null>(null);
  const mounted = useRef(true);
  const end = useRef<HTMLDivElement>(null);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      abort.current?.abort();
    };
  }, []);
  useEffect(() => {
    end.current?.scrollIntoView({ block: "nearest" });
  }, [messages, status]);
  async function send(text: string) {
    if (!text.trim() || busy) return;
    const question = text.trim();
    const history = messages.slice(-10);
    setMessages((m) => [...m, { role: "user", content: question }]);
    setInput("");
    setError("");
    setBusy(true);
    setStatus("Reading the workspace…");
    const controller = new AbortController();
    abort.current = controller;
    let finished = false;
    try {
      await streamIris({
        question,
        history,
        signal: controller.signal,
        onEvent: (e) => {
          if (!mounted.current) return;
          if (e.type === "thinking") setStatus("Assessing the question…");
          if (e.type === "tool_call_start")
            setStatus(`Checking ${words(e.name)}…`);
          if (e.type === "tool_call_result") setStatus("Reading the results…");
          if (e.type === "error") {
            finished = true;
            setError(
              e.message.includes("401") || e.message.includes("422")
                ? "Operator access is required. Configure your key in Method & access."
                : e.message,
            );
          }
          if (e.type === "done") {
            finished = true;
            const r = e.response;
            const content = String(
              r.answer ??
                r.response ??
                r.message ??
                r.content ??
                r.text ??
                "No answer was returned.",
            );
            setMessages((m) => [...m, { role: "assistant", content }]);
          }
        },
      });
      if (!finished && !controller.signal.aborted && mounted.current)
        setError("The response ended before an answer arrived. Try again.");
    } catch (e) {
      if (mounted.current && !controller.signal.aborted)
        setError(e instanceof Error ? e.message : "The request failed.");
    } finally {
      if (mounted.current) {
        setBusy(false);
        setStatus("");
      }
    }
  }
  return (
    <>
      <Heading
        title="Iris research assistant"
        kicker="Workspace / Intelligence"
        subtitle="Ask about the current evidence, decisions and results."
      />
      <section className="panel">
        <div className="panel-head">
          <h2>Research conversation</h2>
          <button
            className="button ghost"
            disabled={busy || !messages.length}
            onClick={() => {
              setMessages([]);
              setError("");
            }}
          >
            <Trash2 size={14} />
            Clear
          </button>
        </div>
        <div className="chat-history" aria-live="polite">
          {!messages.length ? (
            <div className="empty">
              <Sparkles size={30} />
              <h3>Start with the evidence.</h3>
              <p>
                Ask Iris to explain a decision, inspect a signal or summarize
                the forward record.
              </p>
              <div className="chat-suggestions">
                {[
                  "What is the system doing right now?",
                  "What evidence supports the current signals?",
                  "What can we conclude from the verified results?",
                ].map((p) => (
                  <button
                    className="button ghost"
                    key={p}
                    onClick={() => void send(p)}
                  >
                    {p}
                  </button>
                ))}
              </div>
            </div>
          ) : (
            messages.map((m, i) => (
              <article className={`chat-message ${m.role}`} key={i}>
                <div className="eyebrow">
                  {m.role === "user" ? "You" : "Iris"}
                </div>
                <p>{m.content}</p>
              </article>
            ))
          )}
          {busy && (
            <div className="small muted" role="status">
              {status}
            </div>
          )}
          <div ref={end} />
        </div>
        {error && (
          <div className="notice error" style={{ margin: 20 }} role="alert">
            {error}
          </div>
        )}
        <form
          className="chat-compose"
          onSubmit={(e) => {
            e.preventDefault();
            void send(input);
          }}
        >
          <textarea
            aria-label="Message to Iris"
            value={input}
            disabled={busy}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask about evidence, decisions or outcomes…"
            rows={2}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                void send(input);
              }
            }}
          />
          {busy ? (
            <button
              className="button"
              type="button"
              onClick={() => abort.current?.abort()}
            >
              <Square size={15} />
              Stop
            </button>
          ) : (
            <button
              className="button primary"
              disabled={!input.trim()}
              type="submit"
            >
              <Send size={15} />
              Send
            </button>
          )}
        </form>
      </section>
      <p className="page-footer">
        Conversations stay in this view and clear when you leave. Check the
        underlying records before treating an answer as a measured result.
      </p>
    </>
  );
}
