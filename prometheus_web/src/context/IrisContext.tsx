import {
  createContext,
  useCallback,
  useContext,
  useState,
  type ReactNode,
} from "react";

/**
 * Iris chat state — shared between the slide-in overlay and the full-page
 * `/iris` route so expanding the panel into the page keeps history intact.
 *
 * NOT persisted across browser reloads (mirrors apatheon's behavior).
 * Messages live in React state only; the LLM context window is what
 * matters, not lifetime audit trail.
 */

export interface IrisChatMessage {
  role: "user" | "assistant";
  content: string;
  timestamp: number;
  /** Tools Iris invoked while answering. */
  tools?: string[];
}

export interface AskIrisPayload {
  /** Pre-filled question the user can edit before sending. */
  question?: string;
  /** Structured context describing what the user selected. */
  context: string;
  /** Component that triggered the ask, for debugging. */
  source: string;
}

interface IrisContextValue {
  isOpen: boolean;
  open: () => void;
  close: () => void;
  toggle: () => void;

  messages: IrisChatMessage[];
  appendMessage: (m: IrisChatMessage) => void;
  clearMessages: () => void;

  streamingStatus: string | null;
  setStreamingStatus: (s: string | null) => void;

  /** "Ask Iris about this" — buttons on panels call askAbout() to open
      Iris with a pre-filled prompt. The overlay consumes this on mount. */
  pendingAsk: AskIrisPayload | null;
  askAbout: (payload: AskIrisPayload) => void;
  consumeAsk: () => AskIrisPayload | null;
}

const IrisCtx = createContext<IrisContextValue | null>(null);

export function useIris(): IrisContextValue {
  const ctx = useContext(IrisCtx);
  if (!ctx) throw new Error("useIris must be used inside IrisProvider");
  return ctx;
}

export function useOptionalIris(): IrisContextValue | null {
  return useContext(IrisCtx);
}

export function IrisProvider({ children }: { children: ReactNode }) {
  const [isOpen, setIsOpen] = useState(false);
  const [messages, setMessages] = useState<IrisChatMessage[]>([]);
  const [streamingStatus, setStreamingStatus] = useState<string | null>(null);
  const [pendingAsk, setPendingAsk] = useState<AskIrisPayload | null>(null);

  const open = useCallback(() => setIsOpen(true), []);
  const close = useCallback(() => setIsOpen(false), []);
  const toggle = useCallback(() => setIsOpen((v) => !v), []);

  const appendMessage = useCallback((m: IrisChatMessage) => {
    setMessages((prev) => [...prev, m]);
  }, []);

  const clearMessages = useCallback(() => {
    setMessages([]);
    setStreamingStatus(null);
  }, []);

  const askAbout = useCallback((payload: AskIrisPayload) => {
    setPendingAsk(payload);
    setIsOpen(true);
  }, []);

  const consumeAsk = useCallback(() => {
    const ask = pendingAsk;
    if (ask) setPendingAsk(null);
    return ask;
  }, [pendingAsk]);

  return (
    <IrisCtx.Provider
      value={{
        isOpen, open, close, toggle,
        messages, appendMessage, clearMessages,
        streamingStatus, setStreamingStatus,
        pendingAsk, askAbout, consumeAsk,
      }}
    >
      {children}
    </IrisCtx.Provider>
  );
}
