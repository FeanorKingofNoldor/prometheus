import { createContext, useContext, useState, useMemo, type ReactNode } from "react";
import { usePortfolios } from "../api/hooks";

export interface PortfolioSummary {
  portfolio_id: string;
  mode: string;
  latest_date: string | null;
  num_positions: number;
  total_market_value: number;
}

interface PortfolioContextValue {
  /** Currently active portfolio_id across all pages */
  activePortfolioId: string;
  setActivePortfolioId: (id: string) => void;
  /** All available portfolios from the backend */
  portfolios: PortfolioSummary[];
  /** Live / Paper trading portfolios */
  tradingPortfolios: PortfolioSummary[];
  /** Backtest portfolios */
  backtestPortfolios: PortfolioSummary[];
  /** Mode of the currently selected portfolio (PAPER / LIVE / BACKTEST) */
  activeMode: string;
  isLoading: boolean;
}

const PortfolioContext = createContext<PortfolioContextValue | null>(null);

const DEFAULT_PORTFOLIO_ID = "IBKR_PAPER";

export function PortfolioProvider({ children }: { children: ReactNode }) {
  const [selectedId, setSelectedId] = useState<string>(DEFAULT_PORTFOLIO_ID);
  const { data: raw, isLoading } = usePortfolios();
  const portfolios = ((raw as { portfolios?: PortfolioSummary[] } | undefined)?.portfolios ?? []) as PortfolioSummary[];

  const { tradingPortfolios, backtestPortfolios } = useMemo(() => {
    const trading: PortfolioSummary[] = [];
    const backtest: PortfolioSummary[] = [];
    for (const p of portfolios) {
      if (p.mode === "PAPER" || p.mode === "LIVE") {
        trading.push(p);
      } else {
        backtest.push(p);
      }
    }
    // Sort trading: LIVE first, then PAPER
    trading.sort((a, b) => (a.mode === "LIVE" ? -1 : 1) - (b.mode === "LIVE" ? -1 : 1));
    return { tradingPortfolios: trading, backtestPortfolios: backtest };
  }, [portfolios]);

  const activePortfolioId = selectedId;
  const activeMode = portfolios.find((p) => p.portfolio_id === activePortfolioId)?.mode ?? "BACKTEST";

  const value = useMemo<PortfolioContextValue>(
    () => ({
      activePortfolioId,
      setActivePortfolioId: setSelectedId,
      portfolios,
      tradingPortfolios,
      backtestPortfolios,
      activeMode,
      isLoading,
    }),
    [activePortfolioId, portfolios, tradingPortfolios, backtestPortfolios, activeMode, isLoading],
  );

  return <PortfolioContext.Provider value={value}>{children}</PortfolioContext.Provider>;
}

export function usePortfolioContext(): PortfolioContextValue {
  const ctx = useContext(PortfolioContext);
  if (!ctx) throw new Error("usePortfolioContext must be used inside <PortfolioProvider>");
  return ctx;
}
