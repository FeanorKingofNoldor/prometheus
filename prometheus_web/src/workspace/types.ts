export type Evidence = {
  title: string;
  url: string;
  source: string;
  quote?: string;
  published_at: string;
};
export type Event = {
  title: string;
  predicate: string;
  entity: string[];
  evidence: Evidence[];
  evidence_state: { label: string; sources: number };
  mapping_reason: string;
};
export type Plan = {
  side: number;
  quantity: number;
  entry_limit: number;
  stop: number;
  target: number;
  hold_hours: number;
  estimated_round_trip_cost: number;
  target_payoff_after_estimated_cost: number;
};
export type Candidate = {
  candidate_id: string;
  event_id: string;
  instrument_id: string;
  strategy: string;
  state: string;
  reason: string;
  created_at: string;
  event: Event;
  payload: {
    company: string;
    sign: number;
    strength: number;
    path: { from: string[]; to: string[]; relation: string }[];
    plan?: Plan;
    proposed_plan?: Plan;
    net_pnl?: number;
    entry_vwap?: number;
    remaining_quantity?: number;
    last_mark?: {
      return: number;
      excess_return: number;
      elapsed_hours: number;
      quote: { timing: string; observed_at: string };
    };
  };
};
export type Desk = {
  candidates: Candidate[];
  events: { event_id: string; payload: Event; first_seen: string }[];
  last_run: {
    completed_at: string;
    payload: {
      status: string;
      market_state: string;
      error?: string;
      policy: {
        book_usd: number;
        max_gross: number;
        max_position: number;
        max_event?: number;
        max_positions?: number;
        drawdown_limit?: number;
        risk_per_trade?: number;
      };
      drawdown_halt?: boolean;
      unresolved_orders?: boolean;
      paper_entries_enabled?: boolean;
    };
  } | null;
  research_outcomes: {
    strategy: string;
    horizon_hours: number;
    observations: number;
    event_groups: number;
    mean_signed_return: number;
    mean_signed_excess_return: number;
  }[];
  paper_results: {
    strategy: string;
    closed_trades: number;
    event_groups: number;
    pnl_after_recorded_fees_and_estimated_borrow: number;
  }[];
  measurement_note: string;
};
