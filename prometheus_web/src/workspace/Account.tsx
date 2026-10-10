import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import {
  Badge,
  Empty,
  Heading,
  Metric,
  money,
  num,
  Panel,
  QueryState,
} from "./ui";
type Account = {
  portfolio_id: string;
  mode: string;
  as_of_date: string | null;
  net_liquidation_value: number | null;
  total_cash: number | null;
  positions: {
    instrument_id: string;
    quantity: number;
    avg_cost: number;
    market_value: number;
    unrealized_pnl: number;
    side: string;
  }[];
};
export default function Account() {
  const q = useQuery({
    queryKey: ["workspace", "account"],
    queryFn: () =>
      api.get<Account>("/status/portfolio?portfolio_id=IBKR_PAPER"),
    refetchInterval: 60000,
  });
  const a = q.data;
  return (
    <>
      <Heading
        title="Broker account"
        kicker="Prometheus / Account"
        subtitle="The last persisted IBKR paper account snapshot, including holdings outside the event book."
        refresh={() => void q.refetch()}
        busy={q.isFetching}
      />
      <QueryState query={q} />
      {a && (
        <>
          <div className="notice">
            <p>
              Snapshot date: <strong>{a.as_of_date ?? "unknown"}</strong>. These
              are stored account values, not a live broker query. Refresh
              reloads the stored snapshot. Event-book results are measured
              separately.
            </p>
          </div>
          <div className="metrics">
            <Metric
              label="Net liquidation value"
              value={money(a.net_liquidation_value)}
              note={`As of ${a.as_of_date ?? "unknown"}`}
            />
            <Metric
              label="Cash"
              value={money(a.total_cash)}
              note="Stored account snapshot"
            />
            <Metric
              label="Holdings"
              value={num(a.positions.length)}
              note="Includes existing unrelated positions"
            />
            <Metric label="Account mode" value={a.mode} note={a.portfolio_id} />
          </div>
          <Panel
            title="Recorded holdings"
            action={<Badge>Snapshot · {a.as_of_date ?? "unknown"}</Badge>}
          >
            {a.positions.length ? (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Instrument</th>
                      <th>Side</th>
                      <th>Quantity</th>
                      <th>Average cost</th>
                      <th>Market value</th>
                      <th>Unrealized P&L</th>
                    </tr>
                  </thead>
                  <tbody>
                    {a.positions.map((p) => (
                      <tr key={p.instrument_id}>
                        <td className="cell-title">{p.instrument_id}</td>
                        <td>{p.side}</td>
                        <td className="mono">{num(p.quantity)}</td>
                        <td className="mono">{money(p.avg_cost)}</td>
                        <td className="mono">{money(p.market_value)}</td>
                        <td
                          className={`mono ${p.unrealized_pnl < 0 ? "negative" : "positive"}`}
                        >
                          {money(p.unrealized_pnl)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <Empty title="No holdings in this snapshot">
                This describes the stored snapshot only.
              </Empty>
            )}
          </Panel>
        </>
      )}
    </>
  );
}
