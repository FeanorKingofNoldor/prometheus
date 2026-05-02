"""Backfill ROE into fundamental_ratios from IS/BS statements.

Uses netIncome / totalStockholderEquity per issuer/period_end and upserts
into fundamental_ratios (roe column and metrics->'ROE').

Usage:
  PYTHONPATH=cpp/build ./venv/bin/python -m prometheus.scripts.ingest.backfill_roe --market-id US_EQ
"""

from __future__ import annotations

import argparse
from typing import Iterable

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger

logger = get_logger(__name__)


UPSERT_SQL = """
WITH derived AS (
  SELECT
    bs.issuer_id,
    COALESCE(
      bs.period_start,
      CASE
        WHEN NULLIF(bs.metadata->>'frequency', '') = 'QUARTERLY' THEN (bs.period_end - INTERVAL '90 days')::date
        ELSE (bs.period_end - INTERVAL '365 days')::date
      END
    ) AS period_start,
    bs.period_end,
    COALESCE(NULLIF(bs.metadata->>'frequency', ''), 'ANNUAL') AS frequency,
    COALESCE(
      (isf.values->>'netIncome')::double precision,
      (isf.values->>'NetIncome')::double precision,
      (isf.values->>'netIncomeApplicableToCommonShares')::double precision,
      (isf.values->>'netIncomeLoss')::double precision,
      (isf.values->>'netIncomeLossAvailableToCommonShareholdersBasic')::double precision
    ) AS net_income,
    COALESCE(
      (bs.values->>'totalStockholderEquity')::double precision,
      (bs.values->>'totalStockholdersEquity')::double precision,
      (bs.values->>'TotalStockholderEquity')::double precision,
      (bs.values->>'TotalStockholdersEquity')::double precision,
      (bs.values->>'totalEquity')::double precision,
      (bs.values->>'totalEquityGrossMinorityInterest')::double precision
    ) AS equity
  FROM financial_statements bs
  JOIN financial_statements isf
    ON bs.issuer_id = isf.issuer_id AND bs.period_end = isf.period_end
  WHERE bs.statement_type = 'BS'
    AND isf.statement_type = 'IS'
    AND bs.period_end IS NOT NULL
    AND bs.issuer_id = ANY(%s)
),
computed AS (
  SELECT issuer_id, period_start, period_end, frequency,
         net_income / NULLIF(equity, 0) AS roe
  FROM derived
  WHERE net_income IS NOT NULL
    AND equity IS NOT NULL
)
INSERT INTO fundamental_ratios (
    issuer_id, period_start, period_end, frequency,
    roe, roic, gross_margin, op_margin, net_margin, leverage,
    interest_coverage, revenue_growth, eps_growth, metrics, metadata
)
SELECT c.issuer_id, c.period_start, c.period_end, c.frequency,
       c.roe, NULL, NULL, NULL, NULL, NULL,
       NULL, NULL, NULL,
       jsonb_build_object('ROE', c.roe), jsonb_build_object('source', 'backfill_roe')
FROM computed c
WHERE c.roe IS NOT NULL AND c.roe NOT IN ('Infinity'::float, '-Infinity'::float, 'NaN'::float)
  AND abs(c.roe) < 2.0
ON CONFLICT (issuer_id, period_start, period_end, frequency)
DO UPDATE SET
  roe = EXCLUDED.roe,
  metrics = COALESCE(fundamental_ratios.metrics, '{}'::jsonb) || jsonb_build_object('ROE', EXCLUDED.roe),
  metadata = COALESCE(fundamental_ratios.metadata, '{}'::jsonb) || jsonb_build_object('source', 'backfill_roe')
"""


def chunked(iterable: Iterable[str], size: int = 500):
    it = iter(iterable)
    while True:
        buf = []
        try:
            for _ in range(size):
                buf.append(next(it))
        except StopIteration:
            if buf:
                yield buf
            break
        yield buf


def main():
    parser = argparse.ArgumentParser(description="Backfill ROE into fundamental_ratios")
    parser.add_argument("--market-id", default="US_EQ", help="Market id to scope issuers (default: US_EQ)")
    parser.add_argument("--batch", type=int, default=500, help="Batch size for upserts (default: 500)")
    args = parser.parse_args()

    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT issuer_id FROM instruments WHERE market_id=%s AND status='ACTIVE' AND issuer_id IS NOT NULL",
            (args.market_id,),
        )
        issuer_ids = [row[0] for row in cur.fetchall()]
        cur.close()

    logger.info("Found %d issuers in market %s", len(issuer_ids), args.market_id)

    total = 0
    with db.get_historical_connection() as conn:
        cur = conn.cursor()
        for batch in chunked(issuer_ids, args.batch):
            cur.execute(UPSERT_SQL, (batch,))
            total += cur.rowcount
            conn.commit()
        cur.close()

    logger.info("Upserted ROE rows (affected): %d", total)


if __name__ == "__main__":  # pragma: no cover
    main()
