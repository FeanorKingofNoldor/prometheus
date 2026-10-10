from __future__ import annotations

from datetime import timedelta

from psycopg2.extras import Json, RealDictCursor

from .model import EVENT_RULES, STRATEGIES, VERSION, evidence_state, exposures, stable_id


class Store:
    def __init__(self, db):
        self.db = db

    def query(self, sql, params=(), write=False):
        with self.db.get_runtime_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = [dict(r) for r in cur.fetchall()] if cur.description else []
            if write:
                conn.commit()
            return rows

    def ingest(self, graph, now):
        from apatheon.graph.company_tickers import get_all_tickers

        claims = self.query(
            """SELECT c.*, n.source,n.title,n.url,n.published_at,n.story_id,n.cluster_key
            FROM structural_claims c JOIN news_articles n ON n.id=c.article_id
            WHERE c.created_at BETWEEN %s AND %s AND n.published_at BETWEEN %s AND %s
              AND n.collected_at <= %s AND c.status <> 'rejected'
              AND c.predicate=ANY(%s) ORDER BY c.created_at,c.claim_id""",
            (now - timedelta(hours=96), now, now - timedelta(hours=72), now, now, list(EVENT_RULES)),
        )
        grouped = {}
        for c in claims:
            if c["effective_date"] and c["effective_date"] < (now - timedelta(days=7)).date():
                continue
            signature = stable_id(
                c["predicate"], c["subject_type"], c["subject_key"], c["object_type"], c["object_key"]
            )
            grouped.setdefault(signature, []).append(c)
        created = 0
        for signature, group in grouped.items():
            previous = self.query(
                "SELECT * FROM event_desk_events WHERE signature=%s AND first_seen>%s ORDER BY first_seen DESC LIMIT 1",
                (signature, now - timedelta(days=7)),
            )
            event_id = previous[0]["event_id"] if previous else stable_id(signature, group[0]["claim_id"])
            first_seen = previous[0]["first_seen"] if previous else now
            c = group[0]
            side, sign = EVENT_RULES[c["predicate"]]
            entity = (c[side + "_type"], c[side + "_key"])
            evidence = [
                {
                    k: (
                        v.isoformat()
                        if hasattr(v, "isoformat")
                        else str(v)
                        if k in ("claim_id", "article_id", "story_id") and v is not None
                        else v
                    )
                    for k, v in row.items()
                }
                for row in group
            ]
            paths = exposures(graph, entity, sign)
            payload = {
                "predicate": c["predicate"],
                "entity": list(entity),
                "title": c["title"],
                "evidence": evidence,
                "evidence_state": evidence_state(group),
                "economic_condition_sign": sign,
                "exposures": paths,
                "mapping_reason": "Mapped explicit economic links"
                if paths
                else "No supported economic path to a company",
            }
            self.query(
                """INSERT INTO event_desk_events VALUES(%s,%s,%s,%s,%s,%s)
                ON CONFLICT(event_id) DO UPDATE SET updated_at=EXCLUDED.updated_at,payload=EXCLUDED.payload""",
                (event_id, signature, first_seen, min(r["published_at"] for r in group), now, Json(payload)),
                True,
            )
            for candidate in self.query(
                "SELECT candidate_id,payload FROM event_desk_candidates WHERE event_id=%s AND state='WATCHING'",
                (event_id,),
            ):
                prior = candidate["payload"]
                current = paths.get(prior["company"])
                if not current or current["sign"] != prior["sign"]:
                    self.query(
                        "UPDATE event_desk_candidates SET state='REJECTED',reason=%s,updated_at=%s WHERE candidate_id=%s",
                        ("Economic mapping no longer supports the original direction", now, candidate["candidate_id"]),
                        True,
                    )
            for company, path in sorted(paths.items(), key=lambda x: -x[1]["strength"])[:10]:
                tickers = [t for t in get_all_tickers(company) if t.endswith(".US")]
                if not tickers:
                    continue
                instrument = tickers[0]
                for strategy in STRATEGIES:
                    cid = stable_id(event_id, strategy, VERSION, instrument)
                    rows = self.query(
                        """INSERT INTO event_desk_candidates VALUES(%s,%s,%s,%s,%s,%s,%s,'WATCHING',%s,%s)
                        ON CONFLICT DO NOTHING RETURNING candidate_id""",
                        (
                            cid,
                            event_id,
                            strategy,
                            VERSION,
                            instrument,
                            now,
                            now,
                            "Waiting for market observations",
                            Json({"company": company, **path, "event_snapshot": payload, "policy_version": VERSION}),
                        ),
                        True,
                    )
                    created += len(rows)
        return {"claims_read": len(claims), "events": len(grouped), "new_candidates": created}

    def candidates(self, active_only=True):
        clause = "WHERE c.state NOT IN ('CLOSED','EXPIRED','NO_FILL','REJECTED')" if active_only else ""
        return self.query(f"""SELECT c.*,e.payload AS event,e.first_seen,e.published_at
            FROM event_desk_candidates c JOIN event_desk_events e USING(event_id) {clause}
            ORDER BY CASE WHEN c.state IN ('OPEN','ENTRY_PENDING','EXIT_PENDING','REVIEW') THEN 0 ELSE 1 END,
            c.created_at DESC,c.candidate_id""")

    def save(self, c, now):
        self.query(
            "UPDATE event_desk_candidates SET state=%s,reason=%s,payload=%s,updated_at=%s WHERE candidate_id=%s",
            (c["state"], c["reason"], Json(c["payload"]), now, c["candidate_id"]),
            True,
        )

    def fills(self, cid):
        return self.query(
            """SELECT f.*,o.metadata->>'event_leg' AS leg FROM fills f JOIN orders o USING(order_id)
            WHERE o.portfolio_id='EVENT_PAPER' AND o.mode='PAPER' AND f.mode='PAPER'
              AND o.metadata->>'candidate_id'=%s ORDER BY f.timestamp,f.fill_id""",
            (cid,),
        )

    def sync_order_states(self, states):
        for oid, status in states.items():
            if oid.startswith("EV_"):
                self.query(
                    "UPDATE orders SET status=%s WHERE order_id=%s AND portfolio_id='EVENT_PAPER' AND mode='PAPER'",
                    (status, oid),
                    True,
                )

    def order_rows(self, cid):
        return self.query(
            "SELECT * FROM orders WHERE portfolio_id='EVENT_PAPER' AND mode='PAPER' AND metadata->>'candidate_id'=%s ORDER BY timestamp",
            (cid,),
        )

    def liquid_instruments(self, instruments, now):
        with self.db.get_historical_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT instrument_id FROM prices_daily
                    WHERE instrument_id=ANY(%s) AND currency='USD'
                      AND trade_date BETWEEN %s AND %s AND close>=5 AND volume>0
                    GROUP BY instrument_id HAVING count(*)>=15 AND avg(close*volume)>=5000000
                      AND max(trade_date)>=%s""",
                    (
                        instruments,
                        (now - timedelta(days=40)).date(),
                        (now - timedelta(days=1)).date(),
                        (now - timedelta(days=5)).date(),
                    ),
                )
                return {r[0] for r in cur.fetchall()}

    def dashboard(self):
        candidates = sorted(
            self.candidates(False),
            key=lambda c: (
                c["state"] in ("OPEN", "ENTRY_PENDING", "EXIT_PENDING", "REVIEW"),
                c["state"] == "WATCHING",
                bool(c["payload"].get("proposed_plan")),
                c["event"]["evidence_state"]["confirmed"],
                c["payload"].get("strength", 0),
                c["created_at"],
            ),
            reverse=True,
        )[:250]
        events = self.query("SELECT * FROM event_desk_events ORDER BY first_seen DESC LIMIT 50")
        runs = self.query("SELECT * FROM event_desk_runs ORDER BY started_at DESC LIMIT 1")
        outcomes = self.query("""WITH groups AS (
            SELECT c.strategy,o.horizon_hours,c.event_id,count(*) AS observations,
            avg((o.payload->>'signed_return')::float) AS signed_return,
            avg((o.payload->>'signed_excess_return')::float) AS signed_excess_return
            FROM event_desk_outcomes o JOIN event_desk_candidates c USING(candidate_id)
            GROUP BY c.strategy,o.horizon_hours,c.event_id)
            SELECT strategy,horizon_hours,sum(observations) AS observations,count(*) AS event_groups,
            avg(signed_return) AS mean_signed_return,avg(signed_excess_return) AS mean_signed_excess_return
            FROM groups GROUP BY strategy,horizon_hours ORDER BY 1,2""")
        trades = self.query("""SELECT strategy,count(*) AS closed_trades,count(DISTINCT event_id) AS event_groups,
            sum((payload->>'net_pnl')::float) AS pnl_after_recorded_fees_and_estimated_borrow
            FROM event_desk_candidates WHERE state='CLOSED' GROUP BY strategy""")
        return {
            "candidates": candidates,
            "events": events,
            "last_run": runs[0] if runs else None,
            "research_outcomes": outcomes,
            "paper_results": trades,
            "mode": "PAPER",
            "version": VERSION,
            "measurement_note": "Forward observations; horizons are wall-clock hours. Delayed quotes are labelled. Research returns are not broker P&L.",
        }
