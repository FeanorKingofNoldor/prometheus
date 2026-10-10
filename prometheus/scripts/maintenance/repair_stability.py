"""Refresh missing daily stability scores using the production profile model.

Only writes stability/profile data; does not advance pipeline phases or orders.
"""
from __future__ import annotations

import argparse
import json
from datetime import date


def repair(db, region: str, as_of: date) -> dict:
    # Use the same dependencies and model settings as run_signals_for_run.
    from prometheus.pipeline import tasks as t
    reader = t.DataReader(db_manager=db)
    calendar = t._region_calendar(region)
    instruments = t._get_region_instruments(db, region)
    ids = [row[0] for row in instruments]
    prices = reader.read_prices(ids, as_of, as_of)
    present = set(prices.loc[prices['close'] > 0, 'instrument_id'].astype(str)) if not prices.empty else set()
    instruments = [row for row in instruments if row[0] in present]
    ids = [row[0] for row in instruments]
    issuers = {row[0]: row[1] for row in instruments}
    storage = t.StabilityStorage(db_manager=db)
    covered = storage.get_entities_with_soft_target_state(entity_type='INSTRUMENT', entity_ids=ids, as_of_date=as_of)
    builder = t.ProfileFeatureBuilder(db_manager=db, data_reader=reader, calendar=calendar)
    profiles = t.ProfileService(storage=t.ProfileStorage(db_manager=db),
        feature_builder=t.RoutedProfileFeatureBuilder(db_manager=db,
            builders_by_issuer_type={'COMPANY':builder, 'SOVEREIGN':t.SovereignProfileFeatureBuilder(db_manager=db)},
            default_builder=builder), embedder=t.BasicProfileEmbedder(embedding_dim=16))
    engine = t.StabilityEngine(model=t.BasicPriceStabilityModel(data_reader=reader, calendar=calendar,
        window_days=63, profile_service=profiles, instrument_to_issuer=issuers.get), storage=storage)
    result = {'as_of':str(as_of), 'instruments_with_prices':len(ids), 'already_fresh':len(covered),
              'refreshed':0, 'unavailable':{}}
    for instrument in ids:
        if instrument in covered:
            continue
        try:
            engine.score_entity(as_of, 'INSTRUMENT', instrument)
            result['refreshed'] += 1
        except ValueError as exc:
            result['unavailable'][instrument] = str(exc)
    return result


def main():
    from apatheon.core.database import get_db_manager
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--region', required=True, choices=['US','UK','EU','JP','HK','KR','AU'])
    parser.add_argument('--as-of', required=True, type=date.fromisoformat)
    args = parser.parse_args()
    if args.as_of > date.today():
        parser.error('as-of must not be in the future')
    print(json.dumps(repair(get_db_manager(), args.region, args.as_of), sort_keys=True))


if __name__ == '__main__':
    main()
