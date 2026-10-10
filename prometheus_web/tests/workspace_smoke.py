"""Browser acceptance tests against a built frontend; all API calls are intercepted.

Usage: python3 <app>_web/tests/workspace_smoke.py --base http://localhost:<preview-port>
Requires Playwright + Chromium. Never submits orders or changes a real backend.
"""
import argparse
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from playwright.sync_api import sync_playwright, expect

args = argparse.ArgumentParser()
args.add_argument('--base', required=True)
args.add_argument('--app', choices=['prometheus', 'cassandra'], default=Path(__file__).parents[2].name)
config = args.parse_args()
now = datetime.now(timezone.utc).isoformat()

with sync_playwright() as playwright:
    browser = playwright.chromium.launch(headless=True)
    page = browser.new_page(viewport={'width': 1440, 'height': 1000})
    errors = []
    page.on('pageerror', lambda e: errors.append(str(e)))
    state = {'fail': False, 'close_rejected': True, 'closed': False, 'searched': False}
    event = {'title': 'Test supply disruption', 'predicate': 'cut_production_of', 'entity': ['RESOURCE', 'oil'], 'evidence': [{'title': 'Original source', 'url': 'https://example.com/source', 'source': 'test', 'published_at': now}], 'evidence_state': {'label': 'confirmed', 'sources': 2}, 'mapping_reason': 'Explicit company exposure'}
    candidates = [{'candidate_id': str(i), 'event_id': 'event', 'instrument_id': f'TEST{i}.US', 'strategy': 'CONTINUATION' if i % 2 == 0 else 'REVERSAL', 'state': 'REJECTED' if i == 39 else 'WATCHING', 'reason': 'Waiting for observations', 'created_at': now, 'event': event, 'payload': {'company': f'TEST{i}', 'sign': 1, 'strength': 1, 'path': []}} for i in range(40)]
    desk = {'candidates': candidates, 'events': [{'event_id': 'event', 'first_seen': now, 'payload': event}], 'last_run': {'completed_at': now, 'payload': {'status': 'OK', 'market_state': 'HOLIDAY', 'paper_entries_enabled': True, 'policy': {'book_usd': 50000, 'max_gross': 25000, 'max_position': 5000}}}, 'research_outcomes': [], 'paper_results': [], 'measurement_note': 'Forward observations only.'}
    market = {'condition_id': 'm0', 'title': 'Will the test event happen?', 'outcome_prices': [0.8, 0.2], 'outcomes': ['No', 'Yes'], 'volume_24h': 1200, 'liquidity': 3000, 'category': 'tier1', 'mapped_entity_id': ['RESOURCE', 'oil'], 'mapping_confidence': .9, 'end_date': now, 'updated_at': now, 'price_verified_at': now}
    position = {'position_id': 'p1', 'condition_id': 'm0', 'title': market['title'], 'side': 'NO', 'entry_price': .7, 'current_price': .65, 'size_usdc': 50, 'unrealized_pnl': -3.57, 'realized_pnl': None, 'status': 'OPEN', 'opened_at': now, 'closed_at': None, 'notes': 'Verified paper entry'}
    summary = {'open_positions': 1, 'closed_positions': 0, 'total_exposure_usdc': 50, 'unrealized_pnl': -3.57, 'realized_pnl': 0}
    health = {'ok': True, 'intelligence_fresh': True, 'trusted_settlements': 0, 'excluded_settlements': 331, 'latest_verified_price': now, 'jobs': [{'job': 'price_snapshots', 'last_success': now, 'fresh': True, 'error': None}]}
    def handler(route):
        url = urlparse(route.request.url)
        path, params = url.path, parse_qs(url.query)
        status = 200
        if state['fail']:
            route.fulfill(status=503, content_type='application/json', body='{"detail":"Unavailable"}')
            return
        if path.endswith('/chat/stream'):
            route.fulfill(content_type='text/event-stream', body='data: '+json.dumps({'type': 'done', 'response': {'answer': 'Verified test answer'}})+'\n\n')
            return
        if config.app == 'prometheus':
            data = desk if path == '/api/event-desk' else {}
        elif path == '/api/health/pipeline': data = health
        elif path == '/api/status': data = {'service': 'Cassandra', 'version': 'test', 'mode': 'dry_run', 'portfolio': summary}
        elif path == '/api/markets/stats': data = {'active': 51, 'mapped': 51, 'resolved': 0, 'total': 51, 'by_category': {'tier1': 51}}
        elif path == '/api/markets':
            state['searched'] = bool(params.get('q'))
            data = [market] if params.get('q') else [{**market, 'condition_id': f'm{i}', 'title': f"{market['title']} {i}"} for i in range(51)]
        elif path.startswith('/api/markets/'):
            data = {'market': {**market, 'description': 'Resolution rules', 'active': True, 'resolved': False}, 'mappings': [], 'price_history': [{'timestamp': now, 'prices': [.8, .2]}, {'timestamp': (datetime.now(timezone.utc)-timedelta(minutes=5)).isoformat(), 'prices': [.75, .25]}]}
        elif path.endswith('/close'):
            if state['close_rejected']: data = {'success': False, 'error': 'Verified mark is stale'}
            else: data = {'success': True}; state['closed'] = True
        elif path == '/api/positions': data = [] if state['closed'] else [position]
        elif path == '/api/positions/history': data = []
        elif path == '/api/calibration/scorecard': data = {'total_resolved': 0}
        elif path == '/api/calibration/scorecard/cohorts': data = {'overall': {'n': 0}, 'by_question_type': [], 'by_entity_type': [], 'by_divergence_bucket': []}
        elif path == '/api/divergences': data = []
        else: data = {}
        route.fulfill(status=status, content_type='application/json', body=json.dumps(data))
    page.route('**/api/**', handler)
    page.route('**/health', handler)
    if config.app == 'prometheus':
        page.goto(config.base+'/opportunities')
        expect(page.locator('tbody tr')).to_have_count(15)
        page.get_by_role('button', name='Next page').click()
        expect(page.locator('tbody tr').first).to_contain_text('TEST15.US')
        page.get_by_label('Strategy', exact=True).select_option('REVERSAL')
        expect(page.locator('tbody tr').first).to_contain_text('TEST1.US')
        page.get_by_label('Search assets, events or decisions…').fill('TEST39.US')
        expect(page.get_by_text('No matching records')).to_be_visible()
        page.get_by_label('Include archived').click()
        expect(page.get_by_label('Include archived')).to_be_checked()
        expect(page.locator('tbody tr')).to_have_count(1)
        page.get_by_role('button', name='TEST39.US', exact=True).click()
        expect(page.get_by_role('dialog')).to_be_visible()
        expect(page.get_by_role('link', name='Original source')).to_have_attribute('href','https://example.com/source')
        page.keyboard.press('Escape')
        expect(page.get_by_role('dialog')).to_have_count(0)
        desk['last_run']['completed_at'] = '2000-01-01T00:00:00Z'
        page.reload()
        expect(page.get_by_role('alert')).to_contain_text('over 10 minutes old')
        failure_route = '/research'
    else:
        page.goto(config.base+'/markets')
        expect(page.locator('tbody tr')).to_have_count(50)
        expect(page.locator('tbody tr').first).to_contain_text('20.0%')
        page.get_by_role('button', name='Next', exact=True).click()
        expect(page).to_have_url(config.base+'/markets?offset=50')
        page.get_by_label('Search all active markets…').fill('test event')
        expect(page.locator('tbody tr')).to_have_count(1)
        assert state['searched']
        page.locator('tbody a').first.click()
        expect(page.get_by_role('heading', name='Will the test event happen?', exact=True)).to_be_visible()
        expect(page.get_by_text('20.0%', exact=True)).to_be_visible()
        page.goto(config.base+'/method')
        page.get_by_label('Operator key', exact=True).fill('test-operator-key')
        page.get_by_role('button', name='Store for this session').click()
        expect(page.get_by_text('Key stored', exact=True)).to_be_visible()
        page.goto(config.base+'/bets')
        page.get_by_role('button', name='Will the test event happen?', exact=True).click()
        page.get_by_role('button', name='Review paper close').click()
        page.get_by_role('button', name='Confirm paper close').click()
        expect(page.get_by_role('alert')).to_contain_text('stale')
        state['close_rejected'] = False
        page.get_by_role('button', name='Confirm paper close').click()
        expect(page.get_by_role('dialog')).to_have_count(0)
        expect(page.get_by_text('No open verified positions')).to_be_visible()
        health['ok'] = False; health['jobs'][0]['fresh'] = False
        page.goto(config.base+'/operations')
        expect(page.get_by_role('alert')).to_contain_text('needs attention')
        expect(page.locator('tbody tr')).to_contain_text('Stale')
        failure_route = '/scorecard'
    # Stream success without any real LLM/API request.
    page.goto(config.base+'/iris')
    page.get_by_label('Message to Iris').fill('Explain the evidence')
    page.get_by_role('button', name='Send', exact=True).click()
    expect(page.get_by_text('Verified test answer', exact=True)).to_be_visible()
    # Outages must show an error, not a zero or fabricated outcome.
    state['fail'] = True
    page.goto(config.base+failure_route)
    expect(page.get_by_role('alert').first).to_contain_text('Data unavailable', timeout=15000)
    expect(page.get_by_text('No verified outcomes to grade yet.', exact=True)).to_have_count(0)
    expect(page.get_by_text('The first closed trade will start the record', exact=True)).to_have_count(0)
    state['fail'] = False
    page.get_by_role('button', name='Retry').first.click()
    if config.app == 'cassandra':
        expect(page.get_by_text('No verified outcomes to grade yet.', exact=True)).to_be_visible()
        page.get_by_role('button', name='Refresh data').click()
    expect(page.get_by_role('alert')).to_have_count(0, timeout=15000)
    page.set_viewport_size({'width': 390, 'height': 844})
    page.get_by_role('button', name='Open navigation').click()
    page.get_by_role('navigation').get_by_text('Overview', exact=True).click()
    expect(page.get_by_role('heading', level=1)).to_contain_text('desk')
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    assert not errors, errors
    browser.close()
print(f'{config.app}: filtering, details, freshness, chat, failures, retry and mobile tests passed')
