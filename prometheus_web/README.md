# Prometheus Web — Trading Dashboard

React frontend for the Prometheus trading system. Provides real-time portfolio monitoring, execution tracking, backtest analysis, and AI-powered trade monitoring.

## Pages

| Page | Route | Purpose |
|------|-------|---------|
| **Dashboard** | `/` | System overview: P&L, regime, pipeline status, alerts |
| **Portfolio** | `/portfolio` | Holdings, equity curve, sector exposure, trading reports |
| **Execution** | `/execution` | Live orders, fill tracking, risk actions |
| **Trade Monitor** | `/monitor` | Kronos weekly report, anomaly detection, sector P&L, trade journal analysis |
| **Backtests** | `/backtests` | Backtest run results and equity curves |
| **Options** | `/options` | Options campaign results and strategy P&L |
| **Intelligence** | `/intelligence` | ML proposals, scorecard, config change tracking |
| **Logs & Reports** | `/logs` | System logs, trading report generation |
| **Settings** | `/settings` | LLM config, engine parameters |
| **Kronos** | `/kronos` | AI assistant chat interface |
| **Docs** | `/docs` | Documentation pages |

## Tech Stack

- **React 19** with TypeScript
- **Vite** for build/dev server
- **TailwindCSS** for styling (dark theme, custom color variables)
- **TanStack Query** (React Query) for data fetching with smart caching
- **Recharts** for charts (equity curves, regime history, sector health)
- **React Router** for client-side routing
- **Mermaid** for flowchart rendering

## Component Library

| Component | Purpose |
|-----------|---------|
| `Panel` | Container with title, actions, tooltip |
| `KpiCard` | Single metric display with sentiment coloring |
| `DataTable` | Sortable, pageable data grid with custom renderers |
| `PageHeader` | Page title + subtitle + refresh button |
| `StatusBadge` | Status indicator (positive/negative/warning/neutral) |
| `Charts` | LineChart, BarChart with zoom controls (1M-5Y) |
| `ConnectionLed` | IBKR connection status indicator |

## API Connection

Connects to Prometheus backend at `http://localhost:8200/api/`. All data fetching uses React Query hooks defined in `src/api/hooks.ts` (45+ hooks). Key refetch intervals:

- Portfolio/execution data: 30 seconds
- System logs: 5 seconds
- Activity feed: 15 seconds
- Trade monitor: 60 seconds

## Development

```bash
# Install dependencies
npm ci

# Dev server (port 5173, hot reload)
npm run dev

# Production build
npm run build

# Type check
npx tsc --noEmit
```

## Styling

Dark theme using Tailwind with custom CSS variables:
- `surface`, `surface-raised`, `surface-overlay` — background layers
- `border-dim`, `border-bright` — borders
- `accent` — primary accent color
- `positive`, `negative`, `warning` — sentiment colors
- `muted` — secondary text
