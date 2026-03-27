import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "./client";

// Default identifiers — match what the backend seeds / expects
const DEFAULT_STRATEGY = "MAIN";
const DEFAULT_PORTFOLIO = "IBKR_PAPER";
const DEFAULT_MARKET = "US_EQ";

// ── Status ──────────────────────────────────────────────
export const useOverview = () =>
  useQuery({ queryKey: ["status", "overview"], queryFn: () => api.get("/status/overview"), refetchInterval: 10_000 });

export const usePipelines = () =>
  useQuery({ queryKey: ["status", "pipelines"], queryFn: () => api.get("/status/pipelines"), refetchInterval: 30_000 });

export const usePipeline = (marketId: string) =>
  useQuery({ queryKey: ["status", "pipeline", marketId], queryFn: () => api.get(`/status/pipeline?market_id=${marketId}`), enabled: !!marketId });

export const useRegime = () =>
  useQuery({ queryKey: ["status", "regime"], queryFn: () => api.get("/status/regime"), refetchInterval: 30_000 });

export const useStability = () =>
  useQuery({ queryKey: ["status", "stability"], queryFn: () => api.get("/status/stability") });

export const useFragilityList = () =>
  useQuery({ queryKey: ["status", "fragility"], queryFn: () => api.get("/status/fragility") });

export const useFragilityDetail = (entityId: string) =>
  useQuery({ queryKey: ["status", "fragility", entityId], queryFn: () => api.get(`/status/fragility/${entityId}`), enabled: !!entityId });

export const useAssessment = (strategyId = DEFAULT_STRATEGY) =>
  useQuery({ queryKey: ["status", "assessment", strategyId], queryFn: () => api.get(`/status/assessment?strategy_id=${strategyId}`) });

export const useUniverse = (strategyId = DEFAULT_STRATEGY) =>
  useQuery({ queryKey: ["status", "universe", strategyId], queryFn: () => api.get(`/status/universe?strategy_id=${strategyId}`) });

export const usePortfolios = () =>
  useQuery({ queryKey: ["status", "portfolios"], queryFn: () => api.get("/status/portfolios") });

export const usePortfolio = (portfolioId = DEFAULT_PORTFOLIO) =>
  useQuery({ queryKey: ["status", "portfolio", portfolioId], queryFn: () => api.get(`/status/portfolio?portfolio_id=${portfolioId}`), refetchInterval: 30_000 });

export const usePortfolioRisk = (portfolioId = DEFAULT_PORTFOLIO) =>
  useQuery({ queryKey: ["status", "portfolio_risk", portfolioId], queryFn: () => api.get(`/status/portfolio_risk?portfolio_id=${portfolioId}`) });

export const useExecution = (portfolioId = DEFAULT_PORTFOLIO) =>
  useQuery({ queryKey: ["status", "execution", portfolioId], queryFn: () => api.get(`/status/execution?portfolio_id=${portfolioId}`), refetchInterval: 30_000 });

export const useExecutionDecisions = (portfolioId = DEFAULT_PORTFOLIO) =>
  useQuery({ queryKey: ["status", "execution_decisions", portfolioId], queryFn: () => api.get(`/status/execution/decisions?portfolio_id=${portfolioId}`) });

export const useRiskActions = (strategyId = DEFAULT_STRATEGY) =>
  useQuery({ queryKey: ["status", "risk_actions", strategyId], queryFn: () => api.get(`/status/risk_actions?strategy_id=${strategyId}`) });

// ── Options ─────────────────────────────────────────────
export const useOptionsResults = () =>
  useQuery({ queryKey: ["options", "results"], queryFn: () => api.get("/options/results") });

export const useOptionsResult = (id: string) =>
  useQuery({ queryKey: ["options", "results", id], queryFn: () => api.get(`/options/results/${id}`), enabled: !!id });

export const useOptionsCampaigns = () =>
  useQuery({ queryKey: ["options", "campaigns"], queryFn: () => api.get("/options/campaigns") });

export const useOptionsCampaignSummary = (id: string) =>
  useQuery({ queryKey: ["options", "campaigns", id, "summary"], queryFn: () => api.get(`/options/campaigns/${id}/summary`), enabled: !!id });

export const useOptionsCampaignDistribution = (id: string) =>
  useQuery({ queryKey: ["options", "campaigns", id, "distribution"], queryFn: () => api.get(`/options/campaigns/${id}/distribution`), enabled: !!id });

// ── Backtests ───────────────────────────────────────────
export const useBacktestRuns = () =>
  useQuery({ queryKey: ["backtests", "runs"], queryFn: () => api.get("/backtests/runs") });

export const useBacktestEquity = (runId: string) =>
  useQuery({ queryKey: ["backtests", "runs", runId, "equity"], queryFn: () => api.get(`/backtests/runs/${runId}/daily_equity`), enabled: !!runId });

// ── Control ─────────────────────────────────────────────
export const useAllocatorRegistry = () =>
  useQuery({ queryKey: ["control", "allocator_registry"], queryFn: () => api.get("/control/allocator_registry") });

export const useJobStatus = (jobId: string) =>
  useQuery({
    queryKey: ["control", "jobs", jobId],
    queryFn: () => api.get(`/control/jobs/${jobId}`),
    enabled: !!jobId,
    refetchInterval: 2_000,
  });

export const useRunBacktest = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (params: Record<string, unknown>) => api.post("/control/run_allocator_backtest", params),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["backtests"] }),
  });
};

export const useApplyConfig = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (params: Record<string, unknown>) => api.post("/control/apply_config_change", params),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["meta"] }),
  });
};

export const useScheduleDag = () =>
  useMutation({ mutationFn: (params: Record<string, unknown>) => api.post("/control/schedule_dag", params) });

export const useCreateSynthetic = () =>
  useMutation({ mutationFn: (params: Record<string, unknown>) => api.post("/control/create_synthetic_dataset", params) });
type SyncDataParams = {
  sources?: string[];
  portfolioId?: string;
};

const DEFAULT_SYNC_SOURCES = ["ibkr", "engines", "nations"];
const DEFAULT_SYNC_PORTFOLIO = "IBKR_PAPER";

function isIbkrPortfolioId(portfolioId: string): boolean {
  return portfolioId.startsWith("IBKR_");
}

export const useSyncData = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (params: SyncDataParams = {}) => {
      const sources = params.sources?.length ? params.sources : DEFAULT_SYNC_SOURCES;
      const requestedPortfolioId = (params.portfolioId ?? DEFAULT_SYNC_PORTFOLIO).trim() || DEFAULT_SYNC_PORTFOLIO;
      const includesIbkr = sources.includes("ibkr") || sources.includes("all");
      const safePortfolioId =
        includesIbkr && !isIbkrPortfolioId(requestedPortfolioId)
          ? DEFAULT_SYNC_PORTFOLIO
          : requestedPortfolioId;

      return api.post("/control/sync_data", {
        sources,
        portfolio_id: safePortfolioId,
      });
    },
    onSuccess: () => {
      // Invalidate all data queries so they refetch
      qc.invalidateQueries({ queryKey: ["status"] });
      qc.invalidateQueries({ queryKey: ["meta"] });
      qc.invalidateQueries({ queryKey: ["entities"] });
      qc.invalidateQueries({ queryKey: ["control"] });
      qc.invalidateQueries({ queryKey: ["nation"] });
    },
  });
};

// ── Intelligence ────────────────────────────────────────
export const useDiagnostics = (strategyId: string) =>
  useQuery({ queryKey: ["intelligence", "diagnostics", strategyId], queryFn: () => api.get(`/intelligence/diagnostics/${strategyId}`), enabled: !!strategyId });

export const useProposals = () =>
  useQuery({ queryKey: ["intelligence", "proposals"], queryFn: () => api.get("/intelligence/proposals") });

export const useGenerateProposals = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (strategyId: string) => api.post(`/intelligence/proposals/generate/${strategyId}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["intelligence", "proposals"] }),
  });
};

export const useApproveProposal = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.post(`/intelligence/proposals/${id}/approve`, { user_id: "web_ui" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["intelligence", "proposals"] }),
  });
};

export const useRejectProposal = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.post(`/intelligence/proposals/${id}/reject`, { user_id: "web_ui" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["intelligence", "proposals"] }),
  });
};

export const useApplyProposal = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.post(`/intelligence/proposals/${id}/apply`, { user_id: "web_ui" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["intelligence"] }),
  });
};

export const useApplyBatchProposals = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (strategyId?: string) => api.post(`/intelligence/proposals/apply-batch${strategyId ? `?strategy_id=${strategyId}` : ""}`, { user_id: "web_ui" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["intelligence"] }),
  });
};

export const useConfigChanges = () =>
  useQuery({ queryKey: ["intelligence", "changes"], queryFn: () => api.get("/intelligence/changes") });

export const useScorecard = (horizonDays = 21) =>
  useQuery({ queryKey: ["intelligence", "scorecard", horizonDays], queryFn: () => api.get(`/intelligence/scorecard?horizon_days=${horizonDays}`), staleTime: 300_000 });

export const useLambdaScorecard = (marketId = "US_EQ") =>
  useQuery({ queryKey: ["intelligence", "lambda-scorecard", marketId], queryFn: () => api.get(`/intelligence/lambda-scorecard?market_id=${marketId}`), staleTime: 300_000 });

export const useRevertChange = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, reason }: { id: string; reason: string }) => api.post(`/intelligence/changes/${id}/revert`, { reason, user_id: "web_ui" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["intelligence"] }),
  });
};

// ── Entities ────────────────────────────────────────────
export const useEntityTypes = () =>
  useQuery({ queryKey: ["entities", "types"], queryFn: () => api.get("/entities/types") });

export const useEntitySectors = (issuerType?: string) =>
  useQuery({ queryKey: ["entities", "sectors", issuerType], queryFn: () => api.get(`/entities/sectors${issuerType ? `?issuer_type=${issuerType}` : ""}`) });

export const useEntityCountries = () =>
  useQuery({ queryKey: ["entities", "countries"], queryFn: () => api.get("/entities/countries") });

export const useEntities = (params?: string) =>
  useQuery({ queryKey: ["entities", "list", params], queryFn: () => api.get(`/entities${params ? `?${params}` : ""}`) });

export const useEntityDetail = (id: string) =>
  useQuery({ queryKey: ["entities", id], queryFn: () => api.get(`/entities/${id}`), enabled: !!id });

export const useCompareEntityPrices = (ids: string[], days = 365, metric = "close") =>
  useQuery({
    queryKey: ["entities", "compare_prices", ids.join(","), days, metric],
    queryFn: () => api.get(`/entities/compare_prices?ids=${ids.join(",")}&days=${days}&metric=${metric}`),
    enabled: ids.length > 0,
    staleTime: 60_000,
  });

export const useEntityProfiles = (id: string) =>
  useQuery({ queryKey: ["entities", id, "profiles"], queryFn: () => api.get(`/entities/${id}/profiles`), enabled: !!id });

// ── Meta ────────────────────────────────────────────────
export const useConfigs = () =>
  useQuery({ queryKey: ["meta", "configs"], queryFn: () => api.get("/meta/configs") });

export const useEngineParameters = () =>
  useQuery({
    queryKey: ["meta", "engine_parameters"],
    queryFn: () => api.get("/meta/engine_parameters"),
    staleTime: 30_000,
  });

export const usePerformance = (engineName = "regime") =>
  useQuery({ queryKey: ["meta", "performance", engineName], queryFn: () => api.get(`/meta/performance?engine_name=${engineName}`) });

export const usePolicy = (marketId: string) =>
  useQuery({ queryKey: ["meta", "policy", marketId], queryFn: () => api.get(`/meta/policy/${marketId}`), enabled: !!marketId });

export const usePolicyDecisions = (marketId = DEFAULT_MARKET) =>
  useQuery({ queryKey: ["meta", "policy_decisions", marketId], queryFn: () => api.get(`/meta/policy/decisions?market_id=${marketId}`) });

// ── Market Overview ──────────────────────────────────────
export const useMarketOverview = () =>
  useQuery({ queryKey: ["status", "market_overview"], queryFn: () => api.get("/status/market_overview"), staleTime: 60_000 });

// ── Portfolio Equity ─────────────────────────────────────
export const usePortfolioEquity = (portfolioId: string, benchmark = "SPY.US") =>
  useQuery({
    queryKey: ["status", "portfolio_equity", portfolioId, benchmark],
    queryFn: () => api.get(`/status/portfolio_equity?portfolio_id=${portfolioId}&benchmark=${benchmark}`),
    enabled: !!portfolioId,
    staleTime: 60_000,
  });

// ── Position P&L History ─────────────────────────────────
export const usePositionPnlHistory = (portfolioId: string) =>
  useQuery({
    queryKey: ["status", "position_pnl_history", portfolioId],
    queryFn: () => api.get(`/status/position_pnl_history?portfolio_id=${portfolioId}`),
    enabled: !!portfolioId,
    staleTime: 60_000,
  });

// ── Computed Risk ────────────────────────────────────────
export const usePortfolioRiskComputed = (portfolioId: string) =>
  useQuery({
    queryKey: ["status", "portfolio_risk_computed", portfolioId],
    queryFn: () => api.get(`/status/portfolio_risk_computed?portfolio_id=${portfolioId}`),
    enabled: !!portfolioId,
    staleTime: 60_000,
  });

// ── IBKR Status ─────────────────────────────────────────
export const useIbkrStatus = () =>
  useQuery({
    queryKey: ["control", "ibkr_status"],
    queryFn: () => api.get("/control/ibkr_status"),
    refetchInterval: 15_000,
  });

// ── Kronos ──────────────────────────────────────────────
export const useKronosChat = () =>
  useMutation({ mutationFn: (payload: { question: string; context?: Record<string, unknown> }) => api.post("/kronos/chat", payload) });

export const useLlmConfig = () =>
  useQuery({ queryKey: ["kronos", "llm_config"], queryFn: () => api.get("/kronos/llm/config") });

export const useSetLlmConfig = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (payload: { provider: string; model?: string; api_key?: string; base_url?: string }) =>
      api.post("/kronos/llm/config", payload),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["kronos", "llm_config"] }),
  });
};

export const useLlmHealth = () =>
  useQuery({ queryKey: ["kronos", "llm_health"], queryFn: () => api.get("/kronos/llm/health"), enabled: false });

// ── Nation ───────────────────────────────────────────────
// Geo data changes infrequently — cache aggressively (5 min stale, 10 min GC).
const GEO_STALE = 5 * 60_000;
const GEO_GC    = 10 * 60_000;

export const useNationList = () =>
  useQuery({ queryKey: ["nation", "list"], queryFn: () => api.get("/nation/list"), staleTime: GEO_STALE, gcTime: GEO_GC });

export const useNationMapSummary = () =>
  useQuery({ queryKey: ["nation", "map-summary"], queryFn: () => api.get("/nation/map-summary"), staleTime: GEO_STALE, gcTime: GEO_GC });

export const useNationScores = (nation: string) =>
  useQuery({ queryKey: ["nation", nation, "scores"], queryFn: () => api.get(`/nation/${nation}/scores`), enabled: !!nation });

export const useNationScoreHistory = (nation: string, days = 90) =>
  useQuery({ queryKey: ["nation", nation, "scores_history", days], queryFn: () => api.get(`/nation/${nation}/scores/history?days=${days}`), enabled: !!nation });

export const useNationIndicators = (nation: string) =>
  useQuery({ queryKey: ["nation", nation, "indicators"], queryFn: () => api.get(`/nation/${nation}/indicators`), enabled: !!nation });

export const useNationPersons = (nation: string) =>
  useQuery({ queryKey: ["nation", nation, "persons"], queryFn: () => api.get(`/nation/${nation}/persons`), enabled: !!nation });

// ── Nation Industries ────────────────────────────────────
export const useNationIndustries = (nation: string) =>
  useQuery({ queryKey: ["nation", nation, "industries"], queryFn: () => api.get(`/nation/${nation}/industries`), enabled: !!nation, staleTime: GEO_STALE, gcTime: GEO_GC });

export const useNationIndustryHealth = (nation: string) =>
  useQuery({ queryKey: ["nation", nation, "industry-health"], queryFn: () => api.get(`/nation/${nation}/industry-health`), enabled: !!nation });

// ── Conflicts ────────────────────────────────────────────
export const useConflicts = () =>
  useQuery({ queryKey: ["nation", "conflicts"], queryFn: () => api.get("/nation/conflicts"), staleTime: GEO_STALE, gcTime: GEO_GC });

export const useNationConflicts = (nation: string) =>
  useQuery({ queryKey: ["nation", nation, "conflicts"], queryFn: () => api.get(`/nation/${nation}/conflicts`), enabled: !!nation, staleTime: GEO_STALE, gcTime: GEO_GC });

// ── Geo Overlays ────────────────────────────────────────
export const useChokepoints = () =>
  useQuery({ queryKey: ["nation", "chokepoints"], queryFn: () => api.get("/nation/chokepoints"), staleTime: GEO_STALE, gcTime: GEO_GC });

export const useTradeRoutes = (category?: string) =>
  useQuery({
    queryKey: ["nation", "trade-routes", category],
    queryFn: () => api.get(`/nation/trade-routes${category ? `?category=${category}` : ""}`),
    staleTime: GEO_STALE,
    gcTime: GEO_GC,
  });

export const usePorts = (portType?: string) =>
  useQuery({
    queryKey: ["nation", "ports", portType],
    queryFn: () => api.get(`/nation/ports${portType ? `?port_type=${portType}` : ""}`),
    staleTime: GEO_STALE,
    gcTime: GEO_GC,
  });

export const useResourceInfo = (resource?: string) =>
  useQuery({
    queryKey: ["nation", "resource-info", resource],
    queryFn: () => api.get(`/nation/resource-info${resource ? `?resource=${resource}` : ""}`),
    staleTime: GEO_STALE,
    gcTime: GEO_GC,
  });

export const useNationInfo = (nation?: string) =>
  useQuery({
    queryKey: ["nation", "info", nation],
    queryFn: () => api.get(`/nation/info${nation ? `?nation=${nation}` : ""}`),
    staleTime: GEO_STALE,
    gcTime: GEO_GC,
  });

// ── Live Tracking (vessels + flights) ─────────────────────
// Shorter poll intervals for real-time data.
const TRACKING_STALE = 15_000;  // 15s
const TRACKING_GC    = 30_000;  // 30s

export const useVessels = (category?: string) =>
  useQuery({
    queryKey: ["nation", "vessels", category],
    queryFn: () => api.get(`/nation/vessels${category ? `?category=${category}` : ""}`),
    refetchInterval: 30_000,
    staleTime: TRACKING_STALE,
    gcTime: TRACKING_GC,
  });

export const useFlights = (category?: string) =>
  useQuery({
    queryKey: ["nation", "flights", category],
    queryFn: () => api.get(`/nation/flights${category ? `?category=${category}` : ""}`),
    refetchInterval: 15_000,
    staleTime: TRACKING_STALE,
    gcTime: TRACKING_GC,
  });

export const useVesselCount = () =>
  useQuery({
    queryKey: ["nation", "vessels", "count"],
    queryFn: () => api.get("/nation/vessels/count"),
    refetchInterval: 30_000,
    staleTime: TRACKING_STALE,
  });

export const useFlightCount = () =>
  useQuery({
    queryKey: ["nation", "flights", "count"],
    queryFn: () => api.get("/nation/flights/count"),
    refetchInterval: 15_000,
    staleTime: TRACKING_STALE,
  });

export const useNavalDeployments = (category?: string, nation?: string) =>
  useQuery({
    queryKey: ["nation", "naval-deployments", category, nation],
    queryFn: () => {
      const params = new URLSearchParams();
      if (category) params.set("category", category);
      if (nation) params.set("nation", nation);
      const qs = params.toString();
      return api.get(`/nation/naval-deployments${qs ? `?${qs}` : ""}`);
    },
    staleTime: 5 * 60_000, // 5 min — data changes weekly
    gcTime: 10 * 60_000,
  });

export const useResources = (nation?: string, resource?: string) =>
  useQuery({
    queryKey: ["nation", "resources", nation, resource],
    queryFn: () => {
      const params = new URLSearchParams();
      if (nation) params.set("nation", nation);
      if (resource) params.set("resource", resource);
      const qs = params.toString();
      return api.get(`/nation/resources${qs ? `?${qs}` : ""}`);
    },
    staleTime: GEO_STALE,
    gcTime: GEO_GC,
  });

// ── Geo ─────────────────────────────────────────────────
export const useCountries = () =>
  useQuery({ queryKey: ["geo", "countries"], queryFn: () => api.get("/geo/countries") });

export const useCountry = (code: string) =>
  useQuery({ queryKey: ["geo", "country", code], queryFn: () => api.get(`/geo/country/${code}`), enabled: !!code });

// ── Logs ────────────────────────────────────────────────
export const useSystemLogs = (params?: { level?: string; category?: string; search?: string; limit?: number }) =>
  useQuery({
    queryKey: ["logs", "system", params],
    queryFn: () => {
      const qs = new URLSearchParams();
      if (params?.level) qs.set("level", params.level);
      if (params?.category) qs.set("category", params.category);
      if (params?.search) qs.set("search", params.search);
      if (params?.limit) qs.set("limit", String(params.limit));
      const q = qs.toString();
      return api.get(`/logs/system${q ? `?${q}` : ""}`);
    },
    refetchInterval: 5_000,
  });

export const useLogCategories = () =>
  useQuery({ queryKey: ["logs", "system", "categories"], queryFn: () => api.get("/logs/system/categories") });

export const useEngineRuns = (params?: { status?: string; region?: string; limit?: number }) =>
  useQuery({
    queryKey: ["logs", "runs", params],
    queryFn: () => {
      const qs = new URLSearchParams();
      if (params?.status) qs.set("status", params.status);
      if (params?.region) qs.set("region", params.region);
      if (params?.limit) qs.set("limit", String(params.limit));
      const q = qs.toString();
      return api.get(`/logs/runs${q ? `?${q}` : ""}`);
    },
    refetchInterval: 15_000,
  });

export const useReports = (reportType?: string) =>
  useQuery({
    queryKey: ["logs", "reports", reportType],
    queryFn: () => api.get(`/logs/reports${reportType ? `?report_type=${reportType}` : ""}`),
  });

export const useReport = (id: string) =>
  useQuery({ queryKey: ["logs", "reports", id], queryFn: () => api.get(`/logs/reports/${id}`), enabled: !!id });

export const useGenerateReport = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (params: { report_type: string; start_date?: string; end_date?: string }) =>
      api.post("/logs/reports/generate", params),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["logs", "reports"] }),
  });
};

export const useGenerateTradingReport = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (params: { report_type: string; portfolio_id?: string; start_date?: string; end_date?: string }) =>
      api.post("/logs/reports/generate-trading", params),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["logs", "reports"] }),
  });
};

export const useTradingReports = (portfolioId?: string) =>
  useQuery({
    queryKey: ["logs", "reports", "trading", portfolioId],
    queryFn: async () => {
      // Fetch all trading report types and combine
      const results = await Promise.all([
        api.get("/logs/reports?report_type=trading_daily"),
        api.get("/logs/reports?report_type=trading_weekly"),
        api.get("/logs/reports?report_type=trading_custom"),
      ]);
      const all = [...(results[0] as unknown[]), ...(results[1] as unknown[]), ...(results[2] as unknown[])];
      // Sort by generated_at descending
      return all.sort((a: unknown, b: unknown) => {
        const aDate = (a as Record<string, string>).generated_at || "";
        const bDate = (b as Record<string, string>).generated_at || "";
        return bDate.localeCompare(aDate);
      });
    },
  });

export const useActivity = (params?: { source?: string; engine?: string; search?: string; limit?: number }) =>
  useQuery({
    queryKey: ["logs", "activity", params],
    queryFn: () => {
      const qs = new URLSearchParams();
      if (params?.source) qs.set("source", params.source);
      if (params?.engine) qs.set("engine", params.engine);
      if (params?.search) qs.set("search", params.search);
      if (params?.limit) qs.set("limit", String(params.limit));
      const q = qs.toString();
      return api.get(`/logs/activity${q ? `?${q}` : ""}`);
    },
    refetchInterval: 15_000,
  });

export const useEngineNames = () =>
  useQuery({ queryKey: ["logs", "activity", "engines"], queryFn: () => api.get("/logs/activity/engines") });

export const useRunDetail = (runId: string) =>
  useQuery({ queryKey: ["logs", "runs", runId], queryFn: () => api.get(`/logs/runs/${runId}`), enabled: !!runId });

// ── Intel Briefing Center ───────────────────────────────

export const useIntelBriefs = (params?: {
  brief_type?: string;
  severity?: string;
  domain?: string;
  unread_only?: boolean;
  limit?: number;
}) =>
  useQuery({
    queryKey: ["intel", "briefs", params],
    queryFn: () => {
      const qs = new URLSearchParams();
      if (params?.brief_type) qs.set("brief_type", params.brief_type);
      if (params?.severity) qs.set("severity", params.severity);
      if (params?.domain) qs.set("domain", params.domain);
      if (params?.unread_only) qs.set("unread_only", "true");
      if (params?.limit) qs.set("limit", String(params.limit));
      const q = qs.toString();
      return api.get(`/intel/briefs${q ? `?${q}` : ""}`);
    },
    refetchInterval: 60_000,
  });

export const useIntelBrief = (id: string) =>
  useQuery({ queryKey: ["intel", "briefs", id], queryFn: () => api.get(`/intel/briefs/${id}`), enabled: !!id });

export const useFlashAlerts = (limit = 20) =>
  useQuery({
    queryKey: ["intel", "flash-alerts", limit],
    queryFn: () => api.get(`/intel/briefs/flash-alerts?limit=${limit}`),
    refetchInterval: 30_000,
  });

export const useIntelUnreadCount = () =>
  useQuery({
    queryKey: ["intel", "unread-count"],
    queryFn: () => api.get("/intel/briefs/unread-count"),
    refetchInterval: 30_000,
  });

export const useMarkBriefRead = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.post(`/intel/briefs/${id}/read`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["intel", "briefs"] });
      qc.invalidateQueries({ queryKey: ["intel", "unread-count"] });
    },
  });
};

export const useGenerateSitrep = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => api.post("/intel/generate/sitrep"),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["intel"] }),
  });
};

export const useGenerateFlashCheck = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => api.post("/intel/generate/flash-check"),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["intel"] }),
  });
};

export const useGenerateWeekly = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => api.post("/intel/generate/weekly"),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["intel"] }),
  });
};

export const useIntelJob = (jobId: string | null) =>
  useQuery({
    queryKey: ["intel", "jobs", jobId],
    queryFn: () => api.get(`/intel/jobs/${jobId}`),
    enabled: !!jobId,
    retry: 1, // don't hammer server on 404
    refetchInterval: (query) => {
      // Stop polling if the job completed, errored, or the query itself failed (e.g. 404 after restart)
      if (query.state.status === "error") return false;
      const data = query.state.data as Record<string, unknown> | undefined;
      return data?.status === "done" || data?.status === "error" ? false : 2_000;
    },
  });

// ── Kronos Trade Monitor ────────────────────────────────────
export const useWeeklyReport = () =>
  useQuery({ queryKey: ["meta", "weekly_report"], queryFn: () => api.get("/meta/weekly_report"), staleTime: 60_000 });

export const useTradeJournal = (lookbackDays = 63) =>
  useQuery({ queryKey: ["meta", "trade_journal", lookbackDays], queryFn: () => api.get(`/meta/trade_journal?lookback_days=${lookbackDays}`), staleTime: 60_000 });

export const useMetaFeedback = (lookbackDays = 63) =>
  useQuery({ queryKey: ["meta", "feedback", lookbackDays], queryFn: () => api.get(`/meta/feedback?lookback_days=${lookbackDays}`), staleTime: 60_000 });
