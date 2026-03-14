import { useMemo } from "react";
import { useSearchParams, Link } from "react-router-dom";
import { PageHeader } from "../components/PageHeader";
import { Panel } from "../components/Panel";
import { useNationPersons } from "../api/hooks";
import { NATION_FLAGS, NATION_NAMES } from "../data/countryMapping";

// ── Types ────────────────────────────────────────────────────

interface WikipediaData {
  thumbnail_url?: string;
  extract?: string;
  page_url?: string;
}

interface Person {
  profile_id: string;
  person_name: string;
  nation: string;
  role: string;
  role_tier: number;
  in_role_since: string;
  expected_term_end?: string | null;
  policy_stance: Record<string, number>;
  scores: Record<string, number>;
  background: Record<string, unknown>;
  behavioral: Record<string, unknown>;
  confidence: number;
  last_updated?: string | null;
  metadata?: { wikipedia?: WikipediaData; social?: Record<string, string> };
}

// ── Role relevance descriptions ──────────────────────────────

const ROLE_DESCRIPTIONS: Record<string, string> = {
  head_of_state: "Sets top-level national policy, controls executive direction, drives geopolitical posture. Key for sanctions, trade deals, diplomatic shifts.",
  head_of_government: "Directs day-to-day governance, fiscal policy, and regulatory agenda. Immediate impact on business climate and reform trajectory.",
  central_bank_governor: "Controls monetary policy, interest rates, and currency stability. Directly affects bond yields, FX, and inflation expectations.",
  finance_minister: "Manages fiscal budget, taxation, debt issuance, and sovereign credit outlook. Key signal for bond markets and fiscal sustainability.",
  foreign_minister: "Shapes diplomatic relationships, trade negotiations, and alliance dynamics. Leading indicator for geopolitical risk shifts.",
  defense_minister: "Drives defense spending, arms procurement, and military posture. Critical for defense-sector exposure and conflict probability.",
  trade_minister: "Leads trade agreements, tariff policy, and export controls. Direct impact on supply chains and bilateral trade flows.",
  energy_minister: "Oversees energy policy, fossil fuel production targets, and transition strategy. Key for commodity pricing and energy security.",
};

function getWhyWeTrack(role: string, tier: number): string {
  const base = ROLE_DESCRIPTIONS[role];
  if (base) return base;
  if (tier === 1) return "Tier 1 leader — top decision-maker with direct impact on national policy, international relations, and market sentiment.";
  if (tier === 2) return "Tier 2 official — significant influence on sector-specific policy, regulatory direction, and institutional positioning.";
  return "Tier 3 official — relevant to specific policy areas tracked by the system for early-warning signals.";
}

// ── Component ────────────────────────────────────────────────

export default function PersonProfile() {
  const [searchParams] = useSearchParams();
  const profileId = searchParams.get("id") ?? "";
  const nation = searchParams.get("nation") ?? "";

  const personsQuery = useNationPersons(nation);
  const allPersons = (personsQuery.data ?? []) as Person[];

  const person = useMemo(
    () => allPersons.find((p) => p.profile_id === profileId) ?? null,
    [allPersons, profileId],
  );

  if (!nation || !profileId) {
    return (
      <div className="py-20 text-center text-sm text-muted">
        Missing nation or profile ID. <Link to="/geo" className="text-accent hover:underline">Back to map</Link>
      </div>
    );
  }

  if (personsQuery.isLoading) {
    return <div className="py-20 text-center text-xs text-muted">Loading profile…</div>;
  }

  if (!person) {
    return (
      <div className="py-20 text-center text-sm text-muted">
        Profile not found. <Link to={`/nation?n=${nation}`} className="text-accent hover:underline">Back to {NATION_NAMES[nation] ?? nation}</Link>
      </div>
    );
  }

  const wiki = person.metadata?.wikipedia;
  const social = person.metadata?.social ?? {};
  const flag = NATION_FLAGS[person.nation] ?? "";
  const nationName = NATION_NAMES[person.nation] ?? person.nation;
  const tierColors: Record<number, string> = { 1: "text-accent", 2: "text-blue-400", 3: "text-zinc-400" };
  const tierLabels: Record<number, string> = { 1: "Tier 1 · Head of State / Key Leader", 2: "Tier 2 · Senior Official", 3: "Tier 3 · Policy Official" };
  const stances = Object.entries(person.policy_stance ?? {});
  const scores = Object.entries(person.scores ?? {});
  const bgEntries = Object.entries(person.background ?? {});
  const behEntries = Object.entries(person.behavioral ?? {});

  return (
    <div className="space-y-4">
      <PageHeader
        title={person.person_name}
        subtitle={`${flag} ${nationName} · ${person.role.replace(/_/g, " ")}`}
        onRefresh={() => personsQuery.refetch()}
      />

      <div className="grid gap-4 lg:grid-cols-3">
        {/* ── Left column: Photo + Bio ── */}
        <div className="lg:col-span-1 space-y-4">
          <Panel>
            <div className="flex flex-col items-center">
              {wiki?.thumbnail_url ? (
                <img
                  src={wiki.thumbnail_url}
                  alt={person.person_name}
                  className="h-32 w-32 rounded-full object-cover border-2 border-border-dim mb-3"
                />
              ) : (
                <div className="flex h-32 w-32 items-center justify-center rounded-full bg-surface-raised text-4xl text-muted mb-3">
                  {person.person_name.split(" ").map((n) => n[0]).join("").slice(0, 2)}
                </div>
              )}

              <h2 className={`text-lg font-bold ${tierColors[person.role_tier] ?? "text-zinc-100"}`}>
                {person.person_name}
              </h2>
              <div className="text-xs text-muted mt-0.5">
                {person.role.replace(/_/g, " ")}
              </div>
              <div className="text-[10px] text-muted mt-0.5">
                {tierLabels[person.role_tier] ?? `Tier ${person.role_tier}`}
              </div>

              {/* Time in role */}
              <div className="flex gap-4 mt-3 text-[10px] text-zinc-400">
                <div>
                  <span className="text-muted">In role since:</span>{" "}
                  {person.in_role_since}
                </div>
                {person.expected_term_end && (
                  <div>
                    <span className="text-muted">Term ends:</span>{" "}
                    {person.expected_term_end}
                  </div>
                )}
              </div>

              {/* Confidence meter */}
              <div className="w-full mt-4">
                <div className="flex justify-between text-[9px] text-muted mb-0.5">
                  <span>Profile confidence</span>
                  <span>{(person.confidence * 100).toFixed(0)}%</span>
                </div>
                <div className="h-1.5 rounded-full bg-surface-raised overflow-hidden">
                  <div
                    className="h-full rounded-full bg-accent transition-all"
                    style={{ width: `${person.confidence * 100}%` }}
                  />
                </div>
              </div>
            </div>
          </Panel>

          {/* Links */}
          <Panel title="Links">
            <div className="space-y-1.5">
              {wiki?.page_url && (
                <a
                  href={wiki.page_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-2 rounded bg-surface-overlay px-3 py-1.5 text-xs text-zinc-300 hover:text-zinc-100 hover:bg-surface-raised transition-colors"
                >
                  <span className="text-sm">📖</span> Wikipedia
                </a>
              )}
              {Object.entries(social).map(([platform, url]) => (
                <a
                  key={platform}
                  href={url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-2 rounded bg-surface-overlay px-3 py-1.5 text-xs text-zinc-300 hover:text-zinc-100 hover:bg-surface-raised transition-colors"
                >
                  <span className="text-sm">
                    {platform === "twitter" || platform === "x" ? "𝕏" : platform === "linkedin" ? "in" : "🔗"}
                  </span>{" "}
                  {platform.charAt(0).toUpperCase() + platform.slice(1)}
                </a>
              ))}
              <Link
                to={`/nation?n=${person.nation}`}
                className="flex items-center gap-2 rounded bg-surface-overlay px-3 py-1.5 text-xs text-zinc-300 hover:text-zinc-100 hover:bg-surface-raised transition-colors"
              >
                <span className="text-sm">{flag}</span> {nationName} Profile
              </Link>
            </div>
          </Panel>
        </div>

        {/* ── Right column: Details ── */}
        <div className="lg:col-span-2 space-y-4">
          {/* Bio */}
          {wiki?.extract && (
            <Panel title="Biography">
              <p className="text-xs leading-relaxed text-zinc-300">{wiki.extract}</p>
            </Panel>
          )}

          {/* Why We Track */}
          <Panel title="Why We Track This Position">
            <p className="text-xs leading-relaxed text-zinc-300">
              {getWhyWeTrack(person.role, person.role_tier)}
            </p>
          </Panel>

          {/* Policy Stances */}
          {stances.length > 0 && (
            <Panel title="Policy Stances">
              <div className="space-y-2">
                {stances.map(([key, value]) => (
                  <div key={key}>
                    <div className="flex justify-between text-[10px] mb-0.5">
                      <span className="text-zinc-300">{key.replace(/_/g, " ")}</span>
                      <span className="text-zinc-400 tabular-nums">{typeof value === "number" ? value.toFixed(2) : String(value)}</span>
                    </div>
                    {typeof value === "number" && (
                      <div className="h-1.5 rounded-full bg-surface-raised overflow-hidden">
                        <div
                          className="h-full rounded-full transition-all"
                          style={{
                            width: `${Math.min(Math.abs(value) * 100, 100)}%`,
                            backgroundColor: value >= 0.5 ? "#22c55e" : value >= 0 ? "#facc15" : "#ef4444",
                          }}
                        />
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </Panel>
          )}

          {/* Scores */}
          {scores.length > 0 && (
            <Panel title="Assessment Scores">
              <div className="grid gap-3 sm:grid-cols-2">
                {scores.map(([key, value]) => (
                  <div key={key} className="rounded bg-surface-overlay p-2">
                    <div className="text-[10px] text-muted">{key.replace(/_/g, " ")}</div>
                    <div className="text-sm font-bold text-zinc-100 tabular-nums">
                      {typeof value === "number" ? value.toFixed(2) : String(value)}
                    </div>
                  </div>
                ))}
              </div>
            </Panel>
          )}

          {/* Background + Behavioral */}
          {(bgEntries.length > 0 || behEntries.length > 0) && (
            <div className="grid gap-4 sm:grid-cols-2">
              {bgEntries.length > 0 && (
                <Panel title="Background">
                  <div className="space-y-1.5">
                    {bgEntries.map(([key, value]) => (
                      <div key={key}>
                        <span className="text-[10px] text-muted">{key.replace(/_/g, " ")}: </span>
                        <span className="text-[11px] text-zinc-300">{String(value)}</span>
                      </div>
                    ))}
                  </div>
                </Panel>
              )}
              {behEntries.length > 0 && (
                <Panel title="Behavioral Profile">
                  <div className="space-y-1.5">
                    {behEntries.map(([key, value]) => (
                      <div key={key}>
                        <span className="text-[10px] text-muted">{key.replace(/_/g, " ")}: </span>
                        <span className="text-[11px] text-zinc-300">{String(value)}</span>
                      </div>
                    ))}
                  </div>
                </Panel>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
