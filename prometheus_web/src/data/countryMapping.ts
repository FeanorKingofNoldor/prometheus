/**
 * ISO 3166-1 alpha-3 → numeric code mapping for topojson feature matching.
 * The world-atlas topojson uses numeric codes as feature IDs.
 *
 * Also includes approximate country centroids [lon, lat] for markers
 * and contagion line endpoints.
 */

// ── Numeric → ISO3 reverse lookup ──────────────────────────────

export const NUMERIC_TO_ISO3: Record<string, string> = {
  "004": "AFG", "008": "ALB", "012": "DZA", "024": "AGO", "032": "ARG",
  "036": "AUS", "040": "AUT", "050": "BGD", "056": "BEL", "064": "BTN",
  "068": "BOL", "070": "BIH", "072": "BWA", "076": "BRA", "096": "BRN",
  "100": "BGR", "104": "MMR", "108": "BDI", "112": "BLR", "116": "KHM",
  "120": "CMR", "124": "CAN", "140": "CAF", "144": "LKA", "148": "TCD",
  "152": "CHL", "156": "CHN", "170": "COL", "178": "COG", "180": "COD",
  "188": "CRI", "191": "HRV", "192": "CUB", "196": "CYP", "203": "CZE",
  "204": "BEN", "208": "DNK", "214": "DOM", "218": "ECU", "818": "EGY",
  "222": "SLV", "226": "GNQ", "232": "ERI", "233": "EST", "231": "ETH",
  "246": "FIN", "250": "FRA", "266": "GAB", "270": "GMB", "268": "GEO",
  "276": "DEU", "288": "GHA", "300": "GRC", "304": "GRL", "320": "GTM",
  "324": "GIN", "328": "GUY", "332": "HTI", "340": "HND", "348": "HUN",
  "352": "ISL", "356": "IND", "360": "IDN", "364": "IRN", "368": "IRQ",
  "372": "IRL", "376": "ISR", "380": "ITA", "384": "CIV", "388": "JAM",
  "392": "JPN", "400": "JOR", "398": "KAZ", "404": "KEN", "408": "PRK",
  "410": "KOR", "414": "KWT", "417": "KGZ", "418": "LAO", "422": "LBN",
  "426": "LSO", "428": "LVA", "430": "LBR", "434": "LBY", "440": "LTU",
  "442": "LUX", "450": "MDG", "454": "MWI", "458": "MYS", "466": "MLI",
  "478": "MRT", "484": "MEX", "496": "MNG", "504": "MAR", "508": "MOZ",
  "512": "OMN", "516": "NAM", "524": "NPL", "528": "NLD", "540": "NCL",
  "554": "NZL", "558": "NIC", "562": "NER", "566": "NGA", "578": "NOR",
  "586": "PAK", "591": "PAN", "598": "PNG", "600": "PRY", "604": "PER",
  "608": "PHL", "616": "POL", "620": "PRT", "634": "QAT", "642": "ROU",
  "643": "RUS", "646": "RWA", "682": "SAU", "686": "SEN", "688": "SRB",
  "694": "SLE", "702": "SGP", "703": "SVK", "704": "VNM", "705": "SVN",
  "706": "SOM", "710": "ZAF", "716": "ZWE", "724": "ESP", "728": "SSD",
  "729": "SDN", "740": "SUR", "748": "SWZ", "752": "SWE", "756": "CHE",
  "760": "SYR", "762": "TJK", "764": "THA", "768": "TGO", "780": "TTO",
  "784": "ARE", "788": "TUN", "792": "TUR", "800": "UGA", "804": "UKR",
  "826": "GBR", "834": "TZA", "840": "USA", "854": "BFA", "858": "URY",
  "860": "UZB", "862": "VEN", "887": "YEM", "894": "ZMB",
  "498": "MDA", "499": "MNE", "807": "MKD", "585": "PLW",
  "732": "ESH", "275": "PSE", "010": "ATA", "-99": "N/A",
};

// Build reverse lookup: ISO3 → numeric string.
export const ISO3_TO_NUMERIC: Record<string, string> = {};
for (const [num, iso] of Object.entries(NUMERIC_TO_ISO3)) {
  ISO3_TO_NUMERIC[iso] = num;
}

/**
 * Convert a topojson feature ID to ISO3.
 * Returns undefined if unknown.
 */
export function featureIdToISO3(id: string): string | undefined {
  return NUMERIC_TO_ISO3[id];
}


// ── Territory → parent sovereign mapping ───────────────────────

/**
 * Maps dependent territories / autonomous regions to their parent
 * sovereign nation for data lookups and interaction on the map.
 */
export const TERRITORY_TO_PARENT: Record<string, string> = {
  GRL: "DNK",  // Greenland → Denmark
  NCL: "FRA",  // New Caledonia → France
  ESH: "MAR",  // Western Sahara → Morocco (administered)
  PSE: "ISR",  // Palestine → Israel (for map interaction only)
};

/**
 * Resolve an ISO3 code: if it's a territory, return the parent nation;
 * otherwise return the original code.
 */
export function resolveISO3(iso3: string): string {
  return TERRITORY_TO_PARENT[iso3] ?? iso3;
}


// ── Country centroids [longitude, latitude] ────────────────────

export const COUNTRY_CENTROIDS: Record<string, [number, number]> = {
  // ── Tier A: G7 + China ─────────────────────────────────
  USA: [-98, 39],
  GBR: [-2, 54],
  JPN: [138, 36],
  CHN: [104, 35],
  DEU: [10, 51],
  FRA: [2, 46],
  CAN: [-106, 56],
  ITA: [12, 43],

  // ── Tier B: Major economies ────────────────────────────
  IND: [78, 22],
  KOR: [128, 36],
  BRA: [-51, -10],
  AUS: [134, -25],
  RUS: [90, 60],
  MEX: [-102, 24],
  ESP: [-4, 40],
  IDN: [118, -2],

  // ── Tier C: Advanced / Regional powers ─────────────────
  NLD: [5, 52],
  SAU: [45, 24],
  TUR: [35, 39],
  CHE: [8, 47],
  TWN: [121, 24],
  POL: [20, 52],
  SWE: [16, 62],
  BEL: [4, 51],
  NOR: [10, 62],
  AUT: [14, 47],
  ARE: [54, 24],
  ISR: [35, 31],

  // ── Tier D: Strategic mid-tier ─────────────────────────
  IRL: [-8, 53],
  THA: [101, 15],
  SGP: [104, 1],
  DNK: [10, 56],
  MYS: [110, 3],
  ZAF: [25, -29],
  PHL: [122, 12],
  COL: [-74, 4],
  CHL: [-71, -30],
  FIN: [26, 64],
  EGY: [30, 27],
  KWT: [48, 29],

  // ── Tier E: Emerging / Niche ───────────────────────────
  CZE: [15, 50],
  VNM: [106, 16],
  PRT: [-8, 39],
  NZL: [174, -41],
  PER: [-76, -10],
  ROU: [25, 46],
  GRC: [22, 39],
  QAT: [51, 25],
  NGA: [8, 10],
  ARG: [-64, -34],

  // ── Tier F: Africa Resource Powers ─────────────────────
  AGO: [17, -12],
  COD: [24, -3],
  GHA: [-2, 8],
  KEN: [38, 0],
  ETH: [40, 9],
  TZA: [35, -6],
  CIV: [-5, 7],
  MOZ: [35, -18],
  ZMB: [28, -13],
  BWA: [24, -22],

  // ── Tier G: Africa Strategic ───────────────────────────
  NAM: [17, -22],
  GAB: [12, -1],
  GIN: [-10, 11],
  MAR: [-5, 32],
  LBY: [17, 27],
  SEN: [-14, 14],
  UGA: [32, 1],
  ZWE: [30, -20],
  SDN: [30, 13],
  CMR: [12, 6],

  // ── Tier H: Eurasia & ME Fills ─────────────────────────
  UKR: [32, 49],
  PAK: [69, 30],
  BGD: [90, 24],
  KAZ: [67, 48],
  IRN: [53, 33],
  IRQ: [44, 33],
  HUN: [20, 47],
  SRB: [21, 44],
  SVK: [19, 49],
  GEO: [44, 42],

  // ── Tier I: Americas & Asia-Pacific Fills ──────────────
  ECU: [-78, -2],
  VEN: [-66, 8],
  BOL: [-65, -17],
  URY: [-56, -33],
  PAN: [-80, 9],
  MMR: [96, 20],
  KHM: [105, 13],
  BRN: [115, 5],
  LKA: [81, 8],
  NPL: [84, 28],

  // ── Tier J: EU/Baltic + Misc ───────────────────────────
  BGR: [25, 43],
  HRV: [16, 45],
  LTU: [24, 56],
  EST: [26, 59],
  JOR: [36, 31],
  OMN: [56, 21],
  MNG: [104, 47],
  DOM: [-70, 19],
  CRI: [-84, 10],
  GTM: [-90, 15],

  // ── Non-tracked (used by chokepoints / routes) ─────────
  DJI: [43, 12],
  ERI: [39, 15],
  YEM: [48, 15],
  GNQ: [10, 2],

  // ── Remaining world nations ─────────────────────────────
  // Europe
  ISL: [-19, 65],
  LVA: [25, 57],
  BLR: [28, 53],
  LUX: [6, 50],
  MDA: [29, 47],
  SVN: [15, 46],
  BIH: [18, 44],
  MNE: [19, 43],
  ALB: [20, 41],
  MKD: [22, 41],
  CYP: [33, 35],

  // Central & West Asia
  UZB: [64, 41],
  KGZ: [74, 41],
  TJK: [71, 39],
  AFG: [67, 33],
  SYR: [38, 35],
  LBN: [36, 34],
  LAO: [102, 18],
  BTN: [90, 27],
  PRK: [127, 40],

  // Africa
  TUN: [9, 34],
  DZA: [3, 28],
  MRT: [-10, 20],
  MLI: [-2, 17],
  NER: [8, 16],
  TCD: [19, 15],
  BFA: [-2, 12],
  BEN: [2, 9],
  TGO: [1, 8],
  SLE: [-12, 9],
  LBR: [-10, 6],
  GMB: [-15, 13],
  CAF: [21, 6],
  COG: [16, -1],
  RWA: [30, -2],
  BDI: [30, -3],
  SOM: [46, 5],
  SSD: [30, 8],
  MDG: [47, -19],
  MWI: [34, -14],
  LSO: [29, -29],
  SWZ: [31, -27],

  // Americas
  CUB: [-80, 22],
  HTI: [-72, 19],
  JAM: [-77, 18],
  HND: [-87, 15],
  SLV: [-89, 14],
  NIC: [-85, 13],
  TTO: [-61, 10],
  GUY: [-59, 5],
  SUR: [-56, 4],
  PRY: [-58, -23],

  // Oceania
  PNG: [147, -6],
  NCL: [165, -22],
  PLW: [134, 7],

  // Territories
  GRL: [-42, 72],
  ESH: [-13, 24],
  PSE: [35, 32],
};


// ── Nation labels ──────────────────────────────────────────────

export const NATION_FLAGS: Record<string, string> = {
  // Tier A
  USA: "🇺🇸", GBR: "🇬🇧", JPN: "🇯🇵", CHN: "🇨🇳", DEU: "🇩🇪",
  FRA: "🇫🇷", CAN: "🇨🇦", ITA: "🇮🇹",
  // Tier B
  IND: "🇮🇳", KOR: "🇰🇷", BRA: "🇧🇷", AUS: "🇦🇺",
  RUS: "🇷🇺", MEX: "🇲🇽", ESP: "🇪🇸", IDN: "🇮🇩",
  // Tier C
  NLD: "🇳🇱", SAU: "🇸🇦", TUR: "🇹🇷", CHE: "🇨🇭",
  TWN: "🇹🇼", POL: "🇵🇱", SWE: "🇸🇪", BEL: "🇧🇪",
  NOR: "🇳🇴", AUT: "🇦🇹", ARE: "🇦🇪", ISR: "🇮🇱",
  // Tier D
  IRL: "🇮🇪", THA: "🇹🇭", SGP: "🇸🇬", DNK: "🇩🇰",
  MYS: "🇲🇾", ZAF: "🇿🇦", PHL: "🇵🇭", COL: "🇨🇴",
  CHL: "🇨🇱", FIN: "🇫🇮", EGY: "🇪🇬", KWT: "🇰🇼",
  // Tier E
  CZE: "🇨🇿", VNM: "🇻🇳", PRT: "🇵🇹", NZL: "🇳🇿",
  PER: "🇵🇪", ROU: "🇷🇴", GRC: "🇬🇷", QAT: "🇶🇦",
  NGA: "🇳🇬", ARG: "🇦🇷",
  // Tier F: Africa Resource Powers
  AGO: "🇦🇴", COD: "🇨🇩", GHA: "🇬🇭", KEN: "🇰🇪", ETH: "🇪🇹",
  TZA: "🇹🇿", CIV: "🇨🇮", MOZ: "🇲🇿", ZMB: "🇿🇲", BWA: "🇧🇼",
  // Tier G: Africa Strategic
  NAM: "🇳🇦", GAB: "🇬🇦", GIN: "🇬🇳", MAR: "🇲🇦", LBY: "🇱🇾",
  SEN: "🇸🇳", UGA: "🇺🇬", ZWE: "🇿🇼", SDN: "🇸🇩", CMR: "🇨🇲",
  // Tier H: Eurasia & ME Fills
  UKR: "🇺🇦", PAK: "🇵🇰", BGD: "🇧🇩", KAZ: "🇰🇿", IRN: "🇮🇷",
  IRQ: "🇮🇶", HUN: "🇭🇺", SRB: "🇷🇸", SVK: "🇸🇰", GEO: "🇬🇪",
  // Tier I: Americas & Asia-Pacific Fills
  ECU: "🇪🇨", VEN: "🇻🇪", BOL: "🇧🇴", URY: "🇺🇾", PAN: "🇵🇦",
  MMR: "🇲🇲", KHM: "🇰🇭", BRN: "🇧🇳", LKA: "🇱🇰", NPL: "🇳🇵",
  // Tier J: EU/Baltic + Misc
  BGR: "🇧🇬", HRV: "🇭🇷", LTU: "🇱🇹", EST: "🇪🇪", JOR: "🇯🇴",
  OMN: "🇴🇲", MNG: "🇲🇳", DOM: "🇩🇴", CRI: "🇨🇷", GTM: "🇬🇹",
  // Non-tracked (overlays only)
  DJI: "🇩🇯", ERI: "🇪🇷", YEM: "🇾🇪", GNQ: "🇬🇶",
  // Remaining world nations
  ISL: "🇮🇸", LVA: "🇱🇻", BLR: "🇧🇾", LUX: "🇱🇺", MDA: "🇲🇩",
  SVN: "🇸🇮", BIH: "🇧🇦", MNE: "🇲🇪", ALB: "🇦🇱", MKD: "🇲🇰",
  CYP: "🇨🇾", UZB: "🇺🇿", KGZ: "🇰🇬", TJK: "🇹🇯", AFG: "🇦🇫",
  SYR: "🇸🇾", LBN: "🇱🇧", LAO: "🇱🇦", BTN: "🇧🇹", PRK: "🇰🇵",
  TUN: "🇹🇳", DZA: "🇩🇿", MRT: "🇲🇷", MLI: "🇲🇱", NER: "🇳🇪",
  TCD: "🇹🇩", BFA: "🇧🇫", BEN: "🇧🇯", TGO: "🇹🇬", SLE: "🇸🇱",
  LBR: "🇱🇷", GMB: "🇬🇲", CAF: "🇨🇫", COG: "🇨🇬", RWA: "🇷🇼",
  BDI: "🇧🇮", SOM: "🇸🇴", SSD: "🇸🇸", MDG: "🇲🇬", MWI: "🇲🇼",
  LSO: "🇱🇸", SWZ: "🇸🇿", CUB: "🇨🇺", HTI: "🇭🇹", JAM: "🇯🇲",
  HND: "🇭🇳", SLV: "🇸🇻", NIC: "🇳🇮", TTO: "🇹🇹", GUY: "🇬🇾",
  SUR: "🇸🇷", PRY: "🇵🇾", PNG: "🇵🇬", NCL: "🇳🇨", PLW: "🇵🇼",
  GRL: "🇬🇱", ESH: "🇪🇭", PSE: "🇵🇸",
};

export const NATION_NAMES: Record<string, string> = {
  // Tier A
  USA: "United States", GBR: "United Kingdom", JPN: "Japan",
  CHN: "China", DEU: "Germany", FRA: "France", CAN: "Canada", ITA: "Italy",
  // Tier B
  IND: "India", KOR: "South Korea", BRA: "Brazil", AUS: "Australia",
  RUS: "Russia", MEX: "Mexico", ESP: "Spain", IDN: "Indonesia",
  // Tier C
  NLD: "Netherlands", SAU: "Saudi Arabia", TUR: "Turkey", CHE: "Switzerland",
  TWN: "Taiwan", POL: "Poland", SWE: "Sweden", BEL: "Belgium",
  NOR: "Norway", AUT: "Austria", ARE: "UAE", ISR: "Israel",
  // Tier D
  IRL: "Ireland", THA: "Thailand", SGP: "Singapore", DNK: "Denmark",
  MYS: "Malaysia", ZAF: "South Africa", PHL: "Philippines", COL: "Colombia",
  CHL: "Chile", FIN: "Finland", EGY: "Egypt", KWT: "Kuwait",
  // Tier E
  CZE: "Czechia", VNM: "Vietnam", PRT: "Portugal", NZL: "New Zealand",
  PER: "Peru", ROU: "Romania", GRC: "Greece", QAT: "Qatar",
  NGA: "Nigeria", ARG: "Argentina",
  // Tier F: Africa Resource Powers
  AGO: "Angola", COD: "DR Congo", GHA: "Ghana", KEN: "Kenya", ETH: "Ethiopia",
  TZA: "Tanzania", CIV: "Côte d'Ivoire", MOZ: "Mozambique", ZMB: "Zambia", BWA: "Botswana",
  // Tier G: Africa Strategic
  NAM: "Namibia", GAB: "Gabon", GIN: "Guinea", MAR: "Morocco", LBY: "Libya",
  SEN: "Senegal", UGA: "Uganda", ZWE: "Zimbabwe", SDN: "Sudan", CMR: "Cameroon",
  // Tier H: Eurasia & ME Fills
  UKR: "Ukraine", PAK: "Pakistan", BGD: "Bangladesh", KAZ: "Kazakhstan", IRN: "Iran",
  IRQ: "Iraq", HUN: "Hungary", SRB: "Serbia", SVK: "Slovakia", GEO: "Georgia",
  // Tier I: Americas & Asia-Pacific Fills
  ECU: "Ecuador", VEN: "Venezuela", BOL: "Bolivia", URY: "Uruguay", PAN: "Panama",
  MMR: "Myanmar", KHM: "Cambodia", BRN: "Brunei", LKA: "Sri Lanka", NPL: "Nepal",
  // Tier J: EU/Baltic + Misc
  BGR: "Bulgaria", HRV: "Croatia", LTU: "Lithuania", EST: "Estonia", JOR: "Jordan",
  OMN: "Oman", MNG: "Mongolia", DOM: "Dominican Republic", CRI: "Costa Rica", GTM: "Guatemala",
  // Non-tracked
  DJI: "Djibouti", ERI: "Eritrea", YEM: "Yemen", GNQ: "Equatorial Guinea",
  // Remaining world nations – Europe
  ISL: "Iceland", LVA: "Latvia", BLR: "Belarus", LUX: "Luxembourg",
  MDA: "Moldova", SVN: "Slovenia", BIH: "Bosnia & Herzegovina",
  MNE: "Montenegro", ALB: "Albania", MKD: "North Macedonia", CYP: "Cyprus",
  // Central & West Asia
  UZB: "Uzbekistan", KGZ: "Kyrgyzstan", TJK: "Tajikistan",
  AFG: "Afghanistan", SYR: "Syria", LBN: "Lebanon", LAO: "Laos",
  BTN: "Bhutan", PRK: "North Korea",
  // Africa
  TUN: "Tunisia", DZA: "Algeria", MRT: "Mauritania", MLI: "Mali",
  NER: "Niger", TCD: "Chad", BFA: "Burkina Faso", BEN: "Benin",
  TGO: "Togo", SLE: "Sierra Leone", LBR: "Liberia", GMB: "Gambia",
  CAF: "Central African Republic", COG: "Republic of the Congo",
  RWA: "Rwanda", BDI: "Burundi", SOM: "Somalia", SSD: "South Sudan",
  MDG: "Madagascar", MWI: "Malawi", LSO: "Lesotho", SWZ: "Eswatini",
  // Americas
  CUB: "Cuba", HTI: "Haiti", JAM: "Jamaica", HND: "Honduras",
  SLV: "El Salvador", NIC: "Nicaragua", TTO: "Trinidad & Tobago",
  GUY: "Guyana", SUR: "Suriname", PRY: "Paraguay",
  // Oceania
  PNG: "Papua New Guinea", NCL: "New Caledonia", PLW: "Palau",
  // Territories
  GRL: "Greenland", ESH: "Western Sahara", PSE: "Palestine",
};
