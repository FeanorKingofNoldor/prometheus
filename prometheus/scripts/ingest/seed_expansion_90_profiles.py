"""Prometheus v2 – Seed person profiles for 90-nation expansion.

Seeds 3 Tier 1 officials per nation (head of state/government, central
bank governor, finance minister) for the 90 nations that lack DB data.

Nations already seeded (10): USA, GBR, JPN, CHN, DEU, FRA, CAN, AUS, CHE, KOR

Usage:
    python -m prometheus.scripts.ingest.seed_expansion_90_profiles
    python -m prometheus.scripts.ingest.seed_expansion_90_profiles --dry-run
    python -m prometheus.scripts.ingest.seed_expansion_90_profiles --no-llm
    python -m prometheus.scripts.ingest.seed_expansion_90_profiles --nation ITA
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import date
from typing import Optional, Sequence

from apatheon.core.database import get_db_manager
from apatheon.core.ids import generate_uuid
from apatheon.core.logging import get_logger
from psycopg2.extras import Json

logger = get_logger(__name__)


# ── Official definition ──────────────────────────────────────────────────


@dataclass
class OfficialDef:
    """Compact definition for a Tier 1 official to seed."""

    nation: str
    profile_id: str
    position_id: str
    person_name: str
    role: str
    role_description: str
    in_role_since: date
    expected_term_end: date | None = None


# ── Compact tuple data ───────────────────────────────────────────────────
# Format: (nation, prof_sfx, pos_sfx, name, role, role_desc,
#           (y, m, d), end_tuple | None)
# profile_id  = f"{nation}_{prof_sfx}_PROFILE"
# position_id = f"{nation}_{pos_sfx}"

_RAW: list[tuple] = [
    # ══════════════════════════════════════════════════════════════════════
    # TIER A: G7 + China  (only ITA missing)
    # ══════════════════════════════════════════════════════════════════════

    # ── ITA ───────────────────────────────────────────────────────────
    ("ITA", "PM", "PRIME_MINISTER", "Giorgia Meloni", "PRIME_MINISTER",
     "Prime Minister of Italy", (2022, 10, 22), None),
    ("ITA", "BDI_GOV", "BDI_GOVERNOR", "Fabio Panetta", "BDI_GOVERNOR",
     "Governor of the Bank of Italy", (2023, 11, 1), None),
    ("ITA", "FM", "FINANCE_MINISTER", "Giancarlo Giorgetti", "FINANCE_MINISTER",
     "Minister of Economy and Finance of Italy", (2022, 10, 22), None),

    # ══════════════════════════════════════════════════════════════════════
    # TIER B: Major economies  (IND, BRA, RUS, MEX, ESP, IDN)
    # ══════════════════════════════════════════════════════════════════════

    # ── IND ───────────────────────────────────────────────────────────
    ("IND", "PM", "PRIME_MINISTER", "Narendra Modi", "PRIME_MINISTER",
     "Prime Minister of India", (2014, 5, 26), None),
    ("IND", "RBI_GOV", "RBI_GOVERNOR", "Sanjay Malhotra", "RBI_GOVERNOR",
     "Governor of the Reserve Bank of India", (2024, 12, 11), None),
    ("IND", "FM", "FINANCE_MINISTER", "Nirmala Sitharaman", "FINANCE_MINISTER",
     "Minister of Finance of India", (2019, 5, 31), None),

    # ── BRA ───────────────────────────────────────────────────────────
    ("BRA", "PRESIDENT", "PRESIDENT", "Luiz Inácio Lula da Silva", "PRESIDENT",
     "President of Brazil", (2023, 1, 1), (2027, 1, 1)),
    ("BRA", "BCB_GOV", "BCB_GOVERNOR", "Gabriel Galípolo", "BCB_GOVERNOR",
     "Governor of the Central Bank of Brazil", (2025, 1, 1), None),
    ("BRA", "FM", "FINANCE_MINISTER", "Fernando Haddad", "FINANCE_MINISTER",
     "Minister of Finance of Brazil", (2023, 1, 1), None),

    # ── RUS ───────────────────────────────────────────────────────────
    ("RUS", "PRESIDENT", "PRESIDENT", "Vladimir Putin", "PRESIDENT",
     "President of Russia", (2024, 5, 7), (2030, 5, 7)),
    ("RUS", "CBR_GOV", "CBR_GOVERNOR", "Elvira Nabiullina", "CBR_GOVERNOR",
     "Governor of the Central Bank of Russia", (2013, 6, 24), None),
    ("RUS", "FM", "FINANCE_MINISTER", "Anton Siluanov", "FINANCE_MINISTER",
     "Minister of Finance of Russia", (2011, 12, 16), None),

    # ── MEX ───────────────────────────────────────────────────────────
    ("MEX", "PRESIDENT", "PRESIDENT", "Claudia Sheinbaum", "PRESIDENT",
     "President of Mexico", (2024, 10, 1), (2030, 10, 1)),
    ("MEX", "BANXICO_GOV", "BANXICO_GOVERNOR", "Victoria Rodríguez Ceja",
     "BANXICO_GOVERNOR", "Governor of the Bank of Mexico", (2022, 1, 1),
     (2027, 12, 31)),
    ("MEX", "FM", "FINANCE_MINISTER", "Rogelio Ramírez de la O", "FINANCE_MINISTER",
     "Secretary of Finance of Mexico", (2024, 10, 1), None),

    # ── ESP ───────────────────────────────────────────────────────────
    ("ESP", "PM", "PRIME_MINISTER", "Pedro Sánchez", "PRIME_MINISTER",
     "Prime Minister of Spain", (2018, 6, 2), None),
    ("ESP", "BDE_GOV", "BDE_GOVERNOR", "José Luis Escrivá", "BDE_GOVERNOR",
     "Governor of the Bank of Spain", (2024, 9, 1), None),
    ("ESP", "FM", "FINANCE_MINISTER", "Carlos Cuerpo", "FINANCE_MINISTER",
     "Minister of Economy of Spain", (2023, 11, 21), None),

    # ── IDN ───────────────────────────────────────────────────────────
    ("IDN", "PRESIDENT", "PRESIDENT", "Prabowo Subianto", "PRESIDENT",
     "President of Indonesia", (2024, 10, 20), (2029, 10, 20)),
    ("IDN", "BI_GOV", "BI_GOVERNOR", "Perry Warjiyo", "BI_GOVERNOR",
     "Governor of Bank Indonesia", (2018, 5, 24), None),
    ("IDN", "FM", "FINANCE_MINISTER", "Sri Mulyani Indrawati", "FINANCE_MINISTER",
     "Minister of Finance of Indonesia", (2024, 10, 21), None),

    # ══════════════════════════════════════════════════════════════════════
    # TIER C: Advanced / Regional powers
    # (NLD, SAU, TUR, TWN, POL, SWE, BEL, NOR, AUT, ARE, ISR)
    # ══════════════════════════════════════════════════════════════════════

    # ── NLD ───────────────────────────────────────────────────────────
    ("NLD", "PM", "PRIME_MINISTER", "Dick Schoof", "PRIME_MINISTER",
     "Prime Minister of the Netherlands", (2024, 7, 2), None),
    ("NLD", "DNB_PRES", "DNB_PRESIDENT", "Klaas Knot", "DNB_PRESIDENT",
     "President of De Nederlandsche Bank", (2011, 7, 1), None),
    ("NLD", "FM", "FINANCE_MINISTER", "Eelco Heinen", "FINANCE_MINISTER",
     "Minister of Finance of the Netherlands", (2024, 7, 2), None),

    # ── SAU ───────────────────────────────────────────────────────────
    ("SAU", "PM", "PRIME_MINISTER", "Mohammed bin Salman", "CROWN_PRINCE_PM",
     "Crown Prince and Prime Minister of Saudi Arabia", (2022, 9, 27), None),
    ("SAU", "SAMA_GOV", "SAMA_GOVERNOR", "Ayman al-Sayari", "SAMA_GOVERNOR",
     "Governor of the Saudi Central Bank (SAMA)", (2023, 2, 1), None),
    ("SAU", "FM", "FINANCE_MINISTER", "Mohammed Al-Jadaan", "FINANCE_MINISTER",
     "Minister of Finance of Saudi Arabia", (2017, 1, 1), None),

    # ── TUR ───────────────────────────────────────────────────────────
    ("TUR", "PRESIDENT", "PRESIDENT", "Recep Tayyip Erdoğan", "PRESIDENT",
     "President of Turkey", (2023, 6, 3), (2028, 6, 3)),
    ("TUR", "CBRT_GOV", "CBRT_GOVERNOR", "Fatih Karahan", "CBRT_GOVERNOR",
     "Governor of the Central Bank of Turkey", (2024, 2, 6), None),
    ("TUR", "FM", "FINANCE_MINISTER", "Mehmet Şimşek", "FINANCE_MINISTER",
     "Minister of Treasury and Finance of Turkey", (2023, 6, 3), None),

    # ── TWN ───────────────────────────────────────────────────────────
    ("TWN", "PRESIDENT", "PRESIDENT", "Lai Ching-te", "PRESIDENT",
     "President of the Republic of China (Taiwan)", (2024, 5, 20), (2028, 5, 20)),
    ("TWN", "CBC_GOV", "CBC_GOVERNOR", "Yang Chin-long", "CBC_GOVERNOR",
     "Governor of the Central Bank of the Republic of China", (2018, 2, 26), None),
    ("TWN", "FM", "FINANCE_MINISTER", "Chuang Tsui-yun", "FINANCE_MINISTER",
     "Minister of Finance of Taiwan", (2023, 2, 1), None),

    # ── POL ───────────────────────────────────────────────────────────
    ("POL", "PM", "PRIME_MINISTER", "Donald Tusk", "PRIME_MINISTER",
     "Prime Minister of Poland", (2023, 12, 13), None),
    ("POL", "NBP_PRES", "NBP_PRESIDENT", "Adam Glapiński", "NBP_PRESIDENT",
     "President of the National Bank of Poland", (2016, 6, 21), None),
    ("POL", "FM", "FINANCE_MINISTER", "Andrzej Domański", "FINANCE_MINISTER",
     "Minister of Finance of Poland", (2023, 12, 13), None),

    # ── SWE ───────────────────────────────────────────────────────────
    ("SWE", "PM", "PRIME_MINISTER", "Ulf Kristersson", "PRIME_MINISTER",
     "Prime Minister of Sweden", (2022, 10, 18), None),
    ("SWE", "RIKS_GOV", "RIKSBANK_GOVERNOR", "Erik Thedéen", "RIKSBANK_GOVERNOR",
     "Governor of the Sveriges Riksbank", (2023, 1, 1), None),
    ("SWE", "FM", "FINANCE_MINISTER", "Elisabeth Svantesson", "FINANCE_MINISTER",
     "Minister of Finance of Sweden", (2022, 10, 18), None),

    # ── BEL ───────────────────────────────────────────────────────────
    ("BEL", "PM", "PRIME_MINISTER", "Alexander De Croo", "PRIME_MINISTER",
     "Prime Minister of Belgium", (2020, 10, 1), None),
    ("BEL", "NBB_GOV", "NBB_GOVERNOR", "Pierre Wunsch", "NBB_GOVERNOR",
     "Governor of the National Bank of Belgium", (2019, 1, 2), None),
    ("BEL", "FM", "FINANCE_MINISTER", "Vincent Van Peteghem", "FINANCE_MINISTER",
     "Minister of Finance of Belgium", (2020, 10, 1), None),

    # ── NOR ───────────────────────────────────────────────────────────
    ("NOR", "PM", "PRIME_MINISTER", "Jonas Gahr Støre", "PRIME_MINISTER",
     "Prime Minister of Norway", (2021, 10, 14), None),
    ("NOR", "NB_GOV", "NORGES_BANK_GOVERNOR", "Ida Wolden Bache",
     "NORGES_BANK_GOVERNOR", "Governor of Norges Bank", (2022, 3, 1), None),
    ("NOR", "FM", "FINANCE_MINISTER", "Trygve Slagsvold Vedum", "FINANCE_MINISTER",
     "Minister of Finance of Norway", (2021, 10, 14), None),

    # ── AUT ───────────────────────────────────────────────────────────
    ("AUT", "CHANCELLOR", "CHANCELLOR", "Karl Nehammer", "CHANCELLOR",
     "Chancellor of Austria", (2021, 12, 6), None),
    ("AUT", "OENB_GOV", "OENB_GOVERNOR", "Robert Holzmann", "OENB_GOVERNOR",
     "Governor of the Oesterreichische Nationalbank", (2019, 9, 1), None),
    ("AUT", "FM", "FINANCE_MINISTER", "Magnus Brunner", "FINANCE_MINISTER",
     "Minister of Finance of Austria", (2021, 12, 6), None),

    # ── ARE ───────────────────────────────────────────────────────────
    ("ARE", "PRESIDENT", "PRESIDENT", "Mohammed bin Zayed Al Nahyan", "PRESIDENT",
     "President of the United Arab Emirates", (2022, 5, 14), None),
    ("ARE", "CBUAE_GOV", "CBUAE_GOVERNOR", "Khaled Mohamed Balama", "CBUAE_GOVERNOR",
     "Governor of the Central Bank of the UAE", (2021, 1, 1), None),
    ("ARE", "FM", "FINANCE_MINISTER", "Mohamed Hadi Al Hussaini", "FINANCE_MINISTER",
     "Minister of State for Financial Affairs of the UAE", (2021, 7, 1), None),

    # ── ISR ───────────────────────────────────────────────────────────
    ("ISR", "PM", "PRIME_MINISTER", "Benjamin Netanyahu", "PRIME_MINISTER",
     "Prime Minister of Israel", (2022, 12, 29), None),
    ("ISR", "BOI_GOV", "BOI_GOVERNOR", "Amir Yaron", "BOI_GOVERNOR",
     "Governor of the Bank of Israel", (2018, 11, 13), None),
    ("ISR", "FM", "FINANCE_MINISTER", "Bezalel Smotrich", "FINANCE_MINISTER",
     "Minister of Finance of Israel", (2022, 12, 29), None),

    # ══════════════════════════════════════════════════════════════════════
    # TIER D: Strategic mid-tier
    # (IRL, THA, SGP, DNK, MYS, ZAF, PHL, COL, CHL, FIN, EGY, KWT)
    # ══════════════════════════════════════════════════════════════════════

    # ── IRL ───────────────────────────────────────────────────────────
    ("IRL", "PM", "TAOISEACH", "Simon Harris", "TAOISEACH",
     "Taoiseach of Ireland", (2024, 4, 9), None),
    ("IRL", "CBI_GOV", "CBI_GOVERNOR", "Gabriel Makhlouf", "CBI_GOVERNOR",
     "Governor of the Central Bank of Ireland", (2019, 9, 1), None),
    ("IRL", "FM", "FINANCE_MINISTER", "Jack Chambers", "FINANCE_MINISTER",
     "Minister for Finance of Ireland", (2024, 4, 9), None),

    # ── THA ───────────────────────────────────────────────────────────
    ("THA", "PM", "PRIME_MINISTER", "Paetongtarn Shinawatra", "PRIME_MINISTER",
     "Prime Minister of Thailand", (2024, 8, 18), None),
    ("THA", "BOT_GOV", "BOT_GOVERNOR", "Sethaput Suthiwartnarueput", "BOT_GOVERNOR",
     "Governor of the Bank of Thailand", (2020, 10, 1), None),
    ("THA", "FM", "FINANCE_MINISTER", "Pichai Chunhavajira", "FINANCE_MINISTER",
     "Minister of Finance of Thailand", (2024, 9, 3), None),

    # ── SGP ───────────────────────────────────────────────────────────
    ("SGP", "PM", "PRIME_MINISTER", "Lawrence Wong", "PRIME_MINISTER",
     "Prime Minister of Singapore", (2024, 5, 15), None),
    ("SGP", "MAS_MD", "MAS_MANAGING_DIRECTOR", "Chia Der Jiun", "MAS_MANAGING_DIRECTOR",
     "Managing Director of the Monetary Authority of Singapore", (2022, 1, 1), None),
    ("SGP", "FM", "FINANCE_MINISTER", "Indranee Rajah", "FINANCE_MINISTER",
     "Second Minister for Finance of Singapore", (2024, 5, 15), None),

    # ── DNK ───────────────────────────────────────────────────────────
    ("DNK", "PM", "PRIME_MINISTER", "Mette Frederiksen", "PRIME_MINISTER",
     "Prime Minister of Denmark", (2019, 6, 27), None),
    ("DNK", "DN_GOV", "DN_GOVERNOR", "Christian Kettel Thomsen", "DN_GOVERNOR",
     "Governor of Danmarks Nationalbank", (2023, 2, 1), None),
    ("DNK", "FM", "FINANCE_MINISTER", "Nicolai Wammen", "FINANCE_MINISTER",
     "Minister of Finance of Denmark", (2019, 6, 27), None),

    # ── MYS ───────────────────────────────────────────────────────────
    ("MYS", "PM", "PRIME_MINISTER", "Anwar Ibrahim", "PRIME_MINISTER",
     "Prime Minister of Malaysia", (2022, 11, 24), None),
    ("MYS", "BNM_GOV", "BNM_GOVERNOR", "Abdul Rasheed Ghaffour", "BNM_GOVERNOR",
     "Governor of Bank Negara Malaysia", (2024, 7, 1), None),
    ("MYS", "FM", "FINANCE_MINISTER", "Amir Hamzah Azizan", "FINANCE_MINISTER",
     "Second Minister of Finance of Malaysia", (2022, 12, 3), None),

    # ── ZAF ───────────────────────────────────────────────────────────
    ("ZAF", "PRESIDENT", "PRESIDENT", "Cyril Ramaphosa", "PRESIDENT",
     "President of South Africa", (2018, 2, 15), None),
    ("ZAF", "SARB_GOV", "SARB_GOVERNOR", "Lesetja Kganyago", "SARB_GOVERNOR",
     "Governor of the South African Reserve Bank", (2014, 11, 9), None),
    ("ZAF", "FM", "FINANCE_MINISTER", "Enoch Godongwana", "FINANCE_MINISTER",
     "Minister of Finance of South Africa", (2021, 8, 5), None),

    # ── PHL ───────────────────────────────────────────────────────────
    ("PHL", "PRESIDENT", "PRESIDENT", "Ferdinand Marcos Jr.", "PRESIDENT",
     "President of the Philippines", (2022, 6, 30), (2028, 6, 30)),
    ("PHL", "BSP_GOV", "BSP_GOVERNOR", "Eli Remolona Jr.", "BSP_GOVERNOR",
     "Governor of the Bangko Sentral ng Pilipinas", (2023, 7, 3), None),
    ("PHL", "FM", "FINANCE_MINISTER", "Ralph Recto", "FINANCE_MINISTER",
     "Secretary of Finance of the Philippines", (2024, 7, 1), None),

    # ── COL ───────────────────────────────────────────────────────────
    ("COL", "PRESIDENT", "PRESIDENT", "Gustavo Petro", "PRESIDENT",
     "President of Colombia", (2022, 8, 7), (2026, 8, 7)),
    ("COL", "BANREP_GOV", "BANREP_GOVERNOR", "Leonardo Villar", "BANREP_GOVERNOR",
     "Governor of the Bank of the Republic (Colombia)", (2021, 1, 4), None),
    ("COL", "FM", "FINANCE_MINISTER", "Ricardo Bonilla", "FINANCE_MINISTER",
     "Minister of Finance of Colombia", (2023, 7, 1), None),

    # ── CHL ───────────────────────────────────────────────────────────
    ("CHL", "PRESIDENT", "PRESIDENT", "Gabriel Boric", "PRESIDENT",
     "President of Chile", (2022, 3, 11), (2026, 3, 11)),
    ("CHL", "BCCH_GOV", "BCCH_GOVERNOR", "Rosanna Costa", "BCCH_GOVERNOR",
     "Governor of the Central Bank of Chile", (2022, 1, 18), None),
    ("CHL", "FM", "FINANCE_MINISTER", "Mario Marcel", "FINANCE_MINISTER",
     "Minister of Finance of Chile", (2022, 3, 11), None),

    # ── FIN ───────────────────────────────────────────────────────────
    ("FIN", "PM", "PRIME_MINISTER", "Petteri Orpo", "PRIME_MINISTER",
     "Prime Minister of Finland", (2023, 6, 20), None),
    ("FIN", "BOF_GOV", "BOF_GOVERNOR", "Olli Rehn", "BOF_GOVERNOR",
     "Governor of the Bank of Finland", (2018, 7, 12), None),
    ("FIN", "FM", "FINANCE_MINISTER", "Riikka Purra", "FINANCE_MINISTER",
     "Minister of Finance of Finland", (2023, 6, 20), None),

    # ── EGY ───────────────────────────────────────────────────────────
    ("EGY", "PRESIDENT", "PRESIDENT", "Abdel Fattah el-Sisi", "PRESIDENT",
     "President of Egypt", (2024, 6, 2), (2030, 6, 2)),
    ("EGY", "CBE_GOV", "CBE_GOVERNOR", "Hassan Abdalla", "CBE_GOVERNOR",
     "Governor of the Central Bank of Egypt", (2022, 8, 18), None),
    ("EGY", "FM", "FINANCE_MINISTER", "Ahmed Kouchouk", "FINANCE_MINISTER",
     "Minister of Finance of Egypt", (2024, 7, 3), None),

    # ── KWT ───────────────────────────────────────────────────────────
    ("KWT", "PM", "PRIME_MINISTER", "Ahmad Abdullah Al-Ahmad Al-Sabah",
     "PRIME_MINISTER", "Prime Minister of Kuwait", (2024, 5, 1), None),
    ("KWT", "CBK_GOV", "CBK_GOVERNOR", "Basel A. Al-Haroon", "CBK_GOVERNOR",
     "Governor of the Central Bank of Kuwait", (2022, 3, 1), None),
    ("KWT", "FM", "FINANCE_MINISTER", "Anwar Ali Al-Mudhaf", "FINANCE_MINISTER",
     "Minister of Finance of Kuwait", (2024, 5, 1), None),

    # ══════════════════════════════════════════════════════════════════════
    # TIER E: Emerging / Niche
    # (CZE, VNM, PRT, NZL, PER, ROU, GRC, QAT, NGA, ARG)
    # ══════════════════════════════════════════════════════════════════════

    # ── CZE ───────────────────────────────────────────────────────────
    ("CZE", "PM", "PRIME_MINISTER", "Petr Fiala", "PRIME_MINISTER",
     "Prime Minister of the Czech Republic", (2021, 11, 28), None),
    ("CZE", "CNB_GOV", "CNB_GOVERNOR", "Aleš Michl", "CNB_GOVERNOR",
     "Governor of the Czech National Bank", (2022, 7, 1), None),
    ("CZE", "FM", "FINANCE_MINISTER", "Zbyněk Stanjura", "FINANCE_MINISTER",
     "Minister of Finance of the Czech Republic", (2021, 11, 28), None),

    # ── VNM ───────────────────────────────────────────────────────────
    ("VNM", "PM", "PRIME_MINISTER", "Phạm Minh Chính", "PRIME_MINISTER",
     "Prime Minister of Vietnam", (2021, 4, 5), None),
    ("VNM", "SBV_GOV", "SBV_GOVERNOR", "Nguyễn Thị Hồng", "SBV_GOVERNOR",
     "Governor of the State Bank of Vietnam", (2020, 11, 12), None),
    ("VNM", "FM", "FINANCE_MINISTER", "Hồ Đức Phớc", "FINANCE_MINISTER",
     "Minister of Finance of Vietnam", (2021, 8, 1), None),

    # ── PRT ───────────────────────────────────────────────────────────
    ("PRT", "PM", "PRIME_MINISTER", "Luís Montenegro", "PRIME_MINISTER",
     "Prime Minister of Portugal", (2024, 4, 2), None),
    ("PRT", "BDP_GOV", "BDP_GOVERNOR", "Mário Centeno", "BDP_GOVERNOR",
     "Governor of Banco de Portugal", (2020, 7, 20), None),
    ("PRT", "FM", "FINANCE_MINISTER", "Joaquim Miranda Sarmento", "FINANCE_MINISTER",
     "Minister of Finance of Portugal", (2024, 4, 2), None),

    # ── NZL ───────────────────────────────────────────────────────────
    ("NZL", "PM", "PRIME_MINISTER", "Christopher Luxon", "PRIME_MINISTER",
     "Prime Minister of New Zealand", (2023, 11, 27), None),
    ("NZL", "RBNZ_GOV", "RBNZ_GOVERNOR", "Adrian Orr", "RBNZ_GOVERNOR",
     "Governor of the Reserve Bank of New Zealand", (2018, 3, 27), None),
    ("NZL", "FM", "FINANCE_MINISTER", "Nicola Willis", "FINANCE_MINISTER",
     "Minister of Finance of New Zealand", (2023, 11, 27), None),

    # ── PER ───────────────────────────────────────────────────────────
    ("PER", "PRESIDENT", "PRESIDENT", "Dina Boluarte", "PRESIDENT",
     "President of Peru", (2022, 12, 7), None),
    ("PER", "BCRP_GOV", "BCRP_GOVERNOR", "Julio Velarde", "BCRP_GOVERNOR",
     "Governor of the Central Reserve Bank of Peru", (2006, 9, 1), None),
    ("PER", "FM", "FINANCE_MINISTER", "José Arista", "FINANCE_MINISTER",
     "Minister of Economy and Finance of Peru", (2024, 3, 1), None),

    # ── ROU ───────────────────────────────────────────────────────────
    ("ROU", "PM", "PRIME_MINISTER", "Marcel Ciolacu", "PRIME_MINISTER",
     "Prime Minister of Romania", (2023, 6, 15), None),
    ("ROU", "BNR_GOV", "BNR_GOVERNOR", "Mugur Isărescu", "BNR_GOVERNOR",
     "Governor of the National Bank of Romania", (2009, 9, 1), None),
    ("ROU", "FM", "FINANCE_MINISTER", "Marcel Boloș", "FINANCE_MINISTER",
     "Minister of Finance of Romania", (2023, 11, 1), None),

    # ── GRC ───────────────────────────────────────────────────────────
    ("GRC", "PM", "PRIME_MINISTER", "Kyriakos Mitsotakis", "PRIME_MINISTER",
     "Prime Minister of Greece", (2023, 6, 26), None),
    ("GRC", "BOG_GOV", "BOG_GOVERNOR", "Yannis Stournaras", "BOG_GOVERNOR",
     "Governor of the Bank of Greece", (2014, 6, 20), None),
    ("GRC", "FM", "FINANCE_MINISTER", "Kostis Hatzidakis", "FINANCE_MINISTER",
     "Minister of Finance of Greece", (2023, 6, 26), None),

    # ── QAT ───────────────────────────────────────────────────────────
    ("QAT", "PM", "PRIME_MINISTER", "Mohammed bin Abdulrahman Al Thani",
     "PRIME_MINISTER", "Prime Minister of Qatar", (2023, 3, 7), None),
    ("QAT", "QCB_GOV", "QCB_GOVERNOR", "Bandar bin Mohammed Al Thani", "QCB_GOVERNOR",
     "Governor of Qatar Central Bank", (2022, 11, 1), None),
    ("QAT", "FM", "FINANCE_MINISTER", "Ali bin Ahmed Al Kuwari", "FINANCE_MINISTER",
     "Minister of Finance of Qatar", (2023, 3, 7), None),

    # ── NGA ───────────────────────────────────────────────────────────
    ("NGA", "PRESIDENT", "PRESIDENT", "Bola Tinubu", "PRESIDENT",
     "President of Nigeria", (2023, 5, 29), (2027, 5, 29)),
    ("NGA", "CBN_GOV", "CBN_GOVERNOR", "Olayemi Cardoso", "CBN_GOVERNOR",
     "Governor of the Central Bank of Nigeria", (2023, 9, 22), None),
    ("NGA", "FM", "FINANCE_MINISTER", "Wale Edun", "FINANCE_MINISTER",
     "Minister of Finance of Nigeria", (2023, 8, 21), None),

    # ── ARG ───────────────────────────────────────────────────────────
    ("ARG", "PRESIDENT", "PRESIDENT", "Javier Milei", "PRESIDENT",
     "President of Argentina", (2023, 12, 10), (2027, 12, 10)),
    ("ARG", "BCRA_PRES", "BCRA_PRESIDENT", "Santiago Bausili", "BCRA_PRESIDENT",
     "President of the Central Bank of Argentina", (2023, 12, 10), None),
    ("ARG", "FM", "FINANCE_MINISTER", "Luis Caputo", "ECONOMY_MINISTER",
     "Minister of Economy of Argentina", (2023, 12, 10), None),

    # ══════════════════════════════════════════════════════════════════════
    # TIER F: Africa Resource Powers
    # (AGO, COD, GHA, KEN, ETH, TZA, CIV, MOZ, ZMB, BWA)
    # ══════════════════════════════════════════════════════════════════════

    # ── AGO ───────────────────────────────────────────────────────────
    ("AGO", "PRESIDENT", "PRESIDENT", "João Lourenço", "PRESIDENT",
     "President of Angola", (2017, 9, 26), None),
    ("AGO", "BNA_GOV", "BNA_GOVERNOR", "Manuel Tiago Dias", "BNA_GOVERNOR",
     "Governor of the National Bank of Angola", (2023, 1, 1), None),
    ("AGO", "FM", "FINANCE_MINISTER", "Vera Daves de Sousa", "FINANCE_MINISTER",
     "Minister of Finance of Angola", (2019, 10, 1), None),

    # ── COD ───────────────────────────────────────────────────────────
    ("COD", "PRESIDENT", "PRESIDENT", "Félix Tshisekedi", "PRESIDENT",
     "President of the Democratic Republic of the Congo", (2019, 1, 24), None),
    ("COD", "BCC_GOV", "BCC_GOVERNOR", "Malangu Kabedi Mbuyi", "BCC_GOVERNOR",
     "Governor of the Central Bank of the Congo", (2021, 7, 1), None),
    ("COD", "FM", "FINANCE_MINISTER", "Nicolas Kazadi", "FINANCE_MINISTER",
     "Minister of Finance of the DRC", (2021, 4, 1), None),

    # ── GHA ───────────────────────────────────────────────────────────
    ("GHA", "PRESIDENT", "PRESIDENT", "John Mahama", "PRESIDENT",
     "President of Ghana", (2025, 1, 7), (2029, 1, 7)),
    ("GHA", "BOG_GOV", "BOG_GOVERNOR", "Ernest Addison", "BOG_GOVERNOR",
     "Governor of the Bank of Ghana", (2017, 4, 1), None),
    ("GHA", "FM", "FINANCE_MINISTER", "Cassiel Ato Forson", "FINANCE_MINISTER",
     "Minister of Finance of Ghana", (2025, 1, 7), None),

    # ── KEN ───────────────────────────────────────────────────────────
    ("KEN", "PRESIDENT", "PRESIDENT", "William Ruto", "PRESIDENT",
     "President of Kenya", (2022, 9, 13), (2027, 9, 13)),
    ("KEN", "CBK_GOV", "CBK_GOVERNOR", "Kamau Thugge", "CBK_GOVERNOR",
     "Governor of the Central Bank of Kenya", (2023, 6, 19), None),
    ("KEN", "FM", "FINANCE_MINISTER", "John Mbadi", "FINANCE_MINISTER",
     "Cabinet Secretary for the National Treasury (Kenya)", (2024, 8, 1), None),

    # ── ETH ───────────────────────────────────────────────────────────
    ("ETH", "PM", "PRIME_MINISTER", "Abiy Ahmed", "PRIME_MINISTER",
     "Prime Minister of Ethiopia", (2018, 4, 2), None),
    ("ETH", "NBE_GOV", "NBE_GOVERNOR", "Mamo Mihretu", "NBE_GOVERNOR",
     "Governor of the National Bank of Ethiopia", (2023, 6, 1), None),
    ("ETH", "FM", "FINANCE_MINISTER", "Ahmed Shide", "FINANCE_MINISTER",
     "Minister of Finance of Ethiopia", (2018, 4, 1), None),

    # ── TZA ───────────────────────────────────────────────────────────
    ("TZA", "PRESIDENT", "PRESIDENT", "Samia Suluhu Hassan", "PRESIDENT",
     "President of Tanzania", (2021, 3, 19), None),
    ("TZA", "BOT_GOV", "BOT_GOVERNOR", "Emmanuel Tutuba", "BOT_GOVERNOR",
     "Governor of the Bank of Tanzania", (2024, 1, 1), None),
    ("TZA", "FM", "FINANCE_MINISTER", "Mwigulu Nchemba", "FINANCE_MINISTER",
     "Minister of Finance of Tanzania", (2022, 1, 1), None),

    # ── CIV ───────────────────────────────────────────────────────────
    ("CIV", "PRESIDENT", "PRESIDENT", "Alassane Ouattara", "PRESIDENT",
     "President of Côte d'Ivoire", (2020, 12, 14), None),
    ("CIV", "BCEAO_GOV", "BCEAO_GOVERNOR", "Jean-Claude Kassi Brou", "BCEAO_GOVERNOR",
     "Governor of BCEAO (West African Central Bank)", (2023, 1, 1), None),
    ("CIV", "FM", "FINANCE_MINISTER", "Adama Coulibaly", "FINANCE_MINISTER",
     "Minister of Economy and Finance of Côte d'Ivoire", (2021, 4, 1), None),

    # ── MOZ ───────────────────────────────────────────────────────────
    ("MOZ", "PRESIDENT", "PRESIDENT", "Daniel Chapo", "PRESIDENT",
     "President of Mozambique", (2025, 1, 15), None),
    ("MOZ", "BM_GOV", "BM_GOVERNOR", "Rogério Zandamela", "BM_GOVERNOR",
     "Governor of the Bank of Mozambique", (2016, 6, 1), None),
    ("MOZ", "FM", "FINANCE_MINISTER", "Ernesto Max Tonela", "FINANCE_MINISTER",
     "Minister of Economy and Finance of Mozambique", (2023, 3, 1), None),

    # ── ZMB ───────────────────────────────────────────────────────────
    ("ZMB", "PRESIDENT", "PRESIDENT", "Hakainde Hichilema", "PRESIDENT",
     "President of Zambia", (2021, 8, 24), None),
    ("ZMB", "BOZ_GOV", "BOZ_GOVERNOR", "Denny Kalyalya", "BOZ_GOVERNOR",
     "Governor of the Bank of Zambia", (2015, 6, 1), None),
    ("ZMB", "FM", "FINANCE_MINISTER", "Situmbeko Musokotwane", "FINANCE_MINISTER",
     "Minister of Finance of Zambia", (2021, 8, 24), None),

    # ── BWA ───────────────────────────────────────────────────────────
    ("BWA", "PRESIDENT", "PRESIDENT", "Duma Boko", "PRESIDENT",
     "President of Botswana", (2024, 11, 1), None),
    ("BWA", "BOB_GOV", "BOB_GOVERNOR", "Cornelius Dekop", "BOB_GOVERNOR",
     "Governor of the Bank of Botswana", (2022, 1, 1), None),
    ("BWA", "FM", "FINANCE_MINISTER", "Ndaba Gaolathe", "FINANCE_MINISTER",
     "Minister of Finance of Botswana", (2024, 11, 1), None),

    # ══════════════════════════════════════════════════════════════════════
    # TIER G: Africa Strategic
    # (NAM, GAB, GIN, MAR, LBY, SEN, UGA, ZWE, SDN, CMR)
    # ══════════════════════════════════════════════════════════════════════

    # ── NAM ───────────────────────────────────────────────────────────
    ("NAM", "PRESIDENT", "PRESIDENT", "Netumbo Nandi-Ndaitwah", "PRESIDENT",
     "President of Namibia", (2025, 3, 21), None),
    ("NAM", "BON_GOV", "BON_GOVERNOR", "Johannes !Gawaxab", "BON_GOVERNOR",
     "Governor of the Bank of Namibia", (2020, 4, 1), None),
    ("NAM", "FM", "FINANCE_MINISTER", "Iipumbu Shiimi", "FINANCE_MINISTER",
     "Minister of Finance of Namibia", (2020, 3, 21), None),

    # ── GAB ───────────────────────────────────────────────────────────
    ("GAB", "PRESIDENT", "PRESIDENT", "Brice Clotaire Oligui Nguema", "PRESIDENT",
     "Transitional President of Gabon", (2023, 8, 30), None),
    ("GAB", "BEAC_GOV", "BEAC_GOVERNOR", "Abbas Mahamat Tolli", "BEAC_GOVERNOR",
     "Governor of BEAC (Central African Central Bank)", (2017, 3, 1), None),
    ("GAB", "FM", "FINANCE_MINISTER", "Mays Mouissi", "FINANCE_MINISTER",
     "Minister of Economy and Finance of Gabon", (2023, 9, 7), None),

    # ── GIN ───────────────────────────────────────────────────────────
    ("GIN", "PRESIDENT", "PRESIDENT", "Mamadi Doumbouya", "PRESIDENT",
     "Transitional President of Guinea", (2021, 9, 5), None),
    ("GIN", "BCRG_GOV", "BCRG_GOVERNOR", "Karamo Kaba", "BCRG_GOVERNOR",
     "Governor of the Central Bank of Guinea", (2023, 1, 1), None),
    ("GIN", "FM", "FINANCE_MINISTER", "Mourana Soumah", "FINANCE_MINISTER",
     "Minister of Economy and Finance of Guinea", (2023, 1, 1), None),

    # ── MAR ───────────────────────────────────────────────────────────
    ("MAR", "PM", "PRIME_MINISTER", "Aziz Akhannouch", "PRIME_MINISTER",
     "Prime Minister of Morocco", (2021, 10, 7), None),
    ("MAR", "BAM_GOV", "BAM_GOVERNOR", "Abdellatif Jouahri", "BAM_GOVERNOR",
     "Governor of Bank Al-Maghrib", (2003, 4, 1), None),
    ("MAR", "FM", "FINANCE_MINISTER", "Nadia Fettah", "FINANCE_MINISTER",
     "Minister of Economy and Finance of Morocco", (2021, 10, 7), None),

    # ── LBY ───────────────────────────────────────────────────────────
    ("LBY", "PM", "PRIME_MINISTER", "Abdul Hamid Dbeibeh", "PRIME_MINISTER",
     "Prime Minister of Libya (GNU)", (2021, 3, 15), None),
    ("LBY", "CBL_GOV", "CBL_GOVERNOR", "Sadiq al-Kabir", "CBL_GOVERNOR",
     "Governor of the Central Bank of Libya", (2012, 9, 1), None),
    ("LBY", "FM", "FINANCE_MINISTER", "Khalid Al-Mabrouk", "FINANCE_MINISTER",
     "Minister of Finance of Libya", (2023, 1, 1), None),

    # ── SEN ───────────────────────────────────────────────────────────
    ("SEN", "PRESIDENT", "PRESIDENT", "Bassirou Diomaye Faye", "PRESIDENT",
     "President of Senegal", (2024, 4, 2), (2029, 4, 2)),
    ("SEN", "BCEAO_GOV", "BCEAO_GOVERNOR", "Jean-Claude Kassi Brou", "BCEAO_GOVERNOR",
     "Governor of BCEAO (West African Central Bank)", (2023, 1, 1), None),
    ("SEN", "FM", "FINANCE_MINISTER", "Cheikh Diba", "FINANCE_MINISTER",
     "Minister of Finance and Budget of Senegal", (2024, 4, 5), None),

    # ── UGA ───────────────────────────────────────────────────────────
    ("UGA", "PRESIDENT", "PRESIDENT", "Yoweri Museveni", "PRESIDENT",
     "President of Uganda", (2021, 5, 12), None),
    ("UGA", "BOU_GOV", "BOU_GOVERNOR", "Michael Atingi-Ego", "BOU_GOVERNOR",
     "Governor of the Bank of Uganda", (2023, 1, 1), None),
    ("UGA", "FM", "FINANCE_MINISTER", "Matia Kasaija", "FINANCE_MINISTER",
     "Minister of Finance of Uganda", (2016, 3, 1), None),

    # ── ZWE ───────────────────────────────────────────────────────────
    ("ZWE", "PRESIDENT", "PRESIDENT", "Emmerson Mnangagwa", "PRESIDENT",
     "President of Zimbabwe", (2017, 11, 24), None),
    ("ZWE", "RBZ_GOV", "RBZ_GOVERNOR", "John Mushayavanhu", "RBZ_GOVERNOR",
     "Governor of the Reserve Bank of Zimbabwe", (2024, 3, 28), None),
    ("ZWE", "FM", "FINANCE_MINISTER", "Mthuli Ncube", "FINANCE_MINISTER",
     "Minister of Finance of Zimbabwe", (2018, 9, 10), None),

    # ── SDN ───────────────────────────────────────────────────────────
    ("SDN", "HEAD_STATE", "HEAD_OF_STATE", "Abdel Fattah al-Burhan", "HEAD_OF_STATE",
     "Chairman of the Sovereignty Council of Sudan", (2021, 10, 25), None),
    ("SDN", "CBOS_GOV", "CBOS_GOVERNOR", "Mohamed Elfatih Zeinelabdin", "CBOS_GOVERNOR",
     "Governor of the Central Bank of Sudan", (2021, 1, 1), None),
    ("SDN", "FM", "FINANCE_MINISTER", "Gibril Ibrahim", "FINANCE_MINISTER",
     "Minister of Finance of Sudan", (2021, 2, 1), None),

    # ── CMR ───────────────────────────────────────────────────────────
    ("CMR", "PRESIDENT", "PRESIDENT", "Paul Biya", "PRESIDENT",
     "President of Cameroon", (1982, 11, 6), None),
    ("CMR", "BEAC_GOV", "BEAC_GOVERNOR", "Abbas Mahamat Tolli", "BEAC_GOVERNOR",
     "Governor of BEAC (Central African Central Bank)", (2017, 3, 1), None),
    ("CMR", "FM", "FINANCE_MINISTER", "Louis Paul Motaze", "FINANCE_MINISTER",
     "Minister of Finance of Cameroon", (2019, 1, 4), None),

    # ══════════════════════════════════════════════════════════════════════
    # TIER H: Eurasia & Middle East
    # (UKR, PAK, BGD, KAZ, IRN, IRQ, HUN, SRB, SVK, GEO)
    # ══════════════════════════════════════════════════════════════════════

    # ── UKR ───────────────────────────────────────────────────────────
    ("UKR", "PRESIDENT", "PRESIDENT", "Volodymyr Zelenskyy", "PRESIDENT",
     "President of Ukraine", (2019, 5, 20), None),
    ("UKR", "NBU_GOV", "NBU_GOVERNOR", "Andriy Pyshnyi", "NBU_GOVERNOR",
     "Governor of the National Bank of Ukraine", (2022, 10, 6), None),
    ("UKR", "FM", "FINANCE_MINISTER", "Serhiy Marchenko", "FINANCE_MINISTER",
     "Minister of Finance of Ukraine", (2020, 3, 30), None),

    # ── PAK ───────────────────────────────────────────────────────────
    ("PAK", "PM", "PRIME_MINISTER", "Shehbaz Sharif", "PRIME_MINISTER",
     "Prime Minister of Pakistan", (2024, 3, 4), None),
    ("PAK", "SBP_GOV", "SBP_GOVERNOR", "Jameel Ahmad", "SBP_GOVERNOR",
     "Governor of the State Bank of Pakistan", (2022, 9, 19), None),
    ("PAK", "FM", "FINANCE_MINISTER", "Muhammad Aurangzeb", "FINANCE_MINISTER",
     "Minister of Finance of Pakistan", (2024, 3, 11), None),

    # ── BGD ───────────────────────────────────────────────────────────
    ("BGD", "HEAD_STATE", "CHIEF_ADVISER", "Muhammad Yunus", "CHIEF_ADVISER",
     "Chief Adviser of Bangladesh", (2024, 8, 8), None),
    ("BGD", "BB_GOV", "BB_GOVERNOR", "Ahsan H. Mansur", "BB_GOVERNOR",
     "Governor of Bangladesh Bank", (2024, 7, 14), None),
    ("BGD", "FM", "FINANCE_MINISTER", "Salehuddin Ahmed", "FINANCE_ADVISER",
     "Finance Adviser of Bangladesh", (2024, 8, 11), None),

    # ── KAZ ───────────────────────────────────────────────────────────
    ("KAZ", "PRESIDENT", "PRESIDENT", "Kassym-Jomart Tokayev", "PRESIDENT",
     "President of Kazakhstan", (2019, 6, 12), None),
    ("KAZ", "NBK_GOV", "NBK_GOVERNOR", "Timur Suleimenov", "NBK_GOVERNOR",
     "Governor of the National Bank of Kazakhstan", (2024, 1, 1), None),
    ("KAZ", "FM", "FINANCE_MINISTER", "Marat Sultangaziyev", "FINANCE_MINISTER",
     "Minister of Finance of Kazakhstan", (2023, 1, 1), None),

    # ── IRN ───────────────────────────────────────────────────────────
    ("IRN", "PRESIDENT", "PRESIDENT", "Masoud Pezeshkian", "PRESIDENT",
     "President of Iran", (2024, 7, 30), (2028, 7, 30)),
    ("IRN", "CBI_GOV", "CBI_GOVERNOR", "Mohammad Reza Farzin", "CBI_GOVERNOR",
     "Governor of the Central Bank of Iran", (2024, 8, 1), None),
    ("IRN", "FM", "FINANCE_MINISTER", "Abdolnaser Hemmati", "ECONOMY_MINISTER",
     "Minister of Economy of Iran", (2024, 8, 1), None),

    # ── IRQ ───────────────────────────────────────────────────────────
    ("IRQ", "PM", "PRIME_MINISTER", "Mohammed Shia al-Sudani", "PRIME_MINISTER",
     "Prime Minister of Iraq", (2022, 10, 27), None),
    ("IRQ", "CBI_GOV", "CBI_GOVERNOR", "Ali Mohsen Al-Allaq", "CBI_GOVERNOR",
     "Governor of the Central Bank of Iraq", (2023, 9, 1), None),
    ("IRQ", "FM", "FINANCE_MINISTER", "Taif Sami Mohammed", "FINANCE_MINISTER",
     "Minister of Finance of Iraq", (2022, 10, 27), None),

    # ── HUN ───────────────────────────────────────────────────────────
    ("HUN", "PM", "PRIME_MINISTER", "Viktor Orbán", "PRIME_MINISTER",
     "Prime Minister of Hungary", (2010, 5, 29), None),
    ("HUN", "MNB_GOV", "MNB_GOVERNOR", "György Matolcsy", "MNB_GOVERNOR",
     "Governor of the Magyar Nemzeti Bank", (2013, 3, 4), None),
    ("HUN", "FM", "FINANCE_MINISTER", "Mihály Varga", "FINANCE_MINISTER",
     "Minister of Finance of Hungary", (2018, 6, 1), None),

    # ── SRB ───────────────────────────────────────────────────────────
    ("SRB", "PM", "PRIME_MINISTER", "Miloš Vučević", "PRIME_MINISTER",
     "Prime Minister of Serbia", (2024, 5, 2), None),
    ("SRB", "NBS_GOV", "NBS_GOVERNOR", "Jorgovanka Tabaković", "NBS_GOVERNOR",
     "Governor of the National Bank of Serbia", (2012, 8, 6), None),
    ("SRB", "FM", "FINANCE_MINISTER", "Siniša Mali", "FINANCE_MINISTER",
     "Minister of Finance of Serbia", (2020, 10, 28), None),

    # ── SVK ───────────────────────────────────────────────────────────
    ("SVK", "PM", "PRIME_MINISTER", "Robert Fico", "PRIME_MINISTER",
     "Prime Minister of Slovakia", (2023, 10, 25), None),
    ("SVK", "NBS_GOV", "NBS_GOVERNOR", "Peter Kažimír", "NBS_GOVERNOR",
     "Governor of the National Bank of Slovakia", (2019, 6, 1), None),
    ("SVK", "FM", "FINANCE_MINISTER", "Ladislav Kamenický", "FINANCE_MINISTER",
     "Minister of Finance of Slovakia", (2023, 10, 25), None),

    # ── GEO ───────────────────────────────────────────────────────────
    ("GEO", "PM", "PRIME_MINISTER", "Irakli Kobakhidze", "PRIME_MINISTER",
     "Prime Minister of Georgia", (2024, 2, 8), None),
    ("GEO", "NBG_PRES", "NBG_PRESIDENT", "Koba Gvenetadze", "NBG_PRESIDENT",
     "President of the National Bank of Georgia", (2016, 12, 1), None),
    ("GEO", "FM", "FINANCE_MINISTER", "Lasha Khutsishvili", "FINANCE_MINISTER",
     "Minister of Finance of Georgia", (2024, 2, 8), None),

    # ══════════════════════════════════════════════════════════════════════
    # TIER I: Americas & Asia-Pacific
    # (ECU, VEN, BOL, URY, PAN, MMR, KHM, BRN, LKA, NPL)
    # ══════════════════════════════════════════════════════════════════════

    # ── ECU ───────────────────────────────────────────────────────────
    ("ECU", "PRESIDENT", "PRESIDENT", "Daniel Noboa", "PRESIDENT",
     "President of Ecuador", (2023, 11, 23), (2025, 5, 24)),
    ("ECU", "BCE_MGR", "BCE_MANAGER", "Guillermo Avellán", "BCE_MANAGER",
     "General Manager of the Central Bank of Ecuador", (2021, 1, 1), None),
    ("ECU", "FM", "FINANCE_MINISTER", "Juan Carlos Vega", "FINANCE_MINISTER",
     "Minister of Economy and Finance of Ecuador", (2024, 1, 1), None),

    # ── VEN ───────────────────────────────────────────────────────────
    ("VEN", "PRESIDENT", "PRESIDENT", "Nicolás Maduro", "PRESIDENT",
     "President of Venezuela", (2013, 4, 19), None),
    ("VEN", "BCV_PRES", "BCV_PRESIDENT", "Calixto Ortega", "BCV_PRESIDENT",
     "President of the Central Bank of Venezuela", (2023, 1, 1), None),
    ("VEN", "FM", "FINANCE_MINISTER", "Anabel Pereira", "FINANCE_MINISTER",
     "Minister of Finance of Venezuela", (2024, 1, 1), None),

    # ── BOL ───────────────────────────────────────────────────────────
    ("BOL", "PRESIDENT", "PRESIDENT", "Luis Arce", "PRESIDENT",
     "President of Bolivia", (2020, 11, 8), None),
    ("BOL", "BCB_PRES", "BCB_PRESIDENT", "Edwin Rojas", "BCB_PRESIDENT",
     "President of the Central Bank of Bolivia", (2022, 1, 1), None),
    ("BOL", "FM", "FINANCE_MINISTER", "Marcelo Montenegro", "FINANCE_MINISTER",
     "Minister of Economy and Public Finance of Bolivia", (2020, 11, 8), None),

    # ── URY ───────────────────────────────────────────────────────────
    ("URY", "PRESIDENT", "PRESIDENT", "Yamandú Orsi", "PRESIDENT",
     "President of Uruguay", (2025, 3, 1), (2030, 3, 1)),
    ("URY", "BCU_PRES", "BCU_PRESIDENT", "Washington Ribeiro", "BCU_PRESIDENT",
     "President of the Central Bank of Uruguay", (2023, 1, 1), None),
    ("URY", "FM", "FINANCE_MINISTER", "Gabriel Oddone", "ECONOMY_MINISTER",
     "Minister of Economy and Finance of Uruguay", (2025, 3, 1), None),

    # ── PAN ───────────────────────────────────────────────────────────
    ("PAN", "PRESIDENT", "PRESIDENT", "José Raúl Mulino", "PRESIDENT",
     "President of Panama", (2024, 7, 1), (2029, 7, 1)),
    ("PAN", "SBP_SUPT", "SBP_SUPERINTENDENT", "Amauri Castillo",
     "SBP_SUPERINTENDENT",
     "Superintendent of Banks of Panama", (2024, 7, 1), None),
    ("PAN", "FM", "FINANCE_MINISTER", "Felipe Chapman", "FINANCE_MINISTER",
     "Minister of Economy and Finance of Panama", (2024, 7, 1), None),

    # ── MMR ───────────────────────────────────────────────────────────
    ("MMR", "HEAD_STATE", "SAC_CHAIRMAN", "Min Aung Hlaing", "SAC_CHAIRMAN",
     "Chairman of the State Administration Council of Myanmar", (2021, 2, 1), None),
    ("MMR", "CBM_GOV", "CBM_GOVERNOR", "Than Than Swe", "CBM_GOVERNOR",
     "Governor of the Central Bank of Myanmar", (2021, 2, 1), None),
    ("MMR", "FM", "FINANCE_MINISTER", "Win Shein", "FINANCE_MINISTER",
     "Minister of Planning and Finance of Myanmar", (2021, 2, 1), None),

    # ── KHM ───────────────────────────────────────────────────────────
    ("KHM", "PM", "PRIME_MINISTER", "Hun Manet", "PRIME_MINISTER",
     "Prime Minister of Cambodia", (2023, 8, 22), None),
    ("KHM", "NBC_GOV", "NBC_GOVERNOR", "Chea Chanto", "NBC_GOVERNOR",
     "Governor of the National Bank of Cambodia", (1998, 11, 1), None),
    ("KHM", "FM", "FINANCE_MINISTER", "Aun Pornmoniroth", "FINANCE_MINISTER",
     "Minister of Economy and Finance of Cambodia", (2023, 8, 22), None),

    # ── BRN ───────────────────────────────────────────────────────────
    ("BRN", "SULTAN", "SULTAN", "Hassanal Bolkiah", "SULTAN",
     "Sultan of Brunei", (1967, 10, 5), None),
    ("BRN", "AMBD_MD", "AMBD_MANAGING_DIRECTOR", "Dato Javed Ahmad", "AMBD_MD",
     "Managing Director of AMBD (Brunei Monetary Authority)", (2020, 1, 1), None),
    ("BRN", "FM", "FINANCE_MINISTER", "Dato Amin Liew Abdullah", "FINANCE_MINISTER",
     "Second Minister of Finance and Economy of Brunei", (2018, 7, 1), None),

    # ── LKA ───────────────────────────────────────────────────────────
    ("LKA", "PRESIDENT", "PRESIDENT", "Anura Kumara Dissanayake", "PRESIDENT",
     "President of Sri Lanka", (2024, 9, 23), None),
    ("LKA", "CBSL_GOV", "CBSL_GOVERNOR", "Nandalal Weerasinghe", "CBSL_GOVERNOR",
     "Governor of the Central Bank of Sri Lanka", (2022, 4, 7), None),
    ("LKA", "PM", "PRIME_MINISTER", "Harini Amarasuriya", "PRIME_MINISTER",
     "Prime Minister of Sri Lanka", (2024, 9, 24), None),

    # ── NPL ───────────────────────────────────────────────────────────
    ("NPL", "PM", "PRIME_MINISTER", "KP Sharma Oli", "PRIME_MINISTER",
     "Prime Minister of Nepal", (2024, 7, 15), None),
    ("NPL", "NRB_GOV", "NRB_GOVERNOR", "Maha Prasad Adhikari", "NRB_GOVERNOR",
     "Governor of Nepal Rastra Bank", (2020, 7, 1), None),
    ("NPL", "FM", "FINANCE_MINISTER", "Bishnu Prasad Paudel", "FINANCE_MINISTER",
     "Minister of Finance of Nepal", (2024, 7, 15), None),

    # ══════════════════════════════════════════════════════════════════════
    # TIER J: EU/Baltic + Misc
    # (BGR, HRV, LTU, EST, JOR, OMN, MNG, DOM, CRI, GTM)
    # ══════════════════════════════════════════════════════════════════════

    # ── BGR ───────────────────────────────────────────────────────────
    ("BGR", "PM", "PRIME_MINISTER", "Dimitar Glavchev", "PRIME_MINISTER",
     "Prime Minister of Bulgaria", (2024, 4, 9), None),
    ("BGR", "BNB_GOV", "BNB_GOVERNOR", "Dimitar Radev", "BNB_GOVERNOR",
     "Governor of the Bulgarian National Bank", (2015, 7, 15), None),
    ("BGR", "FM", "FINANCE_MINISTER", "Lyudmila Petkova", "FINANCE_MINISTER",
     "Minister of Finance of Bulgaria", (2024, 4, 9), None),

    # ── HRV ───────────────────────────────────────────────────────────
    ("HRV", "PM", "PRIME_MINISTER", "Andrej Plenković", "PRIME_MINISTER",
     "Prime Minister of Croatia", (2016, 10, 19), None),
    ("HRV", "HNB_GOV", "HNB_GOVERNOR", "Boris Vujčić", "HNB_GOVERNOR",
     "Governor of the Croatian National Bank", (2012, 7, 8), None),
    ("HRV", "FM", "FINANCE_MINISTER", "Marko Primorac", "FINANCE_MINISTER",
     "Minister of Finance of Croatia", (2022, 4, 1), None),

    # ── LTU ───────────────────────────────────────────────────────────
    ("LTU", "PM", "PRIME_MINISTER", "Gintautas Paluckas", "PRIME_MINISTER",
     "Prime Minister of Lithuania", (2024, 12, 12), None),
    ("LTU", "LB_CHAIR", "LB_BOARD_CHAIR", "Gediminas Šimkus", "LB_BOARD_CHAIR",
     "Chairman of the Board of the Bank of Lithuania", (2020, 4, 1), None),
    ("LTU", "FM", "FINANCE_MINISTER", "Gintarė Skaistė", "FINANCE_MINISTER",
     "Minister of Finance of Lithuania", (2024, 12, 12), None),

    # ── EST ───────────────────────────────────────────────────────────
    ("EST", "PM", "PRIME_MINISTER", "Kristen Michal", "PRIME_MINISTER",
     "Prime Minister of Estonia", (2024, 7, 23), None),
    ("EST", "EP_GOV", "EP_GOVERNOR", "Madis Müller", "EP_GOVERNOR",
     "Governor of Eesti Pank", (2019, 6, 1), None),
    ("EST", "FM", "FINANCE_MINISTER", "Jürgen Ligi", "FINANCE_MINISTER",
     "Minister of Finance of Estonia", (2024, 7, 23), None),

    # ── JOR ───────────────────────────────────────────────────────────
    ("JOR", "PM", "PRIME_MINISTER", "Jafar Hassan", "PRIME_MINISTER",
     "Prime Minister of Jordan", (2024, 9, 15), None),
    ("JOR", "CBJ_GOV", "CBJ_GOVERNOR", "Adel Al-Sharkas", "CBJ_GOVERNOR",
     "Governor of the Central Bank of Jordan", (2020, 1, 1), None),
    ("JOR", "FM", "FINANCE_MINISTER", "Abdul Hakim Shibli", "FINANCE_MINISTER",
     "Minister of Finance of Jordan", (2024, 9, 15), None),

    # ── OMN ───────────────────────────────────────────────────────────
    ("OMN", "SULTAN", "SULTAN", "Haitham bin Tariq", "SULTAN",
     "Sultan of Oman", (2020, 1, 11), None),
    ("OMN", "CBO_PRES", "CBO_EXEC_PRESIDENT", "Tahir Al Amri", "CBO_EXEC_PRESIDENT",
     "Executive President of the Central Bank of Oman", (2020, 1, 1), None),
    ("OMN", "FM", "FINANCE_MINISTER", "Sultan Al Habsi", "FINANCE_MINISTER",
     "Minister of Finance of Oman", (2020, 8, 1), None),

    # ── MNG ───────────────────────────────────────────────────────────
    ("MNG", "PM", "PRIME_MINISTER", "Luvsannamsrain Oyun-Erdene", "PRIME_MINISTER",
     "Prime Minister of Mongolia", (2021, 1, 27), None),
    ("MNG", "BOM_GOV", "BOM_GOVERNOR", "Byambasuren Lkhagvasuren", "BOM_GOVERNOR",
     "Governor of the Bank of Mongolia", (2022, 1, 1), None),
    ("MNG", "FM", "FINANCE_MINISTER", "Bold Javkhlan", "FINANCE_MINISTER",
     "Minister of Finance of Mongolia", (2024, 7, 1), None),

    # ── DOM ───────────────────────────────────────────────────────────
    ("DOM", "PRESIDENT", "PRESIDENT", "Luis Abinader", "PRESIDENT",
     "President of the Dominican Republic", (2024, 8, 16), (2028, 8, 16)),
    ("DOM", "BCRD_GOV", "BCRD_GOVERNOR", "Héctor Valdez Albizu", "BCRD_GOVERNOR",
     "Governor of the Central Bank of the Dominican Republic", (2004, 8, 16), None),
    ("DOM", "FM", "FINANCE_MINISTER", "Jochi Vicente", "FINANCE_MINISTER",
     "Minister of Finance of the Dominican Republic", (2020, 8, 16), None),

    # ── CRI ───────────────────────────────────────────────────────────
    ("CRI", "PRESIDENT", "PRESIDENT", "Rodrigo Chaves", "PRESIDENT",
     "President of Costa Rica", (2022, 5, 8), (2026, 5, 8)),
    ("CRI", "BCCR_PRES", "BCCR_PRESIDENT", "Roger Madrigal", "BCCR_PRESIDENT",
     "President of the Central Bank of Costa Rica", (2022, 7, 1), None),
    ("CRI", "FM", "FINANCE_MINISTER", "Nogui Acosta", "FINANCE_MINISTER",
     "Minister of Finance of Costa Rica", (2022, 5, 8), None),

    # ── GTM ───────────────────────────────────────────────────────────
    ("GTM", "PRESIDENT", "PRESIDENT", "Bernardo Arévalo", "PRESIDENT",
     "President of Guatemala", (2024, 1, 14), (2028, 1, 14)),
    ("GTM", "BANGUAT_PRES", "BANGUAT_PRESIDENT", "Álvaro González Ricci",
     "BANGUAT_PRESIDENT",
     "President of the Bank of Guatemala", (2020, 10, 1), None),
    ("GTM", "FM", "FINANCE_MINISTER", "Jonathan Menkos", "FINANCE_MINISTER",
     "Minister of Public Finance of Guatemala", (2024, 1, 14), None),
]


# ── Expand tuples into OfficialDef ───────────────────────────────────────


def _expand_raw() -> list[OfficialDef]:
    officials: list[OfficialDef] = []
    for row in _RAW:
        nation, prof_sfx, pos_sfx, name, role, desc, since_t, end_t = row
        officials.append(OfficialDef(
            nation=nation,
            profile_id=f"{nation}_{prof_sfx}_PROFILE",
            position_id=f"{nation}_{pos_sfx}",
            person_name=name,
            role=role,
            role_description=desc,
            in_role_since=date(*since_t),
            expected_term_end=date(*end_t) if end_t else None,
        ))
    return officials


EXPANSION_90: list[OfficialDef] = _expand_raw()


# ── Seed functions ───────────────────────────────────────────────────────


def _seed_positions(officials: list[OfficialDef], *, dry_run: bool) -> int:
    """Upsert position_occupancy rows for all officials."""

    if dry_run:
        for o in officials:
            print(f"  [dry] {o.position_id}: {o.person_name} ({o.nation})")
        return len(officials)

    sql = """
        INSERT INTO position_occupancy (
            occupancy_id, position_id, person_name, nation,
            start_date, end_date, metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (position_id, start_date) DO UPDATE SET
            person_name = EXCLUDED.person_name,
            nation = EXCLUDED.nation,
            end_date = EXCLUDED.end_date,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
    """

    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            for o in officials:
                cur.execute(
                    sql,
                    (
                        generate_uuid(),
                        o.position_id,
                        o.person_name,
                        o.nation,
                        o.in_role_since,
                        o.expected_term_end,
                        Json({
                            "role_description": o.role_description,
                            "tier": 1,
                        }),
                    ),
                )
            conn.commit()
        finally:
            cur.close()

    return len(officials)


def _seed_profiles(
    officials: list[OfficialDef],
    *,
    dry_run: bool,
    use_llm: bool,
) -> tuple[int, int]:
    """Seed person_profiles via PersonProfileService.

    Returns (total, llm_enriched) counts.
    """

    if dry_run:
        for o in officials:
            print(f"  [dry] {o.profile_id}: {o.person_name} – {o.role} ({o.nation})")
        return len(officials), 0

    from apatheon.nation.person_service import PersonProfileService
    from apatheon.nation.storage import PersonProfileStorage

    db = get_db_manager()
    storage = PersonProfileStorage(db_manager=db)

    llm = None
    tool_agent = None
    if use_llm:
        from apatheon.llm.gateway import get_llm
        llm = get_llm()
        print(f"  LLM configured: {llm.__class__.__name__}")

        try:
            from apatheon.llm.agent import ToolAgent
            tool_agent = ToolAgent(
                provider=llm,
                tool_names=[
                    "search_wikipedia", "search_web",
                    "get_nation_indicators", "get_current_date",
                ],
                max_rounds=4,
            )
            print(f"  Tool agent: enabled ({len(tool_agent.tool_names)} tools)")
        except Exception as exc:
            print(f"  Tool agent: disabled ({exc})")

    svc = PersonProfileService(storage=storage, llm=llm, tool_agent=tool_agent)

    enriched = 0
    for i, o in enumerate(officials, 1):
        t0 = time.time()
        print(
            f"  [{i}/{len(officials)}] {o.person_name} – {o.role} ({o.nation}) ...",
            end=" ", flush=True,
        )

        profile = svc.seed_profile(
            profile_id=o.profile_id,
            person_name=o.person_name,
            nation=o.nation,
            role=o.role,
            role_tier=1,
            in_role_since=o.in_role_since,
            expected_term_end=o.expected_term_end,
        )

        elapsed = time.time() - t0
        if profile.confidence > 0.5:
            enriched += 1
            print(f"OK (conf={profile.confidence:.2f}, {elapsed:.1f}s)")
        else:
            print(f"skeleton (conf={profile.confidence:.2f}, {elapsed:.1f}s)")

    return len(officials), enriched


# ── Main ─────────────────────────────────────────────────────────────────


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Seed 90-nation expansion: person profiles (270 officials)"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be seeded")
    parser.add_argument("--no-llm", action="store_true",
                        help="Seed skeletons only (no LLM enrichment)")
    parser.add_argument("--nation", type=str, default=None,
                        help="Seed a single nation only (e.g. ITA)")
    args = parser.parse_args(argv)

    officials = EXPANSION_90
    if args.nation:
        officials = [o for o in officials if o.nation == args.nation.upper()]
        if not officials:
            print(f"No officials defined for nation: {args.nation}")
            return

    nations = sorted(set(o.nation for o in officials))
    print(f"=== Seeding {len(officials)} officials across {len(nations)} nations ===")
    print(f"    Nations: {', '.join(nations)}")

    print("\n--- Position occupancy ---")
    n_pos = _seed_positions(officials, dry_run=args.dry_run)
    print(f"  → {n_pos} positions {'planned' if args.dry_run else 'upserted'}")

    llm_status = "OFF" if args.no_llm else "ON"
    print(f"\n--- Person profiles (LLM={llm_status}) ---")
    n_prof, n_enrich = _seed_profiles(
        officials,
        dry_run=args.dry_run,
        use_llm=not args.no_llm,
    )
    print(f"  → {n_prof} profiles {'planned' if args.dry_run else 'seeded'}, "
          f"{n_enrich} LLM-enriched")

    print("\nDone.")


if __name__ == "__main__":  # pragma: no cover
    main()
