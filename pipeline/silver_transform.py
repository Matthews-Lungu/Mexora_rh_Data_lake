"""
silver_transform.py
===================
Mexora RH Intelligence — Silver Zone Transformation
Step 2.3 of the pipeline: Bronze (raw JSON) → Silver (clean Parquet)

WHAT THIS STEP DOES:
  Reads all raw offers from Bronze zone partitions.
  Applies the following transformations on each field:
    - titre_poste    : normalize profile category from dirty titles
    - ville          : normalize city name + add region
    - type_contrat   : normalize contract type
    - salaire_brut   : parse salary string → sal_min, sal_max (MAD integers)
    - experience     : parse experience string → exp_min, exp_max (years)
    - date_*         : normalize to ISO format, flag incoherent dates
    - competences_brut: kept raw for silver_nlp.py to process
  Adds data quality flags for each transformation.
  Writes two Parquet files:
    - data_lake/silver/offres_clean/offres_clean.parquet
    - data_lake/silver/metadonnees_qualite/qualite_rapport.parquet

Usage:
    python pipeline/silver_transform.py
    (or called from main.py)
"""

import re
import json
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Import shared utilities
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import (
    log, log_separator,
    BRONZE_ROOT, SILVER_ROOT,
    normaliser_ville,
    normaliser_contrat,
    valider_dates,
)


from bronze_ingestion import charger_depuis_bronze as lire_bronze


# ==============================================================================
# SECTION 1 — TITLE → PROFILE NORMALIZATION
# ==============================================================================
# Maps any dirty title string to one of 11 canonical profile names.
# Strategy: sorted keyword matching, longest match wins.

PROFIL_KEYWORDS = {
    "Data Scientist":         ["data scientist", "machine learning engineer",
                               "ml engineer", "ingénieur machine learning",
                               "ai engineer", "ingénieur ia", "deep learning"],
    "Data Engineer":          ["data engineer", "ingénieur data", "ingénieur big data",
                               "big data engineer", "ingénieur etl", "etl developer",
                               "pipeline developer", "dev data"],
    "Data Analyst":           ["data analyst", "analyste data", "analyste bi",
                               "business analyst data", "bi analyst", "analyste reporting",
                               "chargé de reporting", "bi developer", "développeur bi",
                               "ingénieur bi", "analyste de données", "reporting analyst"],
    "Développeur Full Stack": ["full stack", "fullstack", "full-stack"],
    "Développeur Backend":    ["backend", "back-end", "développeur api",
                               "ingénieur logiciel backend", "développeur serveur"],
    "Développeur Frontend":   ["frontend", "front-end", "développeur react",
                               "développeur angular", "ui developer"],
    "Développeur Mobile":     ["mobile", "react native", "flutter developer",
                               "développeur ios", "développeur android"],
    "DevOps / SRE":           ["devops", "sre", "site reliability", "devsecops",
                               "ingénieur infra"],
    "Cloud Engineer":         ["cloud engineer", "ingénieur cloud", "architecte cloud",
                               "cloud architect", "aws engineer", "azure engineer",
                               "gcp engineer"],
    "Cybersécurité":          ["cybersécurité", "cybersecurity", "sécurité",
                               "pentest", "analyste soc", "soc analyst", "rssi"],
    "Chef de Projet IT":      ["chef de projet", "project manager", "scrum master",
                               "product owner", "pmo", "responsable projet"],
}

# Pre-sort keywords by length descending — longer matches take priority
# e.g. "data scientist" must match before "data"
PROFIL_KEYWORDS_SORTED = {
    profil: sorted(kws, key=len, reverse=True)
    for profil, kws in PROFIL_KEYWORDS.items()
}


def normaliser_titre(titre_raw: str) -> tuple[str, bool]:
    """
    Map a dirty job title to its canonical profile name.

    Returns:
        (profil_normalise, is_mapped)
        is_mapped = False if no keyword matched (title kept as-is)
    """
    if not titre_raw or not isinstance(titre_raw, str):
        return "Non classifié", False

    titre_lower = titre_raw.lower().strip()

    for profil, keywords in PROFIL_KEYWORDS_SORTED.items():
        for kw in keywords:
            if kw in titre_lower:
                return profil, True

    return "Non classifié", False


# ==============================================================================
# SECTION 2 — SALARY PARSING
# ==============================================================================
# Salary field contains 7 different dirty formats + nulls + "Confidentiel".
# We parse each into (sal_min_mad, sal_max_mad) as integers in MAD.
# EUR values are converted: 1 EUR ≈ 10.8 MAD (approximate 2024 rate)

EUR_TO_MAD = 10.8

# Regex patterns for each salary format
RE_RANGE_MAD  = re.compile(r'(\d[\d\s]*)\s*[-–]\s*(\d[\d\s]*)\s*(mad|dh|dhs)?', re.I)
RE_RANGE_K    = re.compile(r'(\d+)\s*[Kk]\s*[-–]\s*(\d+)\s*[Kk]')
RE_SINGLE_K   = re.compile(r'(\d+)\s*[Kk]')
RE_SINGLE_MAD = re.compile(r'(\d[\d\s]*)\s*(mad|dh|dhs)', re.I)
RE_RANGE_EUR  = re.compile(r'(\d+)\s*[-–]\s*(\d+)\s*eur', re.I)
RE_SINGLE_EUR = re.compile(r'(\d+)\s*eur', re.I)


def parser_salaire(salaire_raw) -> tuple[int | None, int | None, str]:
    """
    Parse a raw salary string into (sal_min_mad, sal_max_mad, devise).

    Returns:
        (sal_min, sal_max, devise)
        sal_min and sal_max are integers in MAD, or None if unparseable.
        devise is 'MAD', 'EUR' (converted), or 'Inconnu'

    Examples:
        "15000-20000 MAD"  → (15000, 20000, 'MAD')
        "15K-20K MAD"      → (15000, 20000, 'MAD')
        "1500-1800 EUR"    → (16200, 19440, 'EUR→MAD')
        "Selon profil"     → (None, None, 'Confidentiel')
    """
    if not salaire_raw or not isinstance(salaire_raw, str):
        return None, None, 'Non renseigné'

    s = salaire_raw.strip()

    # Confidential / negotiable
    mots_confidentiel = ['confidentiel', 'selon profil', 'à négocier',
                         'negocier', 'profil', 'a negocier']
    if any(m in s.lower() for m in mots_confidentiel) or s == '':
        return None, None, 'Confidentiel'

    # EUR range: "1500-1800 EUR"
    m = RE_RANGE_EUR.search(s)
    if m:
        sal_min = round(int(m.group(1)) * EUR_TO_MAD / 500) * 500
        sal_max = round(int(m.group(2)) * EUR_TO_MAD / 500) * 500
        return sal_min, sal_max, 'EUR→MAD'

    # Single EUR: "1500 EUR"
    m = RE_SINGLE_EUR.search(s)
    if m:
        val = round(int(m.group(1)) * EUR_TO_MAD / 500) * 500
        return val, val, 'EUR→MAD'

    # K range: "15K-20K" or "15k-20k dh"
    m = RE_RANGE_K.search(s)
    if m:
        return int(m.group(1)) * 1000, int(m.group(2)) * 1000, 'MAD'

    # Single K: "15K" or "15k dh"
    m = RE_SINGLE_K.search(s)
    if m and 'eur' not in s.lower():
        val = int(m.group(1)) * 1000
        return val, val, 'MAD'

    # Numeric range with MAD/DH: "15000 - 20000 MAD"
    m = RE_RANGE_MAD.search(s)
    if m:
        sal_min = int(re.sub(r'\s', '', m.group(1)))
        sal_max = int(re.sub(r'\s', '', m.group(2)))
        return sal_min, sal_max, 'MAD'

    # Single numeric with MAD/DH: "15000 MAD"
    m = RE_SINGLE_MAD.search(s)
    if m:
        val = int(re.sub(r'\s', '', m.group(1)))
        return val, val, 'MAD'

    return None, None, 'Non parseable'


# ==============================================================================
# SECTION 3 — EXPERIENCE PARSING
# ==============================================================================

RE_EXP_RANGE = re.compile(r'(\d+)\s*[-àa]\s*(\d+)\s*an', re.I)
RE_EXP_MIN   = re.compile(r'(?:min(?:imum)?|au moins)?\s*(\d+)\s*an', re.I)
RE_EXP_PLUS  = re.compile(r'(\d+)\s*\+', re.I)


def parser_experience(exp_raw) -> tuple[int | None, int | None]:
    """
    Parse a raw experience string into (exp_min_years, exp_max_years).

    Returns:
        (exp_min, exp_max) as integers, or (None, None) if unparseable.

    Examples:
        "3-5 ans"          → (3, 5)
        "min 2 ans"        → (2, None)
        "Débutant accepté" → (0, 1)
        "Senior (7+ ans)"  → (7, None)
        None               → (None, None)
    """
    if not exp_raw or not isinstance(exp_raw, str):
        return None, None

    s = exp_raw.strip().lower()

    # Débutant / entry level
    if any(w in s for w in ['débutant', 'debutant', '0 à 1', '0-1', '0 an']):
        return 0, 1

    # Range: "3-5 ans", "3 à 5 ans"
    m = RE_EXP_RANGE.search(s)
    if m:
        return int(m.group(1)), int(m.group(2))

    # X+ : "7+ ans", "5+"
    m = RE_EXP_PLUS.search(s)
    if m:
        return int(m.group(1)), None

    # "min X ans", "minimum X ans", "X ans"
    m = RE_EXP_MIN.search(s)
    if m:
        val = int(m.group(1))
        return val, None

    return None, None


# ==============================================================================
# SECTION 4 — MAIN TRANSFORMATION FUNCTION
# (implements the function from the project statement)
# ==============================================================================

def transformer_silver(offres_brutes: list) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply all Silver transformations to the list of raw offers from Bronze.

    Args:
        offres_brutes: list of raw offer dicts loaded from Bronze zone

    Returns:
        (df_offres_clean, df_qualite)
        df_offres_clean : cleaned DataFrame ready for Parquet
        df_qualite      : data quality report DataFrame
    """
    log("SILVER", f"Starting transformation of {len(offres_brutes)} offers...")

    rows_clean  = []
    rows_qualite = []

    for offre in offres_brutes:
        offre_id = offre.get('id_offre', 'UNKNOWN')
        qualite  = {'id_offre': offre_id, 'anomalies': []}

        # ── 1. TITLE → PROFILE ─────────────────────────────────────────────
        titre_raw = offre.get('titre_poste', '')
        profil_normalise, titre_mapped = normaliser_titre(titre_raw)
        if not titre_mapped:
            qualite['anomalies'].append('titre_non_classifie')

        # ── 2. CITY ────────────────────────────────────────────────────────
        ville_raw = offre.get('ville', '')
        ville_std, region = normaliser_ville(ville_raw)
        if ville_std == 'Autre' or ville_std == 'Inconnu':
            qualite['anomalies'].append('ville_non_reconnue')

        # ── 3. CONTRACT ────────────────────────────────────────────────────
        contrat_raw = offre.get('type_contrat', '')
        contrat_std = normaliser_contrat(contrat_raw)
        if contrat_std in ['Non précisé', 'Autre']:
            qualite['anomalies'].append('contrat_non_reconnu')

        # ── 4. SALARY ──────────────────────────────────────────────────────
        salaire_raw = offre.get('salaire_brut', '')
        sal_min, sal_max, sal_devise = parser_salaire(salaire_raw)

        # Sanity check: salary range must be coherent
        if sal_min and sal_max and sal_min > sal_max:
            sal_min, sal_max = sal_max, sal_min   # swap silently
            qualite['anomalies'].append('salaire_inverti')
        if sal_min and (sal_min < 1500 or sal_min > 100000):
            qualite['anomalies'].append('salaire_hors_plage')
        if sal_devise == 'Non parseable':
            qualite['anomalies'].append('salaire_non_parseable')

        # ── 5. EXPERIENCE ──────────────────────────────────────────────────
        exp_raw = offre.get('experience_requise', '')
        exp_min, exp_max = parser_experience(exp_raw)
        if exp_min is None:
            qualite['anomalies'].append('experience_non_parseable')

        # ── 6. DATES ───────────────────────────────────────────────────────
        date_pub_raw = offre.get('date_publication', '')
        date_exp_raw = offre.get('date_expiration', '')
        date_pub_iso, date_exp_iso, dates_valides = valider_dates(
            date_pub_raw, date_exp_raw
        )
        if not dates_valides:
            qualite['anomalies'].append('dates_incoherentes')

        # ── 7. EXTRACT MONTH/YEAR for partitioning ─────────────────────────
        mois_publication = None
        annee_publication = None
        if date_pub_iso:
            try:
                dt = datetime.strptime(date_pub_iso, "%Y-%m-%d")
                mois_publication  = dt.month
                annee_publication = dt.year
            except ValueError:
                pass

        # ── BUILD CLEAN ROW ────────────────────────────────────────────────
        row = {
            # Identifiers
            'id_offre':            offre_id,
            'source':              offre.get('source', ''),

            # Normalized title and profile
            'titre_poste_original': titre_raw,
            'profil_normalise':    profil_normalise,

            # Normalized location
            'ville_originale':     ville_raw,
            'ville':               ville_std,
            'region':              region,

            # Normalized contract
            'contrat_original':    contrat_raw,
            'type_contrat':        contrat_std,

            # Parsed salary
            'salaire_brut_original': str(salaire_raw) if salaire_raw else None,
            'salaire_min_mad':     sal_min,
            'salaire_max_mad':     sal_max,
            'salaire_devise':      sal_devise,

            # Parsed experience
            'experience_originale': exp_raw,
            'experience_min_ans':  exp_min,
            'experience_max_ans':  exp_max,

            # Normalized dates
            'date_publication':    date_pub_iso,
            'date_expiration':     date_exp_iso,
            'dates_valides':       dates_valides,
            'mois_publication':    mois_publication,
            'annee_publication':   annee_publication,

            # Fields passed through for NLP step
            'description':         offre.get('description', ''),
            'competences_brut':    offre.get('competences_brut', ''),

            # Other fields
            'entreprise':          offre.get('entreprise', ''),
            'secteur':             offre.get('secteur', ''),
            'niveau_etudes':       offre.get('niveau_etudes', ''),
            'nb_postes':           offre.get('nb_postes', 1),
            'teletravail':         offre.get('teletravail', ''),
            'langue_requise':      json.dumps(
                offre.get('langue_requise', []), ensure_ascii=False
            ),

            # Data quality
            'nb_anomalies':        len(qualite['anomalies']),
            'anomalies':           ', '.join(qualite['anomalies']),
        }

        rows_clean.append(row)

        # Quality report row
        rows_qualite.append({
            'id_offre':         offre_id,
            'profil':           profil_normalise,
            'ville':            ville_std,
            'nb_anomalies':     len(qualite['anomalies']),
            'anomalies':        ', '.join(qualite['anomalies']),
            'titre_mapped':     titre_mapped,
            'dates_valides':    dates_valides,
            'salaire_parseable': sal_devise not in ['Confidentiel',
                                                    'Non parseable',
                                                    'Non renseigné'],
        })

    df_offres_clean = pd.DataFrame(rows_clean)
    df_qualite      = pd.DataFrame(rows_qualite)

    log("SILVER", f"Transformation complete. Shape: {df_offres_clean.shape}")
    return df_offres_clean, df_qualite


# ==============================================================================
# SECTION 5 — WRITE TO PARQUET
# ==============================================================================

def ecrire_silver_parquet(df_offres_clean: pd.DataFrame,
                          df_qualite: pd.DataFrame):
    """
    Write the cleaned DataFrame to Silver zone as Parquet.
    Two files produced:
      - silver/offres_clean/offres_clean.parquet
      - silver/metadonnees_qualite/qualite_rapport.parquet
    """
    # ── Offres clean ──────────────────────────────────────────────────────
    dir_offres = SILVER_ROOT / 'offres_clean'
    dir_offres.mkdir(parents=True, exist_ok=True)
    path_offres = dir_offres / 'offres_clean.parquet'

    table_offres = pa.Table.from_pandas(df_offres_clean)
    pq.write_table(table_offres, path_offres, compression='snappy')

    size_kb = path_offres.stat().st_size // 1024
    log("SILVER", f"offres_clean.parquet written — {len(df_offres_clean)} rows, {size_kb} KB")

    # ── Quality report ────────────────────────────────────────────────────
    dir_qualite = SILVER_ROOT / 'metadonnees_qualite'
    dir_qualite.mkdir(parents=True, exist_ok=True)
    path_qualite = dir_qualite / 'qualite_rapport.parquet'

    table_qualite = pa.Table.from_pandas(df_qualite)
    pq.write_table(table_qualite, path_qualite, compression='snappy')

    size_kb2 = path_qualite.stat().st_size // 1024
    log("SILVER", f"qualite_rapport.parquet written — {len(df_qualite)} rows, {size_kb2} KB")


# ==============================================================================
# SECTION 6 — QUALITY REPORT PRINTER
# ==============================================================================

def afficher_rapport_qualite(df_offres: pd.DataFrame, df_qualite: pd.DataFrame):
    """Print a human-readable data quality report after Silver transformation."""
    log_separator("SILVER QUALITY REPORT")

    total = len(df_offres)

    # Profile distribution
    print("\n  Profile distribution (after normalization):")
    profil_counts = df_offres['profil_normalise'].value_counts()
    for profil, count in profil_counts.items():
        pct = count * 100 / total
        print(f"    {profil:<30} {count:>5}  ({pct:5.1f}%)")

    # City distribution
    print("\n  Top 5 cities (after normalization):")
    ville_counts = df_offres['ville'].value_counts().head(5)
    for ville, count in ville_counts.items():
        pct = count * 100 / total
        print(f"    {ville:<20} {count:>5}  ({pct:5.1f}%)")

    # Contract distribution
    print("\n  Contract types:")
    contrat_counts = df_offres['type_contrat'].value_counts()
    for contrat, count in contrat_counts.items():
        pct = count * 100 / total
        print(f"    {contrat:<20} {count:>5}  ({pct:5.1f}%)")

    # Salary coverage
    sal_renseignes = df_offres['salaire_min_mad'].notna().sum()
    print(f"\n  Salary coverage  : {sal_renseignes}/{total} "
          f"({sal_renseignes*100/total:.1f}%) offers have parseable salary")

    # Anomaly summary
    print(f"\n  Data quality:")
    propres = (df_qualite['nb_anomalies'] == 0).sum()
    print(f"    Clean offers (0 anomalies) : {propres}/{total} "
          f"({propres*100/total:.1f}%)")
    print(f"    1 anomaly                  : "
          f"{(df_qualite['nb_anomalies'] == 1).sum()}")
    print(f"    2+ anomalies               : "
          f"{(df_qualite['nb_anomalies'] >= 2).sum()}")

    if df_qualite['nb_anomalies'].sum() > 0:
        print(f"\n  Most frequent anomaly types:")
        all_anomalies = []
        for a in df_qualite['anomalies']:
            if a:
                all_anomalies.extend(a.split(', '))
        from collections import Counter
        for anomaly, count in Counter(all_anomalies).most_common(5):
            print(f"    {anomaly:<35} {count}")

    log_separator()


# ==============================================================================
# SECTION 7 — ENTRY POINT
# ==============================================================================

def run_silver():
    """
    Main Silver transformation entry point.
    Called by main.py orchestrator.
    """
    log_separator("STEP 2.2 — SILVER TRANSFORMATION")

    # 1. Load all offers from Bronze
    offres_brutes = lire_bronze(str(BRONZE_ROOT.parent))

    if not offres_brutes:
        log("SILVER", "❌  No offers found in Bronze zone. Run bronze_ingestion.py first.")
        return None, None

    # 2. Transform
    df_offres_clean, df_qualite = transformer_silver(offres_brutes)

    # 3. Write to Parquet
    ecrire_silver_parquet(df_offres_clean, df_qualite)

    # 4. Print quality report
    afficher_rapport_qualite(df_offres_clean, df_qualite)
    

    log("SILVER", "Silver transformation complete ✅")
    return df_offres_clean, df_qualite


# ==============================================================================
# SECTION 8 — DIRECT RUN
# ==============================================================================

if __name__ == "__main__":
    run_silver()