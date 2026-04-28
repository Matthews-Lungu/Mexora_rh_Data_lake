"""
utils.py
========
Mexora RH Intelligence — Shared Utility Functions
Used by: bronze_ingestion.py, silver_transform.py, silver_nlp.py, gold_aggregation.py

Contains:
  - Path resolution (DATA_LAKE_ROOT, pipeline paths)
  - Logging helpers (consistent timestamped output)
  - City normalization (dirty → standard name)
  - Contract normalization
  - Data validation helpers
"""

import os
import re
from datetime import datetime
from pathlib import Path

# ==============================================================================
# SECTION 1 — PATH RESOLUTION
# ==============================================================================
# All paths are resolved relative to the project root.
# This works on both Windows and Linux without hardcoding drive letters.

# Project root = parent of the pipeline/ folder where utils.py lives
PROJECT_ROOT   = Path(__file__).resolve().parent.parent

# Data Lake root
DATA_LAKE_ROOT = PROJECT_ROOT / "data_lake"

# Data generation folder (where raw input files live)
DATA_GEN_DIR   = PROJECT_ROOT / "data_generation"

# Input file paths
PATH_OFFRES_JSON      = DATA_GEN_DIR / "offres_emploi_it_maroc.json"
PATH_REFERENTIEL_JSON = DATA_GEN_DIR / "referentiel_competences_it.json"
PATH_ENTREPRISES_CSV  = DATA_GEN_DIR / "entreprises_it_maroc.csv"

# Data Lake zone paths
BRONZE_ROOT = DATA_LAKE_ROOT / "bronze"
SILVER_ROOT = DATA_LAKE_ROOT / "silver"
GOLD_ROOT   = DATA_LAKE_ROOT / "gold"

# Silver Parquet file paths (used by gold_aggregation.py)
SILVER_OFFRES_PARQUET = SILVER_ROOT / "offres_clean"    / "offres_clean.parquet"
SILVER_COMP_PARQUET   = SILVER_ROOT / "competences_extraites" / "competences.parquet"

# Gold Parquet file paths (used by analyse_marche.py)
GOLD_TOP_COMPETENCES  = GOLD_ROOT / "top_competences.parquet"
GOLD_SALAIRES         = GOLD_ROOT / "salaires_par_profil.parquet"
GOLD_VILLES           = GOLD_ROOT / "offres_par_ville.parquet"
GOLD_ENTREPRISES      = GOLD_ROOT / "entreprises_recruteurs.parquet"
GOLD_TENDANCES        = GOLD_ROOT / "tendances_mensuelles.parquet"


# ==============================================================================
# SECTION 2 — LOGGING
# ==============================================================================

def log(zone: str, message: str):
    """
    Consistent timestamped log output.
    Example: [2024-11-15 14:32:01] [BRONZE] 5000 offers ingested
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{zone.upper()}] {message}")


def log_separator(title: str = ""):
    """Print a visual separator for readability in terminal output."""
    if title:
        print(f"\n{'─' * 20} {title} {'─' * 20}")
    else:
        print("─" * 60)


# ==============================================================================
# SECTION 3 — CITY NORMALIZATION
# ==============================================================================
# Maps every dirty city variant found in raw data → standard name.
# This is the master normalization dictionary used by silver_transform.py.

CITY_NORMALIZATION = {
    # Casablanca variants
    "casablanca":       "Casablanca",
    "casa":             "Casablanca",
    "dar el beida":     "Casablanca",
    "casablanque":      "Casablanca",
    "casablanca":       "Casablanca",

    # Rabat variants
    "rabat":            "Rabat",
    "rabat-salé":       "Rabat",
    "rabat salé":       "Rabat",

    # Tanger variants — critical for Mexora analysis
    "tanger":           "Tanger",
    "tangier":          "Tanger",
    "tanja":            "Tanger",
    "tanger-assilah":   "Tanger",
    "tanger med":       "Tanger",
    "tanger assilah":   "Tanger",

    # Marrakech variants
    "marrakech":        "Marrakech",
    "marrakesh":        "Marrakech",

    # Fès variants
    "fès":              "Fès",
    "fes":              "Fès",

    # Agadir
    "agadir":           "Agadir",

    # Oujda
    "oujda":            "Oujda",

    # Meknès variants
    "meknès":           "Meknès",
    "meknes":           "Meknès",

    # Tétouan variants
    "tétouan":          "Tétouan",
    "tetouan":          "Tétouan",

    # El Jadida variants
    "el jadida":        "El Jadida",
    "el-jadida":        "El Jadida",

    # Kenitra variants
    "kenitra":          "Kenitra",
    "kénitra":          "Kenitra",

    # Mohammedia
    "mohammedia":       "Mohammedia",

    # Settat
    "settat":           "Settat",
}

# Moroccan administrative regions — used in Gold zone for geographic aggregation
REGION_PAR_VILLE = {
    "Casablanca":  "Casablanca-Settat",
    "Mohammedia":  "Casablanca-Settat",
    "El Jadida":   "Casablanca-Settat",
    "Settat":      "Casablanca-Settat",
    "Rabat":       "Rabat-Salé-Kénitra",
    "Kenitra":     "Rabat-Salé-Kénitra",
    "Tanger":      "Tanger-Tétouan-Al Hoceïma",
    "Tétouan":     "Tanger-Tétouan-Al Hoceïma",
    "Marrakech":   "Marrakech-Safi",
    "Agadir":      "Souss-Massa",
    "Fès":         "Fès-Meknès",
    "Meknès":      "Fès-Meknès",
    "Oujda":       "Oriental",
}


def normaliser_ville(ville_raw: str) -> tuple[str, str]:
    """
    Normalize a raw city string to its standard name and region.

    Args:
        ville_raw: dirty city string from raw data

    Returns:
        (ville_standard, region_admin)
        If unrecognized, returns ("Autre", "Autre")

    Examples:
        normaliser_ville("CASABLANCA") → ("Casablanca", "Casablanca-Settat")
        normaliser_ville("Tanja")      → ("Tanger", "Tanger-Tétouan-Al Hoceïma")
        normaliser_ville("casa")       → ("Casablanca", "Casablanca-Settat")
    """
    if not ville_raw or not isinstance(ville_raw, str):
        return "Inconnu", "Inconnu"

    # Lowercase and strip for matching
    ville_clean = ville_raw.lower().strip()

    ville_std = CITY_NORMALIZATION.get(ville_clean, None)

    # If no exact match, try partial match (handles "Casablanca - Zone Industrielle" etc.)
    if not ville_std:
        for dirty, standard in CITY_NORMALIZATION.items():
            if dirty in ville_clean:
                ville_std = standard
                break

    if not ville_std:
        ville_std = "Autre"

    region = REGION_PAR_VILLE.get(ville_std, "Autre")
    return ville_std, region


# ==============================================================================
# SECTION 4 — CONTRACT NORMALIZATION
# ==============================================================================

CONTRACT_NORMALIZATION = {
    # CDI variants
    "cdi":                          "CDI",
    "contrat à durée indéterminée": "CDI",
    "permanent":                    "CDI",
    "cdi - temps plein":            "CDI",
    "contrat cdi":                  "CDI",

    # CDD variants
    "cdd":                          "CDD",
    "contrat à durée déterminée":   "CDD",
    "temporaire":                   "CDD",
    "mission":                      "CDD",

    # Freelance variants
    "freelance":                    "Freelance",
    "consultant":                   "Freelance",
    "auto-entrepreneur":            "Freelance",
    "indépendant":                  "Freelance",
    "mission freelance":            "Freelance",

    # Stage variants
    "stage":                        "Stage",
    "internship":                   "Stage",
    "stage pfe":                    "Stage",
    "stage de fin d'études":        "Stage",
    "stage conventionné":           "Stage",
    "stage 6 mois":                 "Stage",

    # Alternance variants
    "alternance":                   "Alternance",
    "apprentissage":                "Alternance",
    "contrat pro":                  "Alternance",
    "contrat d'apprentissage":      "Alternance",
}


def normaliser_contrat(contrat_raw: str) -> str:
    """
    Normalize a raw contract type string to its standard name.

    Examples:
        normaliser_contrat("cdi")                     → "CDI"
        normaliser_contrat("Contrat à durée indéterminée") → "CDI"
        normaliser_contrat("FREELANCE")               → "Freelance"
    """
    if not contrat_raw or not isinstance(contrat_raw, str):
        return "Non précisé"

    contrat_clean = contrat_raw.lower().strip()
    return CONTRACT_NORMALIZATION.get(contrat_clean, "Autre")


# ==============================================================================
# SECTION 5 — DATE VALIDATION
# ==============================================================================

DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"]


def parser_date(date_str: str):
    """
    Try to parse a date string across multiple known formats.

    Returns:
        datetime object if successful, None if unparseable.
    """
    if not date_str or not isinstance(date_str, str):
        return None

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def valider_dates(date_pub_str: str, date_exp_str: str) -> tuple[str, str, bool]:
    """
    Validate that publication date is before expiration date.
    Normalize both dates to ISO format YYYY-MM-DD.

    Returns:
        (date_pub_iso, date_exp_iso, is_valid)
        is_valid = False if pub > exp or either date is unparseable
    """
    date_pub = parser_date(date_pub_str)
    date_exp = parser_date(date_exp_str)

    if date_pub is None:
        return None, None, False

    date_pub_iso = date_pub.strftime("%Y-%m-%d")
    date_exp_iso = date_exp.strftime("%Y-%m-%d") if date_exp else None

    # Coherence check: publication must be before expiration
    if date_exp and date_pub > date_exp:
        return date_pub_iso, date_exp_iso, False

    return date_pub_iso, date_exp_iso, True


# ==============================================================================
# SECTION 6 — GENERAL VALIDATION
# ==============================================================================

def verifier_fichiers_entree():
    """
    Verify all 3 input files exist before running the pipeline.
    Raises FileNotFoundError with clear message if any file is missing.
    """
    fichiers = {
        "Job offers JSON":      PATH_OFFRES_JSON,
        "Skills referentiel":   PATH_REFERENTIEL_JSON,
        "Companies CSV":        PATH_ENTREPRISES_CSV,
    }

    tous_presents = True
    for nom, chemin in fichiers.items():
        if chemin.exists():
            size_kb = chemin.stat().st_size // 1024
            log("CHECK", f"✅  {nom}: {chemin.name} ({size_kb} KB)")
        else:
            log("CHECK", f"❌  {nom}: NOT FOUND at {chemin}")
            tous_presents = False

    if not tous_presents:
        raise FileNotFoundError(
            "One or more input files are missing. "
            "Run: python data_generation/generate_data.py"
        )

    return True


def verifier_dossiers_lake():
    """
    Ensure Bronze, Silver, Gold directories exist.
    Creates them if missing.
    """
    for zone, path in [("Bronze", BRONZE_ROOT),
                       ("Silver", SILVER_ROOT),
                       ("Gold",   GOLD_ROOT)]:
        path.mkdir(parents=True, exist_ok=True)
        log("SETUP", f"✅  {zone} directory ready: {path}")


# ==============================================================================
# SECTION 7 — QUICK SELF-TEST
# ==============================================================================

if __name__ == "__main__":
    log_separator("utils.py self-test")

    # Test city normalization
    test_cities = [
        "CASABLANCA", "casa", "Dar El Beida",
        "TANGER", "Tanja", "Tangier", "tanger med",
        "RABAT", "Fes", "fès", "marrakesh",
    ]
    print("\nCity normalization test:")
    for raw in test_cities:
        std, region = normaliser_ville(raw)
        print(f"  '{raw}' → '{std}' ({region})")

    # Test contract normalization
    print("\nContract normalization test:")
    test_contracts = ["cdi", "FREELANCE", "Contrat à durée indéterminée",
                      "stage pfe", "Mission freelance"]
    for raw in test_contracts:
        print(f"  '{raw}' → '{normaliser_contrat(raw)}'")

    # Test date validation
    print("\nDate validation test:")
    test_dates = [
        ("2024-08-15", "2024-09-15"),   # valid ISO
        ("15/08/2024", "15/09/2024"),   # valid FR format
        ("2024-09-01", "2024-08-01"),   # invalid: pub > exp
        ("not-a-date", "2024-09-15"),   # invalid: unparseable
    ]
    for pub, exp in test_dates:
        p, e, valid = valider_dates(pub, exp)
        print(f"  pub='{pub}' exp='{exp}' → valid={valid}, normalized=({p}, {e})")

    # Test path resolution
    print(f"\nPath resolution:")
    print(f"  PROJECT_ROOT:    {PROJECT_ROOT}")
    print(f"  DATA_LAKE_ROOT:  {DATA_LAKE_ROOT}")
    print(f"  PATH_OFFRES:     {PATH_OFFRES_JSON}")

    log_separator("self-test complete")