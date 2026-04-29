"""
silver_nlp.py
=============
Mexora RH Intelligence — NLP Skill Extraction
Step 2.4 of the pipeline: Silver (clean Parquet) → Skill extraction Parquet

WHAT THIS STEP DOES:
  Loads offres_clean.parquet from Silver zone.
  Loads referentiel_competences_it.json (skill alias dictionary).
  For each offer, scans two text fields:
    - competences_brut  (structured but dirty skill list)
    - description       (free text job description)
  Extracts normalized skill mentions using word-boundary regex.
  Outputs one row per (id_offre × competence) — long format.
  Writes: data_lake/silver/competences_extraites/competences.parquet

KEY TECHNICAL DECISIONS:
  1. Word-boundary regex (\b) — prevents "R" matching inside "React"
  2. Aliases sorted by LENGTH descending — "apache spark" matches before "spark"
  3. Either field sufficient — source column tracks where match was found
  4. Deduplication per offer — each normalized skill counted once per offer
  5. Family tagging — each skill linked to its technology family

Usage:
    python pipeline/silver_nlp.py
    (or called from main.py)
"""

import re
import sys
import json
from pathlib import Path
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Import shared utilities
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import (
    log, log_separator,
    SILVER_ROOT,
    PATH_REFERENTIEL_JSON,
    SILVER_OFFRES_PARQUET,
    SILVER_COMP_PARQUET,
)


# ==============================================================================
# SECTION 1 — LOAD AND COMPILE REFERENTIEL
# ==============================================================================

def charger_referentiel(path_referentiel: str) -> list[dict]:
    """
    Load the skill referentiel JSON and compile one regex pattern per alias.

    Strategy:
      - For each normalized skill, collect all its aliases
      - Sort aliases by LENGTH DESCENDING (longest match wins)
      - Compile a word-boundary regex for each alias
      - Return a flat list of compiled patterns with metadata

    Returns:
        List of dicts, each containing:
          {
            'skill_normalise': str,   e.g. "spark"
            'famille':         str,   e.g. "data_engineering"
            'alias':           str,   e.g. "apache spark"
            'pattern':         re.Pattern  compiled regex
          }
        Sorted globally by alias length descending.

    Why word boundaries (\b)?
      Without \b, the alias "r" would match the letter r inside every word.
      The alias "spark" would match inside "sparkle".
      \b anchors the match to word boundaries only.

    Why sort by length?
      If aliases are ["apache spark", "spark"] and text contains "apache spark",
      we want ONE match for "apache spark", not two separate matches.
      Sorting longest-first ensures the longer alias is found first.
    """
    log("NLP", f"Loading referentiel from {path_referentiel}")

    with open(path_referentiel, 'r', encoding='utf-8') as f:
        ref = json.load(f)

    familles = ref.get('familles', {})
    patterns = []

    for famille_nom, skills in familles.items():
        for skill_normalise, aliases in skills.items():
            for alias in aliases:
                alias_clean = alias.strip().lower()
                if not alias_clean:
                    continue

                # Build word-boundary regex
                # re.escape handles special chars like "c++", ".net", "c#"
                alias_escaped = re.escape(alias_clean)

                # For aliases that end/start with special chars (c++, .net),
                # word boundary \b may not work — use lookahead/lookbehind instead
                if re.search(r'[^\w\s]$', alias_clean) or \
                   re.search(r'^[^\w\s]', alias_clean):
                    # Use space or start/end of string as boundary
                    pattern_str = r'(?<![a-zA-Z0-9])' + alias_escaped + \
                                  r'(?![a-zA-Z0-9])'
                else:
                    pattern_str = r'\b' + alias_escaped + r'\b'

                try:
                    compiled = re.compile(pattern_str, re.IGNORECASE)
                    patterns.append({
                        'skill_normalise': skill_normalise,
                        'famille':         famille_nom,
                        'alias':           alias_clean,
                        'pattern':         compiled,
                    })
                except re.error:
                    # Skip malformed patterns silently
                    continue

    # Sort globally by alias length DESCENDING
    # This ensures "apache spark" is tried before "spark"
    patterns.sort(key=lambda x: len(x['alias']), reverse=True)

    log("NLP", f"Referentiel compiled: {len(patterns)} patterns across "
        f"{sum(len(s) for s in familles.values())} normalized skills")

    return patterns


# ==============================================================================
# SECTION 2 — SKILL EXTRACTION FOR ONE OFFER
# ==============================================================================

def extraire_competences_offre(
    id_offre: str,
    competences_brut: str,
    description: str,
    patterns: list[dict]
) -> list[dict]:
    """
    Extract all skill mentions from a single offer.

    Scans both competences_brut and description.
    Uses word-boundary regex matching with longest-alias-first ordering.
    Deduplicates: each normalized skill appears at most once per offer.

    Args:
        id_offre         : offer identifier
        competences_brut : raw skill string from offer
        description      : free-text job description
        patterns         : compiled pattern list from charger_referentiel()

    Returns:
        List of dicts, one per unique skill found:
          {
            'id_offre':          str,
            'skill_normalise':   str,
            'famille':           str,
            'alias_trouve':      str,   which alias triggered the match
            'source':            str,   'competences_brut' | 'description' | 'both'
            'nb_mentions_desc':  int,   mention count in description
          }
    """
    text_comp = (competences_brut or '').lower()
    text_desc = (description or '').lower()

    # Track found skills: skill_normalise → {source info}
    # Key = skill_normalise to ensure deduplication
    found: dict[str, dict] = {}

    for p in patterns:
        skill  = p['skill_normalise']
        alias  = p['alias']
        pattern = p['pattern']

        # Skip if this normalized skill already found by a longer alias
        if skill in found:
            continue

        in_comp = bool(pattern.search(text_comp))
        in_desc = bool(pattern.search(text_desc))

        if not in_comp and not in_desc:
            continue

        # Determine source
        if in_comp and in_desc:
            source = 'both'
        elif in_comp:
            source = 'competences_brut'
        else:
            source = 'description'

        # Count mentions in description (useful for weighting later)
        nb_mentions = len(pattern.findall(text_desc))

        found[skill] = {
            'id_offre':         id_offre,
            'skill_normalise':  skill,
            'famille':          p['famille'],
            'alias_trouve':     alias,
            'source':           source,
            'nb_mentions_desc': nb_mentions,
        }

    return list(found.values())


# ==============================================================================
# SECTION 3 — MAIN EXTRACTION FUNCTION
# (implements the function from the project statement)
# ==============================================================================

def extraire_competences_silver(
    df_offres: pd.DataFrame,
    patterns: list[dict]
) -> pd.DataFrame:
    """
    Apply skill extraction to all offers in the Silver DataFrame.

    Args:
        df_offres : Silver cleaned DataFrame (from offres_clean.parquet)
        patterns  : compiled pattern list from charger_referentiel()

    Returns:
        Long-format DataFrame — one row per (id_offre × skill_normalise)
        Columns:
          id_offre, profil_normalise, ville, source_offre,
          skill_normalise, famille, alias_trouve, source, nb_mentions_desc
    """
    log("NLP", f"Starting skill extraction for {len(df_offres)} offers...")

    all_rows = []
    nb_sans_competences = 0

    for idx, row in df_offres.iterrows():
        id_offre        = row['id_offre']
        competences_brut = str(row.get('competences_brut', '') or '')
        description      = str(row.get('description', '') or '')

        competences_trouvees = extraire_competences_offre(
            id_offre        = id_offre,
            competences_brut = competences_brut,
            description      = description,
            patterns         = patterns,
        )

        if not competences_trouvees:
            nb_sans_competences += 1

        # Enrich each skill row with offer-level context
        for comp in competences_trouvees:
            comp['profil_normalise'] = row.get('profil_normalise', '')
            comp['ville']            = row.get('ville', '')
            comp['type_contrat']     = row.get('type_contrat', '')
            comp['annee_publication'] = row.get('annee_publication', None)
            comp['mois_publication']  = row.get('mois_publication', None)
            comp['source_offre']      = row.get('source', '')
            all_rows.append(comp)

        # Progress indicator every 1000 offers
        if (idx + 1) % 1000 == 0:
            log("NLP", f"  Processed {idx + 1}/{len(df_offres)} offers "
                f"({len(all_rows)} skill matches so far)...")

    df_competences = pd.DataFrame(all_rows)

    log("NLP", f"Extraction complete:")
    log("NLP", f"  Total skill matches  : {len(df_competences)}")
    log("NLP", f"  Unique (offer,skill) : {len(df_competences)}")
    log("NLP", f"  Offers with 0 skills : {nb_sans_competences}")
    if len(df_offres) > 0:
        avg = len(df_competences) / len(df_offres)
        log("NLP", f"  Avg skills per offer : {avg:.1f}")

    return df_competences


# ==============================================================================
# SECTION 4 — WRITE TO PARQUET
# ==============================================================================

def ecrire_competences_parquet(df_competences: pd.DataFrame):
    """
    Write the extracted skills DataFrame to Silver zone as Parquet.
    Output: silver/competences_extraites/competences.parquet
    """
    dir_comp = SILVER_ROOT / 'competences_extraites'
    dir_comp.mkdir(parents=True, exist_ok=True)

    path_comp = dir_comp / 'competences.parquet'

    table = pa.Table.from_pandas(df_competences)
    pq.write_table(table, path_comp, compression='snappy')

    size_kb = path_comp.stat().st_size // 1024
    log("NLP", f"competences.parquet written — {len(df_competences)} rows, {size_kb} KB")

    return path_comp


# ==============================================================================
# SECTION 5 — STATS REPORTER
# ==============================================================================

def afficher_stats_nlp(df_competences: pd.DataFrame):
    """Print a clear NLP extraction summary."""
    log_separator("NLP EXTRACTION SUMMARY")

    if df_competences.empty:
        print("  No skills extracted.")
        return

    total_matches = len(df_competences)
    nb_offres     = df_competences['id_offre'].nunique()
    nb_skills     = df_competences['skill_normalise'].nunique()

    print(f"\n  Total skill matches     : {total_matches}")
    print(f"  Offers with ≥1 skill    : {nb_offres}")
    print(f"  Unique skills found     : {nb_skills}")
    print(f"  Avg skills per offer    : {total_matches/nb_offres:.1f}")

    # Top 15 skills overall
    print(f"\n  Top 15 most demanded skills (all profiles):")
    top_skills = (
        df_competences.groupby('skill_normalise')['id_offre']
        .nunique()
        .sort_values(ascending=False)
        .head(15)
    )
    for skill, count in top_skills.items():
        pct = count * 100 / nb_offres
        bar = '█' * int(pct / 3)
        print(f"    {skill:<25} {count:>5} offers  ({pct:5.1f}%)  {bar}")

    # Skills by family
    print(f"\n  Skills by technology family:")
    famille_counts = (
        df_competences.groupby('famille')['id_offre']
        .nunique()
        .sort_values(ascending=False)
    )
    for famille, count in famille_counts.items():
        pct = count * 100 / nb_offres
        print(f"    {famille:<25} {count:>5} offers  ({pct:5.1f}%)")

    # Source breakdown
    print(f"\n  Match source breakdown:")
    source_counts = df_competences['source'].value_counts()
    for source, count in source_counts.items():
        pct = count * 100 / total_matches
        print(f"    {source:<25} {count:>6}  ({pct:5.1f}%)")

    # Top skills per profile (Data Engineer focus — relevant for Mexora)
    print(f"\n  Top 8 skills for Data Engineer profile:")
    df_de = df_competences[df_competences['profil_normalise'] == 'Data Engineer']
    if not df_de.empty:
        top_de = (
            df_de.groupby('skill_normalise')['id_offre']
            .nunique()
            .sort_values(ascending=False)
            .head(8)
        )
        for skill, count in top_de.items():
            print(f"    {skill:<25} {count:>5} offers")

    log_separator()


# ==============================================================================
# SECTION 6 — ENTRY POINT
# ==============================================================================

def run_nlp():
    """
    Main NLP extraction entry point.
    Called by main.py orchestrator.
    """
    log_separator("STEP 2.3 — NLP SKILL EXTRACTION")

    # 1. Load Silver Parquet
    path_silver = SILVER_ROOT / 'offres_clean' / 'offres_clean.parquet'
    if not path_silver.exists():
        log("NLP", "❌  offres_clean.parquet not found. Run silver_transform.py first.")
        return None

    log("NLP", f"Loading Silver Parquet: {path_silver}")
    df_offres = pd.read_parquet(path_silver)
    log("NLP", f"{len(df_offres)} offers loaded from Silver")

    # 2. Load and compile referentiel
    if not PATH_REFERENTIEL_JSON.exists():
        log("NLP", f"❌  Referentiel not found at {PATH_REFERENTIEL_JSON}")
        return None

    patterns = charger_referentiel(str(PATH_REFERENTIEL_JSON))

    # 3. Extract skills
    df_competences = extraire_competences_silver(df_offres, patterns)

    if df_competences.empty:
        log("NLP", "⚠️  No skills extracted. Check referentiel and text fields.")
        return None

    # 4. Write to Parquet
    ecrire_competences_parquet(df_competences)

    # 5. Print stats
    afficher_stats_nlp(df_competences)

    log("NLP", "NLP skill extraction complete ✅")
    return df_competences


# ==============================================================================
# SECTION 7 — DIRECT RUN
# ==============================================================================

if __name__ == "__main__":
    run_nlp()