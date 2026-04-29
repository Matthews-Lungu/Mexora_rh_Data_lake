"""
gold_aggregation.py
===================
Mexora RH Intelligence — Gold Zone Aggregation
Step 2.5 of the pipeline: Silver Parquet → 5 Gold analytical tables

WHAT THIS STEP DOES:
  Reads Silver Parquet files directly via DuckDB (no pandas loading).
  Builds 5 business-ready Gold tables, one per analytical question.

  Table 1: top_competences.parquet
    → Skills ranked by demand, per profile and globally ('tous')
    → Columns match Step 3 Query 1 exactly

  Table 2: offres_par_ville.parquet
    → Offer counts and remote work rates by city × profile
    → Columns match Step 3 Query 2 exactly

  Table 3: salaires_par_profil.parquet
    → Salary statistics (median, Q1, Q3, min, max) by profile × city
    → Columns match Step 3 Query 3 exactly

  Table 4: entreprises_recruteurs.parquet
    → Company recruiting activity with profile arrays
    → Columns match Step 3 Query 5 exactly

  Table 5: tendances_mensuelles.parquet
    → Monthly offer volume by profile
    → Used for trend analysis

Usage:
    python pipeline/gold_aggregation.py
    (or called from main.py)
"""

import sys
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

# Import shared utilities
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import (
    log, log_separator,
    SILVER_ROOT, GOLD_ROOT,
)

# Silver file paths (inputs)
SILVER_OFFRES = SILVER_ROOT / 'offres_clean'    / 'offres_clean.parquet'
SILVER_COMP   = SILVER_ROOT / 'competences_extraites' / 'competences.parquet'


# ==============================================================================
# SECTION 1 — DUCKDB CONNECTION HELPER
# ==============================================================================

def get_con():
    """Return an in-memory DuckDB connection."""
    return duckdb.connect()


# ==============================================================================
# SECTION 2 — GOLD TABLE 1: TOP COMPETENCES
# Columns required by Query 1:
#   famille, competence, nb_offres_mentionnent,
#   pct_offres_total, rang_dans_profil, profil
# ==============================================================================

def construire_top_competences(con) -> pd.DataFrame:
    """
    Build top_competences Gold table.
    One row per (profil × competence).
    Also includes profil = 'tous' for global aggregation (required by Query 1).
    rang_dans_profil is pre-computed here so Query 1 can filter directly.
    """
    log("GOLD", "Building top_competences table...")

    path_comp   = str(SILVER_COMP).replace('\\', '/')
    path_offres = str(SILVER_OFFRES).replace('\\', '/')

    # Total number of offers — used for pct_offres_total
    total_offres = con.execute(
        f"SELECT COUNT(DISTINCT id_offre) FROM read_parquet('{path_offres}')"
    ).fetchone()[0]

    # ── Per-profile aggregation ───────────────────────────────────────────
    df_profil = con.execute(f"""
        SELECT
            profil_normalise                        AS profil,
            famille,
            skill_normalise                         AS competence,
            COUNT(DISTINCT id_offre)                AS nb_offres_mentionnent,
            ROUND(COUNT(DISTINCT id_offre) * 100.0
                  / {total_offres}, 2)              AS pct_offres_total
        FROM read_parquet('{path_comp}')
        WHERE profil_normalise != 'Non classifié'
          AND profil_normalise IS NOT NULL
        GROUP BY profil_normalise, famille, skill_normalise
        ORDER BY profil_normalise, nb_offres_mentionnent DESC
    """).df()

    # Add rank within each profile
    df_profil['rang_dans_profil'] = (
        df_profil
        .groupby('profil')['nb_offres_mentionnent']
        .rank(method='dense', ascending=False)
        .astype(int)
    )

    # ── Global 'tous' aggregation (required by Query 1 WHERE profil='tous') ─
    df_tous = con.execute(f"""
        SELECT
            'tous'                                  AS profil,
            famille,
            skill_normalise                         AS competence,
            COUNT(DISTINCT id_offre)                AS nb_offres_mentionnent,
            ROUND(COUNT(DISTINCT id_offre) * 100.0
                  / {total_offres}, 2)              AS pct_offres_total
        FROM read_parquet('{path_comp}')
        GROUP BY famille, skill_normalise
        ORDER BY nb_offres_mentionnent DESC
    """).df()

    # Rank within 'tous'
    df_tous['rang_dans_profil'] = (
        df_tous['nb_offres_mentionnent']
        .rank(method='dense', ascending=False)
        .astype(int)
    )

    # Combine per-profile + global
    df_final = pd.concat([df_profil, df_tous], ignore_index=True)

    log("GOLD", f"top_competences: {len(df_final)} rows "
        f"({df_profil['profil'].nunique()} profiles + 'tous')")
    return df_final


# ==============================================================================
# SECTION 3 — GOLD TABLE 2: OFFRES PAR VILLE
# Columns required by Query 2:
#   ville, profil, nb_offres, nb_offres_remote, pct_remote
# ==============================================================================

def construire_offres_par_ville(con) -> pd.DataFrame:
    """
    Build offres_par_ville Gold table.
    One row per (ville × profil).
    nb_offres_remote counts offers with teletravail IN ('Hybride', 'Télétravail complet').
    """
    log("GOLD", "Building offres_par_ville table...")

    path_offres = str(SILVER_OFFRES).replace('\\', '/')

    df = con.execute(f"""
        SELECT
            ville,
            profil_normalise                        AS profil,
            COUNT(*)                                AS nb_offres,
            COUNT(*) FILTER (
                WHERE teletravail IN ('Hybride', 'Télétravail complet')
            )                                       AS nb_offres_remote,
            ROUND(
                COUNT(*) FILTER (
                    WHERE teletravail IN ('Hybride', 'Télétravail complet')
                ) * 100.0 / NULLIF(COUNT(*), 0), 1
            )                                       AS pct_remote
        FROM read_parquet('{path_offres}')
        WHERE ville != 'Autre'
          AND ville != 'Inconnu'
          AND profil_normalise != 'Non classifié'
          AND profil_normalise IS NOT NULL
        GROUP BY ville, profil_normalise
        ORDER BY ville, nb_offres DESC
    """).df()

    log("GOLD", f"offres_par_ville: {len(df)} rows "
        f"({df['ville'].nunique()} cities × {df['profil'].nunique()} profiles)")
    return df


# ==============================================================================
# SECTION 4 — GOLD TABLE 3: SALAIRES PAR PROFIL
# Columns required by Query 3:
#   profil, ville, nb_offres, nb_offres_avec_salaire,
#   salaire_median_mad, salaire_min_observe, salaire_max_observe,
#   salaire_q1_mad, salaire_q3_mad
# ==============================================================================

def construire_salaires_par_profil(con) -> pd.DataFrame:
    """
    Build salaires_par_profil Gold table.
    One row per (profil × ville).
    Computes median, Q1, Q3, min, max from individual offer salary data.
    Only offers with salaire_connu = TRUE contribute to salary stats.
    """
    log("GOLD", "Building salaires_par_profil table...")

    path_offres = str(SILVER_OFFRES).replace('\\', '/')

    df = con.execute(f"""
        SELECT
            profil_normalise                            AS profil,
            ville,
            COUNT(*)                                    AS nb_offres,
            COUNT(*) FILTER (WHERE salaire_connu)       AS nb_offres_avec_salaire,
            ROUND(
                COUNT(*) FILTER (WHERE salaire_connu) * 100.0
                / NULLIF(COUNT(*), 0), 1
            )                                           AS pct_salaire_communique,
            ROUND(MEDIAN(salaire_median_mad)
                FILTER (WHERE salaire_connu), 0)        AS salaire_median_mad,
            ROUND(QUANTILE_CONT(salaire_median_mad, 0.25)
                FILTER (WHERE salaire_connu), 0)        AS salaire_q1_mad,
            ROUND(QUANTILE_CONT(salaire_median_mad, 0.75)
                FILTER (WHERE salaire_connu), 0)        AS salaire_q3_mad,
            ROUND(MIN(salaire_min_mad)
                FILTER (WHERE salaire_connu), 0)        AS salaire_min_observe,
            ROUND(MAX(salaire_max_mad)
                FILTER (WHERE salaire_connu), 0)        AS salaire_max_observe
        FROM read_parquet('{path_offres}')
        WHERE ville != 'Autre'
          AND ville != 'Inconnu'
          AND profil_normalise != 'Non classifié'
          AND profil_normalise IS NOT NULL
        GROUP BY profil_normalise, ville
        HAVING COUNT(*) >= 3
        ORDER BY profil, salaire_median_mad DESC NULLS LAST
    """).df()

    log("GOLD", f"salaires_par_profil: {len(df)} rows "
        f"({df['profil'].nunique()} profiles × {df['ville'].nunique()} cities)")
    return df


# ==============================================================================
# SECTION 5 — GOLD TABLE 4: ENTREPRISES RECRUTEURS
# Columns required by Query 5:
#   entreprise, ville, nb_offres_publiees,
#   nb_profils_differents, salaire_moyen_propose, profils_recrutes (array)
# ==============================================================================

def construire_entreprises_recruteurs(con) -> pd.DataFrame:
    """
    Build entreprises_recruteurs Gold table.
    One row per (entreprise × ville).
    profils_recrutes is a Python list (stored as JSON string in Parquet,
    then converted to list for DuckDB array_contains() compatibility).
    """
    log("GOLD", "Building entreprises_recruteurs table...")

    path_offres = str(SILVER_OFFRES).replace('\\', '/')

    # Base aggregation
    df_base = con.execute(f"""
        SELECT
            entreprise,
            ville,
            COUNT(*)                                    AS nb_offres_publiees,
            COUNT(DISTINCT profil_normalise)
                FILTER (WHERE profil_normalise != 'Non classifié')
                                                        AS nb_profils_differents,
            ROUND(AVG(salaire_median_mad)
                FILTER (WHERE salaire_connu), 0)        AS salaire_moyen_propose
        FROM read_parquet('{path_offres}')
        WHERE entreprise IS NOT NULL
          AND entreprise != ''
          AND ville != 'Autre'
        GROUP BY entreprise, ville
        ORDER BY nb_offres_publiees DESC
    """).df()

    # Build profils_recrutes array per (entreprise × ville)
    df_profils = con.execute(f"""
        SELECT
            entreprise,
            ville,
            LIST(DISTINCT profil_normalise
                 ORDER BY profil_normalise)
                FILTER (WHERE profil_normalise != 'Non classifié')
                                                        AS profils_recrutes
        FROM read_parquet('{path_offres}')
        WHERE entreprise IS NOT NULL
          AND entreprise != ''
          AND ville != 'Autre'
        GROUP BY entreprise, ville
    """).df()

    # Merge
    df = df_base.merge(df_profils, on=['entreprise', 'ville'], how='left')

    # Convert list to JSON string for Parquet storage
    # (DuckDB can query list columns in Parquet natively)
    df['profils_recrutes'] = df['profils_recrutes'].apply(
        lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else '[]'
    )

    log("GOLD", f"entreprises_recruteurs: {len(df)} rows "
        f"({df['entreprise'].nunique()} companies)")
    return df


# ==============================================================================
# SECTION 6 — GOLD TABLE 5: TENDANCES MENSUELLES
# ==============================================================================

def construire_tendances_mensuelles(con) -> pd.DataFrame:
    """
    Build tendances_mensuelles Gold table.
    One row per (annee × mois × profil).
    Tracks monthly offer volume trends across the 23-month observation window.
    """
    log("GOLD", "Building tendances_mensuelles table...")

    path_offres = str(SILVER_OFFRES).replace('\\', '/')

    df = con.execute(f"""
        SELECT
            annee_publication                           AS annee,
            mois_publication                            AS mois,
            profil_normalise                            AS profil,
            COUNT(*)                                    AS nb_offres,
            COUNT(*) FILTER (WHERE salaire_connu)       AS nb_avec_salaire,
            ROUND(AVG(salaire_median_mad)
                FILTER (WHERE salaire_connu), 0)        AS salaire_moyen_mois
        FROM read_parquet('{path_offres}')
        WHERE annee_publication IS NOT NULL
          AND mois_publication IS NOT NULL
          AND profil_normalise != 'Non classifié'
          AND profil_normalise IS NOT NULL
        GROUP BY annee_publication, mois_publication, profil_normalise
        ORDER BY annee, mois, profil
    """).df()

    log("GOLD", f"tendances_mensuelles: {len(df)} rows "
        f"({df['annee'].nunique()} years, {df['mois'].nunique()} months)")
    return df


# ==============================================================================
# SECTION 7 — WRITE GOLD PARQUET FILES
# ==============================================================================

def ecrire_gold(df: pd.DataFrame, nom_fichier: str):
    """Write a Gold DataFrame to Parquet with Snappy compression."""
    GOLD_ROOT.mkdir(parents=True, exist_ok=True)
    path = GOLD_ROOT / nom_fichier

    table = pa.Table.from_pandas(df)
    pq.write_table(table, path, compression='snappy')

    size_kb = path.stat().st_size // 1024
    log("GOLD", f"{nom_fichier} written — {len(df)} rows, {size_kb} KB")
    return path


# ==============================================================================
# SECTION 8 — GOLD SUMMARY REPORTER
# ==============================================================================

def afficher_resume_gold(con):
    """Quick DuckDB verification queries on the Gold files just written."""
    log_separator("GOLD ZONE SUMMARY")

    gold_path = str(GOLD_ROOT).replace('\\', '/')

    print("\n  top_competences — Global top 10:")
    df = con.execute(f"""
        SELECT competence, famille, nb_offres_mentionnent, pct_offres_total
        FROM read_parquet('{gold_path}/top_competences.parquet')
        WHERE profil = 'tous'
        ORDER BY nb_offres_mentionnent DESC
        LIMIT 10
    """).df()
    for _, r in df.iterrows():
        print(f"    {r['competence']:<20} {r['nb_offres_mentionnent']:>5} "
              f"({r['pct_offres_total']:5.1f}%)  [{r['famille']}]")

    print("\n  salaires_par_profil — Tanger salary overview:")
    df2 = con.execute(f"""
        SELECT profil, nb_offres, nb_offres_avec_salaire,
               salaire_median_mad, salaire_q1_mad, salaire_q3_mad
        FROM read_parquet('{gold_path}/salaires_par_profil.parquet')
        WHERE ville = 'Tanger'
        ORDER BY salaire_median_mad DESC NULLS LAST
        LIMIT 8
    """).df()
    for _, r in df2.iterrows():
        print(f"    {r['profil']:<30} n={r['nb_offres']:>3} "
              f"med={r['salaire_median_mad'] or 'N/A'} MAD")

    print("\n  offres_par_ville — Top 5 cities for Data Engineer:")
    df3 = con.execute(f"""
        SELECT ville, nb_offres, nb_offres_remote, pct_remote
        FROM read_parquet('{gold_path}/offres_par_ville.parquet')
        WHERE profil = 'Data Engineer'
        ORDER BY nb_offres DESC
        LIMIT 5
    """).df()
    for _, r in df3.iterrows():
        print(f"    {r['ville']:<20} {r['nb_offres']:>4} offers  "
              f"remote={r['pct_remote']}%")

    print("\n  entreprises_recruteurs — Top 10 recruiters:")
    df4 = con.execute(f"""
        SELECT entreprise, ville, nb_offres_publiees, salaire_moyen_propose
        FROM read_parquet('{gold_path}/entreprises_recruteurs.parquet')
        ORDER BY nb_offres_publiees DESC
        LIMIT 10
    """).df()
    for _, r in df4.iterrows():
        sal = f"{int(r['salaire_moyen_propose'])} MAD" \
              if pd.notna(r['salaire_moyen_propose']) else "N/A"
        print(f"    {r['entreprise']:<30} {r['nb_offres_publiees']:>4} offers  "
              f"avg={sal}")

    log_separator()


# ==============================================================================
# SECTION 9 — ENTRY POINT
# ==============================================================================

def run_gold():
    """
    Main Gold aggregation entry point.
    Called by main.py orchestrator.
    """
    log_separator("STEP 2.4 — GOLD AGGREGATION")

    # Verify Silver files exist
    if not SILVER_OFFRES.exists():
        log("GOLD", "❌  offres_clean.parquet not found. Run silver_transform.py first.")
        return
    if not SILVER_COMP.exists():
        log("GOLD", "❌  competences.parquet not found. Run silver_nlp.py first.")
        return

    con = get_con()

    # Build all 5 Gold tables
    df_top_comp    = construire_top_competences(con)
    df_villes      = construire_offres_par_ville(con)
    df_salaires    = construire_salaires_par_profil(con)
    df_entreprises = construire_entreprises_recruteurs(con)
    df_tendances   = construire_tendances_mensuelles(con)

    # Write all to Parquet
    ecrire_gold(df_top_comp,    'top_competences.parquet')
    ecrire_gold(df_villes,      'offres_par_ville.parquet')
    ecrire_gold(df_salaires,    'salaires_par_profil.parquet')
    ecrire_gold(df_entreprises, 'entreprises_recruteurs.parquet')
    ecrire_gold(df_tendances,   'tendances_mensuelles.parquet')

    # Reconnect for verification reads
    con2 = get_con()
    afficher_resume_gold(con2)

    log("GOLD", "Gold aggregation complete ✅")


# ==============================================================================
# SECTION 10 — DIRECT RUN
# ==============================================================================

if __name__ == "__main__":
    run_gold()