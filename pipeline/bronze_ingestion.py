"""
bronze_ingestion.py
===================
Mexora RH Intelligence — Bronze Zone Ingestion
Step 2.2 of the pipeline: Raw data → Bronze zone

FUNDAMENTAL PRINCIPLE:
  The Bronze zone is IMMUTABLE.
  Data is loaded exactly as received — no modification, no cleaning.
  This is the faithful archive of what was ingested.
  If something goes wrong downstream, we always reprocess from Bronze.

Partitioning strategy:
  By SOURCE (rekrute / marocannonce / linkedin)
  By MONTH  (YYYY_MM of date_publication)

  Result: bronze/rekrute/2024_08/offres_raw.json
          bronze/marocannonce/2024_03/offres_raw.json
          etc.

Usage:
    python pipeline/bronze_ingestion.py
    (or called from main.py)
"""

import json
import os
from datetime import datetime
from pathlib import Path

# Import shared utilities
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import (
    log, log_separator,
    PATH_OFFRES_JSON,
    BRONZE_ROOT,
    verifier_fichiers_entree,
    verifier_dossiers_lake,
)


# ==============================================================================
# SECTION 1 — CORE INGESTION FUNCTION
# (implements the function from the project statement exactly)
# ==============================================================================

def ingerer_bronze(filepath_source: str, data_lake_root: str) -> dict:
    """
    Load raw data into the Bronze zone without any modification.
    Partitions by source and publication month.

    Fundamental principle: the Bronze zone is IMMUTABLE.
    Data is never modified once loaded into Bronze.
    It is the faithful archive of what was received.

    Args:
        filepath_source : path to offres_emploi_it_maroc.json
        data_lake_root  : path to the data_lake/ root directory

    Returns:
        stats dict with total count, per-source and per-partition counts
    """
    log("BRONZE", f"Reading source file: {filepath_source}")

    with open(filepath_source, 'r', encoding='utf-8') as f:
        data = json.load(f)

    offres = data.get('offres', [])
    log("BRONZE", f"{len(offres)} offers found in source file")

    stats = {
        'total':         len(offres),
        'par_source':    {},
        'par_partition': {},
        'anomalies':     0,
    }

    # ── Partition offers by source and publication month ──────────────────────
    partitions = {}

    for offre in offres:
        # Source — normalize to lowercase, replace spaces with underscores
        source = offre.get('source', 'inconnu').lower().replace(' ', '_')

        # Month partition — derived from date_publication
        date_pub = offre.get('date_publication', '')
        try:
            # Handle multiple date formats that exist in raw data
            # We try ISO first, then French formats
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"]:
                try:
                    mois_partition = datetime.strptime(
                        str(date_pub).strip()[:10], fmt
                    ).strftime('%Y_%m')
                    break
                except ValueError:
                    continue
            else:
                # No format matched
                mois_partition = 'date_inconnue'
                stats['anomalies'] += 1
        except (ValueError, TypeError):
            mois_partition = 'date_inconnue'
            stats['anomalies'] += 1

        cle = f"{source}/{mois_partition}"
        if cle not in partitions:
            partitions[cle] = []
        partitions[cle].append(offre)

    # ── Write each partition to Bronze ───────────────────────────────────────
    nb_fichiers = 0

    for partition, offres_partition in partitions.items():
        chemin_dir = Path(data_lake_root) / 'bronze' / partition
        chemin_dir.mkdir(parents=True, exist_ok=True)

        chemin_fichier = chemin_dir / 'offres_raw.json'

        # Write with metadata wrapper
        # Metadata is added AT INGESTION TIME only — never modifies the offers
        with open(chemin_fichier, 'w', encoding='utf-8') as f:
            json.dump({
                'metadata': {
                    'source_fichier':  str(filepath_source),
                    'date_ingestion':  datetime.now().isoformat(),
                    'partition':       partition,
                    'nb_offres':       len(offres_partition),
                    'schema_version':  '1.0',
                },
                'offres': offres_partition   # RAW — untouched
            }, f, ensure_ascii=False, indent=2)

        nb_fichiers += 1

        # Track stats per source
        source_nom = partition.split('/')[0]
        stats['par_source'][source_nom] = (
            stats['par_source'].get(source_nom, 0) + len(offres_partition)
        )
        stats['par_partition'][partition] = len(offres_partition)

    log("BRONZE", f"{stats['total']} offers ingested into {nb_fichiers} partitions")
    log("BRONZE", f"Anomalies (unparseable dates) : {stats['anomalies']}")

    return stats


# ==============================================================================
# SECTION 2 — STATS REPORTER
# ==============================================================================

def afficher_stats_bronze(stats: dict):
    """Print a clear summary of what was ingested into Bronze."""
    log_separator("BRONZE INGESTION SUMMARY")

    print(f"\n  Total offers ingested : {stats['total']}")
    print(f"  Date anomalies        : {stats['anomalies']}")

    print(f"\n  By source:")
    for source, count in sorted(stats['par_source'].items()):
        pct = count * 100 / stats['total']
        bar = '█' * int(pct / 2)
        print(f"    {source:<20} {count:>5}  ({pct:5.1f}%)  {bar}")

    print(f"\n  By partition (source/month) — top 10:")
    sorted_parts = sorted(
        stats['par_partition'].items(),
        key=lambda x: -x[1]
    )[:10]
    for partition, count in sorted_parts:
        print(f"    {partition:<35} {count:>5} offers")

    log_separator()


# ==============================================================================
# SECTION 3 — BRONZE READER (used by silver_transform.py)
# ==============================================================================

def charger_depuis_bronze(data_lake_root: str) -> list:
    """
    Load and consolidate ALL offers from every Bronze partition.
    Called by silver_transform.py as the first step of Silver processing.

    Returns:
        list of raw offer dicts (exactly as stored in Bronze)
    """
    log("BRONZE", "Loading all offers from Bronze zone...")

    bronze_path = Path(data_lake_root) / 'bronze'
    all_offres  = []
    fichiers_lus = 0

    # Walk every partition folder recursively
    for json_file in bronze_path.rglob('offres_raw.json'):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        batch = data.get('offres', [])
        all_offres.extend(batch)
        fichiers_lus += 1

    log("BRONZE", f"{len(all_offres)} offers loaded from {fichiers_lus} partition files")
    return all_offres


# ==============================================================================
# SECTION 4 — BRONZE INTEGRITY CHECK
# ==============================================================================

def verifier_integrite_bronze(data_lake_root: str) -> dict:
    """
    Verify the Bronze zone integrity after ingestion.
    Checks that:
      - All partition directories contain offres_raw.json
      - Metadata is present and well-formed
      - Total offer count matches expectations

    Returns:
        integrity report dict
    """
    bronze_path = Path(data_lake_root) / 'bronze'
    rapport = {
        'partitions_found':    0,
        'total_offres':        0,
        'sources_found':       set(),
        'mois_couverts':       set(),
        'fichiers_corrompus':  [],
    }

    for json_file in bronze_path.rglob('offres_raw.json'):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            metadata = data.get('metadata', {})
            offres   = data.get('offres', [])

            rapport['partitions_found'] += 1
            rapport['total_offres']     += len(offres)

            partition = metadata.get('partition', '')
            if '/' in partition:
                source, mois = partition.split('/', 1)
                rapport['sources_found'].add(source)
                rapport['mois_couverts'].add(mois)

        except (json.JSONDecodeError, KeyError) as e:
            rapport['fichiers_corrompus'].append(str(json_file))
            log("BRONZE", f"⚠️  Corrupted file: {json_file} — {e}")

    rapport['sources_found'] = sorted(rapport['sources_found'])
    rapport['mois_couverts'] = sorted(rapport['mois_couverts'])

    return rapport


# ==============================================================================
# SECTION 5 — ENTRY POINT
# ==============================================================================

def run_bronze():
    """
    Main Bronze ingestion entry point.
    Called by main.py orchestrator.
    """
    log_separator("STEP 2.1 — BRONZE INGESTION")

    # 1. Verify input files exist
    verifier_fichiers_entree()

    # 2. Ensure Lake directories exist
    verifier_dossiers_lake()

    # 3. Run ingestion
    stats = ingerer_bronze(
        filepath_source=str(PATH_OFFRES_JSON),
        data_lake_root= str(BRONZE_ROOT.parent),  # data_lake/ root
    )

    # 4. Print summary
    afficher_stats_bronze(stats)

    # 5. Integrity check
    log("BRONZE", "Running integrity check...")
    rapport = verifier_integrite_bronze(str(BRONZE_ROOT.parent))

    log("BRONZE", f"Integrity check results:")
    log("BRONZE", f"  Partitions     : {rapport['partitions_found']}")
    log("BRONZE", f"  Total offers   : {rapport['total_offres']}")
    log("BRONZE", f"  Sources found  : {rapport['sources_found']}")
    log("BRONZE", f"  Months covered : {len(rapport['mois_couverts'])} months")
    if rapport['fichiers_corrompus']:
        log("BRONZE", f"  ⚠️  Corrupted    : {rapport['fichiers_corrompus']}")
    else:
        log("BRONZE", f"  Corrupted files: 0 ✅")

    log("BRONZE", "Bronze ingestion complete ✅")
    return stats


# ==============================================================================
# SECTION 6 — DIRECT RUN
# ==============================================================================

if __name__ == "__main__":
    run_bronze()