"""
main.py
=======
Mexora RH Intelligence — Pipeline Orchestrator
Runs the complete Bronze → Silver → Gold pipeline in sequence.

Usage:
    python main.py

Steps executed:
    1. Bronze ingestion   — raw JSON → partitioned Bronze zone
    2. Silver transform   — Bronze → cleaned Parquet
    3. Silver NLP         — free text → skill extraction Parquet
    4. Gold aggregation   — Silver → 5 analytical Gold tables
"""

import sys
import time
from pathlib import Path
from datetime import datetime

# Add pipeline folder to path
sys.path.insert(0, str(Path(__file__).resolve().parent / 'pipeline'))

from utils import log, log_separator
from bronze_ingestion import run_bronze
from silver_transform  import run_silver
from silver_nlp        import run_nlp
from gold_aggregation  import run_gold


def run_pipeline():
    start_total = time.time()
    resultats   = {}

    log_separator("MEXORA RH — FULL PIPELINE START")
    log("MAIN", f"Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # ── Step 1 — Bronze ───────────────────────────────────────────────────
    log_separator("STEP 1 — BRONZE")
    t = time.time()
    try:
        stats = run_bronze()
        resultats['bronze'] = f"✅  {stats['total']} offers, {len(stats['par_partition'])} partitions"
    except Exception as e:
        resultats['bronze'] = f"❌  FAILED: {e}"
        log("MAIN", f"Bronze failed — stopping pipeline. Error: {e}")
        return
    log("MAIN", f"Bronze completed in {time.time()-t:.1f}s")

    # ── Step 2 — Silver Transform ─────────────────────────────────────────
    log_separator("STEP 2 — SILVER TRANSFORM")
    t = time.time()
    try:
        df_offres, df_qualite = run_silver()
        resultats['silver'] = f"✅  {len(df_offres)} rows, {df_offres.shape[1]} columns"
    except Exception as e:
        resultats['silver'] = f"❌  FAILED: {e}"
        log("MAIN", f"Silver failed — stopping pipeline. Error: {e}")
        return
    log("MAIN", f"Silver completed in {time.time()-t:.1f}s")

    # ── Step 3 — Silver NLP ───────────────────────────────────────────────
    log_separator("STEP 3 — NLP EXTRACTION")
    t = time.time()
    try:
        df_comp = run_nlp()
        resultats['nlp'] = f"✅  {len(df_comp)} skill matches, {df_comp['skill_normalise'].nunique()} unique skills"
    except Exception as e:
        resultats['nlp'] = f"❌  FAILED: {e}"
        log("MAIN", f"NLP failed — stopping pipeline. Error: {e}")
        return
    log("MAIN", f"NLP completed in {time.time()-t:.1f}s")

    # ── Step 4 — Gold ─────────────────────────────────────────────────────
    log_separator("STEP 4 — GOLD AGGREGATION")
    t = time.time()
    try:
        run_gold()
        resultats['gold'] = "✅  5 Gold tables written"
    except Exception as e:
        resultats['gold'] = f"❌  FAILED: {e}"
        log("MAIN", f"Gold failed. Error: {e}")
        return
    log("MAIN", f"Gold completed in {time.time()-t:.1f}s")

    # ── Final summary ─────────────────────────────────────────────────────
    elapsed = time.time() - start_total
    log_separator("PIPELINE COMPLETE")
    print(f"\n  Total time : {elapsed:.1f} seconds")
    print(f"\n  Results:")
    for step, result in resultats.items():
        print(f"    {step.upper():<10} {result}")
    print(f"\n  Data Lake ready at: data_lake/")
    print(f"  Bronze : data_lake/bronze/  (69 partitions)")
    print(f"  Silver : data_lake/silver/  (offres_clean.parquet + competences.parquet)")
    print(f"  Gold   : data_lake/gold/    (5 analytical tables)")
    log_separator()


if __name__ == "__main__":
    run_pipeline()