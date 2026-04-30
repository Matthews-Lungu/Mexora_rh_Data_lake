# Mexora RH Intelligence — Pipeline Technical Report
## Step 2: Data Engineering Pipeline Documentation

**Project:** Mexora RH Intelligence — IT Job Market Analysis, Morocco  
**Author:** Mathew Lungu & Adjii Charles  
**Date:** April 2026  
**Pipeline Version:** 1.0  

---

## 1. Overview

This report documents the complete data engineering pipeline built for the Mexora HR Intelligence project. The pipeline processes 5,000 Moroccan IT job offers from three sources (Rekrute, MarocAnnonce, LinkedIn) through a Bronze/Silver/Gold Data Lake architecture, producing five analytical Gold tables used for HR decision-making.

**Pipeline execution time:** 81.9 seconds end-to-end  
**Total records processed:** 5,000 job offers  
**Output:** 5 Gold Parquet tables + 47,560 NLP skill extractions  

---

## 2. Pipeline Architecture Summary

```
Raw JSON (6.4 MB)
      │
      ▼
┌─────────────┐
│   BRONZE    │  bronze_ingestion.py
│  Immutable  │  → 69 partitions (source/month)
│  Raw JSON   │  → 5,000 offers preserved exactly
└─────────────┘
      │
      ▼
┌─────────────┐
│   SILVER    │  silver_transform.py + silver_nlp.py
│   Cleaned   │  → offres_clean.parquet (787 KB, 33 cols)
│   Parquet   │  → competences.parquet (209 KB, 47,560 rows)
└─────────────┘
      │
      ▼
┌─────────────┐
│    GOLD     │  gold_aggregation.py (DuckDB)
│ Aggregated  │  → 5 analytical tables
│   Parquet   │  → Ready for BI consumption
└─────────────┘
```

---

## 3. File Descriptions

### 3.1 `utils.py` — Shared Utilities

Central module imported by all pipeline files. Contains:

- **Path resolution:** All project paths defined once (`PROJECT_ROOT`, `DATA_LAKE_ROOT`, `BRONZE_ROOT`, `SILVER_ROOT`, `GOLD_ROOT`). No hardcoded paths anywhere else in the codebase.
- **Logging:** Timestamped `log(zone, message)` function used consistently across all pipeline steps.
- **City normalization:** `normaliser_ville()` maps 50+ dirty city variants to 13 standard Moroccan city names plus their administrative region. Handles uppercase, lowercase, Arabic names (Dar El Beida), English spellings (Tangier), Darija variants (Tanja), and partial matches.
- **Contract normalization:** `normaliser_contrat()` maps dirty contract strings to 5 standard types (CDI, CDD, Freelance, Stage, Alternance).
- **Date validation:** `valider_dates()` parses 4 date formats and flags incoherent dates where publication > expiration.
- **Input validation:** `verifier_fichiers_entree()` checks all 3 input files exist before pipeline starts.

**Self-test result:**
```
City normalization:    11/11 test cases passed
Contract normalization: 5/5 test cases passed
Date validation:        4/4 test cases passed (including error detection)
Path resolution:        Correct Windows paths confirmed
```

---

### 3.2 `bronze_ingestion.py` — Raw Ingestion

**Function:** `ingerer_bronze(filepath_source, data_lake_root)`

Reads the raw JSON source file and partitions offers by source and publication month without any modification. The Bronze zone is immutable — once written, data is never changed, ensuring full data lineage.

**Partitioning strategy:** `bronze/{source}/{YYYY_MM}/offres_raw.json`

Each partition file contains:
- A `metadata` wrapper with ingestion timestamp, source file path, and schema version
- The raw `offres` array, completely untouched

**Results:**
| Metric | Value |
|---|---|
| Total offers ingested | 5,000 |
| Partitions created | 69 |
| Sources | rekrute (45.4%), marocannonce (29.3%), linkedin (25.2%) |
| Months covered | 23 months (Jan 2023 – Nov 2024) |
| Date anomalies | 0 |
| Corrupted files after integrity check | 0 |
| Execution time | 2.8 seconds |

---

### 3.3 `silver_transform.py` — Data Cleaning

**Function:** `transformer_silver(offres_brutes)`

Reads all Bronze partitions via `charger_depuis_bronze()` and applies the following transformations to each of the 5,000 offers:

**Transformations applied:**

| Field | Problem in Raw Data | Solution Applied |
|---|---|---|
| `titre_poste` | 10+ dirty variants per profile | Keyword matching → 11 canonical profiles |
| `ville` | 50+ dirty variants | Dictionary lookup → 13 standard cities + region |
| `type_contrat` | Inconsistent strings | Mapping → CDI/CDD/Freelance/Stage/Alternance |
| `salaire_brut` | 7 different formats + nulls | 6 regex patterns → (sal_min_mad, sal_max_mad) |
| `experience_requise` | Free text, 8+ formats | Regex parsing → (exp_min, exp_max) integers |
| `date_publication` | 4 different date formats | Parsed to ISO YYYY-MM-DD, coherence checked |
| `salaire_median_mad` | Derived field needed | Computed as (min + max) / 2 |
| `salaire_connu` | Boolean flag needed | True if salary was successfully parsed |

**Salary parsing — format coverage:**
- `15000-20000 MAD` → standard MAD range
- `15K-20K MAD` → thousands notation
- `15k-20k dh` → lowercase variants
- `1500-1800 EUR` → EUR converted at 10.8 rate
- `15000 MAD` → single value
- `Selon profil` / `Confidentiel` → flagged as confidential

**Data quality results:**
| Metric | Value |
|---|---|
| Output shape | (5,000 × 33) |
| Clean offers (0 anomalies) | 3,772 (75.4%) |
| Offers with 1 anomaly | 1,082 |
| Offers with 2+ anomalies | 146 |
| Salary coverage | 3,463/5,000 (69.3%) |
| Execution time | 2.8 seconds |

**Top anomaly types detected:**
1. `experience_non_parseable` — 371 offers (None value in raw data)
2. `salaire_non_parseable` — 298 offers (non-standard format)
3. `titre_non_classifie` — 276 offers (ambiguous titles)
4. `dates_incoherentes` — 238 offers (publication > expiration)
5. `contrat_non_reconnu` — 198 offers (edge case formats)

---

### 3.4 `silver_nlp.py` — NLP Skill Extraction

**Function:** `extraire_competences_silver(df_offres, patterns)`

Extracts technology skill mentions from free text using the `referentiel_competences_it.json` dictionary.

**Technical approach:**

1. **Referentiel compilation:** 281 alias patterns compiled across 97 normalized skills in 10 technology families. Each alias compiled as a word-boundary regex (`\b` anchors) to prevent partial matches (e.g., `"r"` must not match inside `"React"`).

2. **Special character handling:** Aliases ending in non-word characters (`c++`, `.net`, `c#`) use lookahead/lookbehind instead of `\b` since word boundaries do not function reliably next to punctuation.

3. **Length-first ordering:** All aliases sorted by length descending before any matching. This ensures `"apache spark"` is matched before `"spark"` on the same text, preventing double-counting.

4. **Dual-field extraction:** Each offer's `competences_brut` and `description` fields are scanned independently. A `source` column records whether the skill was found in `"competences_brut"`, `"description"`, or `"both"`.

5. **Deduplication:** Each normalized skill counted at most once per offer, regardless of how many aliases triggered a match.

**Output format:** Long format — one row per `(id_offre × skill_normalise)` pair.

**Results:**
| Metric | Value |
|---|---|
| Total skill matches | 47,560 |
| Offers with ≥1 skill extracted | 5,000 (100%) |
| Unique skills identified | 64 out of 97 |
| Average skills per offer | 9.5 |
| Source: description only | 18,522 (38.9%) |
| Source: competences_brut only | 15,817 (33.3%) |
| Source: both fields | 13,221 (27.8%) |
| Execution time | 74.1 seconds |

**Top 5 skills extracted:**
1. `sql` — 2,931 offers (58.6%)
2. `git` — 2,736 offers (54.7%)
3. `agile` — 2,525 offers (50.5%)
4. `aws` — 2,463 offers (49.3%)
5. `postgresql` — 2,114 offers (42.3%)

---

### 3.5 `gold_aggregation.py` — Business Aggregations

**Tool:** DuckDB 1.0.0 — reads Parquet files directly without loading into memory.

Builds 5 analytical Gold tables aligned to the 5 business questions:

| Table | Grain | Rows | Size |
|---|---|---|---|
| `top_competences.parquet` | profil × skill | 201 | 8 KB |
| `offres_par_ville.parquet` | ville × profil | 143 | 5 KB |
| `salaires_par_profil.parquet` | profil × ville | 136 | 11 KB |
| `entreprises_recruteurs.parquet` | entreprise × ville | 323 | 8 KB |
| `tendances_mensuelles.parquet` | annee × mois × profil | 253 | 7 KB |

**Key design decisions:**
- `top_competences` includes both per-profile rows AND a `profil = 'tous'` global aggregate row, enabling both global and segmented queries without joins.
- `rang_dans_profil` is pre-computed in Gold, avoiding window functions at query time.
- `profils_recrutes` in `entreprises_recruteurs` is stored as a JSON array string, compatible with DuckDB's `array_contains()` function.
- `HAVING COUNT(*) >= 3` guard in salary aggregation ensures no statistics computed on single-offer samples.
- Execution time: 2.2 seconds

---

### 3.6 `main.py` — Orchestrator

Runs the complete pipeline in sequence with error handling at each step. If any step fails, the pipeline stops immediately and reports the error — preventing corrupt downstream data.

**Full pipeline execution summary:**
```
BRONZE    ✅  5,000 offers, 69 partitions          2.8s
SILVER    ✅  5,000 rows, 33 columns               2.8s
NLP       ✅  47,560 skill matches, 64 skills      74.1s
GOLD      ✅  5 Gold tables written                2.2s
─────────────────────────────────────────────────
TOTAL                                             81.9s
```

---

## 4. Data Quality Summary

The pipeline detected and flagged — but did not silently discard — the following data quality issues in the raw source data:

| Issue Type | Count | % of Dataset | Action Taken |
|---|---|---|---|
| Incoherent dates (pub > exp) | 238 | 4.8% | Flagged, dates normalized |
| Unparseable salary format | 298 | 5.96% | Flagged, salary set to NULL |
| Unclassified job title | 276 | 5.5% | Flagged, excluded from Gold aggregations |
| Unrecognized contract type | 198 | 3.96% | Flagged, mapped to "Autre" |
| Unparseable experience | 371 | 7.4% | Flagged, exp_min/max set to NULL |

**75.4% of offers are fully clean** (zero anomalies across all fields). The remaining 24.6% carry quality flags that allow downstream queries to filter or include them as needed.

---

## 5. Technology Stack

| Component | Technology | Version | Purpose |
|---|---|---|---|
| Language | Python | 3.12 | All pipeline code |
| Data frames | pandas | 2.2.2 | Transformation layer |
| Columnar format | pyarrow | 16.1.0 | Parquet read/write |
| SQL engine | duckdb | 1.0.0 | Gold aggregations |
| NLP | re (stdlib) | — | Regex skill extraction |
| Visualization | matplotlib / seaborn | 3.9.0 / 0.13.2 | Charts |
| Notebook | jupyter | 1.0.0 | Analysis presentation |
| Version control | git + GitHub | — | Code repository |

---

## 6. Reproducibility

The complete pipeline is fully reproducible from scratch:

```bash
# Step 1: Install dependencies
pip install -r requirements.txt

# Step 2: Generate raw data
python data_generation/generate_data.py

# Step 3: Run full pipeline
python main.py

# Step 4: Run analysis
python analysis/analyse_marche.py
# or open: jupyter notebook analysis/analyse_marche_it_maroc.ipynb
```

All random seeds are fixed (`random.seed(42)`) in the data generation script, ensuring identical output on every run.

---

*End of Pipeline Technical Report*
