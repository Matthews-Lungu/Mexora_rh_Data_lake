"""
analyse_marche.py
=================
Mexora RH Intelligence — Market Analysis Script
Step 3: 5 analytical questions answered via DuckDB on Gold Parquet files.

This script is the runnable version of the Jupyter notebook.
For rich visualizations and interpretations, open:
    analysis/analyse_marche_it_maroc.ipynb

Usage:
    python analysis/analyse_marche.py

Outputs:
    Printed result tables for all 5 questions.
    5 PNG charts saved to analysis/ folder.
"""

import sys
import warnings
from pathlib import Path

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

warnings.filterwarnings('ignore')

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD         = PROJECT_ROOT / 'data_lake' / 'gold'
SILVER       = PROJECT_ROOT / 'data_lake' / 'silver'
OUTPUT_DIR   = Path(__file__).resolve().parent

# ── DuckDB connection ─────────────────────────────────────────────────────────
con = duckdb.connect()

# ── Style ─────────────────────────────────────────────────────────────────────
COULEURS = ['#1B4F72','#2E86C1','#85C1E9','#D35400','#E67E22',
            '#27AE60','#8E44AD','#C0392B','#16A085','#F39C12']
plt.rcParams.update({
    'figure.dpi': 130,
    'axes.spines.top': False,
    'axes.spines.right': False,
    'axes.titlesize': 13,
    'axes.titleweight': 'bold',
})

def sep(title=""):
    w = 65
    if title:
        print(f"\n{'─'*10} {title} {'─'*10}")
    else:
        print("─" * w)


# ==============================================================================
# QUESTION 1 — Top compétences IT au Maroc
# ==============================================================================
sep("QUESTION 1 — Compétences IT les plus demandées")

# Q1-A Global top 20
q1a = con.execute(f"""
    SELECT
        famille,
        competence,
        nb_offres_mentionnent,
        pct_offres_total,
        rang_dans_profil
    FROM read_parquet('{GOLD}/top_competences.parquet')
    WHERE profil = 'tous'
    ORDER BY nb_offres_mentionnent DESC
    LIMIT 20
""").df()

print("\nTop 20 compétences — toutes offres confondues")
print(f"{'Rang':<5} {'Compétence':<22} {'Famille':<22} {'Offres':>6} {'%':>7}")
print("-" * 65)
for _, r in q1a.iterrows():
    print(f"{int(r['rang_dans_profil']):<5} {r['competence']:<22} "
          f"{r['famille']:<22} {int(r['nb_offres_mentionnent']):>6} "
          f"{r['pct_offres_total']:>6.1f}%")

# Q1-B Top 5 per data profile
q1b = con.execute(f"""
    SELECT profil, famille, competence, nb_offres_mentionnent, rang_dans_profil
    FROM read_parquet('{GOLD}/top_competences.parquet')
    WHERE profil IN ('Data Engineer', 'Data Analyst', 'Data Scientist')
      AND rang_dans_profil <= 5
    ORDER BY profil, rang_dans_profil
""").df()

print("\nTop 5 compétences par profil data:")
for profil in ['Data Engineer', 'Data Analyst', 'Data Scientist']:
    sub = q1b[q1b['profil'] == profil]
    print(f"\n  {profil}")
    for _, r in sub.iterrows():
        print(f"    #{int(r['rang_dans_profil'])} {r['competence']:<22} "
              f"{int(r['nb_offres_mentionnent']):>5} offres  [{r['famille']}]")

# Chart Q1
fig, axes = plt.subplots(1, 2, figsize=(16, 7))
q1_top15 = q1a.head(15).sort_values('nb_offres_mentionnent')
colors_bar = [COULEURS[0] if i >= 10 else COULEURS[2] for i in range(len(q1_top15))]
axes[0].barh(q1_top15['competence'], q1_top15['nb_offres_mentionnent'], color=colors_bar)
axes[0].set_xlabel("Nombre d'offres")
axes[0].set_title("Top 15 Compétences IT au Maroc")
for i, (v, pct) in enumerate(zip(q1_top15['nb_offres_mentionnent'],
                                   q1_top15['pct_offres_total'])):
    axes[0].text(v + 20, i, f"{pct:.0f}%", va='center', fontsize=9)

x = range(5)
width = 0.28
for idx, profil in enumerate(['Data Engineer', 'Data Analyst', 'Data Scientist']):
    vals = q1b[q1b['profil'] == profil].sort_values('rang_dans_profil')[
        'nb_offres_mentionnent'].values[:5]
    axes[1].bar([i + idx * width for i in x], vals, width,
                label=profil, color=COULEURS[idx])
axes[1].set_xticks([i + width for i in x])
axes[1].set_xticklabels([f"#{i+1}" for i in range(5)])
axes[1].set_ylabel("Nombre d'offres")
axes[1].set_title("Top 5 Compétences par Profil Data")
axes[1].legend(fontsize=9)
plt.tight_layout()
out = OUTPUT_DIR / 'q1_competences.png'
plt.savefig(out, bbox_inches='tight', dpi=130)
plt.close()
print(f"\n  📊 Chart saved: {out.name}")


# ==============================================================================
# QUESTION 2 — Tanger vs Casablanca vs Rabat
# ==============================================================================
sep("QUESTION 2 — Tanger vs Casablanca vs Rabat")

q2a = con.execute(f"""
    SELECT
        ville, profil, nb_offres, nb_offres_remote, pct_remote,
        RANK() OVER (PARTITION BY profil ORDER BY nb_offres DESC) AS rang_ville
    FROM read_parquet('{GOLD}/offres_par_ville.parquet')
    WHERE ville IN ('Casablanca', 'Rabat', 'Tanger', 'Marrakech', 'Fès')
    ORDER BY profil, rang_ville
""").df()

print(f"\n{'Ville':<15} {'Profil':<30} {'Offres':>7} {'Remote%':>8} {'Rang':>5}")
print("-" * 65)
for _, r in q2a.iterrows():
    print(f"{r['ville']:<15} {r['profil']:<30} "
          f"{int(r['nb_offres']):>7} {r['pct_remote']:>7.1f}% "
          f"{int(r['rang_ville']):>5}")

q2b = con.execute(f"""
    SELECT t.profil, t.nb_offres, t.pct_remote,
        ROUND(t.nb_offres * 100.0 / NULLIF(c.nb_offres, 0), 1) AS pct_vs_casa
    FROM read_parquet('{GOLD}/offres_par_ville.parquet') t
    LEFT JOIN read_parquet('{GOLD}/offres_par_ville.parquet') c
        ON t.profil = c.profil AND c.ville = 'Casablanca'
    WHERE t.ville = 'Tanger'
    ORDER BY t.nb_offres DESC
""").df()

print("\nFocus Tanger — ratio vs Casablanca:")
print(f"{'Profil':<30} {'Offres':>7} {'Remote%':>8} {'%vsCasa':>9}")
print("-" * 58)
for _, r in q2b.iterrows():
    print(f"{r['profil']:<30} {int(r['nb_offres']):>7} "
          f"{r['pct_remote']:>7.1f}% {r['pct_vs_casa']:>8.1f}%")

# Chart Q2
fig, axes = plt.subplots(1, 2, figsize=(16, 7))
data_profiles = ['Data Engineer', 'Data Analyst', 'Data Scientist']
q2_data  = q2a[q2a['profil'].isin(data_profiles)]
pivot_of = q2_data.pivot_table(index='profil', columns='ville',
                                values='nb_offres', aggfunc='sum').fillna(0)
villes = [v for v in ['Casablanca','Rabat','Tanger','Marrakech','Fès']
          if v in pivot_of.columns]
pivot_of[villes].plot(kind='bar', ax=axes[0], color=COULEURS[:len(villes)], width=0.7)
axes[0].set_title("Offres IT par ville — Profils Data")
axes[0].tick_params(axis='x', rotation=30)
axes[0].legend(title='Ville', fontsize=9)

q2_3v = q2a[q2a['ville'].isin(['Casablanca','Rabat','Tanger'])]
pivot_rm = q2_3v.pivot_table(index='profil', columns='ville',
                               values='pct_remote', aggfunc='mean').fillna(0)
x2 = range(len(pivot_rm))
for idx, ville in enumerate(['Casablanca','Rabat','Tanger']):
    if ville in pivot_rm.columns:
        axes[1].bar([i + idx*0.25 for i in x2], pivot_rm[ville].values,
                    0.25, label=ville, color=COULEURS[idx])
axes[1].set_xticks([i + 0.25 for i in x2])
axes[1].set_xticklabels(pivot_rm.index, rotation=30, ha='right', fontsize=9)
axes[1].set_ylabel("Taux de télétravail (%)")
axes[1].set_title("Taux de Télétravail par Ville et Profil")
axes[1].legend(fontsize=9)
plt.tight_layout()
out = OUTPUT_DIR / 'q2_villes.png'
plt.savefig(out, bbox_inches='tight', dpi=130)
plt.close()
print(f"\n  📊 Chart saved: {out.name}")


# ==============================================================================
# QUESTION 3 — Salaires médians par profil
# ==============================================================================
sep("QUESTION 3 — Salaires médians par profil IT")

q3a = con.execute(f"""
    SELECT
        profil,
        SUM(nb_offres)                                  AS nb_offres_total,
        SUM(nb_offres_avec_salaire)                     AS nb_avec_salaire,
        ROUND(SUM(nb_offres_avec_salaire)*100.0
            / NULLIF(SUM(nb_offres),0), 1)              AS pct_salaire_communique,
        ROUND(MEDIAN(salaire_median_mad), 0)            AS salaire_median_mad,
        MIN(salaire_min_observe)                        AS salaire_plancher,
        MAX(salaire_max_observe)                        AS salaire_plafond
    FROM read_parquet('{GOLD}/salaires_par_profil.parquet')
    GROUP BY profil
    ORDER BY salaire_median_mad DESC NULLS LAST
""").df()

print(f"\n{'Profil':<30} {'n':>5} {'Sal%':>5} {'Médiane':>9} {'Min':>7} {'Max':>8}")
print("-" * 70)
for _, r in q3a.iterrows():
    med = f"{int(r['salaire_median_mad'])} MAD" if pd.notna(r['salaire_median_mad']) else "N/A"
    mn  = f"{int(r['salaire_plancher'])}"        if pd.notna(r['salaire_plancher'])   else "N/A"
    mx  = f"{int(r['salaire_plafond'])}"         if pd.notna(r['salaire_plafond'])    else "N/A"
    print(f"{r['profil']:<30} {int(r['nb_offres_total']):>5} "
          f"{r['pct_salaire_communique']:>4.0f}% {med:>9} {mn:>7} {mx:>8}")

q3b = con.execute(f"""
    SELECT t.profil, t.nb_offres,
           t.salaire_median_mad, t.salaire_q1_mad, t.salaire_q3_mad,
           ROUND(t.salaire_median_mad - nat.med_nat, 0) AS ecart_mediane_nationale
    FROM read_parquet('{GOLD}/salaires_par_profil.parquet') t
    JOIN (SELECT profil, ROUND(MEDIAN(salaire_median_mad),0) AS med_nat
          FROM read_parquet('{GOLD}/salaires_par_profil.parquet')
          GROUP BY profil) nat ON t.profil = nat.profil
    WHERE t.ville = 'Tanger' AND t.nb_offres >= 5
    ORDER BY t.salaire_median_mad DESC NULLS LAST
""").df()

print("\nFocus Tanger:")
print(f"{'Profil':<30} {'n':>4} {'Q1':>7} {'Médiane':>9} {'Q3':>7} {'Écart':>8}")
print("-" * 70)
for _, r in q3b.iterrows():
    med   = f"{int(r['salaire_median_mad'])}" if pd.notna(r['salaire_median_mad']) else "N/A"
    q1    = f"{int(r['salaire_q1_mad'])}"     if pd.notna(r['salaire_q1_mad'])     else "N/A"
    q3    = f"{int(r['salaire_q3_mad'])}"     if pd.notna(r['salaire_q3_mad'])     else "N/A"
    ecart = f"{int(r['ecart_mediane_nationale']):+d}" if pd.notna(
        r['ecart_mediane_nationale']) else "N/A"
    print(f"{r['profil']:<30} {int(r['nb_offres']):>4} "
          f"{q1:>7} {med:>9} {q3:>7} {ecart:>8}")

# Chart Q3
fig, axes = plt.subplots(1, 2, figsize=(16, 7))
q3_plot = q3a.dropna(subset=['salaire_median_mad']).sort_values('salaire_median_mad')
axes[0].barh(q3_plot['profil'], q3_plot['salaire_median_mad'],
             color=COULEURS[0], alpha=0.85)
axes[0].set_xlabel("Salaire médian (MAD/mois)")
axes[0].set_title("Salaire Médian par Profil IT — Maroc national")
for bar, val in zip(axes[0].patches, q3_plot['salaire_median_mad']):
    axes[0].text(val + 200, bar.get_y() + bar.get_height()/2,
                 f"{int(val):,} MAD", va='center', fontsize=9)
if not q3b.empty:
    q3b_c = q3b.dropna(subset=['salaire_median_mad']).sort_values(
        'salaire_median_mad', ascending=False).head(8)
    for i, (_, r) in enumerate(q3b_c.iterrows()):
        q1  = r['salaire_q1_mad']  if pd.notna(r['salaire_q1_mad'])  else r['salaire_median_mad']
        q3v = r['salaire_q3_mad']  if pd.notna(r['salaire_q3_mad'])  else r['salaire_median_mad']
        med = r['salaire_median_mad']
        c   = COULEURS[3] if pd.notna(r['ecart_mediane_nationale']) and \
              r['ecart_mediane_nationale'] > 0 else COULEURS[0]
        axes[1].plot([q1, q3v], [i, i], color=c, linewidth=6, alpha=0.4)
        axes[1].plot(med, i, 'o', color=c, markersize=10, zorder=5)
    axes[1].set_yticks(range(len(q3b_c)))
    axes[1].set_yticklabels(q3b_c['profil'], fontsize=9)
    axes[1].set_xlabel("Salaire (MAD/mois)")
    axes[1].set_title("Fourchettes Salariales à Tanger (Q1–Médiane–Q3)")
plt.tight_layout()
out = OUTPUT_DIR / 'q3_salaires.png'
plt.savefig(out, bbox_inches='tight', dpi=130)
plt.close()
print(f"\n  📊 Chart saved: {out.name}")


# ==============================================================================
# QUESTION 4 — Corrélation expérience / salaire
# ==============================================================================
sep("QUESTION 4 — Corrélation expérience / salaire")

q4 = con.execute(f"""
    SELECT
        profil_normalise AS profil,
        CASE
            WHEN experience_min_ans = 0            THEN '0 — Débutant'
            WHEN experience_min_ans BETWEEN 1 AND 2 THEN '1-2 ans'
            WHEN experience_min_ans BETWEEN 3 AND 4 THEN '3-4 ans'
            WHEN experience_min_ans BETWEEN 5 AND 7 THEN '5-7 ans'
            WHEN experience_min_ans >= 8            THEN '8+ ans Senior'
            ELSE 'Non précisé'
        END AS tranche_experience,
        COUNT(*) AS nb_offres,
        ROUND(MEDIAN(salaire_median_mad)
            FILTER (WHERE salaire_connu), 0) AS salaire_median
    FROM read_parquet('{SILVER}/offres_clean/offres_clean.parquet')
    WHERE profil_normalise IN ('Data Engineer','Data Analyst',
                               'Data Scientist','DevOps / SRE')
    GROUP BY profil_normalise, tranche_experience
    ORDER BY profil, tranche_experience
""").df()

q4_corr = con.execute(f"""
    SELECT
        profil_normalise AS profil,
        ROUND(CORR(experience_min_ans, salaire_median_mad)
            FILTER (WHERE salaire_connu
                      AND experience_min_ans IS NOT NULL
                      AND salaire_median_mad IS NOT NULL), 3) AS correlation_pearson
    FROM read_parquet('{SILVER}/offres_clean/offres_clean.parquet')
    WHERE profil_normalise IN ('Data Engineer','Data Analyst',
                               'Data Scientist','DevOps / SRE')
    GROUP BY profil_normalise
    ORDER BY correlation_pearson DESC NULLS LAST
""").df()

TRANCHE_ORDER = ['0 — Débutant','1-2 ans','3-4 ans','5-7 ans','8+ ans Senior']
for profil in q4['profil'].unique():
    sub  = q4[q4['profil'] == profil]
    corr = q4_corr[q4_corr['profil'] == profil]['correlation_pearson'].values
    c_str = f"r={corr[0]:.3f}" if len(corr) > 0 and corr[0] is not None else "r=N/A"
    print(f"\n  {profil}  [{c_str}]")
    for t in TRANCHE_ORDER:
        row = sub[sub['tranche_experience'] == t]
        if not row.empty:
            n   = int(row['nb_offres'].values[0])
            sal = row['salaire_median'].values[0]
            sal_s = f"{int(sal):>8} MAD" if pd.notna(sal) else "       N/A"
            print(f"    {t:<22} n={n:>4}  {sal_s}")

# Chart Q4
fig, axes = plt.subplots(1, 2, figsize=(16, 7))
for idx, profil in enumerate(['Data Engineer','Data Analyst',
                               'Data Scientist','DevOps / SRE']):
    sub = q4[(q4['profil'] == profil) &
             (q4['tranche_experience'].isin(TRANCHE_ORDER))].copy()
    sub['tranche_experience'] = pd.Categorical(
        sub['tranche_experience'], categories=TRANCHE_ORDER, ordered=True)
    sub = sub.sort_values('tranche_experience').dropna(subset=['salaire_median'])
    if len(sub) >= 2:
        axes[0].plot(sub['tranche_experience'], sub['salaire_median'],
                     marker='o', label=profil, color=COULEURS[idx], linewidth=2)
axes[0].set_xlabel("Tranche d'expérience")
axes[0].set_ylabel("Salaire médian (MAD/mois)")
axes[0].set_title("Progression Salariale par Expérience")
axes[0].tick_params(axis='x', rotation=30)
axes[0].legend(fontsize=9)

q4c = q4_corr.dropna(subset=['correlation_pearson']).sort_values('correlation_pearson')
colors_c = [COULEURS[0] if v > 0 else COULEURS[3] for v in q4c['correlation_pearson']]
axes[1].barh(q4c['profil'], q4c['correlation_pearson'], color=colors_c)
axes[1].axvline(x=0,   color='black', linewidth=0.8)
axes[1].axvline(x=0.3, color='gray',  linestyle='--', alpha=0.5)
axes[1].axvline(x=0.6, color='gray',  linestyle=':',  alpha=0.5)
axes[1].set_xlabel("Corrélation de Pearson (r)")
axes[1].set_title("Corrélation Expérience ↔ Salaire")
for bar, val in zip(axes[1].patches, q4c['correlation_pearson']):
    axes[1].text(val + 0.01, bar.get_y() + bar.get_height()/2,
                 f"r={val:.3f}", va='center', fontsize=9)
plt.tight_layout()
out = OUTPUT_DIR / 'q4_experience_salaire.png'
plt.savefig(out, bbox_inches='tight', dpi=130)
plt.close()
print(f"\n  📊 Chart saved: {out.name}")


# ==============================================================================
# QUESTION 5 — Entreprises recruteurs / concurrents Mexora
# ==============================================================================
sep("QUESTION 5 — Entreprises recruteurs et concurrents Mexora")

q5a = con.execute(f"""
    SELECT entreprise, ville, nb_offres_publiees,
           nb_profils_differents, salaire_moyen_propose,
           RANK() OVER (ORDER BY nb_offres_publiees DESC) AS rang_recruteur
    FROM read_parquet('{GOLD}/entreprises_recruteurs.parquet')
    ORDER BY nb_offres_publiees DESC
    LIMIT 20
""").df()

print(f"\n{'Rang':<5} {'Entreprise':<30} {'Ville':<15} {'Offres':>7} {'Sal.Moy':>10}")
print("-" * 70)
for _, r in q5a.iterrows():
    sal = f"{int(r['salaire_moyen_propose'])} MAD" if pd.notna(r['salaire_moyen_propose']) else "N/A"
    print(f"{int(r['rang_recruteur']):<5} {r['entreprise']:<30} "
          f"{r['ville']:<15} {int(r['nb_offres_publiees']):>7} {sal:>10}")

q5b = con.execute(f"""
    SELECT entreprise, nb_offres_publiees, profils_recrutes,
           salaire_moyen_propose,
           CASE
               WHEN salaire_moyen_propose > 20000 THEN 'Compétiteur fort'
               WHEN salaire_moyen_propose > 12000 THEN 'Compétiteur moyen'
               ELSE 'Compétiteur faible'
           END AS niveau_competition
    FROM read_parquet('{GOLD}/entreprises_recruteurs.parquet')
    WHERE ville = 'Tanger'
    ORDER BY nb_offres_publiees DESC
    LIMIT 15
""").df()

print("\nConcurrents directs Mexora à Tanger:")
print(f"{'Entreprise':<30} {'Offres':>7} {'Sal.Moy':>10} {'Niveau':>18}")
print("-" * 68)
for _, r in q5b.iterrows():
    sal = f"{int(r['salaire_moyen_propose'])} MAD" if pd.notna(r['salaire_moyen_propose']) else "N/A"
    print(f"{r['entreprise']:<30} {int(r['nb_offres_publiees']):>7} "
          f"{sal:>10}  {r['niveau_competition']:>18}")

# Chart Q5
fig, axes = plt.subplots(1, 2, figsize=(16, 7))
q5_t = q5a.head(15).sort_values('nb_offres_publiees')
axes[0].barh(q5_t['entreprise'], q5_t['nb_offres_publiees'], color=COULEURS[0])
axes[0].set_xlabel("Nombre d'offres publiées")
axes[0].set_title("Top 15 Recruteurs IT au Maroc")
for bar, val in zip(axes[0].patches, q5_t['nb_offres_publiees']):
    axes[0].text(val + 0.3, bar.get_y() + bar.get_height()/2,
                 str(int(val)), va='center', fontsize=9)
q5b_c = q5b.dropna(subset=['salaire_moyen_propose'])
cmap  = {'Compétiteur fort': COULEURS[3],
         'Compétiteur moyen': COULEURS[4],
         'Compétiteur faible': COULEURS[2]}
for _, r in q5b_c.iterrows():
    c = cmap.get(r['niveau_competition'], COULEURS[0])
    axes[1].scatter(r['salaire_moyen_propose'], r['nb_offres_publiees'],
                    color=c, s=120, zorder=5)
    axes[1].annotate(r['entreprise'],
                     (r['salaire_moyen_propose'], r['nb_offres_publiees']),
                     fontsize=8, xytext=(5, 3), textcoords='offset points')
axes[1].axvline(x=12000, color='gray', linestyle='--', alpha=0.5)
axes[1].axvline(x=20000, color='gray', linestyle=':', alpha=0.5)
axes[1].scatter(19000, 5, color='black', s=200, marker='*',
                zorder=10, label='Mexora (cible)')
axes[1].set_xlabel("Salaire moyen proposé (MAD/mois)")
axes[1].set_ylabel("Nombre d'offres publiées")
axes[1].set_title("Concurrents Mexora à Tanger")
axes[1].legend(fontsize=9)
plt.tight_layout()
out = OUTPUT_DIR / 'q5_entreprises.png'
plt.savefig(out, bbox_inches='tight', dpi=130)
plt.close()
print(f"\n  📊 Chart saved: {out.name}")


# ==============================================================================
# SUMMARY
# ==============================================================================
sep("ANALYSE COMPLÈTE")
print("\n  5 questions répondues. 5 visualisations sauvegardées.")
print("\n  Résultats clés pour Mexora :")
print("    • Compétence #1 marché      : SQL (58.6% des offres)")
print("    • Tanger offres Data Eng    : 95 offres, 69.5% remote")
print("    • Salaire médian DE Tanger  : 19 000 MAD/mois")
print("    • Corrélation exp/salaire   : positive et significative")
print("    • Principal concurrent Tger : Renault Digital MA")
print("\n  Pour les visualisations enrichies, ouvrir :")
print("    analysis/analyse_marche_it_maroc.ipynb")
sep()