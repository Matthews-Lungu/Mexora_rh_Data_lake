"""
morocco_map.py
==============
Run this script to generate the Morocco IT offers bubble map.
Saves to: analysis/morocco_map.png

Run from project root:
    python analysis/morocco_map.py
"""

import duckdb
import plotly.express as px
import plotly.io as pio
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD         = PROJECT_ROOT / 'data_lake' / 'gold'
OUTPUT       = Path(__file__).resolve().parent / 'morocco_map.png'

# ── Load data ─────────────────────────────────────────────────────────────────
con = duckdb.connect()
df = con.execute(f"""
    SELECT ville, SUM(nb_offres) AS total_offres
    FROM read_parquet('{str(GOLD).replace(chr(92), '/')}//offres_par_ville.parquet')
    GROUP BY ville
    ORDER BY total_offres DESC
""").df()

# ── City coordinates ──────────────────────────────────────────────────────────
coords = {
    'Casablanca': (33.5731, -7.5898),
    'Rabat':      (34.0209, -6.8416),
    'Tanger':     (35.7595, -5.8340),
    'Marrakech':  (31.6295, -7.9811),
    'Fès':        (34.0181, -5.0078),
    'Agadir':     (30.4278, -9.5981),
    'Meknès':     (33.8935, -5.5473),
    'Oujda':      (34.6867, -1.9114),
    'Kenitra':    (34.2610, -6.5802),
    'Tétouan':    (35.5785, -5.3684),
    'El Jadida':  (33.2316, -8.5007),
    'Mohammedia': (33.6866, -7.3830),
    'Settat':     (33.0016, -7.6194),
}

df['lat'] = df['ville'].map(lambda v: coords.get(v, (0, 0))[0])
df['lon'] = df['ville'].map(lambda v: coords.get(v, (0, 0))[1])
df = df[df['lat'] != 0].copy()

# ── Build figure ──────────────────────────────────────────────────────────────
fig = px.scatter_geo(
    df,
    lat='lat', lon='lon',
    size='total_offres',
    hover_name='ville',
    hover_data={'total_offres': True, 'lat': False, 'lon': False},
    text='ville',
    color='total_offres',
    color_continuous_scale='Blues',
    size_max=60,
    scope='africa',
    center={"lat": 31.7917, "lon": -7.0926},
    title='Volume of IT Job Offers by City — Morocco 2023–2024',
    labels={'total_offres': 'Number of Offers'}
)
fig.update_geos(
    showcoastlines=True, coastlinecolor="gray",
    showland=True,  landcolor="#F5F5DC",
    showocean=True, oceancolor="#E8F4F8",
    showframe=False,
    lataxis_range=[27, 38],
    lonaxis_range=[-14, 1]
)
fig.update_traces(textposition='top center', textfont_size=11)
fig.update_layout(
    width=900, height=600,
    coloraxis_colorbar_title="Offers",
    margin={"r": 0, "t": 40, "l": 0, "b": 0}
)

# ── Save using matplotlib fallback (avoids kaleido version issues) ────────────
try:
    # Try kaleido first
    fig.write_image(str(OUTPUT), scale=2, engine="kaleido")
    print(f"✅  Morocco map saved (kaleido): {OUTPUT}")
except Exception:
    # Fallback: save as interactive HTML + matplotlib static version
    print("⚠️  kaleido failed — using HTML + matplotlib fallback...")

    # Save interactive HTML (always works, no kaleido needed)
    html_path = str(OUTPUT).replace('.png', '.html')
    fig.write_html(html_path)
    print(f"✅  Interactive map saved as HTML: {html_path}")

    # Also build a clean matplotlib static version as backup PNG
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np

    fig2, ax = plt.subplots(figsize=(12, 8))
    ax.set_facecolor('#E8F4F8')
    fig2.patch.set_facecolor('#E8F4F8')

    # Normalize bubble sizes
    sizes = df['total_offres'].values
    norm_sizes = (sizes / sizes.max()) * 3000 + 100

    # Colour by count
    scatter = ax.scatter(
        df['lon'], df['lat'],
        s=norm_sizes,
        c=df['total_offres'],
        cmap='Blues',
        alpha=0.85,
        edgecolors='#1B4F72',
        linewidths=1.5,
        zorder=5
    )

    # City labels
    for _, row in df.iterrows():
        ax.annotate(
            f"{row['ville']}\n({int(row['total_offres'])})",
            (row['lon'], row['lat']),
            xytext=(6, 6), textcoords='offset points',
            fontsize=9, fontweight='bold', color='#1B4F72',
            zorder=6
        )

    # Morocco approximate outline (simplified bounding box)
    from matplotlib.patches import FancyBboxPatch
    ax.set_xlim(-14, 1)
    ax.set_ylim(27, 38)
    ax.set_xlabel("Longitude", fontsize=10)
    ax.set_ylabel("Latitude", fontsize=10)
    ax.set_title(
        "Volume of IT Job Offers by City — Morocco 2023–2024\n"
        "(bubble size = number of offers)",
        fontsize=14, fontweight='bold', color='#1B4F72', pad=15
    )
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    # Colorbar
    cbar = plt.colorbar(scatter, ax=ax, shrink=0.6)
    cbar.set_label('Number of Offers', fontsize=10)

    # Legend for bubble sizes
    for size_val, label in [(1704, 'Casablanca\n(1,704)'),
                             (597,  'Tanger\n(597)'),
                             (193,  'Agadir\n(193)')]:
        s = (size_val / sizes.max()) * 3000 + 100
        ax.scatter([], [], s=s, c='#2E86C1', alpha=0.6,
                   edgecolors='#1B4F72', label=label)
    ax.legend(title="Reference sizes", loc='lower left',
              fontsize=8, title_fontsize=9)

    plt.tight_layout()
    plt.savefig(str(OUTPUT), dpi=150, bbox_inches='tight',
                facecolor='#E8F4F8')
    plt.close()
    print(f"✅  Morocco map saved (matplotlib fallback): {OUTPUT}")

print("\nDone. Check analysis/morocco_map.png")