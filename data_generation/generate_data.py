"""
generate_data.py
================
Mexora RH Intelligence — Data Generation Script
Generates the 3 raw input files for the Data Lake pipeline:
  1. offres_emploi_it_maroc.json   — 5 000 IT job offers (intentionally messy)
  2. referentiel_competences_it.json — 300-skill reference dictionary
  3. entreprises_it_maroc.csv      — company reference table

Usage:
    python data_generation/generate_data.py

Output files are written to: data_generation/
"""

import json
import csv
import random
import os
from datetime import datetime, timedelta

# ── Reproducibility ────────────────────────────────────────────────────────────
random.seed(42)

# ── Output directory ───────────────────────────────────────────────────────────
OUTPUT_DIR = os.path.join(os.path.dirname(__file__))
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==============================================================================
# SECTION 1 — REFERENCE DATA (clean master lists used during generation)
# ==============================================================================

# Cities with realistic Moroccan IT market weights
# Tangier is intentionally weighted high (Mexora is based there)
VILLES_CONFIG = {
    "Casablanca":   0.35,   # dominant market
    "Rabat":        0.18,
    "Tanger":       0.12,   # ~600 offers → enough for per-profile stats
    "Marrakech":    0.07,
    "Fès":          0.06,
    "Agadir":       0.04,
    "Oujda":        0.03,
    "Meknès":       0.03,
    "Tétouan":      0.03,
    "El Jadida":    0.02,
    "Kenitra":      0.03,
    "Mohammedia":   0.02,
    "Settat":       0.02,
}

# Dirty variants of city names (intentionally introduced as data quality issues)
VILLES_DIRTY = {
    "Casablanca": ["Casablanca", "casablanca", "CASABLANCA", "Casa", "casa",
                   "Casablanque", "CasaBlanca", "Dar El Beida"],
    "Rabat":      ["Rabat", "rabat", "RABAT", "Rabat-Salé", "Rabat Salé"],
    "Tanger":     ["Tanger", "tanger", "TANGER", "Tanger-Assilah", "Tanja",
                   "Tangier", "Tanger Med"],
    "Marrakech":  ["Marrakech", "marrakech", "Marrakesh", "MARRAKECH"],
    "Fès":        ["Fès", "Fes", "FES", "fès", "fes"],
    "Agadir":     ["Agadir", "agadir", "AGADIR"],
    "Oujda":      ["Oujda", "oujda", "OUJDA"],
    "Meknès":     ["Meknès", "Meknes", "meknes", "MEKNES"],
    "Tétouan":    ["Tétouan", "Tetouan", "tetouan", "TETOUAN"],
    "El Jadida":  ["El Jadida", "El-Jadida", "el jadida", "EL JADIDA"],
    "Kenitra":    ["Kenitra", "Kénitra", "kenitra", "KENITRA"],
    "Mohammedia": ["Mohammedia", "mohammedia", "MOHAMMEDIA"],
    "Settat":     ["Settat", "settat", "SETTAT"],
}

SOURCES = ["rekrute", "marocannonce", "linkedin"]
SOURCE_WEIGHTS = [0.45, 0.30, 0.25]

PROFILS = {
    "Data Engineer":          0.16,
    "Data Analyst":           0.14,
    "Data Scientist":         0.08,
    "Développeur Full Stack": 0.18,
    "Développeur Backend":    0.12,
    "Développeur Frontend":   0.08,
    "Développeur Mobile":     0.06,
    "DevOps / SRE":           0.07,
    "Cloud Engineer":         0.04,
    "Cybersécurité":          0.04,
    "Chef de Projet IT":      0.03,
}

# Dirty title variants per profile (the core data quality problem in titre_poste)
TITRES_DIRTY = {
    "Data Engineer": [
        "Data Engineer", "Data Eng.", "Ingénieur Data", "Ingénieur Big Data",
        "Dev Data", "Data Engineering", "data engineer", "DATA ENGINEER",
        "Ingénieur ETL", "ETL Developer", "Pipeline Developer",
        "Data Engineer Junior", "Data Engineer Senior", "Ingénieur Data Junior",
        "Big Data Engineer", "Ingénieur Data & BI", "Lead Data Engineer",
    ],
    "Data Analyst": [
        "Data Analyst", "Analyste Data", "Analyste BI", "Business Analyst Data",
        "Analyste Business Intelligence", "BI Analyst", "Data Analyst Junior",
        "Développeur BI", "Ingénieur BI", "Analyste Reporting",
        "Chargé de Reporting", "data analyst", "DATA ANALYST",
        "Analyste de données", "Reporting Analyst", "BI Developer",
    ],
    "Data Scientist": [
        "Data Scientist", "Machine Learning Engineer", "ML Engineer",
        "Ingénieur Machine Learning", "AI Engineer", "Ingénieur IA",
        "Deep Learning Engineer", "NLP Engineer", "data scientist",
        "Data Science Engineer", "Research Scientist", "Applied ML Engineer",
    ],
    "Développeur Full Stack": [
        "Développeur Full Stack", "Full Stack Developer", "Fullstack Dev",
        "Développeur FullStack", "Full Stack Engineer", "Dev Full Stack",
        "Développeur Web Full Stack", "full stack developer", "FULL STACK DEV",
        "Développeur Full-Stack React/Node", "Ingénieur Full Stack",
    ],
    "Développeur Backend": [
        "Développeur Backend", "Backend Developer", "Développeur Back-End",
        "Ingénieur Backend", "Backend Engineer", "Dev Backend",
        "Développeur Serveur", "backend developer", "BACKEND DEV",
        "Développeur API", "Ingénieur Logiciel Backend",
    ],
    "Développeur Frontend": [
        "Développeur Frontend", "Frontend Developer", "Développeur Front-End",
        "Ingénieur Frontend", "Dev Frontend", "Développeur Web Frontend",
        "frontend developer", "Développeur React", "Développeur Angular",
        "UI Developer", "Développeur Interface",
    ],
    "Développeur Mobile": [
        "Développeur Mobile", "Mobile Developer", "Développeur iOS",
        "Développeur Android", "Développeur React Native", "Dev Mobile",
        "Mobile App Developer", "Ingénieur Mobile", "Flutter Developer",
        "Développeur Applications Mobiles",
    ],
    "DevOps / SRE": [
        "DevOps Engineer", "Ingénieur DevOps", "SRE", "Site Reliability Engineer",
        "DevOps", "Ingénieur Infra DevOps", "DevSecOps", "DevOps Consultant",
        "Infrastructure Engineer", "DevOps / Cloud", "devops engineer",
    ],
    "Cloud Engineer": [
        "Cloud Engineer", "Ingénieur Cloud", "Architecte Cloud",
        "AWS Engineer", "GCP Engineer", "Azure Engineer",
        "Cloud Architect", "Ingénieur Cloud AWS", "Cloud Solutions Architect",
    ],
    "Cybersécurité": [
        "Ingénieur Cybersécurité", "Cybersecurity Engineer", "Analyste SOC",
        "Pentester", "Expert Sécurité", "Security Analyst", "RSSI",
        "Ingénieur Sécurité Informatique", "Consultant Cybersécurité",
        "cybersecurity", "SOC Analyst",
    ],
    "Chef de Projet IT": [
        "Chef de Projet IT", "Project Manager IT", "Chef de Projet Informatique",
        "Responsable Projet IT", "IT Project Manager", "Scrum Master",
        "Product Owner", "Chef de Projet Digital", "PMO",
        "Chargé de Projet IT",
    ],
}

# Skills per profile (used for realistic description generation)
COMPETENCES_PAR_PROFIL = {
    "Data Engineer": [
        "Python", "PySpark", "Apache Spark", "Kafka", "Airflow",
        "dbt", "Hadoop", "PostgreSQL", "SQL", "AWS", "GCP", "Azure",
        "Docker", "Git", "Linux", "ETL", "Parquet", "DuckDB",
    ],
    "Data Analyst": [
        "SQL", "Python", "Power BI", "Tableau", "Excel",
        "Metabase", "Looker", "pandas", "matplotlib", "seaborn",
        "PostgreSQL", "DAX", "R", "Google Analytics", "Looker Studio",
    ],
    "Data Scientist": [
        "Python", "Machine Learning", "scikit-learn", "TensorFlow",
        "PyTorch", "NLP", "Deep Learning", "pandas", "numpy",
        "SQL", "R", "Jupyter", "MLflow", "Hugging Face", "AWS",
    ],
    "Développeur Full Stack": [
        "React", "Node.js", "JavaScript", "TypeScript", "PostgreSQL",
        "MongoDB", "Docker", "Git", "REST API", "GraphQL",
        "Angular", "Vue.js", "HTML", "CSS", "AWS",
    ],
    "Développeur Backend": [
        "Python", "Java", "Node.js", "Django", "Spring Boot",
        "PostgreSQL", "MySQL", "Redis", "Docker", "Git",
        "REST API", "Microservices", "Kafka", "AWS", "Linux",
    ],
    "Développeur Frontend": [
        "React", "Angular", "Vue.js", "JavaScript", "TypeScript",
        "HTML", "CSS", "Git", "Figma", "REST API",
        "Webpack", "Jest", "SASS", "Bootstrap", "Tailwind",
    ],
    "Développeur Mobile": [
        "React Native", "Flutter", "Swift", "Kotlin", "Java",
        "Android", "iOS", "Firebase", "REST API", "Git",
        "Dart", "Redux", "TypeScript", "Xcode", "Android Studio",
    ],
    "DevOps / SRE": [
        "Docker", "Kubernetes", "Jenkins", "Terraform", "Ansible",
        "AWS", "GCP", "Azure", "Linux", "Git",
        "CI/CD", "Prometheus", "Grafana", "Helm", "Bash",
    ],
    "Cloud Engineer": [
        "AWS", "GCP", "Azure", "Terraform", "Kubernetes",
        "Docker", "Linux", "Ansible", "CloudFormation", "Git",
        "IAM", "S3", "Lambda", "EC2", "VPC",
    ],
    "Cybersécurité": [
        "Pentest", "SIEM", "Splunk", "Wireshark", "Python",
        "Linux", "Firewall", "ISO 27001", "OWASP", "Git",
        "Nmap", "Metasploit", "SOC", "Azure", "AWS",
    ],
    "Chef de Projet IT": [
        "Agile", "Scrum", "JIRA", "Confluence", "MS Project",
        "PMP", "Prince2", "Kanban", "Git", "Power BI",
        "Excel", "SQL", "Risk Management", "Stakeholder Management",
    ],
}

CONTRATS = ["CDI", "CDD", "Freelance", "Stage", "Alternance"]
CONTRATS_WEIGHTS = [0.50, 0.15, 0.18, 0.10, 0.07]

# Dirty contract variants
CONTRATS_DIRTY = {
    "CDI": ["CDI", "cdi", "Contrat à durée indéterminée", "Permanent",
            "CDI - Temps plein", "Contrat CDI", "PERMANENT"],
    "CDD": ["CDD", "cdd", "Contrat à durée déterminée", "Temporaire",
            "CDD 6 mois", "CDD 1 an", "Mission"],
    "Freelance": ["Freelance", "freelance", "Consultant", "Auto-entrepreneur",
                  "Mission freelance", "Indépendant", "FREELANCE"],
    "Stage": ["Stage", "stage", "Internship", "Stage PFE", "Stage de fin d'études",
              "Stage conventionné", "Stage 6 mois"],
    "Alternance": ["Alternance", "alternance", "Apprentissage", "Contrat Pro",
                   "Contrat d'apprentissage"],
}

EXPERIENCES = [
    ("0", "Débutant accepté"),
    ("0-1", "0 à 1 an"),
    ("1-2", "1-2 ans"),
    ("1-3", "min 1 an"),
    ("2-4", "2 à 4 ans"),
    ("3-5", "3-5 ans"),
    ("3-5", "3 à 5 ans"),
    ("5-7", "5-7 ans"),
    ("5+",  "min 5 ans"),
    ("7+",  "Senior (7+ ans)"),
    ("7+",  "Expert confirmé"),
    (None,  None),
]
EXP_WEIGHTS = [0.08, 0.06, 0.10, 0.07, 0.12, 0.18, 0.10, 0.09, 0.08, 0.05, 0.04, 0.03]

NIVEAUX_ETUDES = ["Bac+2", "Bac+3", "Bac+5", "Bac+5", "Bac+5", "Bac+8", None]

TELETRAVAIL = ["Présentiel", "Hybride", "Télétravail complet", "Hybride", "Présentiel"]

LANGUES = [
    ["Français"],
    ["Français", "Anglais"],
    ["Français", "Anglais"],
    ["Français", "Anglais", "Arabe"],
    ["Anglais"],
    ["Arabe", "Français"],
]

SECTEURS = [
    "Informatique / Télécom",
    "Banque / Assurance",
    "Conseil / SSII",
    "E-commerce / Retail",
    "Industrie",
    "Santé",
    "Éducation",
    "Média / Communication",
    "Logistique / Transport",
    "Énergie",
]

# ==============================================================================
# SECTION 2 — COMPANY LIST (for entreprises_it_maroc.csv)
# ==============================================================================

ENTREPRISES = [
    # SSII / Consulting
    ("CGI Maroc",          "Conseil",   "Grande Entreprise", "Casablanca", "cgi.com",       "SSII"),
    ("Capgemini Maroc",    "Conseil",   "Grande Entreprise", "Casablanca", "capgemini.com", "Conseil"),
    ("Accenture Maroc",    "Conseil",   "Grande Entreprise", "Casablanca", "accenture.com", "Conseil"),
    ("IBM Maroc",          "Tech",      "Grande Entreprise", "Casablanca", "ibm.com",       "SSII"),
    ("Sopra Steria Maroc", "Conseil",   "ETI",               "Casablanca", "soprasteria.com","Conseil"),
    ("Talan Maroc",        "Conseil",   "ETI",               "Casablanca", "talan.com",     "Conseil"),
    ("Intelcia",           "Conseil",   "Grande Entreprise", "Casablanca", "intelcia.com",  "SSII"),
    ("S2M",                "Fintech",   "ETI",               "Casablanca", "s2m.ma",        "Produit"),
    ("HPS",                "Fintech",   "ETI",               "Casablanca", "hps-worldwide.com","Produit"),
    ("Inwi",               "Telecom",   "Grande Entreprise", "Casablanca", "inwi.ma",       "Telecom"),
    ("Maroc Telecom",      "Telecom",   "Grande Entreprise", "Rabat",      "iam.ma",        "Telecom"),
    ("Orange Maroc",       "Telecom",   "Grande Entreprise", "Casablanca", "orange.ma",     "Telecom"),
    ("Attijariwafa Bank",  "Banque",    "Grande Entreprise", "Casablanca", "attijariwafa.com","Banque"),
    ("BMCE Bank",          "Banque",    "Grande Entreprise", "Casablanca", "bmcebank.ma",   "Banque"),
    ("CIH Bank",           "Banque",    "ETI",               "Casablanca", "cihbank.ma",    "Banque"),
    ("Wafasalaf",          "Banque",    "ETI",               "Casablanca", "wafasalaf.ma",  "Banque"),
    ("TechMaroc SARL",     "Tech",      "PME",               "Casablanca", "techmaroc.ma",  "Produit"),
    ("DataVision MA",      "Data",      "Startup",           "Rabat",      "datavision.ma", "Produit"),
    ("CloudMa Solutions",  "Cloud",     "Startup",           "Casablanca", "cloudma.io",    "Produit"),
    ("Devoteam Maroc",     "Conseil",   "ETI",               "Casablanca", "devoteam.com",  "Conseil"),
    # Tangier-based (important for Mexora competitor analysis)
    ("Tanger Free Zone IT","Tech",      "ETI",               "Tanger",     "tangerfreezone.ma","SSII"),
    ("NearShore Tanger",   "SSII",      "ETI",               "Tanger",     "nearshore.ma",  "SSII"),
    ("Renault Digital MA", "Auto/Tech", "Grande Entreprise", "Tanger",     "renault.com",   "Produit"),
    ("Delphi Technologies","Tech",      "Grande Entreprise", "Tanger",     "delphi.com",    "Produit"),
    ("Lear Corp Tanger",   "Industrie", "Grande Entreprise", "Tanger",     "lear.com",      "Autre"),
    ("Axians Tanger",      "Conseil",   "ETI",               "Tanger",     "axians.com",    "SSII"),
    ("DigitalTanger",      "Tech",      "Startup",           "Tanger",     "digitaltanger.ma","Produit"),
    ("Mexora",             "E-commerce","ETI",               "Tanger",     "mexora.ma",     "Produit"),
    # Other cities
    ("OCP Digital",        "Industrie", "Grande Entreprise", "Casablanca", "ocpgroup.ma",   "Produit"),
    ("Lydec",              "Énergie",   "Grande Entreprise", "Casablanca", "lydec.ma",      "Autre"),
    ("ONCF Digital",       "Transport", "Grande Entreprise", "Rabat",      "oncf.ma",       "Autre"),
    ("Universiapolis Tech","Éducation", "PME",               "Agadir",     "universiapolis.ma","Autre"),
    ("Almadina Tech",      "Tech",      "Startup",           "Fès",        "almadina.ma",   "Produit"),
    ("BO Consulting",      "Conseil",   "PME",               "Rabat",      "boconsulting.ma","Conseil"),
    ("Majorel Maroc",      "Conseil",   "Grande Entreprise", "Rabat",      "majorel.com",   "SSII"),
    ("KPMG Maroc",         "Conseil",   "Grande Entreprise", "Casablanca", "kpmg.ma",       "Conseil"),
    ("PwC Maroc",          "Conseil",   "Grande Entreprise", "Casablanca", "pwc.com",       "Conseil"),
    ("Deloitte Maroc",     "Conseil",   "Grande Entreprise", "Casablanca", "deloitte.com",  "Conseil"),
    ("EY Maroc",           "Conseil",   "Grande Entreprise", "Casablanca", "ey.com",        "Conseil"),
    ("StartupFactory MA",  "Tech",      "Startup",           "Casablanca", "startupfactory.ma","Produit"),
]

# City → company weight mapping so Tangier companies get Tangier offers
ENTREPRISES_PAR_VILLE = {
    ville: [e for e in ENTREPRISES if e[3] == ville or e[3] == "Casablanca"]
    for ville in VILLES_CONFIG.keys()
}
# Ensure Tangier has its own companies prominent
ENTREPRISES_PAR_VILLE["Tanger"] = [e for e in ENTREPRISES if e[3] == "Tanger"] + \
                                    [e for e in ENTREPRISES if e[3] == "Casablanca"][:10]

# ==============================================================================
# SECTION 3 — SALARY GENERATION (intentionally messy formats)
# ==============================================================================

# Realistic salary ranges in MAD by profile
SALAIRES_PAR_PROFIL = {
    "Data Engineer":          (12000, 35000),
    "Data Analyst":           (8000,  25000),
    "Data Scientist":         (15000, 45000),
    "Développeur Full Stack": (10000, 30000),
    "Développeur Backend":    (9000,  28000),
    "Développeur Frontend":   (7000,  22000),
    "Développeur Mobile":     (8000,  25000),
    "DevOps / SRE":           (12000, 35000),
    "Cloud Engineer":         (14000, 40000),
    "Cybersécurité":          (12000, 35000),
    "Chef de Projet IT":      (15000, 45000),
}

def generer_salaire_dirty(profil, contrat):
    """Generate a salary string with intentional formatting issues."""
    # 30% of offers hide salary
    if random.random() < 0.30:
        return random.choice(["Selon profil", "Confidentiel", None, "A négocier", ""])

    sal_min_range, sal_max_range = SALAIRES_PAR_PROFIL.get(profil, (8000, 25000))

    # Stage / alternance → much lower
    if contrat in ["Stage", "Alternance"]:
        sal_min = random.randint(2000, 4000)
        sal_max = sal_min + random.randint(500, 1500)
    else:
        sal_min = random.randint(sal_min_range, sal_min_range + (sal_max_range - sal_min_range) // 2)
        sal_max = sal_min + random.randint(2000, 8000)
        sal_max = min(sal_max, sal_max_range)

    # Round to nearest 500
    sal_min = round(sal_min / 500) * 500
    sal_max = round(sal_max / 500) * 500

    # Choose a random dirty format
    fmt = random.randint(1, 7)
    if fmt == 1:
        return f"{sal_min}-{sal_max} MAD"
    elif fmt == 2:
        return f"{sal_min//1000}K-{sal_max//1000}K MAD"
    elif fmt == 3:
        return f"{sal_min//1000}K-{sal_max//1000}K"
    elif fmt == 4:
        # EUR (about 1/10 of MAD)
        eur_min = round(sal_min / 10.8)
        eur_max = round(sal_max / 10.8)
        return f"{eur_min}-{eur_max} EUR"
    elif fmt == 5:
        return f"{sal_min} - {sal_max} DH"
    elif fmt == 6:
        return f"{sal_min} MAD"
    else:
        return f"{sal_min//1000}k-{sal_max//1000}k dh"

# ==============================================================================
# SECTION 4 — DESCRIPTION GENERATION
# ==============================================================================

INTRO_TEMPLATES = [
    "Nous recherchons un {titre} expérimenté pour rejoindre notre équipe dynamique.",
    "Dans le cadre de notre développement, nous recrutons un {titre} passionné.",
    "Vous êtes {titre} et cherchez un nouveau défi ? Rejoignez-nous !",
    "Notre client, leader dans son secteur, recherche un {titre} pour renforcer son équipe.",
    "Poste : {titre} — CDI — {ville}. Profil recherché :",
    "We are looking for a talented {titre} to join our growing team in {ville}.",
    "Offre d'emploi : {titre} — {secteur} — {ville}.",
]

def generer_description(profil, titre, ville, competences, contrat, experience):
    """Generate a realistic free-text job description."""
    intro = random.choice(INTRO_TEMPLATES).format(
        titre=titre, ville=ville, secteur=random.choice(SECTEURS)
    )

    # Pick 5-9 skills to mention in description
    nb_skills = random.randint(5, 9)
    skills_mentions = random.sample(competences, min(nb_skills, len(competences)))

    missions = [
        f"Concevoir et maintenir des pipelines de données robustes.",
        f"Développer des solutions basées sur {random.choice(skills_mentions)}.",
        f"Collaborer avec les équipes métier pour identifier les besoins.",
        f"Assurer la qualité et la fiabilité des données.",
        f"Mettre en place des outils de monitoring et d'alerting.",
        f"Participer aux revues de code et aux cérémonies Agile.",
        f"Rédiger la documentation technique.",
        f"Contribuer à l'amélioration continue des processus.",
    ]

    profil_requis = [
        f"Maîtrise de {skills_mentions[0]} indispensable.",
        f"Bonne connaissance de {skills_mentions[1] if len(skills_mentions) > 1 else 'SQL'}.",
        f"Expérience : {experience if experience else 'à définir'}.",
        f"Connaissance de {random.choice(skills_mentions)} appréciée.",
        f"Esprit d'équipe, rigueur et autonomie.",
    ]

    nb_missions = random.randint(3, 5)
    desc = f"{intro}\n\n"
    desc += "Missions :\n"
    desc += "\n".join(f"- {m}" for m in random.sample(missions, nb_missions))
    desc += "\n\nProfil requis :\n"
    desc += "\n".join(f"- {p}" for p in profil_requis)
    desc += f"\n\nCompétences clés : {', '.join(skills_mentions)}"

    return desc

def generer_competences_brut(competences):
    """Generate messy competences_brut field with inconsistent separators."""
    nb = random.randint(4, 10)
    skills = random.sample(competences, min(nb, len(competences)))

    separateur = random.choice([", ", " / ", " • ", "\n", " | ", " ; "])
    # Add some noise: random case variations
    skills_noisy = []
    for s in skills:
        r = random.random()
        if r < 0.2:
            skills_noisy.append(s.upper())
        elif r < 0.4:
            skills_noisy.append(s.lower())
        else:
            skills_noisy.append(s)

    return separateur.join(skills_noisy)

# ==============================================================================
# SECTION 5 — DATE GENERATION
# ==============================================================================

def generer_dates():
    """Generate publication and expiration dates with some intentional errors."""
    start = datetime(2023, 1, 1)
    end   = datetime(2024, 11, 30)
    delta = (end - start).days
    pub = start + timedelta(days=random.randint(0, delta))

    # Expiration: normally pub + 30 days, but 5% have incoherent dates (pub > exp)
    if random.random() < 0.05:
        exp = pub - timedelta(days=random.randint(1, 15))   # intentional error
    else:
        exp = pub + timedelta(days=random.randint(15, 60))

    # Random date format issues
    fmt_pub = random.choice(["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"])
    fmt_exp = random.choice(["%Y-%m-%d", "%d/%m/%Y"])

    return pub.strftime(fmt_pub), exp.strftime(fmt_exp), pub

# ==============================================================================
# SECTION 6 — MAIN GENERATOR
# ==============================================================================

def generer_offres(n=5000):
    """Generate n job offers with realistic dirty data."""
    print(f"[GEN] Generating {n} job offers...")

    # Pre-compute city list respecting weights
    villes_list  = list(VILLES_CONFIG.keys())
    villes_w     = list(VILLES_CONFIG.values())

    profils_list = list(PROFILS.keys())
    profils_w    = list(PROFILS.values())

    offres = []
    counter = {v: 0 for v in villes_list}

    for i in range(n):
        offre_id = f"RK-{2023 + random.randint(0,1)}-{str(i+1).zfill(5)}"
        source   = random.choices(SOURCES, weights=SOURCE_WEIGHTS)[0]
        profil   = random.choices(profils_list, weights=profils_w)[0]
        ville_std = random.choices(villes_list, weights=villes_w)[0]
        ville_dirty = random.choice(VILLES_DIRTY[ville_std])

        # Dirty title
        titre = random.choice(TITRES_DIRTY[profil])

        # Contract
        contrat_std = random.choices(CONTRATS, weights=CONTRATS_WEIGHTS)[0]
        contrat_dirty = random.choice(CONTRATS_DIRTY[contrat_std])

        # Experience
        exp_tuple = random.choices(EXPERIENCES, weights=EXP_WEIGHTS)[0]
        exp_value, exp_display = exp_tuple

        # Salary
        salaire = generer_salaire_dirty(profil, contrat_std)

        # Dates
        date_pub_str, date_exp_str, date_pub_obj = generer_dates()

        # Competences
        base_competences = COMPETENCES_PAR_PROFIL[profil]
        competences_brut = generer_competences_brut(base_competences)

        # Description
        description = generer_description(
            profil, titre, ville_dirty, base_competences,
            contrat_std, exp_display
        )

        # Company — prefer city-matching companies
        entreprises_disponibles = ENTREPRISES_PAR_VILLE.get(ville_std, ENTREPRISES)
        entreprise = random.choice(entreprises_disponibles)[0]

        # Niveau études — null for 10%
        niveau = random.choice(NIVEAUX_ETUDES)

        # nb_postes — mostly 1, sometimes more
        nb_postes = random.choices([1, 1, 1, 2, 3, 5], weights=[50, 20, 15, 8, 5, 2])[0]

        offre = {
            "id_offre":           offre_id,
            "source":             source,
            "titre_poste":        titre,
            "description":        description,
            "competences_brut":   competences_brut,
            "entreprise":         entreprise,
            "ville":              ville_dirty,
            "type_contrat":       contrat_dirty,
            "experience_requise": exp_display,
            "salaire_brut":       salaire,
            "niveau_etudes":      niveau,
            "secteur":            random.choice(SECTEURS),
            "date_publication":   date_pub_str,
            "date_expiration":    date_exp_str,
            "nb_postes":          nb_postes,
            "teletravail":        random.choice(TELETRAVAIL),
            "langue_requise":     random.choice(LANGUES),
        }

        offres.append(offre)
        counter[ville_std] += 1

    print("[GEN] City distribution:")
    for v, c in sorted(counter.items(), key=lambda x: -x[1]):
        print(f"       {v:<20} {c:>5} offers")

    return offres

# ==============================================================================
# SECTION 7 — REFERENTIEL COMPETENCES
# ==============================================================================

def generer_referentiel():
    """Generate the 300-skill reference dictionary."""
    referentiel = {
        "familles": {
            "langages": {
                "python":      ["python", "python3", "py", "python 3"],
                "javascript":  ["javascript", "js", "node.js", "nodejs", "node", "es6"],
                "java":        ["java", "java8", "java11", "java17", "java ee"],
                "sql":         ["sql", "mysql", "postgresql", "postgres", "oracle", "tsql", "pl/sql"],
                "r":           ["r", "rlang", "r-studio", "rstudio"],
                "typescript":  ["typescript", "ts"],
                "php":         ["php", "php7", "php8", "laravel", "symfony"],
                "csharp":      ["c#", "csharp", ".net", "dotnet", "asp.net"],
                "cpp":         ["c++", "cpp", "c plus plus"],
                "golang":      ["golang", "go", "go lang"],
                "scala":       ["scala"],
                "kotlin":      ["kotlin"],
                "swift":       ["swift", "ios swift"],
                "dart":        ["dart", "flutter dart"],
                "bash":        ["bash", "shell", "bash scripting", "shell script"],
                "rust":        ["rust", "rust lang"],
            },
            "frameworks_web": {
                "react":       ["react", "reactjs", "react.js", "react native"],
                "angular":     ["angular", "angularjs", "angular2", "angular 14"],
                "vue":         ["vue", "vuejs", "vue.js", "nuxt"],
                "django":      ["django", "django rest", "drf", "django rest framework"],
                "spring":      ["spring", "spring boot", "springboot", "spring mvc"],
                "fastapi":     ["fastapi", "fast api"],
                "flask":       ["flask"],
                "laravel":     ["laravel"],
                "symfony":     ["symfony"],
                "nextjs":      ["next.js", "nextjs", "next"],
                "express":     ["express", "expressjs", "express.js"],
                "nestjs":      ["nestjs", "nest.js"],
            },
            "data_engineering": {
                "spark":       ["spark", "apache spark", "pyspark", "spark streaming"],
                "kafka":       ["kafka", "apache kafka", "kafka streams"],
                "airflow":     ["airflow", "apache airflow"],
                "dbt":         ["dbt", "data build tool"],
                "hadoop":      ["hadoop", "hdfs", "mapreduce", "hive", "hbase"],
                "flink":       ["flink", "apache flink"],
                "nifi":        ["nifi", "apache nifi"],
                "databricks":  ["databricks"],
                "duckdb":      ["duckdb", "duck db"],
                "parquet":     ["parquet", "apache parquet"],
                "delta_lake":  ["delta lake", "delta", "delta tables"],
                "superset":    ["superset", "apache superset"],
                "pandas":      ["pandas", "pd"],
                "numpy":       ["numpy", "np"],
                "etl":         ["etl", "etl pipeline", "extract transform load"],
            },
            "cloud": {
                "aws":         ["aws", "amazon web services", "ec2", "s3", "lambda",
                                "rds", "emr", "redshift", "glue", "sagemaker"],
                "gcp":         ["gcp", "google cloud", "bigquery", "cloud storage",
                                "dataflow", "vertex ai"],
                "azure":       ["azure", "microsoft azure", "synapse", "azure devops",
                                "azure functions", "azure ml"],
            },
            "bi_analytics": {
                "power_bi":    ["power bi", "powerbi", "pbi", "dax", "power query"],
                "tableau":     ["tableau", "tableau desktop", "tableau server"],
                "metabase":    ["metabase"],
                "looker":      ["looker", "looker studio", "google looker"],
                "qlik":        ["qlik", "qlikview", "qlik sense"],
                "microstrategy": ["microstrategy", "mstr"],
                "sisense":     ["sisense"],
                "grafana":     ["grafana"],
                "kibana":      ["kibana", "elk", "elasticsearch"],
            },
            "devops_infra": {
                "docker":      ["docker", "docker-compose", "dockerfile"],
                "kubernetes":  ["kubernetes", "k8s", "helm", "kubectl"],
                "terraform":   ["terraform", "tf", "iac"],
                "ansible":     ["ansible"],
                "jenkins":     ["jenkins", "jenkins pipeline"],
                "gitlab_ci":   ["gitlab ci", "gitlab-ci", "gitlab ci/cd"],
                "github_actions": ["github actions", "github workflows"],
                "linux":       ["linux", "ubuntu", "centos", "rhel", "debian"],
                "nginx":       ["nginx"],
                "prometheus":  ["prometheus"],
            },
            "databases": {
                "postgresql":  ["postgresql", "postgres", "pg"],
                "mysql":       ["mysql"],
                "mongodb":     ["mongodb", "mongo"],
                "redis":       ["redis"],
                "elasticsearch": ["elasticsearch", "elastic", "opensearch"],
                "cassandra":   ["cassandra", "apache cassandra"],
                "oracle_db":   ["oracle", "oracle database"],
                "sqlite":      ["sqlite"],
                "neo4j":       ["neo4j", "graph database"],
                "snowflake":   ["snowflake"],
            },
            "ml_ai": {
                "scikit_learn": ["scikit-learn", "sklearn", "scikit learn"],
                "tensorflow":  ["tensorflow", "tf", "keras"],
                "pytorch":     ["pytorch", "torch"],
                "huggingface": ["hugging face", "huggingface", "transformers"],
                "mlflow":      ["mlflow", "ml flow"],
                "opencv":      ["opencv", "cv2"],
                "nlp":         ["nlp", "natural language processing", "spacy", "nltk"],
                "computer_vision": ["computer vision", "image recognition"],
                "deep_learning": ["deep learning", "cnn", "rnn", "lstm"],
                "machine_learning": ["machine learning", "ml", "random forest",
                                     "gradient boosting", "xgboost"],
            },
            "methodologies": {
                "agile":       ["agile", "scrum", "kanban", "sprint", "backlog"],
                "devops_culture": ["devops", "ci/cd", "continuous integration",
                                   "continuous deployment"],
                "git":         ["git", "github", "gitlab", "bitbucket", "version control"],
                "rest_api":    ["rest api", "restful", "rest", "api rest"],
                "graphql":     ["graphql", "graph ql"],
                "microservices": ["microservices", "micro-services", "soa"],
                "tdd":         ["tdd", "test driven", "unit testing", "pytest", "jest"],
            },
            "outils_design": {
                "figma":       ["figma"],
                "jira":        ["jira", "jira software"],
                "confluence":  ["confluence"],
                "excel":       ["excel", "microsoft excel", "vba"],
                "ms_project":  ["ms project", "microsoft project", "project management"],
            },
        }
    }

    # Count total skills
    total = sum(
        len(aliases)
        for famille in referentiel["familles"].values()
        for aliases in famille.values()
    )
    print(f"[GEN] Referentiel: {total} skill aliases across "
          f"{sum(len(f) for f in referentiel['familles'].values())} normalized skills")

    return referentiel

# ==============================================================================
# SECTION 8 — WRITE FILES
# ==============================================================================

def ecrire_offres_json(offres, output_dir):
    path = os.path.join(output_dir, "offres_emploi_it_maroc.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"offres": offres}, f, ensure_ascii=False, indent=2)
    size_kb = os.path.getsize(path) // 1024
    print(f"[GEN] offres_emploi_it_maroc.json written ({len(offres)} offers, {size_kb} KB)")

def ecrire_referentiel_json(referentiel, output_dir):
    path = os.path.join(output_dir, "referentiel_competences_it.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(referentiel, f, ensure_ascii=False, indent=2)
    size_kb = os.path.getsize(path) // 1024
    print(f"[GEN] referentiel_competences_it.json written ({size_kb} KB)")

def ecrire_entreprises_csv(output_dir):
    path = os.path.join(output_dir, "entreprises_it_maroc.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["nom_entreprise", "secteur", "taille",
                         "ville_siege", "site_web", "type"])
        for e in ENTREPRISES:
            writer.writerow(e)
    print(f"[GEN] entreprises_it_maroc.csv written ({len(ENTREPRISES)} companies)")

# ==============================================================================
# SECTION 9 — ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("  Mexora RH Intelligence — Data Generator")
    print("=" * 60)

    # Generate
    offres      = generer_offres(n=5000)
    referentiel = generer_referentiel()

    # Write
    ecrire_offres_json(offres, OUTPUT_DIR)
    ecrire_referentiel_json(referentiel, OUTPUT_DIR)
    ecrire_entreprises_csv(OUTPUT_DIR)

    print("=" * 60)
    print("  All 3 files generated successfully.")
    print(f"  Output: {os.path.abspath(OUTPUT_DIR)}")
    print("=" * 60)