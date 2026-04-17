"""
MédiAccès — Script 02 : Nettoyage et transformation des données
================================================================
Nettoie et transforme les données brutes en fichiers prêts à charger :
- communes.parquet      → référentiel des communes
- praticiens.parquet    → tous les professionnels de santé (RPPS)
- apl.parquet           → scores APL par commune et profession
- equipements.parquet   → équipements de santé (BPE)

Usage :
    python scripts/02_clean.py
"""

import os
import json
import glob
import logging
import pandas as pd
from pathlib import Path

# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ============================================================
# 1. COMMUNES (API Géo + INSEE Population)
# ============================================================

def clean_communes():
    """Fusionne le référentiel géo des communes avec les données de population."""
    logger.info("=" * 50)
    logger.info("1. Nettoyage des COMMUNES")
    logger.info("=" * 50)

    # --- API Géo ---
    geo_path = RAW_DIR / "communes_geo.json"
    with open(geo_path, "r", encoding="utf-8") as f:
        geo_data = json.load(f)

    communes = pd.DataFrame(geo_data)
    # Extraire lat/lon depuis la colonne "centre"
    communes["latitude"] = communes["centre"].apply(
        lambda x: x["coordinates"][1] if isinstance(x, dict) else None
    )
    communes["longitude"] = communes["centre"].apply(
        lambda x: x["coordinates"][0] if isinstance(x, dict) else None
    )
    communes = communes.drop(columns=["centre"])
    communes = communes.rename(columns={
        "code": "code_insee",
        "nom": "nom_commune",
        "codeDepartement": "code_departement",
        "codeRegion": "code_region",
        "population": "population_api_geo"
    })
    logger.info(f"  API Géo : {len(communes)} communes chargées")

    # --- INSEE Population ---
    pop_path = RAW_DIR / "population_insee" / "donnees_communes.csv"
    pop = pd.read_csv(pop_path, sep=";", dtype=str)
    pop = pop.rename(columns={
        "COM": "code_insee",
        "Commune": "nom_commune_insee",
        "Région": "nom_region",
        "DEP": "code_departement_insee",
        "PMUN": "population_municipale"
    })
    pop["population_municipale"] = pd.to_numeric(pop["population_municipale"], errors="coerce")
    pop = pop[["code_insee", "nom_region", "population_municipale"]].drop_duplicates("code_insee")
    logger.info(f"  INSEE Pop : {len(pop)} communes chargées")

    # --- Fusion ---
    communes = communes.merge(pop, on="code_insee", how="left")
    # Utiliser la population INSEE en priorité, sinon API Géo
    communes["population_totale"] = communes["population_municipale"].fillna(
        communes["population_api_geo"]
    ).astype("Int64")
    communes = communes.drop(columns=["population_api_geo", "population_municipale"])

    # Noms des départements
    dep_path = RAW_DIR / "population_insee" / "donnees_departements.csv"
    if dep_path.exists():
        dep = pd.read_csv(dep_path, sep=";", dtype=str)
        dep = dep.rename(columns={"DEP": "code_departement", "Département": "nom_departement"})
        dep = dep[["code_departement", "nom_departement"]].drop_duplicates()
        communes = communes.merge(dep, on="code_departement", how="left")

    communes.to_parquet(PROCESSED_DIR / "communes.parquet", index=False)
    logger.info(f"  ✅ {len(communes)} communes → communes.parquet")
    logger.info(f"     Colonnes : {communes.columns.tolist()}")
    return communes


# ============================================================
# 2. PRATICIENS (RPPS)
# ============================================================

def clean_praticiens():
    """Nettoie le fichier RPPS des professionnels de santé."""
    logger.info("")
    logger.info("=" * 50)
    logger.info("2. Nettoyage des PRATICIENS (RPPS)")
    logger.info("=" * 50)

    rpps_path = RAW_DIR / "ps-libreacces-personne-activite.txt"

    # Colonnes utiles (le fichier en a ~50, on n'en garde que les essentielles)
    colonnes_utiles = [
        "Identifiant PP",
        "Nom d'exercice",
        "Prénom d'exercice",
        "Code profession",
        "Libellé profession",
        "Code catégorie professionnelle",
        "Libellé catégorie professionnelle",
        "Code type savoir-faire",
        "Libellé type savoir-faire",
        "Code savoir-faire",
        "Libellé savoir-faire",
        "Code mode exercice",
        "Libellé mode exercice",
        "Code postal (coord. structure)",
        "Code commune (coord. structure)",
        "Libellé commune (coord. structure)",
        "Code Département (structure)",
        "Libellé Département (structure)",
        "Code secteur d'activité",
        "Libellé secteur d'activité",
        "Code genre activité",
        "Libellé genre activité",
    ]

    logger.info(f"  Lecture du fichier RPPS ({rpps_path.stat().st_size / 1e6:.0f} Mo)...")
    logger.info("  ⏳ Cela peut prendre 1-2 minutes...")

    df = pd.read_csv(
        rpps_path,
        sep="|",
        usecols=colonnes_utiles,
        dtype=str,
        encoding="utf-8",
        low_memory=False
    )
    logger.info(f"  Fichier lu : {len(df)} lignes, {len(df.columns)} colonnes")

    # Renommage
    df = df.rename(columns={
        "Identifiant PP": "id_praticien",
        "Nom d'exercice": "nom_exercice",
        "Prénom d'exercice": "prenom_exercice",
        "Code profession": "code_profession",
        "Libellé profession": "libelle_profession",
        "Code catégorie professionnelle": "code_categorie",
        "Libellé catégorie professionnelle": "libelle_categorie",
        "Code type savoir-faire": "code_type_savoir_faire",
        "Libellé type savoir-faire": "libelle_type_savoir_faire",
        "Code savoir-faire": "code_savoir_faire",
        "Libellé savoir-faire": "libelle_savoir_faire",
        "Code mode exercice": "code_mode_exercice",
        "Libellé mode exercice": "libelle_mode_exercice",
        "Code postal (coord. structure)": "code_postal",
        "Code commune (coord. structure)": "code_commune",
        "Libellé commune (coord. structure)": "libelle_commune",
        "Code Département (structure)": "code_departement",
        "Libellé Département (structure)": "nom_departement",
        "Code secteur d'activité": "code_secteur",
        "Libellé secteur d'activité": "libelle_secteur",
        "Code genre activité": "code_genre_activite",
        "Libellé genre activité": "libelle_genre_activite",
    })

    # Filtrer : garder uniquement les professionnels en activité standard
    # et qui ont une commune d'exercice
    df = df[df["code_commune"].notna() & (df["code_commune"] != "")]
    logger.info(f"  Après filtre commune non vide : {len(df)} lignes")

    # Filtrer les activités standard (exclure les remplaçants, etc.)
    if "libelle_genre_activite" in df.columns:
        logger.info(f"  Genres d'activité présents :")
        for genre, count in df["libelle_genre_activite"].value_counts().head(5).items():
            logger.info(f"    - {genre}: {count:,}")

    # Mapper les modes d'exercice
    mode_map = {
        "L": "libéral",
        "S": "salarié",
        "B": "bénévole",
    }
    df["mode_exercice"] = df["code_mode_exercice"].map(mode_map).fillna(df["libelle_mode_exercice"])

    # Extraire la spécialité depuis le savoir-faire
    # Le type "S" = spécialité ordinale, "CEX" = compétence exclusive
    df["specialite"] = df["libelle_savoir_faire"].where(
        df["code_type_savoir_faire"].isin(["S", "CEX", "PAC"]),
        other=None
    )

    # Stats par profession
    logger.info(f"  Professions présentes :")
    for prof, count in df["libelle_profession"].value_counts().head(10).items():
        logger.info(f"    - {prof}: {count:,}")

    # Sauvegarder
    df.to_parquet(PROCESSED_DIR / "praticiens.parquet", index=False)
    logger.info(f"  ✅ {len(df)} praticiens → praticiens.parquet")
    return df


# ============================================================
# 3. APL (DREES - fichiers Excel)
# ============================================================

def clean_apl():
    """Nettoie les fichiers Excel APL de la DREES."""
    logger.info("")
    logger.info("=" * 50)
    logger.info("3. Nettoyage des APL (DREES)")
    logger.info("=" * 50)

    # Correspondance fichier → profession
    profession_map = {
        "médecins généralistes": "medecin_generaliste",
        "chirurgiens-dentistes": "chirurgien_dentiste",
        "infirmières": "infirmiere",
        "kinésithérapeutes": "kinesitherapeute",
        "sages-femmes": "sage_femme",
    }

    all_apl = []

    # Trouver les fichiers APL Excel
    xlsx_files = list(RAW_DIR.glob("Indicateur*.xlsx"))
    logger.info(f"  {len(xlsx_files)} fichiers APL trouvés")

    for filepath in xlsx_files:
        import unicodedata
        filename = unicodedata.normalize("NFC", filepath.name)
        logger.info(f"  Traitement : {filename[:60]}...")

        # Identifier la profession
        profession_code = None
        for keyword, code in profession_map.items():
            if keyword in filename.lower():
                profession_code = code
                break

        if profession_code is None:
            logger.warning(f"    ⚠️ Profession non identifiée, skip")
            continue

        # Lire le fichier Excel
        xl = pd.ExcelFile(filepath)

        # Prendre l'onglet le plus récent (APL 2023 si dispo, sinon APL 2022)
        target_sheets = [s for s in xl.sheet_names if "APL" in s]
        if not target_sheets:
            logger.warning(f"    ⚠️ Aucun onglet APL trouvé")
            continue

        # Trier pour prendre le plus récent
        target_sheets.sort(reverse=True)

        for sheet_name in target_sheets:
            logger.info(f"    Onglet : {sheet_name}")

            # Lire avec skiprows pour passer les en-têtes descriptifs
            # On teste plusieurs valeurs de skiprows
            for skip in [4, 3, 5, 6]:
                df = pd.read_excel(filepath, sheet_name=sheet_name, skiprows=skip)
                # Vérifier si on a des colonnes qui ressemblent à des codes INSEE
                first_col_values = df.iloc[:, 0].dropna().astype(str)
                if first_col_values.str.match(r"^\d{5}$").any():
                    break

            # Renommer les colonnes (elles sont souvent : code_commune, libelle, APL, pop_std, ...)
            cols = df.columns.tolist()
            logger.info(f"    Colonnes brutes : {cols[:6]}")

            # Identifier les colonnes par position
            # Typiquement : col0=code_commune, col1=libelle, col2=APL, col3=pop_standardisee
            if len(cols) >= 3:
                rename_map = {
                    cols[0]: "code_commune",
                    cols[1]: "libelle_commune",
                }
                # La colonne APL est souvent la 3e ou celle qui contient "APL"
                apl_col = None
                pop_col = None
                for i, col in enumerate(cols):
                    col_str = str(col).lower()
                    if "apl" in col_str or "accessib" in col_str:
                        apl_col = col
                    elif "pop" in col_str or "standard" in col_str:
                        pop_col = col

                # Si pas trouvé par nom, prendre par position
                if apl_col is None and len(cols) >= 3:
                    apl_col = cols[2]
                if pop_col is None and len(cols) >= 4:
                    pop_col = cols[3]

                rename_map[apl_col] = "score_apl"
                if pop_col:
                    rename_map[pop_col] = "population_standardisee"

                df = df.rename(columns=rename_map)

            # Nettoyer
            df["code_commune"] = df["code_commune"].astype(str).str.strip().str.zfill(5)
            df = df[df["code_commune"].str.match(r"^\d{5}$", na=False)]
            df["score_apl"] = pd.to_numeric(df["score_apl"], errors="coerce")
            df["profession"] = profession_code

            # Extraire l'année depuis le nom de l'onglet
            annee = "".join(filter(str.isdigit, sheet_name))
            df["annee"] = int(annee) if annee else 2023

            if "population_standardisee" in df.columns:
                df["population_standardisee"] = pd.to_numeric(
                    df["population_standardisee"], errors="coerce"
                )

            # Garder les colonnes utiles
            keep_cols = ["code_commune", "profession", "annee", "score_apl"]
            if "population_standardisee" in df.columns:
                keep_cols.append("population_standardisee")

            df = df[keep_cols].dropna(subset=["score_apl"])
            all_apl.append(df)
            logger.info(f"    → {len(df)} communes avec score APL")

    if all_apl:
        apl = pd.concat(all_apl, ignore_index=True)
        apl.to_parquet(PROCESSED_DIR / "apl.parquet", index=False)
        logger.info(f"  ✅ {len(apl)} lignes APL → apl.parquet")
        logger.info(f"     Professions : {apl['profession'].unique().tolist()}")
        logger.info(f"     Années : {sorted(apl['annee'].unique())}")
        return apl
    else:
        logger.error("  ❌ Aucune donnée APL n'a pu être extraite")
        return None


# ============================================================
# 4. ÉQUIPEMENTS DE SANTÉ (BPE)
# ============================================================

def clean_equipements():
    """Extrait les équipements de santé du fichier BPE."""
    logger.info("")
    logger.info("=" * 50)
    logger.info("4. Nettoyage des ÉQUIPEMENTS (BPE)")
    logger.info("=" * 50)

    bpe_path = RAW_DIR / "BPE24.csv"

    # Codes des équipements de santé qui nous intéressent
    equipements_sante = {
        # Établissements
        "D101": "hopital_court_sejour",
        "D102": "hopital_moyen_sejour",
        "D103": "hopital_long_sejour",
        "D104": "hopital_psychiatrique",
        "D106": "urgences",
        "D107": "maternite",
        "D108": "centre_sante",
        "D113": "maison_sante_pluripro",
        # Professionnels de santé (comptés aussi dans RPPS, utile pour croisement)
        "D251": "medecin_generaliste",
        "D252": "medecin_specialiste_chir",
        "D253": "medecin_specialiste_dermato",
        "D254": "medecin_specialiste_gastro",
        "D255": "medecin_specialiste_psy",
        "D256": "medecin_specialiste_ophtalmo",
        "D259": "medecin_specialiste_orl",
        "D261": "medecin_specialiste_pediatre",
        "D262": "medecin_specialiste_pneumo",
        "D265": "medecin_specialiste_gyneco",
        "D266": "medecin_specialiste_radio",
        # Services
        "D268": "cardiologue",
        "D269": "medecin_autre",
        "D270": "medecin_autre_2",
        "D274": "orthophoniste",
        "D277": "chirurgien_dentiste",
        "D278": "sage_femme",
        "D279": "kinesitherapeute",
        "D280": "pedicure_podologue",
        "D281": "infirmier",
        "D302": "laboratoire_analyses",
        "D303": "ambulance",
        "D304": "ets_sang",
        "D307": "pharmacie",
    }

    logger.info(f"  Lecture du BPE ({bpe_path.stat().st_size / 1e6:.0f} Mo)...")
    logger.info("  ⏳ Fichier volumineux, patience...")

    # Lire seulement les colonnes utiles et filtrer sur le domaine santé
    df = pd.read_csv(
        bpe_path,
        sep=";",
        usecols=["DEPCOM", "DEP", "TYPEQU", "DOM", "LIBCOM", "LONGITUDE", "LATITUDE"],
        dtype=str,
        low_memory=False
    )

    # Filtrer domaine santé
    df = df[df["DOM"] == "D"]
    logger.info(f"  Équipements santé bruts : {len(df)} lignes")

    # Filtrer sur les types qui nous intéressent
    df = df[df["TYPEQU"].isin(equipements_sante.keys())]
    df["type_equipement"] = df["TYPEQU"].map(equipements_sante)

    # Renommer
    df = df.rename(columns={
        "DEPCOM": "code_commune",
        "DEP": "code_departement",
        "LIBCOM": "libelle_commune",
        "LONGITUDE": "longitude",
        "LATITUDE": "latitude",
    })

    # Agréger par commune et type d'équipement
    equip_commune = (
        df.groupby(["code_commune", "type_equipement"])
        .size()
        .reset_index(name="nombre")
    )

    equip_commune.to_parquet(PROCESSED_DIR / "equipements.parquet", index=False)
    logger.info(f"  ✅ {len(equip_commune)} lignes → equipements.parquet")
    logger.info(f"     Types : {equip_commune['type_equipement'].nunique()} types d'équipements")
    logger.info(f"     Top 5 :")
    top5 = equip_commune.groupby("type_equipement")["nombre"].sum().sort_values(ascending=False).head(5)
    for typ, count in top5.items():
        logger.info(f"       - {typ}: {count:,}")

    return equip_commune


# ============================================================
# 5. RÉSUMÉ
# ============================================================

def print_summary():
    """Affiche un résumé des fichiers produits."""
    logger.info("")
    logger.info("=" * 50)
    logger.info("RÉSUMÉ — Fichiers produits dans data/processed/")
    logger.info("=" * 50)
    for f in sorted(PROCESSED_DIR.glob("*.parquet")):
        size_mb = f.stat().st_size / (1024 * 1024)
        df = pd.read_parquet(f)
        logger.info(f"  📄 {f.name:30s} {size_mb:6.1f} Mo  |  {len(df):>8,} lignes  |  {len(df.columns)} cols")
    logger.info("")
    logger.info("Prochaine étape : python scripts/03_load.py")


# ============================================================
# POINT D'ENTRÉE
# ============================================================

if __name__ == "__main__":
    logger.info("🏥 MédiAccès — Nettoyage des données")
    logger.info("")

    clean_communes()
    clean_praticiens()
    clean_apl()
    clean_equipements()
    print_summary()
