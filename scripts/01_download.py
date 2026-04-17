"""
MédiAccès — Script 01 : Téléchargement des données sources
============================================================
Télécharge les fichiers de données ouvertes nécessaires au projet :
- RPPS (Annuaire Santé) : liste des professionnels de santé
- APL (DREES) : indicateurs d'accessibilité par commune
- INSEE : population par commune
- BPE (INSEE) : équipements de santé

Usage :
    python scripts/01_download.py
"""

import os
import requests
import zipfile
import logging
from pathlib import Path
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"

# Création des répertoires
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ============================================================
# SOURCES DE DONNÉES
# ============================================================

SOURCES = {
    "rpps_medecins": {
        "description": "Annuaire Santé — Médecins (RPPS)",
        "url": "https://annuaire.sante.fr/web/site-pro/extractions-publiques?p_p_id=ExtractionsPubliquesPortlet_WAR_AnnuaireSante_INSTANCE_extractions_publiques&p_p_lifecycle=2&_ExtractionsPubliquesPortlet_WAR_AnnuaireSante_INSTANCE_extractions_publiques_typeProfession=M%C3%A9decin",
        "filename": "rpps_medecins.zip",
        "notes": (
            "⚠️  Le téléchargement direct via script peut ne pas fonctionner "
            "car l'Annuaire Santé utilise un portail avec redirections.\n"
            "ALTERNATIVE : Téléchargez manuellement depuis :\n"
            "  → https://annuaire.sante.fr/web/site-pro/extractions-publiques\n"
            "  → Sélectionnez 'Médecin' puis cliquez sur 'Télécharger'\n"
            "  → Placez le fichier dans data/raw/\n\n"
            "OU utilisez les données data.gouv.fr :\n"
            "  → https://www.data.gouv.fr/datasets/annuaire-sante-extractions-des-donnees-en-libre-acces-des-professionnels-intervenant-dans-le-systeme-de-sante-rpps"
        )
    },
    "rpps_datagouv": {
        "description": "RPPS via data.gouv.fr (alternative plus fiable)",
        "url": "https://www.data.gouv.fr/fr/datasets/r/98bf76e1-670f-4014-a230-6e37e0e2499d",
        "filename": "rpps_datagouv.csv",
        "notes": "Fichier CSV de l'ensemble des professionnels de santé"
    },
    "apl_drees": {
        "description": "APL — Accessibilité Potentielle Localisée (DREES)",
        "url": "https://data.drees.solidarites-sante.gouv.fr/api/explore/v2.1/catalog/datasets/530_l-accessibilite-potentielle-localisee-apl/exports/csv?lang=fr&timezone=Europe%2FParis&use_labels=true&delimiter=%3B",
        "filename": "apl_drees.csv",
        "notes": "Score APL par commune pour médecins généralistes, infirmiers, sages-femmes, kiné, dentistes"
    },
    "population_insee": {
        "description": "Population par commune (INSEE)",
        "url": "https://www.insee.fr/fr/statistiques/fichier/7739582/ensemble.zip",
        "filename": "population_insee.zip",
        "notes": "Population légale par commune — dernier millésime disponible"
    },
    "bpe_insee": {
        "description": "Base Permanente des Équipements — Santé (INSEE)",
        "url": "https://www.insee.fr/fr/statistiques/fichier/3568638/bpe-ens-xy-24.zip",
        "filename": "bpe_sante.zip",
        "notes": "Équipements : pharmacies, labos, urgences, hôpitaux, etc."
    },
    "communes_geo": {
        "description": "Référentiel des communes avec coordonnées GPS",
        "url": "https://geo.api.gouv.fr/communes?fields=nom,code,codeDepartement,codeRegion,population,centre&format=json&geometry=centre",
        "filename": "communes_geo.json",
        "notes": "API Géo : nom, code INSEE, département, région, population, coordonnées"
    }
}


# ============================================================
# FONCTIONS
# ============================================================

def download_file(url: str, filepath: Path, description: str) -> bool:
    """Télécharge un fichier depuis une URL."""
    logger.info(f"Téléchargement : {description}")
    logger.info(f"  URL : {url[:100]}...")
    logger.info(f"  Destination : {filepath}")

    try:
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)

        size_mb = filepath.stat().st_size / (1024 * 1024)
        logger.info(f"  ✅ Téléchargé ({size_mb:.1f} Mo)")
        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"  ❌ Erreur : {e}")
        return False


def extract_zip(filepath: Path, extract_dir: Path) -> bool:
    """Extrait un fichier ZIP."""
    try:
        with zipfile.ZipFile(filepath, "r") as z:
            z.extractall(extract_dir)
            logger.info(f"  📦 Extrait dans {extract_dir}")
            for name in z.namelist()[:5]:
                logger.info(f"      → {name}")
            if len(z.namelist()) > 5:
                logger.info(f"      → ... et {len(z.namelist()) - 5} autres fichiers")
        return True
    except zipfile.BadZipFile:
        logger.error(f"  ❌ Fichier ZIP invalide : {filepath}")
        return False


def download_all():
    """Télécharge toutes les sources de données."""
    logger.info("=" * 60)
    logger.info("MédiAccès — Téléchargement des données sources")
    logger.info(f"Date : {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    logger.info(f"Dossier : {RAW_DIR}")
    logger.info("=" * 60)

    results = {}

    for key, source in SOURCES.items():
        logger.info("")
        logger.info(f"--- {source['description']} ---")

        # Afficher les notes éventuelles
        if source.get("notes"):
            for line in source["notes"].split("\n"):
                logger.info(f"  ℹ️  {line}")

        filepath = RAW_DIR / source["filename"]

        # Vérifier si le fichier existe déjà
        if filepath.exists():
            size_mb = filepath.stat().st_size / (1024 * 1024)
            logger.info(f"  ⏭️  Fichier existant ({size_mb:.1f} Mo) — skip")
            logger.info(f"  💡 Supprimez-le pour re-télécharger")
            results[key] = "existant"
            continue

        # Télécharger
        success = download_file(source["url"], filepath, source["description"])

        if success and filepath.suffix == ".zip":
            extract_dir = RAW_DIR / key
            extract_dir.mkdir(exist_ok=True)
            extract_zip(filepath, extract_dir)

        results[key] = "ok" if success else "erreur"

    # Résumé
    logger.info("")
    logger.info("=" * 60)
    logger.info("RÉSUMÉ")
    logger.info("=" * 60)
    for key, status in results.items():
        icon = {"ok": "✅", "existant": "⏭️", "erreur": "❌"}[status]
        logger.info(f"  {icon} {SOURCES[key]['description']} : {status}")

    logger.info("")
    logger.info("Prochaine étape : python scripts/02_clean.py")


# ============================================================
# POINT D'ENTRÉE
# ============================================================

if __name__ == "__main__":
    download_all()
