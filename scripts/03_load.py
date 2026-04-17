"""
MédiAccès — Script 03 : Chargement en base DuckDB
===================================================
Charge les fichiers parquet nettoyés dans une base DuckDB
et crée les vues analytiques pour l'analyse.

Usage :
    python scripts/03_load.py
"""

import logging
import duckdb
from pathlib import Path

# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
DB_PATH = PROCESSED_DIR / "mediacces.duckdb"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ============================================================
# CHARGEMENT DES DONNÉES
# ============================================================

def load_data(con):
    """Charge les fichiers parquet en tables DuckDB."""
    logger.info("=" * 50)
    logger.info("1. Chargement des données en base")
    logger.info("=" * 50)

    tables = {
        "communes": "communes.parquet",
        "praticiens": "praticiens.parquet",
        "apl": "apl.parquet",
        "equipements": "equipements.parquet",
    }

    for table_name, filename in tables.items():
        filepath = PROCESSED_DIR / filename
        if not filepath.exists():
            logger.warning(f"  ⚠️ {filename} introuvable, skip")
            continue

        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_parquet('{filepath}')
        """)
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"  ✅ {table_name:20s} → {count:>10,} lignes")


# ============================================================
# VUES ANALYTIQUES
# ============================================================

def create_views(con):
    """Crée les vues analytiques pour l'exploitation des données."""
    logger.info("")
    logger.info("=" * 50)
    logger.info("2. Création des vues analytiques")
    logger.info("=" * 50)

    # -------------------------------------------------------
    # Vue 1 : Densité de médecins par commune et spécialité
    # -------------------------------------------------------
    con.execute("""
        CREATE OR REPLACE VIEW vue_densite_medecins AS
        WITH medecins AS (
            SELECT
                code_commune,
                libelle_profession,
                specialite,
                -- Regrouper les spécialités en grandes catégories
                CASE
                    WHEN libelle_profession != 'Médecin' THEN libelle_profession
                    WHEN specialite IS NULL THEN 'Médecin généraliste'
                    WHEN specialite ILIKE '%dermato%' THEN 'Dermatologue'
                    WHEN specialite ILIKE '%ophtalmol%' THEN 'Ophtalmologue'
                    WHEN specialite ILIKE '%gynéco%' OR specialite ILIKE '%gyneco%' THEN 'Gynécologue'
                    WHEN specialite ILIKE '%pédiatr%' OR specialite ILIKE '%pediatr%' THEN 'Pédiatre'
                    WHEN specialite ILIKE '%psychiatr%' THEN 'Psychiatre'
                    WHEN specialite ILIKE '%cardiolog%' THEN 'Cardiologue'
                    WHEN specialite ILIKE '%ORL%' OR specialite ILIKE '%oto-rhino%' THEN 'ORL'
                    WHEN specialite ILIKE '%radiolog%' THEN 'Radiologue'
                    WHEN specialite ILIKE '%anesthési%' OR specialite ILIKE '%anesthesi%' THEN 'Anesthésiste'
                    WHEN specialite ILIKE '%rhumatolog%' THEN 'Rhumatologue'
                    WHEN specialite ILIKE '%neurolog%' THEN 'Neurologue'
                    WHEN specialite ILIKE '%gastro%' THEN 'Gastro-entérologue'
                    WHEN specialite ILIKE '%pneumolog%' THEN 'Pneumologue'
                    WHEN specialite ILIKE '%urolog%' THEN 'Urologue'
                    WHEN specialite ILIKE '%endocrinolog%' THEN 'Endocrinologue'
                    ELSE 'Autre spécialité médicale'
                END AS categorie_specialite,
                mode_exercice,
                id_praticien
            FROM praticiens
            WHERE libelle_profession IN (
                'Médecin', 'Chirurgien-Dentiste', 'Sage-Femme',
                'Infirmier', 'Masseur-Kinésithérapeute',
                'Pharmacien', 'Psychologue'
            )
        )
        SELECT
            m.code_commune,
            c.nom_commune,
            c.code_departement,
            c.nom_departement,
            c.nom_region,
            c.population_totale,
            m.categorie_specialite,
            COUNT(DISTINCT m.id_praticien) AS nb_praticiens,
            ROUND(COUNT(DISTINCT m.id_praticien) * 10000.0
                  / NULLIF(c.population_totale, 0), 2) AS densite_pour_10000_hab
        FROM medecins m
        JOIN communes c ON m.code_commune = c.code_insee
        WHERE c.population_totale > 0
        GROUP BY m.code_commune, c.nom_commune, c.code_departement,
                 c.nom_departement, c.nom_region, c.population_totale,
                 m.categorie_specialite
    """)
    logger.info("  ✅ vue_densite_medecins")

    # -------------------------------------------------------
    # Vue 2 : Score composite par commune (APL + densité propre)
    # -------------------------------------------------------
    con.execute("""
        CREATE OR REPLACE VIEW vue_score_commune AS
        WITH
        -- APL officiel (DREES) — dernière année disponible
        apl_recent AS (
            SELECT
                code_commune,
                profession,
                score_apl,
                annee
            FROM apl
            WHERE annee = (SELECT MAX(annee) FROM apl)
        ),
        -- Moyennes nationales APL
        moyennes_apl AS (
            SELECT
                profession,
                ROUND(AVG(score_apl), 2) AS apl_moyen_national,
                ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY score_apl), 2) AS apl_q1,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score_apl), 2) AS apl_median,
                ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY score_apl), 2) AS apl_q3
            FROM apl_recent
            GROUP BY profession
        )
        SELECT
            c.code_insee,
            c.nom_commune,
            c.code_departement,
            c.nom_departement,
            c.nom_region,
            c.population_totale,
            a.profession,
            a.score_apl,
            m.apl_moyen_national,
            m.apl_median,
            ROUND(a.score_apl / NULLIF(m.apl_moyen_national, 0) * 100, 1) AS pct_vs_moyenne,
            -- Classification
            CASE
                WHEN a.score_apl <= m.apl_q1 * 0.5 THEN '🔴 Désert médical'
                WHEN a.score_apl <= m.apl_q1 THEN '🟠 Zone sous-dotée'
                WHEN a.score_apl <= m.apl_q3 THEN '🟡 Zone correcte'
                ELSE '🟢 Zone bien dotée'
            END AS classification,
            -- Rang départemental
            RANK() OVER (
                PARTITION BY c.code_departement, a.profession
                ORDER BY a.score_apl ASC
            ) AS rang_dept_asc,
            -- Nombre total de communes dans le département
            COUNT(*) OVER (
                PARTITION BY c.code_departement, a.profession
            ) AS nb_communes_dept
        FROM communes c
        LEFT JOIN apl_recent a ON c.code_insee = a.code_commune
        LEFT JOIN moyennes_apl m ON a.profession = m.profession
        WHERE a.score_apl IS NOT NULL
    """)
    logger.info("  ✅ vue_score_commune")

    # -------------------------------------------------------
    # Vue 3 : Densité spécialistes par commune
    # (notre indicateur maison pour dermato, ophtalmo, etc.)
    # -------------------------------------------------------
    con.execute("""
        CREATE OR REPLACE VIEW vue_densite_specialistes AS
        WITH specialistes_commune AS (
            SELECT
                code_commune,
                CASE
                    WHEN specialite ILIKE '%dermato%' THEN 'Dermatologue'
                    WHEN specialite ILIKE '%ophtalmol%' THEN 'Ophtalmologue'
                    WHEN specialite ILIKE '%gynéco%' OR specialite ILIKE '%gyneco%' THEN 'Gynécologue'
                    WHEN specialite ILIKE '%pédiatr%' OR specialite ILIKE '%pediatr%' THEN 'Pédiatre'
                    WHEN specialite ILIKE '%psychiatr%' THEN 'Psychiatre'
                    WHEN specialite ILIKE '%cardiolog%' THEN 'Cardiologue'
                    WHEN specialite ILIKE '%ORL%' OR specialite ILIKE '%oto-rhino%' THEN 'ORL'
                    ELSE NULL
                END AS specialite_groupe,
                id_praticien
            FROM praticiens
            WHERE libelle_profession = 'Médecin'
              AND specialite IS NOT NULL
        ),
        comptage AS (
            SELECT
                code_commune,
                specialite_groupe,
                COUNT(DISTINCT id_praticien) AS nb_specialistes
            FROM specialistes_commune
            WHERE specialite_groupe IS NOT NULL
            GROUP BY code_commune, specialite_groupe
        ),
        moyennes AS (
            SELECT
                specialite_groupe,
                ROUND(AVG(nb_specialistes * 10000.0 / NULLIF(c.population_totale, 0)), 2)
                    AS densite_moyenne_nationale
            FROM comptage co
            JOIN communes c ON co.code_commune = c.code_insee
            WHERE c.population_totale > 0
            GROUP BY specialite_groupe
        )
        SELECT
            c.code_insee,
            c.nom_commune,
            c.code_departement,
            c.nom_departement,
            c.population_totale,
            co.specialite_groupe,
            COALESCE(co.nb_specialistes, 0) AS nb_specialistes,
            ROUND(COALESCE(co.nb_specialistes, 0) * 10000.0
                  / NULLIF(c.population_totale, 0), 2) AS densite_pour_10000_hab,
            mo.densite_moyenne_nationale,
            -- Score relatif
            ROUND(
                (COALESCE(co.nb_specialistes, 0) * 10000.0 / NULLIF(c.population_totale, 0))
                / NULLIF(mo.densite_moyenne_nationale, 0) * 100,
                1
            ) AS pct_vs_moyenne,
            -- Classification
            CASE
                WHEN co.nb_specialistes IS NULL OR co.nb_specialistes = 0
                    THEN '🔴 Aucun praticien'
                WHEN (co.nb_specialistes * 10000.0 / c.population_totale)
                     < mo.densite_moyenne_nationale * 0.3
                    THEN '🟠 Très sous-doté'
                WHEN (co.nb_specialistes * 10000.0 / c.population_totale)
                     < mo.densite_moyenne_nationale
                    THEN '🟡 Sous-doté'
                ELSE '🟢 Bien doté'
            END AS classification
        FROM communes c
        LEFT JOIN comptage co ON c.code_insee = co.code_commune
        LEFT JOIN moyennes mo ON co.specialite_groupe = mo.specialite_groupe
        WHERE c.population_totale > 500  -- Exclure les très petites communes
    """)
    logger.info("  ✅ vue_densite_specialistes (dermato, ophtalmo, gynéco...)")

    # -------------------------------------------------------
    # Vue 4 : Classement des praticiens les plus accessibles
    # Pour un patient non suivi cherchant un spécialiste
    # -------------------------------------------------------
    con.execute("""
        CREATE OR REPLACE VIEW vue_praticiens_accessibles AS
        WITH charge AS (
            SELECT
                p.id_praticien,
                p.code_commune,
                p.nom_exercice,
                p.prenom_exercice,
                p.libelle_profession,
                p.specialite,
                p.mode_exercice,
                p.libelle_secteur AS secteur,
                CASE
                    WHEN p.specialite ILIKE '%dermato%' THEN 'Dermatologue'
                    WHEN p.specialite ILIKE '%ophtalmol%' THEN 'Ophtalmologue'
                    WHEN p.specialite ILIKE '%gynéco%' OR p.specialite ILIKE '%gyneco%' THEN 'Gynécologue'
                    WHEN p.specialite ILIKE '%pédiatr%' OR p.specialite ILIKE '%pediatr%' THEN 'Pédiatre'
                    WHEN p.specialite ILIKE '%psychiatr%' THEN 'Psychiatre'
                    WHEN p.specialite ILIKE '%cardiolog%' THEN 'Cardiologue'
                    ELSE p.specialite
                END AS specialite_groupe,
                c.nom_commune,
                c.nom_departement,
                c.population_totale,
                c.latitude,
                c.longitude,
                -- Nombre de confrères de même spécialité dans la commune
                COUNT(*) OVER (
                    PARTITION BY p.code_commune, p.specialite
                ) AS nb_confreres_commune,
                -- Patients potentiels par praticien
                ROUND(
                    c.population_totale * 1.0
                    / COUNT(*) OVER (PARTITION BY p.code_commune, p.specialite),
                    0
                ) AS patients_par_praticien
            FROM praticiens p
            JOIN communes c ON p.code_commune = c.code_insee
            WHERE p.libelle_profession = 'Médecin'
              AND p.specialite IS NOT NULL
              AND c.population_totale > 0
        )
        SELECT
            *,
            -- Score d'accessibilité (plus bas = plus accessible)
            ROUND(
                patients_par_praticien
                * CASE
                    WHEN secteur ILIKE '%secteur 1%' OR secteur ILIKE '%cabinet%' THEN 0.8
                    WHEN secteur ILIKE '%secteur 2%' THEN 1.0
                    ELSE 1.2
                  END
                * CASE
                    WHEN mode_exercice = 'libéral' THEN 0.9
                    WHEN mode_exercice = 'salarié' THEN 1.1
                    ELSE 1.0
                  END,
                0
            ) AS score_accessibilite
        FROM charge
    """)
    logger.info("  ✅ vue_praticiens_accessibles")

    # -------------------------------------------------------
    # Vue 5 : Bilan santé complet d'une commune
    # -------------------------------------------------------
    con.execute("""
        CREATE OR REPLACE VIEW vue_bilan_commune AS
        WITH
        equipements_pivot AS (
            SELECT
                code_commune,
                SUM(CASE WHEN type_equipement = 'pharmacie' THEN nombre ELSE 0 END) AS nb_pharmacies,
                SUM(CASE WHEN type_equipement IN ('hopital_court_sejour', 'hopital_moyen_sejour',
                    'hopital_long_sejour') THEN nombre ELSE 0 END) AS nb_hopitaux,
                SUM(CASE WHEN type_equipement = 'urgences' THEN nombre ELSE 0 END) AS nb_urgences,
                SUM(CASE WHEN type_equipement = 'laboratoire_analyses' THEN nombre ELSE 0 END) AS nb_labos,
                SUM(CASE WHEN type_equipement = 'maison_sante_pluripro' THEN nombre ELSE 0 END) AS nb_maisons_sante
            FROM equipements
            GROUP BY code_commune
        ),
        praticiens_pivot AS (
            SELECT
                code_commune,
                COUNT(DISTINCT CASE WHEN libelle_profession = 'Médecin'
                    AND (specialite IS NULL OR specialite ILIKE '%médecine générale%'
                         OR specialite ILIKE '%generaliste%')
                    THEN id_praticien END) AS nb_generalistes,
                COUNT(DISTINCT CASE WHEN specialite ILIKE '%dermato%'
                    THEN id_praticien END) AS nb_dermato,
                COUNT(DISTINCT CASE WHEN specialite ILIKE '%ophtalmol%'
                    THEN id_praticien END) AS nb_ophtalmo,
                COUNT(DISTINCT CASE WHEN specialite ILIKE '%gynéco%' OR specialite ILIKE '%gyneco%'
                    THEN id_praticien END) AS nb_gyneco,
                COUNT(DISTINCT CASE WHEN specialite ILIKE '%pédiatr%' OR specialite ILIKE '%pediatr%'
                    THEN id_praticien END) AS nb_pediatre,
                COUNT(DISTINCT CASE WHEN specialite ILIKE '%psychiatr%'
                    THEN id_praticien END) AS nb_psychiatre,
                COUNT(DISTINCT CASE WHEN libelle_profession = 'Chirurgien-Dentiste'
                    THEN id_praticien END) AS nb_dentistes,
                COUNT(DISTINCT CASE WHEN libelle_profession = 'Pharmacien'
                    THEN id_praticien END) AS nb_pharmaciens,
                COUNT(DISTINCT CASE WHEN libelle_profession = 'Infirmier'
                    THEN id_praticien END) AS nb_infirmiers,
                COUNT(DISTINCT CASE WHEN libelle_profession = 'Masseur-Kinésithérapeute'
                    THEN id_praticien END) AS nb_kine
            FROM praticiens
            GROUP BY code_commune
        )
        SELECT
            c.code_insee,
            c.nom_commune,
            c.code_departement,
            c.nom_departement,
            c.nom_region,
            c.population_totale,
            c.latitude,
            c.longitude,
            -- Praticiens
            COALESCE(pp.nb_generalistes, 0) AS nb_generalistes,
            COALESCE(pp.nb_dermato, 0) AS nb_dermato,
            COALESCE(pp.nb_ophtalmo, 0) AS nb_ophtalmo,
            COALESCE(pp.nb_gyneco, 0) AS nb_gyneco,
            COALESCE(pp.nb_pediatre, 0) AS nb_pediatre,
            COALESCE(pp.nb_psychiatre, 0) AS nb_psychiatre,
            COALESCE(pp.nb_dentistes, 0) AS nb_dentistes,
            COALESCE(pp.nb_infirmiers, 0) AS nb_infirmiers,
            COALESCE(pp.nb_kine, 0) AS nb_kine,
            -- Équipements
            COALESCE(ep.nb_pharmacies, 0) AS nb_pharmacies,
            COALESCE(ep.nb_hopitaux, 0) AS nb_hopitaux,
            COALESCE(ep.nb_urgences, 0) AS nb_urgences,
            COALESCE(ep.nb_labos, 0) AS nb_labos,
            COALESCE(ep.nb_maisons_sante, 0) AS nb_maisons_sante,
            -- APL officiel (généralistes)
            apl_mg.score_apl AS apl_generaliste,
            apl_dent.score_apl AS apl_dentiste,
            apl_inf.score_apl AS apl_infirmiere,
            apl_kine.score_apl AS apl_kine,
            apl_sf.score_apl AS apl_sage_femme
        FROM communes c
        LEFT JOIN praticiens_pivot pp ON c.code_insee = pp.code_commune
        LEFT JOIN equipements_pivot ep ON c.code_insee = ep.code_commune
        LEFT JOIN apl apl_mg ON c.code_insee = apl_mg.code_commune
            AND apl_mg.profession = 'medecin_generaliste'
            AND apl_mg.annee = (SELECT MAX(annee) FROM apl)
        LEFT JOIN apl apl_dent ON c.code_insee = apl_dent.code_commune
            AND apl_dent.profession = 'chirurgien_dentiste'
            AND apl_dent.annee = (SELECT MAX(annee) FROM apl)
        LEFT JOIN apl apl_inf ON c.code_insee = apl_inf.code_commune
            AND apl_inf.profession = 'infirmiere'
            AND apl_inf.annee = (SELECT MAX(annee) FROM apl)
        LEFT JOIN apl apl_kine ON c.code_insee = apl_kine.code_commune
            AND apl_kine.profession = 'kinesitherapeute'
            AND apl_kine.annee = (SELECT MAX(annee) FROM apl)
        LEFT JOIN apl apl_sf ON c.code_insee = apl_sf.code_commune
            AND apl_sf.profession = 'sage_femme'
            AND apl_sf.annee = (SELECT MAX(annee) FROM apl)
        WHERE c.population_totale > 0
    """)
    logger.info("  ✅ vue_bilan_commune (vue principale !)")


# ============================================================
# TESTS DE VALIDATION
# ============================================================

def run_tests(con):
    """Exécute quelques requêtes de validation."""
    logger.info("")
    logger.info("=" * 50)
    logger.info("3. Tests de validation")
    logger.info("=" * 50)

    # Test 1 : Bilan d'une grande ville (Montpellier)
    logger.info("")
    logger.info("  📊 Bilan santé de Montpellier (34172) :")
    result = con.execute("""
        SELECT nom_commune, population_totale,
               nb_generalistes, nb_dermato, nb_ophtalmo, nb_gyneco,
               nb_dentistes, nb_pharmacies, nb_hopitaux,
               ROUND(apl_generaliste, 2) AS apl_mg
        FROM vue_bilan_commune
        WHERE code_insee = '34172'
    """).fetchdf()
    if not result.empty:
        for col in result.columns:
            logger.info(f"     {col}: {result[col].iloc[0]}")
    else:
        logger.info("     ⚠️ Commune non trouvée")

    # Test 2 : Top 10 des déserts médicaux (généralistes)
    logger.info("")
    logger.info("  📊 Top 10 communes > 5000 hab les plus sous-dotées en généralistes :")
    result = con.execute("""
        SELECT nom_commune, nom_departement, population_totale,
               score_apl, classification
        FROM vue_score_commune
        WHERE profession = 'medecin_generaliste'
          AND population_totale > 5000
        ORDER BY score_apl ASC
        LIMIT 10
    """).fetchdf()
    for _, row in result.iterrows():
        logger.info(f"     {row['nom_commune']:30s} ({row['nom_departement']:20s}) "
                    f"pop={row['population_totale']:>6}  APL={row['score_apl']:.2f}  "
                    f"{row['classification']}")

    # Test 3 : Dermatos les plus accessibles autour d'une ville
    logger.info("")
    logger.info("  📊 Top 10 dermatologues les plus accessibles (score le plus bas) :")
    result = con.execute("""
        SELECT nom_exercice, prenom_exercice, nom_commune, nom_departement,
               nb_confreres_commune, patients_par_praticien, score_accessibilite
        FROM vue_praticiens_accessibles
        WHERE specialite_groupe = 'Dermatologue'
        ORDER BY score_accessibilite ASC
        LIMIT 10
    """).fetchdf()
    for _, row in result.iterrows():
        logger.info(f"     Dr {row['prenom_exercice']} {row['nom_exercice']:20s} "
                    f"à {row['nom_commune']:20s} — "
                    f"score={row['score_accessibilite']:,.0f}  "
                    f"({row['nb_confreres_commune']} confrères)")

    # Test 4 : Comparaison dermato entre villes
    logger.info("")
    logger.info("  📊 Dermatos par grande ville :")
    result = con.execute("""
        SELECT nom_commune, population_totale, nb_dermato,
               ROUND(nb_dermato * 10000.0 / population_totale, 2) AS densite_dermato
        FROM vue_bilan_commune
        WHERE nom_commune IN ('Paris', 'Lyon', 'Marseille', 'Toulouse',
                              'Montpellier', 'Rennes', 'Bordeaux', 'Nantes')
        ORDER BY densite_dermato DESC
    """).fetchdf()
    for _, row in result.iterrows():
        logger.info(f"     {row['nom_commune']:15s} pop={row['population_totale']:>7}  "
                    f"dermatos={row['nb_dermato']:>3}  "
                    f"densité={row['densite_dermato']:.2f}/10k hab")


# ============================================================
# POINT D'ENTRÉE
# ============================================================

if __name__ == "__main__":
    logger.info("🏥 MédiAccès — Chargement en base DuckDB")
    logger.info(f"   Base : {DB_PATH}")
    logger.info("")

    # Supprimer l'ancienne base si elle existe
    if DB_PATH.exists():
        DB_PATH.unlink()
        logger.info("  🗑️ Ancienne base supprimée")

    con = duckdb.connect(str(DB_PATH))

    load_data(con)
    create_views(con)
    run_tests(con)

    # Stats finales
    logger.info("")
    logger.info("=" * 50)
    logger.info("BASE PRÊTE !")
    logger.info("=" * 50)
    logger.info(f"  📁 {DB_PATH}")
    logger.info(f"  📊 Taille : {DB_PATH.stat().st_size / 1e6:.1f} Mo")
    logger.info("")
    logger.info("  Pour explorer la base :")
    logger.info(f"    python -c \"import duckdb; con = duckdb.connect('{DB_PATH}'); print(con.execute('SELECT * FROM vue_bilan_commune LIMIT 5').fetchdf())\"")
    logger.info("")
    logger.info("  Prochaine étape : python scripts/04_app.py (dashboard Streamlit)")

    con.close()
