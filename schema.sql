-- ============================================================
-- MédiAccès — Schéma de la base de données
-- Base : DuckDB (ou PostgreSQL)
-- ============================================================

-- ============================================================
-- TABLES DE DIMENSIONS
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_commune (
    code_insee       VARCHAR(5) PRIMARY KEY,
    nom_commune      VARCHAR(100) NOT NULL,
    code_departement VARCHAR(3),
    nom_departement  VARCHAR(100),
    code_region      VARCHAR(2),
    nom_region       VARCHAR(100),
    population_totale INTEGER,
    pop_0_14         INTEGER,
    pop_15_29        INTEGER,
    pop_30_44        INTEGER,
    pop_45_59        INTEGER,
    pop_60_74        INTEGER,
    pop_75_plus      INTEGER,
    latitude         DOUBLE,
    longitude        DOUBLE
);

CREATE TABLE IF NOT EXISTS dim_profession (
    code_profession  VARCHAR(10) PRIMARY KEY,
    libelle          VARCHAR(100) NOT NULL,
    categorie        VARCHAR(30) -- 'generaliste', 'specialiste', 'paramedical', 'dentaire'
);

-- ============================================================
-- TABLES DE FAITS
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_praticiens (
    id_praticien            VARCHAR(20) PRIMARY KEY,  -- Numéro RPPS
    nom_exercice            VARCHAR(100),
    prenom_exercice         VARCHAR(100),
    code_profession         VARCHAR(10),
    libelle_specialite      VARCHAR(100),
    code_commune_exercice   VARCHAR(5),
    adresse_exercice        VARCHAR(255),
    mode_exercice           VARCHAR(30),   -- 'libéral', 'salarié', 'mixte'
    secteur_conventionnement VARCHAR(30),  -- 'secteur 1', 'secteur 2', 'non conventionné'
    coord_latitude          DOUBLE,
    coord_longitude         DOUBLE,
    date_extraction         DATE,

    FOREIGN KEY (code_profession) REFERENCES dim_profession(code_profession),
    FOREIGN KEY (code_commune_exercice) REFERENCES dim_commune(code_insee)
);

CREATE TABLE IF NOT EXISTS fact_apl (
    code_commune            VARCHAR(5),
    code_profession         VARCHAR(10),
    annee                   INTEGER,
    score_apl               DOUBLE,        -- Consultations accessibles / hab / an
    population_standardisee DOUBLE,

    PRIMARY KEY (code_commune, code_profession, annee),
    FOREIGN KEY (code_commune) REFERENCES dim_commune(code_insee),
    FOREIGN KEY (code_profession) REFERENCES dim_profession(code_profession)
);

CREATE TABLE IF NOT EXISTS fact_equipements (
    code_commune     VARCHAR(5),
    type_equipement  VARCHAR(50),   -- 'pharmacie', 'laboratoire', 'urgences', 'hopital'
    nombre           INTEGER,
    annee            INTEGER,

    PRIMARY KEY (code_commune, type_equipement, annee),
    FOREIGN KEY (code_commune) REFERENCES dim_commune(code_insee)
);

-- ============================================================
-- VUES ANALYTIQUES
-- ============================================================

-- Vue 1 : Densité de praticiens par commune et spécialité
CREATE OR REPLACE VIEW vue_densite_praticiens AS
SELECT
    c.code_insee,
    c.nom_commune,
    c.nom_departement,
    c.nom_region,
    c.population_totale,
    p.libelle AS profession,
    p.categorie,
    COUNT(pr.id_praticien) AS nb_praticiens,
    ROUND(COUNT(pr.id_praticien) * 10000.0 / NULLIF(c.population_totale, 0), 2)
        AS densite_pour_10000_hab
FROM dim_commune c
LEFT JOIN fact_praticiens pr ON c.code_insee = pr.code_commune_exercice
LEFT JOIN dim_profession p ON pr.code_profession = p.code_profession
WHERE c.population_totale > 0
GROUP BY
    c.code_insee, c.nom_commune, c.nom_departement, c.nom_region,
    c.population_totale, p.libelle, p.categorie;


-- Vue 2 : Score composite par commune
-- Combine densité de praticiens + APL + équipements
CREATE OR REPLACE VIEW vue_score_commune AS
WITH densite AS (
    SELECT
        code_commune_exercice AS code_commune,
        code_profession,
        COUNT(*) AS nb_praticiens
    FROM fact_praticiens
    GROUP BY code_commune_exercice, code_profession
),
moyennes_nationales AS (
    SELECT
        code_profession,
        AVG(score_apl) AS apl_moyen_national
    FROM fact_apl
    WHERE annee = (SELECT MAX(annee) FROM fact_apl)
    GROUP BY code_profession
)
SELECT
    c.code_insee,
    c.nom_commune,
    c.nom_departement,
    c.population_totale,
    p.libelle AS profession,
    COALESCE(d.nb_praticiens, 0) AS nb_praticiens,
    ROUND(COALESCE(d.nb_praticiens, 0) * 10000.0 / NULLIF(c.population_totale, 0), 2)
        AS densite_pour_10000_hab,
    a.score_apl,
    mn.apl_moyen_national,
    ROUND(a.score_apl / NULLIF(mn.apl_moyen_national, 0) * 100, 1)
        AS pct_vs_moyenne_nationale,
    -- Classification de la zone
    CASE
        WHEN a.score_apl < mn.apl_moyen_national * 0.5 THEN 'Désert médical'
        WHEN a.score_apl < mn.apl_moyen_national * 0.75 THEN 'Zone sous-dotée'
        WHEN a.score_apl < mn.apl_moyen_national * 1.25 THEN 'Zone correcte'
        ELSE 'Zone bien dotée'
    END AS classification_zone,
    -- Rang départemental
    RANK() OVER (
        PARTITION BY c.code_departement, p.code_profession
        ORDER BY a.score_apl DESC
    ) AS rang_departemental
FROM dim_commune c
CROSS JOIN dim_profession p
LEFT JOIN densite d
    ON c.code_insee = d.code_commune AND p.code_profession = d.code_profession
LEFT JOIN fact_apl a
    ON c.code_insee = a.code_commune
    AND p.code_profession = a.code_profession
    AND a.annee = (SELECT MAX(annee) FROM fact_apl)
LEFT JOIN moyennes_nationales mn
    ON p.code_profession = mn.code_profession
WHERE c.population_totale > 0;


-- Vue 3 : Classement intelligent des praticiens par accessibilité
-- Pour un patient non suivi cherchant un spécialiste
CREATE OR REPLACE VIEW vue_praticiens_accessibles AS
WITH charge_estimee AS (
    -- Estime la "charge" de chaque praticien
    -- = population de la commune / nb de praticiens de même spécialité dans la commune
    SELECT
        pr.id_praticien,
        pr.code_commune_exercice,
        pr.code_profession,
        c.population_totale,
        COUNT(*) OVER (
            PARTITION BY pr.code_commune_exercice, pr.code_profession
        ) AS nb_confreres_commune,
        ROUND(
            c.population_totale * 1.0
            / COUNT(*) OVER (PARTITION BY pr.code_commune_exercice, pr.code_profession),
            0
        ) AS patients_potentiels_par_praticien
    FROM fact_praticiens pr
    JOIN dim_commune c ON pr.code_commune_exercice = c.code_insee
)
SELECT
    pr.id_praticien,
    pr.nom_exercice,
    pr.prenom_exercice,
    p.libelle AS profession,
    pr.libelle_specialite,
    c.nom_commune,
    c.nom_departement,
    pr.adresse_exercice,
    pr.mode_exercice,
    pr.secteur_conventionnement,
    pr.coord_latitude,
    pr.coord_longitude,
    ce.nb_confreres_commune,
    ce.patients_potentiels_par_praticien,
    -- Score d'accessibilité estimé (plus le score est bas, plus le praticien est accessible)
    -- Favorise : secteur 1, faible charge, mode libéral (accès direct)
    ROUND(
        ce.patients_potentiels_par_praticien
        * CASE pr.secteur_conventionnement
            WHEN 'secteur 1' THEN 0.8    -- bonus secteur 1 (moins cher)
            WHEN 'secteur 2' THEN 1.0
            ELSE 1.3                      -- malus non conventionné
          END
        * CASE pr.mode_exercice
            WHEN 'libéral' THEN 0.9       -- accès direct plus facile
            WHEN 'mixte' THEN 1.0
            ELSE 1.2                       -- salarié = souvent sur rdv hôpital
          END,
        0
    ) AS score_accessibilite
FROM fact_praticiens pr
JOIN dim_profession p ON pr.code_profession = p.code_profession
JOIN dim_commune c ON pr.code_commune_exercice = c.code_insee
JOIN charge_estimee ce ON pr.id_praticien = ce.id_praticien
ORDER BY pr.code_profession, score_accessibilite ASC;


-- Vue 4 : Évolution de l'APL dans le temps par commune
CREATE OR REPLACE VIEW vue_evolution_apl AS
SELECT
    c.code_insee,
    c.nom_commune,
    c.nom_departement,
    p.libelle AS profession,
    a.annee,
    a.score_apl,
    LAG(a.score_apl) OVER (
        PARTITION BY a.code_commune, a.code_profession
        ORDER BY a.annee
    ) AS score_apl_annee_precedente,
    ROUND(
        (a.score_apl - LAG(a.score_apl) OVER (
            PARTITION BY a.code_commune, a.code_profession
            ORDER BY a.annee
        )) / NULLIF(LAG(a.score_apl) OVER (
            PARTITION BY a.code_commune, a.code_profession
            ORDER BY a.annee
        ), 0) * 100,
        1
    ) AS variation_pct
FROM fact_apl a
JOIN dim_commune c ON a.code_commune = c.code_insee
JOIN dim_profession p ON a.code_profession = p.code_profession
ORDER BY a.code_commune, a.code_profession, a.annee;
