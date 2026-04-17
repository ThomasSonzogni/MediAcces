# 🏥 MédiAccès — Cartographie intelligente de l'accessibilité aux soins en France

## 1. Contexte et problème

L'accessibilité aux soins est un enjeu majeur en France. Les données existent (RPPS, DREES, INSEE) mais sont éparpillées, techniques et illisibles pour le grand public. Aujourd'hui, **aucun outil ne permet à un particulier de répondre simplement** à la question :

> « Si je déménage à [ville X], est-ce que je pourrai facilement voir un dermato, un ophtalmo, un généraliste ? Et comment ça se compare à ma ville actuelle ? »

## 2. Objectif du projet

Créer un outil data qui :
- Agrège les données ouvertes de santé en une base structurée
- Calcule un **score d'accessibilité** par commune et par spécialité
- Estime les **délais d'attente probables** par spécialité et zone
- Propose un **classement intelligent des praticiens** les plus accessibles
- Fournit des **liens directs** vers les plateformes de prise de RDV

## 3. Sources de données

| Source | Données | Format | Fréquence |
|--------|---------|--------|-----------|
| RPPS / Annuaire Santé | Praticiens : spécialité, commune, activité, conventionnement | CSV (data.gouv.fr) | Quotidien |
| DREES - APL | Score d'accessibilité potentielle localisée par commune | CSV (data.drees) | Annuel |
| INSEE - Population | Population par commune, tranches d'âge | CSV | Annuel |
| BPE (INSEE) | Équipements de santé : pharmacies, labos, hôpitaux | CSV | Annuel |
| Ameli - Annuaire | Conventionnement, tarifs, acceptation nouveaux patients | Web / API | Variable |

## 4. Architecture technique

```
┌─────────────────────────────────────────────────────┐
│                    SOURCES EXTERNES                  │
│  RPPS · DREES · INSEE · BPE · Ameli                │
└──────────────┬──────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────┐
│              INGESTION (Python)                      │
│  Scripts de téléchargement, nettoyage, validation   │
│  pandas · requests · scheduling                      │
└──────────────┬──────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────┐
│           BASE DE DONNÉES (DuckDB/PostgreSQL)        │
│                                                      │
│  dim_commune        dim_profession                   │
│  fact_praticiens    fact_apl                          │
│  fact_equipements   vue_score_commune                │
└──────────────┬──────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────┐
│            RESTITUTION                               │
│  Dashboard Streamlit / App Flask                     │
│  Carte interactive · Comparateur · Classement        │
└─────────────────────────────────────────────────────┘
```

## 5. Modèle de données

### Tables de dimensions

**dim_commune**
- code_insee (PK)
- nom_commune
- code_departement
- nom_departement
- code_region
- nom_region
- population_totale
- pop_0_14, pop_15_29, pop_30_44, pop_45_59, pop_60_74, pop_75_plus
- latitude, longitude

**dim_profession**
- code_profession (PK)
- libelle_profession
- categorie (généraliste, spécialiste, paramédical)

### Tables de faits

**fact_praticiens**
- id_praticien (PK)
- code_profession (FK)
- libelle_specialite
- code_commune_exercice (FK)
- mode_exercice (libéral, salarié, mixte)
- secteur_conventionnement (1, 2, non conventionné)
- accepte_nouveaux_patients (booléen, si disponible)
- date_extraction

**fact_apl**
- code_commune (FK)
- code_profession (FK)
- annee
- score_apl (consultations/hab/an)
- population_standardisee

**fact_equipements**
- code_commune (FK)
- type_equipement (pharmacie, labo, urgences, hôpital)
- nombre

### Vues calculées

**vue_score_commune**
- code_commune
- specialite
- nb_praticiens
- densite_pour_10000_hab
- score_apl
- ratio_vs_moyenne_nationale
- estimation_delai_jours
- rang_departemental
- rang_national

## 6. Fonctionnalités utilisateur

### MVP (Phase 1)
- [ ] Recherche par ville → bilan santé du territoire
- [ ] Score d'accessibilité par spécialité (généraliste, dermato, ophtalmo, gynéco, dentiste)
- [ ] Comparaison avec la moyenne départementale et nationale
- [ ] Liste des praticiens proches avec secteur de conventionnement

### Phase 2
- [ ] Comparateur entre 2 villes
- [ ] Estimation du délai d'attente par spécialité
- [ ] Carte interactive avec gradient de couleur par score
- [ ] Classement intelligent des praticiens (ratio patients/praticien + conventionnement)

### Phase 3
- [ ] Évolution dans le temps (tendance sur 3-5 ans)
- [ ] Alertes : zones en dégradation rapide
- [ ] Intégration des liens Doctolib/Maiia par praticien

## 7. Compétences développées

| Compétence | Application dans le projet |
|-----------|---------------------------|
| SQL avancé | Modélisation dimensionnelle, window functions, CTEs, vues matérialisées |
| Python - API/Ingestion | Scripts d'extraction multi-sources, nettoyage pandas, scheduling |
| Python - Automatisation | Pipeline reproductible, tests de qualité des données |
| Data Viz | Dashboard interactif, cartes choroplèthes |
| Gestion de projet | Cahier des charges, phases, documentation |

## 8. Planning prévisionnel

| Phase | Durée estimée | Livrables |
|-------|--------------|-----------|
| Cadrage & données | 1 semaine | CDC, exploration des sources, scripts de téléchargement |
| Modélisation & ingestion | 1-2 semaines | Schéma SQL, pipeline d'ingestion, base alimentée |
| Analyses & scoring | 1 semaine | Requêtes SQL, calcul des scores, vues agrégées |
| Dashboard MVP | 1-2 semaines | Interface de restitution fonctionnelle |
| Enrichissement | En continu | Fonctionnalités Phase 2 & 3 |

## 9. Structure du repo

```
mediacces/
├── README.md
├── data/
│   ├── raw/              # Données brutes téléchargées
│   └── processed/        # Données nettoyées
├── scripts/
│   ├── 01_download.py    # Téléchargement des sources
│   ├── 02_clean.py       # Nettoyage et transformation
│   ├── 03_load.py        # Chargement en base
│   └── 04_score.py       # Calcul des scores
├── sql/
│   ├── schema.sql        # Création des tables
│   ├── views.sql         # Vues agrégées
│   └── queries/          # Requêtes d'analyse
├── app/
│   └── streamlit_app.py  # Dashboard
├── tests/
│   └── test_data.py      # Tests de qualité
└── requirements.txt
```
