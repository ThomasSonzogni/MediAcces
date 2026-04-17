"""
MédiAccès — Dashboard Streamlit
================================
Interface interactive pour explorer l'accessibilité aux soins en France.

Usage :
    streamlit run app/streamlit_app.py
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import unicodedata
import re
from pathlib import Path

# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "processed" / "mediacces.duckdb"

st.set_page_config(
    page_title="MédiAccès — Accessibilité aux soins",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# CONNEXION BASE
# ============================================================

@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)

con = get_connection()

# ============================================================
# FONCTIONS UTILITAIRES
# ============================================================

@st.cache_data(ttl=3600)
def query(sql):
    return con.execute(sql).fetchdf()

@st.cache_data(ttl=3600)
def get_liste_communes():
    return query("""
        SELECT code_insee, nom_commune, nom_departement, population_totale
        FROM communes
        WHERE population_totale > 0
        ORDER BY population_totale DESC
    """)

@st.cache_data(ttl=3600)
def get_bilan_commune(code_insee):
    return query(f"""
        SELECT * FROM vue_bilan_commune
        WHERE code_insee = '{code_insee}'
    """)

@st.cache_data(ttl=3600)
def get_score_commune(code_insee):
    return query(f"""
        SELECT * FROM vue_score_commune
        WHERE code_insee = '{code_insee}'
    """)

@st.cache_data(ttl=3600)
def get_specialistes_commune(code_insee):
    return query(f"""
        SELECT * FROM vue_densite_specialistes
        WHERE code_insee = '{code_insee}'
    """)

@st.cache_data(ttl=3600)
def get_dermatos_proches(lat, lon, limit=20):
    """Trouve les dermatologues les plus proches par distance euclidienne."""
    return query(f"""
        WITH dermatos AS (
            SELECT
                id_praticien,
                nom_exercice,
                prenom_exercice,
                nom_commune,
                nom_departement,
                nb_confreres_commune,
                patients_par_praticien,
                score_accessibilite,
                latitude,
                longitude,
                -- Distance approximative en km (formule simplifiée)
                ROUND(
                    111.0 * SQRT(
                        POWER(latitude - {lat}, 2)
                        + POWER((longitude - {lon}) * COS(RADIANS({lat})), 2)
                    ),
                    1
                ) AS distance_km
            FROM vue_praticiens_accessibles
            WHERE specialite_groupe = 'Dermatologue'
              AND latitude IS NOT NULL
        )
        SELECT * FROM dermatos
        ORDER BY distance_km ASC, score_accessibilite ASC
        LIMIT {limit}
    """)


def slugify(text):
    """Convertit un texte en slug URL compatible Doctolib."""
    text = unicodedata.normalize("NFD", text.lower())
    text = re.sub(r"[\u0300-\u036f]", "", text)  # Supprimer les accents
    text = re.sub(r"[^a-z0-9]+", "-", text)       # Remplacer les caractères spéciaux
    text = text.strip("-")
    return text


def doctolib_search_url(ville, specialite="dermatologue"):
    """Génère l'URL de recherche Doctolib avec filtre disponibilités."""
    ville_slug = slugify(ville)
    return f"https://www.doctolib.fr/{specialite}/{ville_slug}?availability=true"


def doctolib_praticien_url(prenom, nom, ville, specialite="dermatologue"):
    """Génère l'URL probable du profil Doctolib d'un praticien."""
    prenom_slug = slugify(prenom)
    nom_slug = slugify(nom)
    ville_slug = slugify(ville)
    return f"https://www.doctolib.fr/{specialite}/{ville_slug}/{prenom_slug}-{nom_slug}"


# ============================================================
# STYLES CSS
# ============================================================

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');

    .main .block-container {
        padding-top: 2rem;
        max-width: 1200px;
    }
    h1, h2, h3 {
        font-family: 'DM Sans', sans-serif !important;
    }
    .metric-card {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        border-radius: 12px;
        padding: 1.2rem;
        text-align: center;
        border-left: 4px solid #4361ee;
    }
    .metric-card h3 {
        font-size: 0.85rem;
        color: #6c757d;
        margin-bottom: 0.3rem;
        font-weight: 500;
    }
    .metric-card .value {
        font-size: 2rem;
        font-weight: 700;
        color: #212529;
    }
    .metric-card .sub {
        font-size: 0.75rem;
        color: #868e96;
    }
    .status-desert { color: #e03131; font-weight: 700; }
    .status-sous { color: #e8590c; font-weight: 700; }
    .status-correct { color: #f08c00; font-weight: 700; }
    .status-bien { color: #2f9e44; font-weight: 700; }
    .header-badge {
        background: linear-gradient(135deg, #4361ee, #3a0ca3);
        color: white;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 500;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================
# EN-TÊTE
# ============================================================

col_title, col_badge = st.columns([4, 1])
with col_title:
    st.markdown("# 🏥 MédiAccès")
    st.markdown("**Cartographie intelligente de l'accessibilité aux soins en France**")
with col_badge:
    st.markdown("")
    st.markdown('<span class="header-badge">📊 Données RPPS · DREES · INSEE</span>',
                unsafe_allow_html=True)

st.markdown("---")

# ============================================================
# SIDEBAR — RECHERCHE
# ============================================================

with st.sidebar:
    st.markdown("## 🔍 Rechercher une commune")

    communes_df = get_liste_communes()

    # Barre de recherche
    search = st.text_input(
        "Tapez le nom d'une ville",
        placeholder="ex: Montpellier, Rennes, Lyon..."
    )

    if search:
        filtered = communes_df[
            communes_df["nom_commune"].str.contains(search, case=False, na=False)
        ].head(20)
    else:
        # Par défaut, montrer les plus grandes villes
        filtered = communes_df.head(30)

    # Sélection
    options = [
        f"{row['nom_commune']} ({row['nom_departement']}) — {row['population_totale']:,} hab"
        for _, row in filtered.iterrows()
    ]
    codes = filtered["code_insee"].tolist()

    if options:
        selected_idx = st.selectbox(
            "Sélectionnez",
            range(len(options)),
            format_func=lambda i: options[i]
        )
        selected_code = codes[selected_idx]
        selected_name = filtered.iloc[selected_idx]["nom_commune"]
    else:
        st.warning("Aucune commune trouvée")
        st.stop()

    st.markdown("---")
    st.markdown("### ⚙️ Options")
    show_map = st.checkbox("Afficher la carte", value=True)
    show_comparateur = st.checkbox("Comparateur de villes", value=False)


# ============================================================
# PAGE PRINCIPALE — BILAN SANTÉ
# ============================================================

bilan = get_bilan_commune(selected_code)
scores = get_score_commune(selected_code)

if bilan.empty:
    st.error(f"Aucune donnée trouvée pour {selected_name}")
    st.stop()

b = bilan.iloc[0]

# Titre de la commune
st.markdown(f"## 📍 {b['nom_commune']} — {b['nom_departement']}")
st.markdown(f"**{b['nom_region']}** · Population : **{int(b['population_totale']):,}** habitants")

# ============================================================
# MÉTRIQUES PRINCIPALES
# ============================================================

st.markdown("### 🩺 Professionnels de santé")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Généralistes", int(b["nb_generalistes"]),
              help="Médecins généralistes exerçant dans la commune")
with col2:
    st.metric("Dentistes", int(b["nb_dentistes"]))
with col3:
    st.metric("Infirmiers", int(b["nb_infirmiers"]))
with col4:
    st.metric("Kinés", int(b["nb_kine"]))
with col5:
    st.metric("Pharmacies", int(b["nb_pharmacies"]))

# Spécialistes
st.markdown("### 🔬 Médecins spécialistes")

col1, col2, col3, col4, col5, col6 = st.columns(6)
with col1:
    st.metric("Dermatos", int(b["nb_dermato"]))
with col2:
    st.metric("Ophtalmos", int(b["nb_ophtalmo"]))
with col3:
    st.metric("Gynécos", int(b["nb_gyneco"]))
with col4:
    st.metric("Pédiatres", int(b["nb_pediatre"]))
with col5:
    st.metric("Psychiatres", int(b["nb_psychiatre"]))
with col6:
    hopitaux = int(b["nb_hopitaux"]) if pd.notna(b["nb_hopitaux"]) else 0
    st.metric("Hôpitaux", hopitaux)


# ============================================================
# SCORES APL (ACCESSIBILITÉ OFFICIELLE)
# ============================================================

st.markdown("### 📊 Accessibilité potentielle localisée (APL)")
st.caption("Score officiel DREES — nombre de consultations accessibles par an et par habitant")

if not scores.empty:
    # Graphique en barres des scores APL
    scores_display = scores[["profession", "score_apl", "apl_moyen_national", "classification"]].copy()

    profession_labels = {
        "medecin_generaliste": "Médecin gén.",
        "chirurgien_dentiste": "Dentiste",
        "infirmiere": "Infirmière",
        "kinesitherapeute": "Kiné",
        "sage_femme": "Sage-femme",
    }
    scores_display["profession_label"] = scores_display["profession"].map(profession_labels)

    fig = go.Figure()

    # Score de la commune
    fig.add_trace(go.Bar(
        x=scores_display["profession_label"],
        y=scores_display["score_apl"],
        name=b["nom_commune"],
        marker_color="#4361ee",
        text=scores_display["score_apl"].round(2),
        textposition="outside"
    ))

    # Moyenne nationale
    fig.add_trace(go.Bar(
        x=scores_display["profession_label"],
        y=scores_display["apl_moyen_national"],
        name="Moyenne nationale",
        marker_color="#dee2e6",
        text=scores_display["apl_moyen_national"].round(2),
        textposition="outside"
    ))

    fig.update_layout(
        barmode="group",
        height=350,
        margin=dict(t=30, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        yaxis_title="Consultations / hab / an",
        font=dict(family="DM Sans"),
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_yaxes(gridcolor="#f1f3f5")

    st.plotly_chart(fig, use_container_width=True)

    # Classification par profession
    cols = st.columns(len(scores_display))
    for i, (_, row) in enumerate(scores_display.iterrows()):
        with cols[i]:
            classif = row["classification"]
            st.markdown(f"**{row['profession_label']}**<br>{classif}",
                       unsafe_allow_html=True)
else:
    st.info("Pas de données APL disponibles pour cette commune")


# ============================================================
# DENSITÉ DE SPÉCIALISTES (NOTRE INDICATEUR MAISON)
# ============================================================

st.markdown("### 🔎 Densité de spécialistes — notre indicateur")
st.caption("Nombre de spécialistes pour 10 000 habitants, comparé à la moyenne nationale des communes de taille similaire")

specialistes = get_specialistes_commune(selected_code)

if not specialistes.empty:
    fig2 = go.Figure()

    fig2.add_trace(go.Bar(
        x=specialistes["specialite_groupe"],
        y=specialistes["densite_pour_10000_hab"],
        name=b["nom_commune"],
        marker_color="#7c3aed",
        text=specialistes["densite_pour_10000_hab"],
        textposition="outside"
    ))

    fig2.add_trace(go.Bar(
        x=specialistes["specialite_groupe"],
        y=specialistes["densite_moyenne_nationale"],
        name="Moyenne nationale",
        marker_color="#e9ecef",
        text=specialistes["densite_moyenne_nationale"],
        textposition="outside"
    ))

    fig2.update_layout(
        barmode="group",
        height=350,
        margin=dict(t=30, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        yaxis_title="Praticiens / 10 000 hab",
        font=dict(family="DM Sans"),
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig2.update_yaxes(gridcolor="#f1f3f5")

    st.plotly_chart(fig2, use_container_width=True)


# ============================================================
# DERMATOS LES PLUS ACCESSIBLES
# ============================================================

st.markdown("### 💡 Dermatologues les plus accessibles près de chez vous")
st.caption("Classement basé sur : ratio patients/praticien × secteur de conventionnement × mode d'exercice. "
           "Un score bas = meilleure accessibilité estimée.")

if pd.notna(b["latitude"]) and pd.notna(b["longitude"]):
    dermatos = get_dermatos_proches(b["latitude"], b["longitude"], limit=15)

    if not dermatos.empty:

        # Bouton de recherche Doctolib global
        doctolib_url = doctolib_search_url(b["nom_commune"], "dermatologue")
        st.markdown(
            f"""
            <div style="background: linear-gradient(135deg, #0596DE 0%, #0473B0 100%);
                        border-radius: 10px; padding: 1rem 1.5rem; margin-bottom: 1rem;
                        display: flex; align-items: center; justify-content: space-between;">
                <div>
                    <span style="color: white; font-weight: 700; font-size: 1rem;">
                        🔍 Voir les dermatologues avec créneaux ouverts à {b['nom_commune']}
                    </span><br>
                    <span style="color: rgba(255,255,255,0.8); font-size: 0.85rem;">
                        Filtrez par "Nouveau patient" ou "Première consultation" sur Doctolib
                    </span>
                </div>
                <a href="{doctolib_url}" target="_blank"
                   style="background: white; color: #0596DE; padding: 0.5rem 1.2rem;
                          border-radius: 8px; text-decoration: none; font-weight: 700;
                          font-size: 0.9rem; white-space: nowrap;">
                    Ouvrir Doctolib →
                </a>
            </div>
            """,
            unsafe_allow_html=True
        )

        # Tableau avec liens individuels
        display_rows = []
        for _, row in dermatos.iterrows():
            praticien_url = doctolib_praticien_url(
                row["prenom_exercice"], row["nom_exercice"],
                row["nom_commune"], "dermatologue"
            )
            display_rows.append({
                "Praticien": f"Dr {row['prenom_exercice']} {row['nom_exercice']}",
                "Commune": row["nom_commune"],
                "Distance (km)": round(row["distance_km"], 1),
                "Confrères": int(row["nb_confreres_commune"]),
                "Patients/praticien": int(row["patients_par_praticien"]),
                "Score": int(row["score_accessibilite"]),
                "Doctolib": praticien_url,
            })

        display_df = pd.DataFrame(display_rows)

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Score": st.column_config.ProgressColumn(
                    "Score accessibilité",
                    min_value=0,
                    max_value=int(display_df["Score"].max()),
                    format="%d"
                ),
                "Distance (km)": st.column_config.NumberColumn(format="%.1f km"),
                "Doctolib": st.column_config.LinkColumn(
                    "📅 Prendre RDV",
                    display_text="Voir sur Doctolib",
                    help="Lien vers le profil Doctolib probable du praticien. "
                         "Vérifiez la disponibilité et filtrez par 'Nouveau patient'."
                ),
            }
        )

        st.caption(
            "💡 **Astuce** : Sur la page Doctolib du praticien, sélectionnez le motif "
            "**\"Première consultation\"** ou **\"Nouveau patient\"** pour voir les créneaux "
            "ouverts aux patients non suivis. Le nom exact du motif varie selon les praticiens."
        )

        # Carte des dermatos
        if show_map and not dermatos.empty:
            st.markdown("#### 🗺️ Carte des dermatologues proches")

            map_data = dermatos[["latitude", "longitude", "nom_commune",
                                "nom_exercice", "prenom_exercice",
                                "score_accessibilite"]].copy()
            map_data = map_data.dropna(subset=["latitude", "longitude"])
            map_data["latitude"] = pd.to_numeric(map_data["latitude"], errors="coerce")
            map_data["longitude"] = pd.to_numeric(map_data["longitude"], errors="coerce")
            map_data = map_data.dropna()
            map_data["praticien"] = "Dr " + map_data["prenom_exercice"] + " " + map_data["nom_exercice"]

            if not map_data.empty:
                fig_map = px.scatter_map(
                    map_data,
                    lat="latitude",
                    lon="longitude",
                    hover_name="praticien",
                    hover_data={"nom_commune": True, "score_accessibilite": True,
                                "latitude": False, "longitude": False},
                    color="score_accessibilite",
                    color_continuous_scale="RdYlGn_r",
                    size_max=15,
                    zoom=9,
                    height=450,
                )
                fig_map.update_layout(
                    margin=dict(t=0, b=0, l=0, r=0),
                    font=dict(family="DM Sans"),
                    coloraxis_colorbar_title="Score",
                )
                st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.info("Aucun dermatologue trouvé à proximité")
else:
    st.info("Coordonnées GPS non disponibles pour cette commune")


# ============================================================
# COMPARATEUR DE VILLES
# ============================================================

if show_comparateur:
    st.markdown("---")
    st.markdown("### ⚖️ Comparateur de villes")

    col_a, col_b = st.columns(2)

    with col_a:
        search_a = st.text_input("Ville A", value=selected_name, key="comp_a")
        if search_a:
            filtered_a = communes_df[
                communes_df["nom_commune"].str.contains(search_a, case=False, na=False)
            ].head(10)
            if not filtered_a.empty:
                code_a = filtered_a.iloc[0]["code_insee"]
                bilan_a = get_bilan_commune(code_a)

    with col_b:
        search_b = st.text_input("Ville B", placeholder="ex: Rennes", key="comp_b")
        if search_b:
            filtered_b = communes_df[
                communes_df["nom_commune"].str.contains(search_b, case=False, na=False)
            ].head(10)
            if not filtered_b.empty:
                code_b = filtered_b.iloc[0]["code_insee"]
                bilan_b = get_bilan_commune(code_b)

    if search_a and search_b and not bilan_a.empty and not bilan_b.empty:
        a = bilan_a.iloc[0]
        bb = bilan_b.iloc[0]

        compare_data = pd.DataFrame({
            "Indicateur": [
                "Généralistes", "Dermatos", "Ophtalmos", "Gynécos",
                "Dentistes", "Pharmacies", "Hôpitaux"
            ],
            a["nom_commune"]: [
                int(a["nb_generalistes"]), int(a["nb_dermato"]),
                int(a["nb_ophtalmo"]), int(a["nb_gyneco"]),
                int(a["nb_dentistes"]),
                int(a["nb_pharmacies"]) if pd.notna(a["nb_pharmacies"]) else 0,
                int(a["nb_hopitaux"]) if pd.notna(a["nb_hopitaux"]) else 0,
            ],
            bb["nom_commune"]: [
                int(bb["nb_generalistes"]), int(bb["nb_dermato"]),
                int(bb["nb_ophtalmo"]), int(bb["nb_gyneco"]),
                int(bb["nb_dentistes"]),
                int(bb["nb_pharmacies"]) if pd.notna(bb["nb_pharmacies"]) else 0,
                int(bb["nb_hopitaux"]) if pd.notna(bb["nb_hopitaux"]) else 0,
            ],
        })

        # Normaliser pour 10 000 habitants
        pop_a = int(a["population_totale"])
        pop_b = int(bb["population_totale"])

        compare_densite = compare_data.copy()
        compare_densite[a["nom_commune"]] = (
            compare_data[a["nom_commune"]] * 10000 / pop_a
        ).round(2)
        compare_densite[bb["nom_commune"]] = (
            compare_data[bb["nom_commune"]] * 10000 / pop_b
        ).round(2)

        tab1, tab2 = st.tabs(["Nombre absolu", "Pour 10 000 habitants"])

        with tab1:
            st.dataframe(compare_data, use_container_width=True, hide_index=True)

        with tab2:
            st.dataframe(compare_densite, use_container_width=True, hide_index=True)

            fig_comp = go.Figure()
            fig_comp.add_trace(go.Bar(
                x=compare_densite["Indicateur"],
                y=compare_densite[a["nom_commune"]],
                name=f"{a['nom_commune']} ({pop_a:,} hab)",
                marker_color="#4361ee"
            ))
            fig_comp.add_trace(go.Bar(
                x=compare_densite["Indicateur"],
                y=compare_densite[bb["nom_commune"]],
                name=f"{bb['nom_commune']} ({pop_b:,} hab)",
                marker_color="#f72585"
            ))
            fig_comp.update_layout(
                barmode="group", height=350,
                margin=dict(t=30, b=30),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                yaxis_title="Pour 10 000 habitants",
                font=dict(family="DM Sans"),
                plot_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_comp, use_container_width=True)


# ============================================================
# FOOTER
# ============================================================

st.markdown("---")
st.markdown(
    """
    <div style="text-align: center; color: #868e96; font-size: 0.8rem;">
        <strong>MédiAccès</strong> — Projet data analyse par Thomas Sonzogni<br>
        Sources : RPPS (Annuaire Santé) · DREES (APL 2023) · INSEE (Pop & BPE 2024)<br>
        Les scores d'accessibilité sont des estimations basées sur des données ouvertes.
        Ils ne remplacent pas un avis médical.
    </div>
    """,
    unsafe_allow_html=True
)
