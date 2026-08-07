"""
Airbnb Modern Data Pipeline — tableau de bord de la couche Gold.

Deux sources possibles, dans cet ordre :
  1. Un extrait CSV des tables Gold, versionne dans streamlit/data/
  2. A defaut, un jeu de demonstration genere, annonce comme tel dans l'interface

Aucune connexion Snowflake n'est tentee. L'infrastructure cloud du projet est
decommissionnee : une connexion echouerait de toute facon, et le connecteur
alourdirait les dependances pour rien.

Le schema attendu est celui produit par les modeles dbt de ce depot, colonnes en
majuscules comme les rend Snowflake. Toute colonne absente degrade la section qui
en depend, elle ne fait jamais tomber l'application.
"""

import os

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Airbnb Data Pipeline — Gold layer",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Palette unique, utilisee par tous les graphiques. Une seule source de verite
# evite les rendus incoherents d'un onglet a l'autre.
C_PRIMARY = "#FF5A5F"   # corail Airbnb
C_SECOND = "#00A699"    # sarcelle
C_ACCENT = "#FC642D"
C_NEUTRAL = "#767676"
C_INK = "#484848"
SEQUENCE = [C_PRIMARY, C_SECOND, C_ACCENT, "#914669", "#3D9BE9", C_NEUTRAL]

PLOT_LAYOUT = dict(
    margin=dict(l=10, r=10, t=50, b=10),
    height=380,
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
)

st.markdown(
    f"""
    <style>
      .block-container {{ padding-top: 2.2rem; padding-bottom: 3rem; }}
      h1, h2, h3 {{ color: {C_INK}; }}
      [data-testid="stMetricValue"] {{ font-size: 1.7rem; }}
      .source-real {{
        background: {C_SECOND}1A; border-left: 4px solid {C_SECOND};
        padding: .7rem 1rem; border-radius: 4px; margin-bottom: 1.2rem;
      }}
      .source-demo {{
        background: {C_ACCENT}1A; border-left: 4px solid {C_ACCENT};
        padding: .7rem 1rem; border-radius: 4px; margin-bottom: 1.2rem;
      }}
    </style>
    """,
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────
# CHARGEMENT
# ─────────────────────────────────────────────
FICHIERS = {
    "obt": "obt_bookings_analytics.csv",
    "listings": "dim_listings.csv",
    "hosts": "dim_hosts.csv",
}


@st.cache_data
def load_from_extract():
    """Charge l'extrait Gold s'il est complet. Sinon rend (None, None, None, 'absent')."""
    chemins = {k: os.path.join(DATA_DIR, v) for k, v in FICHIERS.items()}
    if not all(os.path.exists(p) for p in chemins.values()):
        return None, None, None, "absent"
    try:
        return (
            pd.read_csv(chemins["obt"]),
            pd.read_csv(chemins["listings"]),
            pd.read_csv(chemins["hosts"]),
            "extrait",
        )
    except Exception as e:  # extrait present mais illisible
        st.warning(f"Extrait illisible ({e}). Bascule sur le jeu de demonstration.")
        return None, None, None, "absent"


@st.cache_data
def generate_demo_data():
    """
    Jeu de demonstration GENERE, ce n'est pas un extrait.
    Le schema reproduit fidelement celui de la couche Gold, y compris ses
    particularites : BOOKING_STATUS en minuscules, comme l'exige le test
    accepted_values des sources, et IS_SUPERHOST en chaine 'TRUE'/'FALSE',
    consequence de la macro trim_upper appliquee en silver a un booleen.
    """
    rng = np.random.default_rng(42)
    n_hosts, n_listings, n_bookings = 100, 300, 1000

    villes = ["Paris", "Lyon", "Marseille", "Bordeaux", "Nice", "Toulouse"]
    types_bien = ["Apartment", "House", "Loft", "Studio", "Villa"]
    types_chambre = ["Entire home/apt", "Private room", "Shared room"]

    # DIM_HOSTS
    host_ids = np.arange(10001, 10001 + n_hosts)
    host_since = pd.to_datetime(
        rng.choice(pd.date_range("2018-01-01", "2025-12-31"), n_hosts)
    )
    response_rate = rng.integers(40, 101, n_hosts)
    hosts = pd.DataFrame(
        {
            "HOST_ID": host_ids,
            "HOST_NAME": [f"HOST {i}" for i in range(1, n_hosts + 1)],
            "HOST_SINCE": host_since,
            "HOST_TENURE_YEARS": ((pd.Timestamp("today") - host_since).days // 365),
            "IS_SUPERHOST": np.where(response_rate >= 90, "TRUE", "FALSE"),
            "RESPONSE_RATE": response_rate,
            "HOST_RESPONSE_SEGMENT": np.where(
                response_rate >= 95, "ELITE", np.where(response_rate >= 80, "GOOD", "LOW")
            ),
        }
    )

    # DIM_LISTINGS
    accommodates = rng.integers(1, 9, n_listings)
    bedrooms = np.maximum(0, accommodates // 2)
    price = rng.integers(40, 400, n_listings)
    listings = pd.DataFrame(
        {
            "LISTING_ID": np.arange(500001, 500001 + n_listings),
            "HOST_ID": rng.choice(host_ids, n_listings),
            "PROPERTY_TYPE": rng.choice(types_bien, n_listings),
            "ROOM_TYPE": rng.choice(types_chambre, n_listings, p=[0.68, 0.28, 0.04]),
            "CITY": rng.choice(villes, n_listings, p=[0.35, 0.15, 0.14, 0.13, 0.13, 0.10]),
            "COUNTRY": "France",
            "ACCOMMODATES": accommodates,
            "BEDROOMS": bedrooms,
            "BATHROOMS": np.maximum(1, bedrooms // 2 + 1),
            "PRICE_PER_NIGHT": price,
            "BEDROOM_DENSITY": np.round(bedrooms / np.maximum(accommodates, 1), 2),
            "PRICE_PER_PERSON": np.round(price / np.maximum(accommodates, 1), 2),
            "PRICE_PER_NIGHT_TAG": np.where(
                price >= 250, "LUXURY", np.where(price >= 100, "MID_RANGE", "BUDGET")
            ),
        }
    )

    # OBT
    idx = rng.integers(0, n_listings, n_bookings)
    nights = rng.choice([1, 2, 3, 4, 5, 7, 10, 14], n_bookings,
                        p=[0.10, 0.20, 0.22, 0.15, 0.10, 0.13, 0.06, 0.04])
    prix = listings["PRICE_PER_NIGHT"].to_numpy()[idx]
    amount = prix * nights
    cleaning = np.round(prix * rng.uniform(0.15, 0.4, n_bookings)).astype(int)
    service = np.round(amount * 0.14).astype(int)
    dates = pd.to_datetime(rng.choice(pd.date_range("2024-01-01", "2026-08-01"), n_bookings))

    obt = pd.DataFrame(
        {
            "BOOKING_ID": [f"BK{i:06d}" for i in range(1, n_bookings + 1)],
            "BOOKING_DATE": dates,
            "BOOKING_YEAR": dates.year,
            "BOOKING_MONTH": dates.month,
            "BOOKING_WEEK": dates.isocalendar().week.values,
            "BOOKING_AMOUNT": amount,
            "NIGHTS_BOOKED": nights,
            "BOOKING_PRICE_PER_NIGHT": np.round(amount / nights, 2),
            "CLEANING_FEE": cleaning,
            "SERVICE_FEE": service,
            "TOTAL_FEES": cleaning + service,
            "TOTAL_BOOKING_VALUE": amount + cleaning + service,
            "NET_REVENUE": amount - (cleaning + service),
            "BOOKING_STATUS": rng.choice(["confirmed", "cancelled"], n_bookings, p=[0.85, 0.15]),
            "LISTING_ID": listings["LISTING_ID"].to_numpy()[idx],
            "HOST_ID": listings["HOST_ID"].to_numpy()[idx],
            "PROPERTY_TYPE": listings["PROPERTY_TYPE"].to_numpy()[idx],
            "ROOM_TYPE": listings["ROOM_TYPE"].to_numpy()[idx],
            "CITY": listings["CITY"].to_numpy()[idx],
            "COUNTRY": "France",
            "ACCOMMODATES": listings["ACCOMMODATES"].to_numpy()[idx],
            "BEDROOMS": listings["BEDROOMS"].to_numpy()[idx],
            "BATHROOMS": listings["BATHROOMS"].to_numpy()[idx],
            "PRICE_PER_NIGHT_TAG": listings["PRICE_PER_NIGHT_TAG"].to_numpy()[idx],
        }
    )
    obt = obt.merge(
        hosts[["HOST_ID", "HOST_NAME", "HOST_SINCE", "IS_SUPERHOST", "HOST_RESPONSE_SEGMENT"]],
        on="HOST_ID", how="left",
    )
    return obt, listings, hosts, "demo"


def normaliser(obt, listings, hosts):
    """
    Met les trois tables en forme pour l'affichage, quelle que soit leur origine.
    Tout ce qui est derive est calcule ICI, jamais suppose present dans l'extrait :
    c'est ce qui manquait a la version precedente et la faisait tomber sur un extrait
    reel, ou BOOKING_MONTH_STR n'existe pas.
    """
    obt = obt.copy()
    listings = listings.copy() if listings is not None else listings

    if "BOOKING_DATE" in obt:
        obt["BOOKING_DATE"] = pd.to_datetime(obt["BOOKING_DATE"], errors="coerce")
        obt["BOOKING_MONTH_STR"] = obt["BOOKING_DATE"].dt.to_period("M").astype(str)
    if "BOOKING_STATUS" in obt:
        obt["BOOKING_STATUS"] = obt["BOOKING_STATUS"].astype(str).str.strip().str.lower()
        # Libelle d'affichage. La valeur technique reste en minuscules : c'est elle
        # que le test accepted_values impose et que tous les filtres comparent.
        obt["STATUT"] = obt["BOOKING_STATUS"].map(
            {"confirmed": "Confirmée", "cancelled": "Annulée"}
        ).fillna(obt["BOOKING_STATUS"])

    # Silver applique trim_lower a city et country, trim_upper a property_type.
    # A l'ecran, ca donnait « paris » a cote de « APARTMENT ». La casse est une
    # decision de modelisation, sa mise en forme est une affaire de presentation :
    # on la traite ici, sans toucher aux modeles.
    for df in (obt, listings):
        if df is None:
            continue
        for col in ("CITY", "COUNTRY", "PROPERTY_TYPE", "ROOM_TYPE", "HOST_NAME"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.title()

    for df in (obt, hosts):
        if "IS_SUPERHOST" in df:
            # Gold rend une chaine 'TRUE'/'FALSE', pandas lirait un booleen sur un
            # autre chemin. On ramene les deux au meme type.
            df["IS_SUPERHOST"] = (
                df["IS_SUPERHOST"].astype(str).str.strip().str.upper().isin(["TRUE", "1"])
            )
    if "HOST_SINCE" in hosts:
        hosts = hosts.copy()
        hosts["HOST_SINCE"] = pd.to_datetime(hosts["HOST_SINCE"], errors="coerce")
    return obt, listings, hosts


def load_data():
    """
    Volontairement NON mise en cache, contrairement aux deux fonctions de
    chargement qu'elle appelle.

    Motif, constate en production le 07/08/2026 : st.cache_data invalide d'apres
    le code de la fonction qu'il decore, PAS d'apres celui des fonctions
    appelees. Une correction apportee a normaliser() ne changeait pas le corps de
    load_data(), donc Streamlit Cloud servait les anciennes donnees avec les
    nouveaux libelles apres un simple rechargement du script.

    Seules les lectures de fichiers restent cachees, la ou le cout est reel. La
    normalisation est une transformation pure sur 1400 lignes : la rejouer a
    chaque execution ne coute rien et supprime toute une classe de peremption.
    """
    obt, listings, hosts, source = load_from_extract()
    if source == "absent":
        obt, listings, hosts, source = generate_demo_data()
    obt, listings, hosts = normaliser(obt, listings, hosts)
    return obt, listings, hosts, source


# ─────────────────────────────────────────────
# OUTILS
# ─────────────────────────────────────────────
def euro(v):
    return f"€{v:,.0f}".replace(",", " ")


def pourcent(v):
    return f"{v:.1f} %"


def manquant(df, colonnes, section):
    """Annonce franchement une section degradee plutot que de lever une exception."""
    absentes = [c for c in colonnes if c not in df.columns]
    if absentes:
        st.info(
            f"Section « {section} » indisponible : la couche Gold ne fournit pas "
            f"{', '.join('`' + c + '`' for c in absentes)}."
        )
        return True
    return False


# Libellés français des colonnes, appliqués aux axes et aux légendes.
LIBELLES = {
    "BOOKING_MONTH_STR": "Mois",
    "BOOKING_STATUS": "Statut",
    "STATUT": "Statut",
    "TOTAL_BOOKING_VALUE": "Valeur totale",
    "BOOKING_AMOUNT": "Montant du séjour",
    "NIGHTS_BOOKED": "Nuits réservées",
    "PRICE_PER_NIGHT": "Prix à la nuit",
    "PRICE_PER_NIGHT_TAG": "Gamme de prix",
    "ACCOMMODATES": "Capacité d'accueil",
    "PROPERTY_TYPE": "Type de bien",
    "ROOM_TYPE": "Type de chambre",
    "CITY": "Ville",
    "RESPONSE_RATE": "Taux de réponse",
    "HOST_RESPONSE_SEGMENT": "Segment",
    "HOST_ID": "Hôte",
    "HOST_NAME": "Nom de l'hôte",
    "reservations": "Réservations",
    "ca": "Chiffre d'affaires",
    "segment": "Segment",
    "hotes": "Hôtes",
}


def styliser(fig, titre):
    fig.update_layout(title=titre, colorway=SEQUENCE, **PLOT_LAYOUT)
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(128,128,128,.18)")
    return fig


# ─────────────────────────────────────────────
# BANDEAU DE SOURCE
# ─────────────────────────────────────────────
def bandeau_source(source, obt):
    if source == "extrait":
        nb = f"{len(obt):,}".replace(",", " ")
        st.markdown(
            f'<div class="source-real"><b>Extrait réel de la couche Gold</b> — '
            f"{nb} réservations, produites par les modèles dbt de ce dépôt "
            f"sur un jeu de données synthétique. Aucune donnée client.</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div class="source-demo"><b>Données de démonstration générées</b> — '
            "l'extrait Gold n'est pas présent dans <code>streamlit/data/</code>. "
            "Les chiffres ci-dessous ne proviennent d'aucun calcul dbt. Le schéma, lui, "
            "est bien celui de la couche Gold.</div>",
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────────
# FILTRES
# ─────────────────────────────────────────────
def render_sidebar(obt, source):
    with st.sidebar:
        st.markdown("### 🏠 Airbnb — Gold layer")
        st.caption("AWS S3 → Snowpipe → Snowflake → dbt → Streamlit")
        st.divider()

        st.markdown(
            f"**Source :** {'extrait Gold' if source == 'extrait' else 'démonstration'}"
        )
        st.divider()
        st.markdown("### Filtres")

        filtres = {}
        if "BOOKING_DATE" in obt and obt["BOOKING_DATE"].notna().any():
            bornes = (obt["BOOKING_DATE"].min().date(), obt["BOOKING_DATE"].max().date())
            filtres["periode"] = st.date_input("Période", value=bornes,
                                               min_value=bornes[0], max_value=bornes[1])
        for col, libelle in [("CITY", "Ville"), ("PROPERTY_TYPE", "Type de bien"),
                             ("STATUT", "Statut")]:
            if col in obt:
                valeurs = sorted(obt[col].dropna().unique().tolist())
                filtres[col] = st.multiselect(libelle, valeurs, default=valeurs)

        st.divider()
        st.caption(
            "Les filtres s'appliquent à tous les onglets. "
            "Le taux d'annulation est calculé avant le filtrage sur le statut."
        )
        return filtres


def appliquer_filtres(obt, filtres):
    df = obt
    periode = filtres.get("periode")
    if periode and isinstance(periode, (list, tuple)) and len(periode) == 2:
        debut, fin = pd.Timestamp(periode[0]), pd.Timestamp(periode[1]) + pd.Timedelta(days=1)
        df = df[(df["BOOKING_DATE"] >= debut) & (df["BOOKING_DATE"] < fin)]
    for col in ("CITY", "PROPERTY_TYPE", "STATUT"):
        if col in filtres and col in df.columns and filtres[col]:
            df = df[df[col].isin(filtres[col])]
    return df


# ─────────────────────────────────────────────
# INDICATEURS
# ─────────────────────────────────────────────
def render_kpis(df, avant_statut):
    confirmees = df[df["BOOKING_STATUS"] == "confirmed"] if "BOOKING_STATUS" in df else df

    ca = confirmees["TOTAL_BOOKING_VALUE"].sum() if "TOTAL_BOOKING_VALUE" in confirmees else 0
    net = confirmees["NET_REVENUE"].sum() if "NET_REVENUE" in confirmees else 0
    panier = confirmees["TOTAL_BOOKING_VALUE"].mean() if len(confirmees) else 0
    nuits = confirmees["NIGHTS_BOOKED"].mean() if "NIGHTS_BOOKED" in confirmees and len(confirmees) else 0
    annulation = (
        (avant_statut["BOOKING_STATUS"] == "cancelled").mean() * 100
        if "BOOKING_STATUS" in avant_statut and len(avant_statut) else 0
    )

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Chiffre d'affaires", euro(ca), help="Réservations confirmées, frais inclus")
    c2.metric("Revenu net", euro(net), help="Montant du séjour moins les frais")
    c3.metric("Réservations", f"{len(confirmees):,}".replace(",", " "))
    c4.metric("Panier moyen", euro(panier))
    c5.metric("Taux d'annulation", pourcent(annulation),
              help="Calculé avant le filtre sur le statut, sinon il vaudrait 0 ou 100 %")
    st.caption(f"Durée moyenne du séjour : {nuits:.1f} nuits")


# ─────────────────────────────────────────────
# ONGLETS
# ─────────────────────────────────────────────
def onglet_temporel(df):
    if manquant(df, ["BOOKING_MONTH_STR", "TOTAL_BOOKING_VALUE"], "Évolution temporelle"):
        return
    confirmees = df[df["BOOKING_STATUS"] == "confirmed"] if "BOOKING_STATUS" in df else df

    mensuel = (
        confirmees.groupby("BOOKING_MONTH_STR")
        .agg(ca=("TOTAL_BOOKING_VALUE", "sum"), reservations=("BOOKING_ID", "count"))
        .reset_index()
        .sort_values("BOOKING_MONTH_STR")
    )
    fig = go.Figure()
    fig.add_bar(x=mensuel["BOOKING_MONTH_STR"], y=mensuel["ca"],
                name="Chiffre d'affaires", marker_color=C_PRIMARY)
    fig.add_scatter(x=mensuel["BOOKING_MONTH_STR"], y=mensuel["reservations"],
                    name="Réservations", yaxis="y2", mode="lines+markers",
                    line=dict(color=C_SECOND, width=3))
    fig.update_layout(
        xaxis_title="Mois",
        yaxis_title="Chiffre d'affaires (€)",
        yaxis2=dict(overlaying="y", side="right", showgrid=False, title="Réservations"),
    )
    st.plotly_chart(styliser(fig, "Chiffre d'affaires et volume par mois"),
                    use_container_width=True)

    if "STATUT" in df:
        statut = (
            df.groupby(["BOOKING_MONTH_STR", "STATUT"])
            .size().reset_index(name="reservations")
        )
        fig2 = px.bar(statut, x="BOOKING_MONTH_STR", y="reservations", color="STATUT",
                      labels=LIBELLES,
                      color_discrete_map={"Confirmée": C_SECOND, "Annulée": C_ACCENT})
        st.plotly_chart(styliser(fig2, "Confirmations et annulations par mois"),
                        use_container_width=True)


def onglet_logements(df, listings):
    colonnes = [c for c in ["CITY", "PROPERTY_TYPE", "ROOM_TYPE"] if c in df.columns]
    if not colonnes:
        st.info("Aucun attribut de logement disponible dans l'extrait.")
        return
    confirmees = df[df["BOOKING_STATUS"] == "confirmed"] if "BOOKING_STATUS" in df else df

    g1, g2 = st.columns(2)
    if "CITY" in confirmees and "TOTAL_BOOKING_VALUE" in confirmees:
        par_ville = (
            confirmees.groupby("CITY")["TOTAL_BOOKING_VALUE"].sum()
            .sort_values(ascending=True).reset_index()
        )
        fig = px.bar(par_ville, x="TOTAL_BOOKING_VALUE", y="CITY", orientation="h",
                     labels=LIBELLES)
        fig.update_traces(marker_color=C_PRIMARY)
        g1.plotly_chart(styliser(fig, "Chiffre d'affaires par ville"), use_container_width=True)

    if "PROPERTY_TYPE" in confirmees:
        repartition = confirmees["PROPERTY_TYPE"].value_counts().reset_index()
        repartition.columns = ["PROPERTY_TYPE", "reservations"]
        fig = px.pie(repartition, names="PROPERTY_TYPE", values="reservations", hole=0.55,
                     labels=LIBELLES)
        g2.plotly_chart(styliser(fig, "Répartition par type de bien"), use_container_width=True)

    if listings is not None and {"PRICE_PER_NIGHT", "ACCOMMODATES"} <= set(listings.columns):
        fig = px.scatter(
            listings, x="ACCOMMODATES", y="PRICE_PER_NIGHT", labels=LIBELLES,
            color="PRICE_PER_NIGHT_TAG" if "PRICE_PER_NIGHT_TAG" in listings else None,
            hover_data=[c for c in ["CITY", "PROPERTY_TYPE"] if c in listings.columns],
        )
        st.plotly_chart(styliser(fig, "Prix à la nuit selon la capacité d'accueil"),
                        use_container_width=True)
    else:
        st.info(
            "Le nuage prix / capacité demande `PRICE_PER_NIGHT` dans `DIM_LISTINGS`. "
            "Cette colonne est calculée en silver ; vérifie qu'elle est bien remontée "
            "dans le modèle `listings`."
        )


def onglet_hotes(df, hosts):
    if hosts is None or hosts.empty:
        st.info("Dimension hôtes indisponible.")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Hôtes", f"{len(hosts):,}".replace(",", " "))
    if "IS_SUPERHOST" in hosts:
        c2.metric("Superhosts", pourcent(hosts["IS_SUPERHOST"].mean() * 100))
    if "RESPONSE_RATE" in hosts:
        c3.metric("Taux de réponse moyen", pourcent(hosts["RESPONSE_RATE"].mean()))

    g1, g2 = st.columns(2)
    if "HOST_RESPONSE_SEGMENT" in hosts:
        seg = hosts["HOST_RESPONSE_SEGMENT"].value_counts().reset_index()
        seg.columns = ["segment", "hotes"]
        fig = px.bar(seg, x="segment", y="hotes", labels=LIBELLES)
        fig.update_traces(marker_color=C_SECOND)
        g1.plotly_chart(styliser(fig, "Segmentation par qualité de réponse"),
                        use_container_width=True)

    if "RESPONSE_RATE" in hosts:
        fig = px.histogram(hosts, x="RESPONSE_RATE", nbins=20, labels=LIBELLES)
        fig.update_traces(marker_color=C_PRIMARY)
        fig.update_layout(yaxis_title="Hôtes")
        g2.plotly_chart(styliser(fig, "Distribution du taux de réponse"),
                        use_container_width=True)
    else:
        g2.info(
            "`RESPONSE_RATE` absente de `DIM_HOSTS`. Elle existe en silver : "
            "vérifie qu'elle est remontée dans le modèle `hosts`."
        )

    if "HOST_ID" in df and "TOTAL_BOOKING_VALUE" in df:
        top = (
            df.groupby("HOST_ID")["TOTAL_BOOKING_VALUE"].sum()
            .sort_values(ascending=False).head(10).reset_index()
        )
        if "HOST_NAME" in hosts:
            top = top.merge(hosts[["HOST_ID", "HOST_NAME"]], on="HOST_ID", how="left")
        top = top.rename(columns={"HOST_ID": "Hôte", "HOST_NAME": "Nom",
                                  "TOTAL_BOOKING_VALUE": "Chiffre d'affaires"})
        st.markdown("##### Dix premiers hôtes par chiffre d'affaires")
        st.dataframe(top, use_container_width=True, hide_index=True)


def onglet_revenu(df):
    if manquant(df, ["BOOKING_AMOUNT", "TOTAL_FEES", "NET_REVENUE"], "Structure du revenu"):
        return
    confirmees = df[df["BOOKING_STATUS"] == "confirmed"] if "BOOKING_STATUS" in df else df

    montant = confirmees["BOOKING_AMOUNT"].sum()
    menage = confirmees["CLEANING_FEE"].sum() if "CLEANING_FEE" in confirmees else 0
    service = confirmees["SERVICE_FEE"].sum() if "SERVICE_FEE" in confirmees else 0

    fig = go.Figure(go.Waterfall(
        orientation="v",
        measure=["absolute", "relative", "relative", "total"],
        x=["Montant des séjours", "Frais de ménage", "Frais de service", "Valeur totale"],
        y=[montant, menage, service, 0],
        connector=dict(line=dict(color=C_NEUTRAL)),
        increasing=dict(marker=dict(color=C_SECOND)),
        totals=dict(marker=dict(color=C_PRIMARY)),
    ))
    fig.update_layout(yaxis_title="Euros")
    st.plotly_chart(styliser(fig, "Décomposition de la valeur encaissée"),
                    use_container_width=True)

    if "NIGHTS_BOOKED" in confirmees:
        fig2 = px.box(confirmees, x="NIGHTS_BOOKED", y="TOTAL_BOOKING_VALUE",
                      points=False, labels=LIBELLES)
        fig2.update_traces(marker_color=C_SECOND)
        st.plotly_chart(styliser(fig2, "Valeur d'une réservation selon la durée du séjour"),
                        use_container_width=True)


# ─────────────────────────────────────────────
# PAGE
# ─────────────────────────────────────────────
def main():
    obt, listings, hosts, source = load_data()

    st.title("Airbnb Modern Data Pipeline")
    st.caption("Couche Gold — indicateurs de réservation, de logement et d'hôte")
    bandeau_source(source, obt)

    filtres = render_sidebar(obt, source)
    # Le taux d'annulation se calcule sur un perimetre ou le statut n'est PAS filtre,
    # sinon il vaut mecaniquement 0 % ou 100 %.
    sans_statut = appliquer_filtres(obt, {k: v for k, v in filtres.items() if k != "STATUT"})
    df = appliquer_filtres(obt, filtres)

    if df.empty:
        st.warning("Aucune réservation ne correspond aux filtres.")
        return

    render_kpis(df, sans_statut)
    st.divider()

    t1, t2, t3, t4 = st.tabs(["Évolution", "Logements", "Hôtes", "Revenu"])
    with t1:
        onglet_temporel(df)
    with t2:
        onglet_logements(df, listings)
    with t3:
        onglet_hotes(df, hosts)
    with t4:
        onglet_revenu(df)

    st.divider()
    st.caption(
        "Malek Abbar · Data Engineer · Snowflake | dbt · ex-Informatica — "
        "pipeline AWS S3, Snowpipe, Snowflake, dbt, GitHub Actions."
    )


if __name__ == "__main__":
    main()
