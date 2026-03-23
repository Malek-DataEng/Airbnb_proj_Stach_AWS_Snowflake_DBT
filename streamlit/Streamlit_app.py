"""
Airbnb Modern Data Pipeline — Streamlit Portfolio App
======================================================
Connexion : Snowflake (live) avec fallback CSV
Style     : Light mode corporate
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime
import os

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Airbnb Data Pipeline | Portfolio",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# THEME / CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
    /* Main background */
    .main { background-color: #F8FAFC; }
    .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }

    /* Header banner */
    .header-banner {
        background: linear-gradient(135deg, #1E40AF 0%, #2563EB 50%, #3B82F6 100%);
        padding: 2rem 2.5rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
        color: white;
    }
    .header-banner h1 { color: white; margin: 0; font-size: 2rem; font-weight: 700; }
    .header-banner p  { color: #BFDBFE; margin: 0.4rem 0 0; font-size: 1rem; }

    /* KPI cards */
    .kpi-card {
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: 1.2rem 1.5rem;
        text-align: center;
        box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    }
    .kpi-value { font-size: 2rem; font-weight: 700; color: #1E40AF; margin: 0; }
    .kpi-label { font-size: 0.8rem; color: #64748B; margin: 0.2rem 0 0; text-transform: uppercase; letter-spacing: .05em; }
    .kpi-delta { font-size: 0.8rem; color: #16A34A; font-weight: 600; }

    /* Section titles */
    .section-title {
        font-size: 1.1rem; font-weight: 700; color: #1E293B;
        border-left: 4px solid #2563EB;
        padding-left: 0.75rem;
        margin: 1.5rem 0 1rem;
    }

    /* Badge pills */
    .badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    .badge-elite   { background:#DCFCE7; color:#16A34A; }
    .badge-good    { background:#DBEAFE; color:#1D4ED8; }
    .badge-low     { background:#FEE2E2; color:#DC2626; }
    .badge-budget  { background:#F0FDF4; color:#15803D; }
    .badge-mid     { background:#EFF6FF; color:#1D4ED8; }
    .badge-luxury  { background:#FDF4FF; color:#9333EA; }

    /* Sidebar */
    .css-1d391kg { background-color: #F1F5F9; }

    /* Footer */
    .footer {
        text-align: center; color: #94A3B8;
        font-size: 0.78rem; margin-top: 3rem;
        padding-top: 1rem; border-top: 1px solid #E2E8F0;
    }

    /* Source badge */
    .source-badge {
        background: #F0FDF4; border: 1px solid #BBF7D0;
        color: #15803D; border-radius: 6px;
        padding: 4px 10px; font-size: 0.75rem; font-weight: 600;
    }
    .source-badge-csv {
        background: #FFF7ED; border: 1px solid #FED7AA;
        color: #C2410C; border-radius: 6px;
        padding: 4px 10px; font-size: 0.75rem; font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_from_snowflake():
    """
    Load from GOLD layer only:
      - GOLD.OBT_BOOKINGS_ANALYTICS  : table denormalisee principale (bookings + listings + hosts)
      - GOLD.DIM_HOSTS               : dimension hosts
      - GOLD.DIM_LISTINGS            : dimension listings
    """
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"],
            schema="GOLD",
        )
        cur = conn.cursor()

        # OBT = source principale pour toutes les analyses croisees
        cur.execute("SELECT * FROM GOLD.OBT_BOOKINGS_ANALYTICS")
        obt = cur.fetch_pandas_all()

        cur.execute("SELECT * FROM GOLD.DIM_HOSTS")
        hosts = cur.fetch_pandas_all()

        cur.execute("SELECT * FROM GOLD.DIM_LISTINGS")
        listings = cur.fetch_pandas_all()

        conn.close()
        return obt, listings, hosts, "snowflake"
    except Exception:
        return None, None, None, "failed"


@st.cache_data
def load_from_csv():
    """
    Fallback CSV demo — colonnes alignees sur GOLD.OBT_BOOKINGS_ANALYTICS.
    Toutes les analyses utilisent l OBT comme source unique.
    DIM_HOSTS et DIM_LISTINGS sont extraits depuis l OBT (deduplication).
    """
    np.random.seed(42)
    n = 500

    cities        = ["Paris", "Lyon", "Marseille", "Nice", "Bordeaux", "Toulouse"]
    countries     = ["France"]
    prop_types    = ["Apartment", "House", "Villa", "Studio", "Loft"]
    room_types    = ["Entire home", "Private room", "Shared room"]
    statuses      = ["confirmed", "cancelled"]
    segments      = ["ELITE", "GOOD", "LOW"]
    price_tags    = ["BUDGET", "MID_RANGE", "LUXURY"]
    host_names    = [f"Host_{i}" for i in range(1, 51)]

    booking_dates = pd.to_datetime(
        np.random.choice(pd.date_range("2023-01-01", "2024-12-31"), n)
    )

    booking_amounts = np.random.randint(100, 3000, n).astype(float)
    nights          = np.random.randint(1, 14, n)
    cleaning        = np.random.randint(20, 100, n).astype(float)
    service         = np.random.randint(10, 60, n).astype(float)
    total_fees      = cleaning + service

    # OBT — table denormalisee principale (schema = GOLD.OBT_BOOKINGS_ANALYTICS)
    obt = pd.DataFrame({
        "BOOKING_ID":               [str(i) for i in range(1, n + 1)],
        "BOOKING_DATE":             booking_dates,
        "BOOKING_YEAR":             booking_dates.year,
        "BOOKING_MONTH":            booking_dates.month,
        "BOOKING_WEEK":             booking_dates.isocalendar().week.values,
        "BOOKING_AMOUNT":           booking_amounts,
        "NIGHTS_BOOKED":            nights,
        "BOOKING_PRICE_PER_NIGHT":  np.round(booking_amounts / nights, 2),
        "CLEANING_FEE":             cleaning,
        "SERVICE_FEE":              service,
        "TOTAL_FEES":               total_fees,
        "TOTAL_BOOKING_VALUE":      booking_amounts + total_fees,
        "NET_REVENUE":              booking_amounts - total_fees,
        "BOOKING_STATUS":           np.random.choice(statuses, n, p=[0.85, 0.15]),
        "LISTING_ID":               np.random.randint(1, 101, n),
        "HOST_ID":                  np.random.randint(1, 51, n),
        "PROPERTY_TYPE":            np.random.choice(prop_types, n),
        "ROOM_TYPE":                np.random.choice(room_types, n),
        "CITY":                     np.random.choice(cities, n),
        "COUNTRY":                  "France",
        "ACCOMMODATES":             np.random.randint(1, 8, n),
        "BEDROOMS":                 np.random.randint(1, 5, n),
        "BATHROOMS":                np.random.randint(1, 3, n),
        "PRICE_PER_NIGHT_TAG":      np.random.choice(price_tags, n, p=[0.4, 0.4, 0.2]),
        "HOST_NAME":                np.random.choice(host_names, n),
        "HOST_SINCE":               pd.to_datetime(
            np.random.choice(pd.date_range("2015-01-01", "2022-01-01"), n)
        ),
        "IS_SUPERHOST":             np.random.choice([True, False], n, p=[0.35, 0.65]),
        "HOST_RESPONSE_SEGMENT":    np.random.choice(segments, n, p=[0.3, 0.4, 0.3]),
    })
    obt["BOOKING_MONTH_STR"] = obt["BOOKING_DATE"].dt.to_period("M").astype(str)

    # DIM_HOSTS : deduplique depuis l OBT
    hosts = (
        obt[["HOST_ID", "HOST_NAME", "HOST_SINCE", "IS_SUPERHOST", "HOST_RESPONSE_SEGMENT"]]
        .drop_duplicates(subset="HOST_ID")
        .reset_index(drop=True)
    )
    hosts["RESPONSE_RATE"]     = np.random.randint(60, 100, len(hosts))
    hosts["HOST_TENURE_YEARS"] = (
        (pd.Timestamp("today") - hosts["HOST_SINCE"]).dt.days // 365
    )

    # DIM_LISTINGS : deduplique depuis l OBT
    listings = (
        obt[["LISTING_ID", "HOST_ID", "PROPERTY_TYPE", "ROOM_TYPE",
             "CITY", "COUNTRY", "ACCOMMODATES", "BEDROOMS", "BATHROOMS",
             "PRICE_PER_NIGHT_TAG"]]
        .drop_duplicates(subset="LISTING_ID")
        .reset_index(drop=True)
    )
    listings["PRICE_PER_NIGHT"] = np.random.randint(40, 400, len(listings)).astype(float)
    listings["BEDROOM_DENSITY"] = np.round(
        listings["BEDROOMS"] / listings["ACCOMMODATES"].replace(0, 1), 2
    )
    listings["PRICE_PER_PERSON"] = np.random.randint(20, 120, len(listings)).astype(float)

    return obt, listings, hosts, "csv"


def load_data():
    """Load data — Snowflake Gold (live) avec fallback CSV demo."""
    obt, listings, hosts, source = load_from_snowflake()
    if source == "failed":
        obt, listings, hosts, source = load_from_csv()
    # Normalize column names to uppercase
    for df in [obt, listings, hosts]:
        df.columns = [c.upper() for c in df.columns]
    # Alias: bookings = obt pour la compatibilite des fonctions de rendu
    bookings = obt
    return bookings, listings, hosts, source


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
BLUE_PALETTE   = ["#1E40AF", "#2563EB", "#3B82F6", "#60A5FA", "#93C5FD", "#BFDBFE"]
STATUS_COLORS  = {"confirmed": "#16A34A", "cancelled": "#DC2626"}
SEGMENT_COLORS = {"ELITE": "#16A34A", "GOOD": "#2563EB", "LOW": "#DC2626"}
TAG_COLORS     = {"BUDGET": "#15803D", "MID_RANGE": "#1D4ED8", "LUXURY": "#9333EA"}

def fmt_currency(v): return f"€{v:,.0f}"
def fmt_pct(v):      return f"{v:.1f}%"


# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
def render_sidebar(bookings, listings, hosts, source):
    with st.sidebar:
        st.markdown("### 🏠 Airbnb Pipeline")
        st.markdown("**Portfolio Demo** — Malek")
        st.markdown("---")

        # Data source badge
        if source == "snowflake":
            st.markdown('<span class="source-badge">🟢 Live — Snowflake</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="source-badge-csv">🟠 Demo — CSV Fallback</span>', unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("**🔗 Links**")
        st.markdown("[📦 GitHub Repo](https://github.com/Malek-DataEng/Airbnb_proj_Stach_AWS_Snowflake_DBT)")
        st.markdown("[📚 dbt Docs](https://malek-dataeng.github.io/Airbnb_proj_Stach_AWS_Snowflake_DBT/)")
        st.markdown("[💼 LinkedIn](https://linkedin.com/in/malek-a-964758201)")

        st.markdown("---")
        st.markdown("**⚙️ Stack**")
        st.markdown("""
        `AWS S3` · `Snowpipe` · `Snowflake`  
        `dbt` · `GitHub Actions` · `Python`
        """)

        st.markdown("---")
        # Filters
        st.markdown("**🔍 Filters**")
        cities = ["All"] + sorted(listings["CITY"].unique().tolist())
        selected_city = st.selectbox("City", cities)

        statuses = ["All", "confirmed", "cancelled"]
        selected_status = st.selectbox("Booking Status", statuses)

        return selected_city, selected_status


# ─────────────────────────────────────────────
# SECTION 1 — KPI OVERVIEW
# ─────────────────────────────────────────────
def render_kpis(bookings, listings, hosts):
    st.markdown('<div class="section-title">📊 Global KPIs</div>', unsafe_allow_html=True)

    confirmed = bookings[bookings["BOOKING_STATUS"] == "confirmed"]
    total_revenue   = confirmed["NET_REVENUE"].sum()
    total_bookings  = len(bookings)
    avg_price_night = confirmed["BOOKING_PRICE_PER_NIGHT"].mean()
    superhost_pct   = hosts["IS_SUPERHOST"].mean() * 100
    cancellation    = (bookings["BOOKING_STATUS"] == "cancelled").mean() * 100
    avg_nights      = bookings["NIGHTS_BOOKED"].mean()

    cols = st.columns(6)
    kpis = [
        ("€{:,.0f}".format(total_revenue),   "Net Revenue",          "Confirmed bookings"),
        (f"{total_bookings:,}",               "Total Bookings",       f"{len(confirmed):,} confirmed"),
        (f"{len(listings):,}",                "Active Listings",      f"{listings['CITY'].nunique()} cities"),
        (f"{len(hosts):,}",                   "Hosts",                f"{superhost_pct:.0f}% Superhosts"),
        ("€{:.0f}".format(avg_price_night),   "Avg Price / Night",    "Confirmed only"),
        (f"{cancellation:.1f}%",              "Cancellation Rate",    f"Avg {avg_nights:.1f} nights"),
    ]
    for col, (val, label, sub) in zip(cols, kpis):
        with col:
            st.markdown(f"""
            <div class="kpi-card">
                <p class="kpi-value">{val}</p>
                <p class="kpi-label">{label}</p>
                <p class="kpi-delta">{sub}</p>
            </div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# SECTION 2 — TEMPORAL ANALYSIS
# ─────────────────────────────────────────────
def render_temporal(bookings):
    st.markdown('<div class="section-title">📅 Booking Trends Over Time</div>', unsafe_allow_html=True)

    monthly = (
        bookings.groupby(["BOOKING_MONTH_STR", "BOOKING_STATUS"])
        .agg(count=("BOOKING_ID", "count"), revenue=("NET_REVENUE", "sum"))
        .reset_index()
    )

    col1, col2 = st.columns(2)

    with col1:
        confirmed_m = monthly[monthly["BOOKING_STATUS"] == "confirmed"]
        fig = px.area(
            confirmed_m, x="BOOKING_MONTH_STR", y="revenue",
            title="Monthly Net Revenue (Confirmed)",
            color_discrete_sequence=["#2563EB"],
            labels={"revenue": "Net Revenue (€)", "MONTH": "Month"},
        )
        fig.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#1E293B", title_font_size=13,
            margin=dict(t=40, b=20, l=10, r=10),
            xaxis=dict(showgrid=False), yaxis=dict(gridcolor="#F1F5F9"),
        )
        fig.update_traces(fillcolor="rgba(37,99,235,0.12)", line_color="#2563EB")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        pivot = monthly.pivot_table(
            index="BOOKING_MONTH_STR", columns="BOOKING_STATUS", values="count", fill_value=0
        ).reset_index()
        fig2 = go.Figure()
        for status, color in STATUS_COLORS.items():
            if status in pivot.columns:
                fig2.add_trace(go.Bar(
                    x=pivot["BOOKING_MONTH_STR"], y=pivot[status],
                    name=status.capitalize(), marker_color=color
                ))
        fig2.update_layout(
            title="Monthly Bookings by Status",
            barmode="stack", plot_bgcolor="white", paper_bgcolor="white",
            font_color="#1E293B", title_font_size=13,
            margin=dict(t=40, b=20, l=10, r=10),
            xaxis=dict(showgrid=False), yaxis=dict(gridcolor="#F1F5F9"),
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(fig2, use_container_width=True)


# ─────────────────────────────────────────────
# SECTION 3 — HOST ANALYSIS
# ─────────────────────────────────────────────
def render_hosts(hosts):
    st.markdown('<div class="section-title">👤 Host Performance Analysis</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)

    with col1:
        seg_counts = hosts["HOST_RESPONSE_SEGMENT"].value_counts().reset_index()
        seg_counts.columns = ["Segment", "Count"]
        fig = px.pie(
            seg_counts, names="Segment", values="Count",
            title="Host Response Segmentation",
            color="Segment",
            color_discrete_map=SEGMENT_COLORS,
            hole=0.45,
        )
        fig.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#1E293B", title_font_size=13,
            margin=dict(t=40, b=10, l=10, r=10),
            legend=dict(orientation="h", y=-0.1),
        )
        fig.update_traces(textposition="outside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = px.histogram(
            hosts, x="RESPONSE_RATE", nbins=20,
            title="Response Rate Distribution",
            color_discrete_sequence=["#2563EB"],
            labels={"RESPONSE_RATE": "Response Rate (%)"},
        )
        fig2.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#1E293B", title_font_size=13,
            margin=dict(t=40, b=20, l=10, r=10),
            xaxis=dict(showgrid=False), yaxis=dict(gridcolor="#F1F5F9"),
        )
        st.plotly_chart(fig2, use_container_width=True)

    with col3:
        tenure_seg = hosts.groupby("HOST_RESPONSE_SEGMENT")["HOST_TENURE_YEARS"].mean().reset_index()
        fig3 = px.bar(
            tenure_seg, x="HOST_RESPONSE_SEGMENT", y="HOST_TENURE_YEARS",
            title="Avg Tenure by Segment (years)",
            color="HOST_RESPONSE_SEGMENT",
            color_discrete_map=SEGMENT_COLORS,
            labels={"HOST_TENURE_YEARS": "Years", "HOST_RESPONSE_SEGMENT": "Segment"},
        )
        fig3.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#1E293B", title_font_size=13,
            margin=dict(t=40, b=20, l=10, r=10),
            showlegend=False,
            xaxis=dict(showgrid=False), yaxis=dict(gridcolor="#F1F5F9"),
        )
        st.plotly_chart(fig3, use_container_width=True)

    # Top hosts table
    st.markdown("**🏆 Top 10 Hosts by Response Rate**")
    top_hosts = (
        hosts.sort_values("RESPONSE_RATE", ascending=False)
        .head(10)[["HOST_NAME", "HOST_RESPONSE_SEGMENT", "RESPONSE_RATE",
                   "IS_SUPERHOST", "HOST_TENURE_YEARS"]]
        .rename(columns={
            "HOST_NAME": "Host", "HOST_RESPONSE_SEGMENT": "Segment",
            "RESPONSE_RATE": "Response Rate (%)",
            "IS_SUPERHOST": "Superhost", "HOST_TENURE_YEARS": "Tenure (yrs)"
        })
    )
    st.dataframe(top_hosts, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────
# SECTION 4 — LISTINGS ANALYSIS
# ─────────────────────────────────────────────
def render_listings(listings):
    st.markdown('<div class="section-title">🏠 Listings Analytics</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)

    with col1:
        city_avg = listings.groupby("CITY")["PRICE_PER_NIGHT"].mean().sort_values(ascending=True).reset_index()
        fig = px.bar(
            city_avg, x="PRICE_PER_NIGHT", y="CITY",
            orientation="h",
            title="Avg Price per Night by City (€)",
            color="PRICE_PER_NIGHT",
            color_continuous_scale=["#BFDBFE", "#1E40AF"],
            labels={"PRICE_PER_NIGHT": "Avg Price (€)", "CITY": ""},
        )
        fig.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#1E293B", title_font_size=13,
            margin=dict(t=40, b=20, l=10, r=10),
            coloraxis_showscale=False,
            xaxis=dict(gridcolor="#F1F5F9"), yaxis=dict(showgrid=False),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        tag_counts = listings["PRICE_PER_NIGHT_TAG"].value_counts().reset_index()
        tag_counts.columns = ["Tag", "Count"]
        fig2 = px.pie(
            tag_counts, names="Tag", values="Count",
            title="Price Segmentation",
            color="Tag",
            color_discrete_map=TAG_COLORS,
            hole=0.45,
        )
        fig2.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#1E293B", title_font_size=13,
            margin=dict(t=40, b=10, l=10, r=10),
            legend=dict(orientation="h", y=-0.1),
        )
        fig2.update_traces(textposition="outside", textinfo="percent+label")
        st.plotly_chart(fig2, use_container_width=True)

    with col3:
        prop_counts = listings["PROPERTY_TYPE"].value_counts().reset_index()
        prop_counts.columns = ["Type", "Count"]
        fig3 = px.bar(
            prop_counts, x="Type", y="Count",
            title="Listings by Property Type",
            color="Count",
            color_continuous_scale=["#BFDBFE", "#1E40AF"],
            labels={"Count": "# Listings", "Type": ""},
        )
        fig3.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#1E293B", title_font_size=13,
            margin=dict(t=40, b=20, l=10, r=10),
            coloraxis_showscale=False,
            xaxis=dict(showgrid=False), yaxis=dict(gridcolor="#F1F5F9"),
        )
        st.plotly_chart(fig3, use_container_width=True)

    # Price vs accommodates scatter
    st.markdown("**💰 Price per Person vs Bedroom Density**")
    fig4 = px.scatter(
        listings, x="BEDROOM_DENSITY", y="PRICE_PER_PERSON",
        color="PRICE_PER_NIGHT_TAG", size="ACCOMMODATES",
        hover_data=["CITY", "PROPERTY_TYPE"],
        color_discrete_map=TAG_COLORS,
        labels={
            "BEDROOM_DENSITY": "Bedroom Density (bedrooms/accommodates)",
            "PRICE_PER_PERSON": "Price per Person (€)",
            "PRICE_PER_NIGHT_TAG": "Segment",
        },
        title="Listing Positioning Map",
    )
    fig4.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        font_color="#1E293B", title_font_size=13,
        margin=dict(t=40, b=20, l=10, r=10),
        xaxis=dict(gridcolor="#F1F5F9"), yaxis=dict(gridcolor="#F1F5F9"),
        legend=dict(orientation="h", y=-0.15),
    )
    st.plotly_chart(fig4, use_container_width=True)


# ─────────────────────────────────────────────
# SECTION 5 — REVENUE DEEP DIVE
# ─────────────────────────────────────────────
def render_revenue(bookings):
    st.markdown('<div class="section-title">💰 Revenue Engineering</div>', unsafe_allow_html=True)

    confirmed = bookings[bookings["BOOKING_STATUS"] == "confirmed"]

    col1, col2 = st.columns(2)

    with col1:
        fig = go.Figure()
        fig.add_trace(go.Box(
            y=confirmed["BOOKING_AMOUNT"], name="Gross Revenue",
            marker_color="#2563EB", boxmean=True
        ))
        fig.add_trace(go.Box(
            y=confirmed["NET_REVENUE"], name="Net Revenue",
            marker_color="#16A34A", boxmean=True
        ))
        fig.add_trace(go.Box(
            y=confirmed["TOTAL_FEES"], name="Total Fees",
            marker_color="#F59E0B", boxmean=True
        ))
        fig.update_layout(
            title="Revenue Components Distribution",
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#1E293B", title_font_size=13,
            margin=dict(t=40, b=20, l=10, r=10),
            yaxis=dict(gridcolor="#F1F5F9", title="Amount (€)"),
            showlegend=True,
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        nights_rev = (
            confirmed.groupby("NIGHTS_BOOKED")
            .agg(avg_net=("NET_REVENUE", "mean"), count=("BOOKING_ID", "count"))
            .reset_index()
        )
        fig2 = make_subplots(specs=[[{"secondary_y": True}]])
        fig2.add_trace(
            go.Bar(x=nights_rev["NIGHTS_BOOKED"], y=nights_rev["avg_net"],
                   name="Avg Net Revenue (€)", marker_color="#2563EB"),
            secondary_y=False
        )
        fig2.add_trace(
            go.Scatter(x=nights_rev["NIGHTS_BOOKED"], y=nights_rev["count"],
                       name="# Bookings", mode="lines+markers",
                       line=dict(color="#F59E0B", width=2)),
            secondary_y=True
        )
        fig2.update_layout(
            title="Revenue vs # Bookings by Nights Stayed",
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#1E293B", title_font_size=13,
            margin=dict(t=40, b=20, l=10, r=10),
            xaxis=dict(showgrid=False, title="Nights Booked"),
            legend=dict(orientation="h", y=-0.2),
        )
        fig2.update_yaxes(title_text="Avg Net Revenue (€)", gridcolor="#F1F5F9", secondary_y=False)
        fig2.update_yaxes(title_text="# Bookings", secondary_y=True)
        st.plotly_chart(fig2, use_container_width=True)

    # Revenue waterfall
    gross    = confirmed["BOOKING_AMOUNT"].sum()
    fees     = confirmed["TOTAL_FEES"].sum()
    net      = confirmed["NET_REVENUE"].sum()
    cleaning = confirmed["CLEANING_FEE"].sum()
    service  = confirmed["SERVICE_FEE"].sum()

    fig3 = go.Figure(go.Waterfall(
        name="Revenue",
        orientation="v",
        measure=["absolute", "relative", "relative", "total"],
        x=["Gross Revenue", "- Cleaning Fee", "- Service Fee", "Net Revenue"],
        y=[gross, -cleaning, -service, 0],
        connector={"line": {"color": "#CBD5E1"}},
        decreasing={"marker": {"color": "#EF4444"}},
        increasing={"marker": {"color": "#22C55E"}},
        totals={"marker": {"color": "#2563EB"}},
        text=[fmt_currency(gross), fmt_currency(-cleaning),
              fmt_currency(-service), fmt_currency(net)],
        textposition="outside",
    ))
    fig3.update_layout(
        title="Revenue Waterfall — Gross → Net",
        plot_bgcolor="white", paper_bgcolor="white",
        font_color="#1E293B", title_font_size=13,
        margin=dict(t=40, b=20, l=10, r=10),
        yaxis=dict(gridcolor="#F1F5F9", title="Amount (€)"),
        showlegend=False,
    )
    st.plotly_chart(fig3, use_container_width=True)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    # Load data
    bookings, listings, hosts, source = load_data()

    # Header
    st.markdown("""
    <div class="header-banner">
        <h1>🏠 Airbnb Modern Data Pipeline</h1>
        <p>AWS S3 · Snowflake · dbt · GitHub Actions — Portfolio by <strong>Malek</strong></p>
    </div>
    """, unsafe_allow_html=True)

    # Sidebar + filters
    selected_city, selected_status = render_sidebar(bookings, listings, hosts, source)

    # Apply filters
    filtered_bookings = bookings.copy()
    filtered_listings = listings.copy()
    filtered_hosts    = hosts.copy()

    if selected_city != "All":
        city_listing_ids = listings[listings["CITY"] == selected_city]["LISTING_ID"].tolist()
        filtered_bookings = filtered_bookings[filtered_bookings["LISTING_ID"].isin(city_listing_ids)]
        filtered_listings = filtered_listings[filtered_listings["CITY"] == selected_city]

    if selected_status != "All":
        filtered_bookings = filtered_bookings[filtered_bookings["BOOKING_STATUS"] == selected_status]

    # Navigation tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Overview", "📅 Booking Trends", "👤 Hosts", "🏠 Listings & Revenue"
    ])

    with tab1:
        render_kpis(filtered_bookings, filtered_listings, filtered_hosts)
        st.markdown("---")
        # Mini architecture note
        st.markdown('<div class="section-title">🏗️ Pipeline Architecture</div>', unsafe_allow_html=True)
        st.markdown("""
        ```
        AWS S3 ──SQS──▶ Snowpipe ──▶ Staging ──▶ Streams ──▶ Tasks ──▶ Bronze
                                                                            │
                                                                    Control Table
                                                                    (RUN_DBT_FLAG)
                                                                            │
                                                                    GitHub Actions
                                                                            │
                                                                        dbt build
                                                                            │
                                                             Silver ──────────── Snapshots SCD2
                                                                    └──────▶ Fact Bookings ──▶ Gold
        ```
        """)

    with tab2:
        render_temporal(filtered_bookings)

    with tab3:
        render_hosts(filtered_hosts)

    with tab4:
        render_listings(filtered_listings)
        st.markdown("---")
        render_revenue(filtered_bookings)

    # Footer
    st.markdown("""
    <div class="footer">
        Built by <strong>Malek</strong> · Data Engineer ·
        <a href="https://github.com/Malek-DataEng/Airbnb_proj_Stach_AWS_Snowflake_DBT" target="_blank">GitHub</a> ·
        <a href="https://malek-dataeng.github.io/Airbnb_proj_Stach_AWS_Snowflake_DBT/" target="_blank">dbt Docs</a> ·
        <a href="https://linkedin.com/in/malek-a-964758201" target="_blank">LinkedIn</a>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
