import os
import math
from datetime import datetime
import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo import MongoClient

# -------------------- CONFIG --------------------
st.set_page_config(page_title="sample_mflix — Cloud Dashboard", layout="wide")

URI = os.getenv(
    "MONGO_URI",
    # fallback (URL-encoded @ -> %40)
    "mongodb+srv://tejaswisandratest2:password%40123@test2-bd.mongocluster.cosmos.azure.com/sample_mflix"
    "?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"
)
DB_NAME = os.getenv("MONGO_DB", "sample_mflix")

# -------------------- HELPERS --------------------
@st.cache_resource(show_spinner=False)
def get_client():
    # Cosmos requires TLS 1.2+ and SCRAM-SHA-256
    return MongoClient(URI, serverSelectionTimeoutMS=20000)

@st.cache_data(show_spinner=False)
def agg_to_df(col, pipeline):
    db = get_client()[DB_NAME]
    docs = list(db[col].aggregate(pipeline, allowDiskUse=True))
    if not docs:
        return pd.DataFrame()
    return pd.json_normalize(docs)

def safe_year(x):
    try:
        return int(x)
    except Exception:
        return None

# -------------------- SIDEBAR --------------------
st.sidebar.title("Filters")

# Basic metadata
with st.sidebar.expander("Connection", expanded=False):
    st.write(f"**DB**: `{DB_NAME}`")
    st.write("**Host**: Azure Cosmos DB (Mongo API)")

# Year range for charts
years_df = agg_to_df("movies", [
    {"$match": {"year": {"$type": "number"}}},
    {"$group": {"_id": None, "miny": {"$min": "$year"}, "maxy": {"$max": "$year"}}}
])
miny = int(years_df["miny"].iloc[0]) if not years_df.empty else 1900
maxy = int(years_df["maxy"].iloc[0]) if not years_df.empty else 2020
yr_range = st.sidebar.slider("Year range", min_value=miny, max_value=maxy, value=(max(miny, 1930), maxy), step=1)

# Genre filter
genres_df = agg_to_df("movies", [
    {"$unwind": "$genres"},
    {"$group": {"_id": {"$toLower": "$genres"}, "n": {"$sum": 1}}},
    {"$sort": {"n": -1}},
    {"$limit": 30},
    {"$project": {"_id": 0, "genre": "$_id", "n": 1}}
])
genre_opts = genres_df["genre"].tolist() if not genres_df.empty else []
sel_genres = st.sidebar.multiselect("Genres (top 30)", options=genre_opts, default=[])

# -------------------- HEADER --------------------
st.title("🎬 sample_mflix — Cloud Analytics Dashboard")
st.caption("Backed by Azure Cosmos DB for MongoDB (vCore). This dashboard reads live data using MQL aggregations.")

# -------------------- KPI CARDS --------------------
kpi_row = st.container()
with kpi_row:
    col1, col2, col3, col4 = st.columns(4)

    movies_kpi = agg_to_df("movies", [{"$count": "n"}])
    comments_kpi = agg_to_df("comments", [{"$count": "n"}])
    users_kpi = agg_to_df("users", [{"$count": "n"}])

    n_movies = int(movies_kpi["n"].iloc[0]) if not movies_kpi.empty else 0
    n_comments = int(comments_kpi["n"].iloc[0]) if not comments_kpi.empty else 0
    n_users = int(users_kpi["n"].iloc[0]) if not users_kpi.empty else 0

    # distinct directors
    dir_kpi = agg_to_df("movies", [
        {"$unwind": {"path": "$directors", "preserveNullAndEmptyArrays": False}},
        {"$group": {"_id": {"$toLower": "$directors"}}},
        {"$count": "n"}
    ])
    n_directors = int(dir_kpi["n"].iloc[0]) if not dir_kpi.empty else 0

    col1.metric("🎞️ Movies", f"{n_movies:,}")
    col2.metric("💬 Comments", f"{n_comments:,}")
    col3.metric("👥 Users", f"{n_users:,}")
    col4.metric("🎬 Directors", f"{n_directors:,}")

st.markdown("---")

# -------------------- MOVIES PER YEAR --------------------
st.subheader("Movies per Year")
match_stage = {"$match": {"year": {"$type": "number", "$gte": yr_range[0], "$lte": yr_range[1]}}}
if sel_genres:
    match_stage["$match"]["genres"] = {"$in": sel_genres}

per_year = agg_to_df("movies", [
    match_stage,
    {"$group": {"_id": "$year", "titles": {"$sum": 1}}},
    {"$project": {"_id": 0, "year": "$_id", "titles": 1}},
    {"$sort": {"year": 1}}
])
if per_year.empty:
    st.info("No data for chosen filters.")
else:
    fig = px.line(per_year, x="year", y="titles", markers=True, title=None)
    fig.update_layout(height=360, margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

# -------------------- TOP GENRES --------------------
st.subheader("Top Genres")
match_g = {"$match": {"genres": {"$type": "array"}}}
if sel_genres:
    match_g["$match"]["genres"]["$in"] = sel_genres
top_gen = agg_to_df("movies", [
    match_g,
    {"$unwind": "$genres"},
    {"$group": {"_id": {"$toLower": "$genres"}, "n": {"$sum": 1}}},
    {"$project": {"_id": 0, "genre": "$_id", "n": 1}},
    {"$sort": {"n": -1}},
    {"$limit": 15}
])
if not top_gen.empty:
    fig = px.bar(top_gen, x="n", y="genre", orientation="h", title=None)
    fig.update_layout(height=500, margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No genre data for filters.")

# -------------------- RATING DISTRIBUTION --------------------
st.subheader("IMDb Rating Distribution")
rating_match = {"$match": {"imdb.rating": {"$type": "number"}}}
if sel_genres:
    rating_match["$match"]["genres"] = {"$in": sel_genres}
rating_hist = agg_to_df("movies", [
    rating_match,
    {"$project": {"r": "$imdb.rating"}},
    {"$bucket": {
        "groupBy": "$r",
        "boundaries": [i/2 for i in range(0, 21)],  # 0.0..10.0 step 0.5
        "default": "other",
        "output": {"n": {"$sum": 1}}
    }},
    {"$project": {"bucket": {"$toString": "$_id"}, "n": 1, "_id": 0}},
    {"$match": {"bucket": {"$ne": "other"}}},
    {"$sort": {"bucket": 1}}
])
if not rating_hist.empty:
    rating_hist["bucket"] = rating_hist["bucket"].astype(str)
    fig = px.bar(rating_hist, x="bucket", y="n", labels={"bucket": "IMDb rating (0.5 bins)", "n": "# Movies"})
    fig.update_layout(height=360, margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No rating data available.")

# -------------------- COMMENTS OVER TIME --------------------
st.subheader("Comments per Month")
cmt = agg_to_df("comments", [
    {"$match": {"date": {"$type": "date"}}},
    {"$project": {"ym": {"$dateToString": {"date": "$date", "format": "%Y-%m"}}}},
    {"$group": {"_id": "$ym", "n": {"$sum": 1}}},
    {"$project": {"_id": 0, "ym": "$_id", "n": 1}},
    {"$sort": {"ym": 1}}
])
if not cmt.empty:
    cmt["ym"] = pd.to_datetime(cmt["ym"] + "-01", errors="coerce")
    fig = px.line(cmt, x="ym", y="n", markers=True, labels={"ym": "Year-Month", "n": "Comments"})
    fig.update_layout(height=360, margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No comments or missing dates.")

# -------------------- TOP DIRECTORS --------------------
st.subheader("Top Directors by Number of Titles")
top_dir = agg_to_df("movies", [
    {"$unwind": {"path": "$directors", "preserveNullAndEmptyArrays": False}},
    {"$group": {"_id": {"$toLower": "$directors"}, "titles": {"$sum": 1}}},
    {"$project": {"_id": 0, "director": "$_id", "titles": 1}},
    {"$sort": {"titles": -1}},
    {"$limit": 15}
])
if not top_dir.empty:
    fig = px.bar(top_dir, x="titles", y="director", orientation="h")
    fig.update_layout(height=500, margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No director data.")

# -------------------- VOTES vs RATING --------------------
st.subheader("IMDb Votes vs Rating (Top by votes)")
scatter = agg_to_df("movies", [
    {"$match": {"imdb.rating": {"$type": "number"}, "imdb.votes": {"$type": "number"}}},
    {"$project": {"rating": "$imdb.rating", "votes": "$imdb.votes", "title": "$title"}},
    {"$sort": {"votes": -1}},
    {"$limit": 1500}
])
if not scatter.empty:
    fig = px.scatter(scatter, x="rating", y="votes", hover_data=["title"], trendline="ols",
                     labels={"rating": "IMDb rating", "votes": "Votes"})
    fig.update_layout(height=450, margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Votes/rating not available.")

# -------------------- FOOTER --------------------
st.markdown("---")
st.caption("Tip: change filters in the sidebar to explore by year range and genres. Data source: Azure Cosmos DB (Mongo API).")
