# app.py — Streamlit dashboard for Azure Cosmos DB (Mongo API) — sample_mflix

import os
from datetime import datetime
import pandas as pd
import streamlit as st
import altair as alt
from pymongo import MongoClient
from dotenv import load_dotenv

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(page_title="Mflix Dashboard", layout="wide")
st.title("🎬 Mflix Metrics — Azure Cosmos DB (Mongo API)")

# -----------------------------
# Connection (uses .env if present, else fallback)
# -----------------------------
load_dotenv()

URI = os.getenv(
    "MONGO_URI",
    # Fallback to your working URI (note the URL-encoded @ -> %40)
    "mongodb+srv://tejaswisandratest2:password%40123@test2-bd.mongocluster.cosmos.azure.com/sample_mflix"
    "?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"
)
DB_NAME = os.getenv("MONGO_DB", "sample_mflix")

@st.cache_resource(show_spinner=True)
def get_db():
    client = MongoClient(URI, serverSelectionTimeoutMS=20000)
    client.admin.command("ping")  # sanity check
    return client[DB_NAME]

try:
    db = get_db()
    st.success(f"Connected to **{DB_NAME}** ✅")
except Exception as e:
    st.error(f"❌ Failed to connect to database: {e}")
    st.stop()

# -----------------------------
# Helpers
# -----------------------------
def to_df(cursor_or_iterable, drop_id=True):
    rows = list(cursor_or_iterable)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if drop_id and "_id" in df.columns:
        df = df.drop(columns=["_id"])
    # stringify any datetime objects for display
    for c in df.columns:
        if df[c].dtype == "object" and df[c].apply(lambda x: isinstance(x, datetime)).any():
            df[c] = df[c].astype(str)
    return df

@st.cache_data(ttl=60)
def list_genres():
    pipeline = [
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres"}},
        {"$project": {"_id": 0, "genre": "$_id"}},
        {"$sort": {"genre": 1}},
    ]
    return [g["genre"] for g in db.movies.aggregate(pipeline)]

@st.cache_data(ttl=60)
def years_min_max():
    doc = db.movies.aggregate([
        {"$match": {"year": {"$exists": True}}},
        {"$group": {"_id": None, "minY": {"$min": "$year"}, "maxY": {"$max": "$year"}}}
    ])
    d = next(doc, None)
    if not d or d["minY"] is None or d["maxY"] is None:
        return (2000, 2025)
    try:
        return (int(d["minY"]), int(d["maxY"]))
    except Exception:
        return (2000, 2025)

# -----------------------------
# Sidebar Filters
# -----------------------------
st.sidebar.header("Filters")

ymin, ymax = years_min_max()
if ymin > ymax:
    ymin, ymax = 2000, 2025

year_range = st.sidebar.slider("Year range", min_value=int(ymin), max_value=int(ymax),
                               value=(int(ymin), int(ymax)), step=1)

all_genres = list_genres()
default_genres = all_genres[:3] if all_genres else []
sel_genres = st.sidebar.multiselect("Genres", options=all_genres, default=default_genres)

min_rating = st.sidebar.slider("Min IMDb rating", 0.0, 10.0, 6.0, 0.1)

# -----------------------------
# Queries
# -----------------------------
@st.cache_data(ttl=60)
def movies_filtered(year_min, year_max, genres, min_r):
    match_stage = {
        "$and": [
            {"year": {"$gte": year_min, "$lte": year_max}},
            {"imdb.rating": {"$type": "number", "$gte": float(min_r)}},
        ]
    }
    if genres:
        match_stage["$and"].append({"genres": {"$in": genres}})
    cur = db.movies.find(
        match_stage,
        {"title": 1, "year": 1, "genres": 1, "imdb.rating": 1, "countries": 1}
    ).sort([("imdb.rating", -1), ("year", -1)]).limit(1000)
    df = to_df(cur, drop_id=True)
    # Type coercion
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("float64")
    if "imdb.rating" in df.columns:
        df["imdb.rating"] = pd.to_numeric(df["imdb.rating"], errors="coerce").astype("float64")
    return df

@st.cache_data(ttl=60)
def agg_avg_rating_by_year(year_min, year_max, genres, min_r):
    match = {
        "year": {"$gte": year_min, "$lte": year_max},
        "imdb.rating": {"$type": "number", "$gte": float(min_r)},
    }
    if genres:
        match["genres"] = {"$in": genres}
    pipeline = [
        {"$match": match},
        {"$group": {"_id": "$year", "avgRating": {"$avg": "$imdb.rating"}, "n": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    df = to_df(db.movies.aggregate(pipeline), drop_id=False)
    # rename and cast
    if "_id" in df.columns:
        df = df.rename(columns={"_id": "year"})
    for col in ("year", "avgRating", "n"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    df = df.dropna(subset=["year", "avgRating"])
    return df

@st.cache_data(ttl=60)
def agg_movies_by_genre(year_min, year_max, min_r):
    pipeline = [
        {"$match": {
            "year": {"$gte": year_min, "$lte": year_max},
            "imdb.rating": {"$type": "number", "$gte": float(min_r)},
            "genres": {"$ne": None}
        }},
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    df = to_df(db.movies.aggregate(pipeline), drop_id=False)
    if "_id" in df.columns:
        df = df.rename(columns={"_id": "genre"})
    if "count" in df.columns:
        df["count"] = pd.to_numeric(df["count"], errors="coerce").astype("float64")
    return df

@st.cache_data(ttl=60)
def comments_over_time():
    pipeline = [
        {"$project": {"date": 1}},
        {"$match": {"date": {"$type": "date"}}},
        {"$group": {
            "_id": {"$dateTrunc": {"date": "$date", "unit": "day"}},
            "comments": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    df = to_df(db.comments.aggregate(pipeline), drop_id=False)
    if "_id" in df.columns:
        df = df.rename(columns={"_id": "date"})
    # Coerce to datetime for plotting
    if "date" in df.columns:
        try:
            df["date"] = pd.to_datetime(df["date"])
        except Exception:
            pass
    if "comments" in df.columns:
        df["comments"] = pd.to_numeric(df["comments"], errors="coerce").astype("float64")
    return df

# Execute with filters
movies_df = movies_filtered(year_range[0], year_range[1], sel_genres, min_rating)
avg_year_df = agg_avg_rating_by_year(year_range[0], year_range[1], sel_genres, min_rating)
genre_df = agg_movies_by_genre(year_range[0], year_range[1], min_rating)
comments_df = comments_over_time()

# -----------------------------
# KPIs
# -----------------------------
col1, col2, col3 = st.columns(3)
col1.metric("Movies (filtered)", int(len(movies_df)))
col2.metric("Genres w/ matches", int(len(genre_df)))
col3.metric("Comments (all time)", 0 if comments_df.empty else int(comments_df["comments"].sum()))

# -----------------------------
# Top Movies Table
# -----------------------------
st.subheader("⭐ Top Movies (Filtered)")
if movies_df.empty:
    st.info("No movies match the current filters.")
else:
    pretty = movies_df.rename(columns={"imdb.rating": "rating"})
    st.dataframe(pretty.head(25), use_container_width=True)

# -----------------------------
# Average IMDb Rating by Year (ROBUST)
# -----------------------------
st.subheader("📈 Average IMDb Rating by Year")

# Keep only needed columns, already renamed inside agg function
needed_cols = [c for c in ["year", "avgRating", "n"] if c in avg_year_df.columns]
avg_year_df = avg_year_df[needed_cols].copy() if needed_cols else pd.DataFrame(columns=["year","avgRating","n"])

# Ensure numeric types (avoid nullable ints)
for c in ("year", "avgRating", "n"):
    if c in avg_year_df.columns:
        avg_year_df[c] = pd.to_numeric(avg_year_df[c], errors="coerce").astype("float64")

avg_year_df = avg_year_df.dropna(subset=["year", "avgRating"])

if avg_year_df.empty:
    st.info("No numeric data available to plot average rating by year with the current filters.")
else:
    # Using ordinal x is more robust when dtype inference is tricky; switch to ':Q' if you prefer numeric axis
    chart1 = (
        alt.Chart(avg_year_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y("avgRating:Q", title="Average IMDb rating"),
            tooltip=["year", "avgRating", "n"]
        )
        .properties(height=300)
    )
    st.altair_chart(chart1, use_container_width=True)

# -----------------------------
# Movies by Genre
# -----------------------------
st.subheader("🎭 Movies by Genre (Count)")
if genre_df.empty or "genre" not in genre_df.columns or "count" not in genre_df.columns:
    st.info("No genres to display.")
    if not genre_df.empty:
        st.dataframe(genre_df)
else:
    chart2 = (
        alt.Chart(genre_df.head(20))
        .mark_bar()
        .encode(
            x=alt.X("count:Q", title="Count"),
            y=alt.Y("genre:N", sort="-x", title="Genre"),
            tooltip=["genre", "count"]
        )
        .properties(height=400)
    )
    st.altair_chart(chart2, use_container_width=True)

# -----------------------------
# Comments Over Time
# -----------------------------
st.subheader("💬 Comments Over Time")
if comments_df.empty or "date" not in comments_df.columns or "comments" not in comments_df.columns:
    st.info("No comment timestamps yet. Insert a few in `comments` with a `date` field to see this fill.")
else:
    chart3 = (
        alt.Chart(comments_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("comments:Q", title="Comments"),
            tooltip=["date", "comments"]
        )
        .properties(height=300)
    )
    st.altair_chart(chart3, use_container_width=True)

# -----------------------------
# Raw Explorer
# -----------------------------
with st.expander("🔎 Raw Explorer — First 100 filtered movies"):
    st.dataframe(movies_df.head(100), use_container_width=True)

st.caption("Backend: Azure Cosmos DB (Mongo API) • Secure TLS connection • SRV URI")
