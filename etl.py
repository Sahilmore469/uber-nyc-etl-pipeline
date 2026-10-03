"""
etl.py
------
A self-contained Extract -> Transform -> Load pipeline for the Uber NYC
trip dataset (FiveThirtyEight's Uber TLC FOIL Response). Everything is
in this one file: download the raw data, clean and reshape it into a
small star schema, run data quality checks, and load it into a SQLite
warehouse you can query with SQL afterward.

Usage:
    python etl.py                  # run the full pipeline
    python etl.py --skip-checks    # skip data quality checks (not recommended)
    python etl.py --verbose        # debug-level logging

Requirements:
    pip install pandas numpy sqlalchemy
"""

import argparse
import logging
import os
import sys
import time
import urllib.request

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# =====================================================================
# CONFIG
# =====================================================================
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
DB_PATH = os.path.join(PROJECT_ROOT, "data", "uber_warehouse.db")
DB_URI = f"sqlite:///{DB_PATH}"

for d in (RAW_DATA_DIR, LOG_DIR, os.path.dirname(DB_PATH)):
    os.makedirs(d, exist_ok=True)

MONTHS = ["apr14", "may14", "jun14", "jul14", "aug14", "sep14"]
BASE_URL = (
    "https://raw.githubusercontent.com/fivethirtyeight/"
    "uber-tlc-foil-response/master/uber-trip-data/uber-raw-data-"
)

# Rough NYC bounding box, used to drop bad GPS coordinates
LAT_MIN, LAT_MAX = 40.4, 41.2
LON_MIN, LON_MAX = -74.3, -73.5

TRIPS_TABLE = "fact_trips"
BASE_DIM_TABLE = "dim_base"
DATE_DIM_TABLE = "dim_date"

logger = logging.getLogger("uber_etl")


# =====================================================================
# EXTRACT
# =====================================================================
def download_raw_files() -> list:
    """Downloads each month's CSV if not already present. Returns list of local paths."""
    paths = []
    for month in MONTHS:
        dest = os.path.join(RAW_DATA_DIR, f"uber-raw-data-{month}.csv")
        if os.path.exists(dest):
            logger.info("Already have %s, skipping download", dest)
            paths.append(dest)
            continue

        url = f"{BASE_URL}{month}.csv"
        logger.info("Downloading %s -> %s", url, dest)
        try:
            urllib.request.urlretrieve(url, dest)
            paths.append(dest)
        except Exception:
            logger.exception("Failed to download %s", url)
            logger.warning("Download manually from %s and save to %s", url, dest)

    if not paths:
        raise RuntimeError("No raw data files available. Extraction failed.")
    return paths


def extract() -> pd.DataFrame:
    """Downloads (if needed) and loads all raw monthly files into one DataFrame."""
    paths = download_raw_files()

    frames = []
    for path in paths:
        df = pd.read_csv(path)
        df["source_file"] = os.path.basename(path)
        frames.append(df)

    raw_df = pd.concat(frames, ignore_index=True)
    logger.info("Extracted %s raw rows from %s files", len(raw_df), len(paths))
    return raw_df


# =====================================================================
# TRANSFORM
# =====================================================================
def clean(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Type-casts, drops bad rows, and removes duplicates."""
    df = raw_df.rename(columns={"Date/Time": "pickup_datetime", "Lat": "lat", "Lon": "lon", "Base": "base_code"})

    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], format="%m/%d/%Y %H:%M:%S", errors="coerce")
    before = len(df)
    df = df.dropna(subset=["pickup_datetime"])
    logger.info("Dropped %s rows with unparseable datetime", before - len(df))

    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["lat", "lon"])
    logger.info("Dropped %s rows with non-numeric coordinates", before - len(df))

    before = len(df)
    df = df[(df["lat"].between(LAT_MIN, LAT_MAX)) & (df["lon"].between(LON_MIN, LON_MAX))]
    logger.info("Dropped %s rows with out-of-range coordinates", before - len(df))

    before = len(df)
    df = df.drop_duplicates(subset=["pickup_datetime", "lat", "lon", "base_code"])
    logger.info("Dropped %s exact duplicate rows", before - len(df))

    df["base_code"] = df["base_code"].str.strip().str.upper()
    return df.reset_index(drop=True)


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Adds derived date/time columns used downstream for analysis."""
    dt = df["pickup_datetime"]
    df["trip_date"] = dt.dt.date
    df["year"] = dt.dt.year
    df["month"] = dt.dt.month
    df["month_name"] = dt.dt.month_name().str.slice(0, 3)
    df["day"] = dt.dt.day
    df["hour"] = dt.dt.hour
    df["weekday_num"] = dt.dt.weekday  # 0 = Monday
    df["weekday_name"] = dt.dt.day_name().str.slice(0, 3)
    df["is_weekend"] = df["weekday_num"].isin([5, 6])
    return df


def build_dim_base(df: pd.DataFrame) -> pd.DataFrame:
    bases = sorted(df["base_code"].unique())
    return pd.DataFrame({"base_id": range(1, len(bases) + 1), "base_code": bases})


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    dates = pd.DataFrame({"trip_date": sorted(df["trip_date"].unique())})
    dates["trip_date"] = pd.to_datetime(dates["trip_date"])
    dates["date_id"] = range(1, len(dates) + 1)
    dates["year"] = dates["trip_date"].dt.year
    dates["month"] = dates["trip_date"].dt.month
    dates["month_name"] = dates["trip_date"].dt.month_name().str.slice(0, 3)
    dates["day"] = dates["trip_date"].dt.day
    dates["weekday_num"] = dates["trip_date"].dt.weekday
    dates["weekday_name"] = dates["trip_date"].dt.day_name().str.slice(0, 3)
    dates["is_weekend"] = dates["weekday_num"].isin([5, 6])
    return dates[["date_id", "trip_date", "year", "month", "month_name", "day", "weekday_num", "weekday_name", "is_weekend"]]


def build_fact_trips(df: pd.DataFrame, dim_base: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["trip_date"] = pd.to_datetime(df["trip_date"])
    df = df.merge(dim_base, on="base_code", how="left")
    df = df.merge(dim_date[["date_id", "trip_date"]], on="trip_date", how="left")

    fact = df[[
        "pickup_datetime", "date_id", "base_id", "lat", "lon",
        "hour", "month", "month_name", "weekday_num", "weekday_name",
        "is_weekend", "source_file"
    ]].copy()
    fact.insert(0, "trip_id", range(1, len(fact) + 1))
    return fact


def transform(raw_df: pd.DataFrame) -> dict:
    """Runs the full transform stage. Returns a dict of DataFrames ready to load."""
    cleaned = clean(raw_df)
    featured = engineer_features(cleaned)

    dim_base = build_dim_base(featured)
    dim_date = build_dim_date(featured)
    fact_trips = build_fact_trips(featured, dim_base, dim_date)

    logger.info(
        "Transform produced fact_trips=%s, dim_base=%s, dim_date=%s",
        len(fact_trips), len(dim_base), len(dim_date)
    )
    return {"fact_trips": fact_trips, "dim_base": dim_base, "dim_date": dim_date}


# =====================================================================
# DATA QUALITY
# =====================================================================
class DataQualityError(Exception):
    pass


def run_checks(tables: dict):
    fact, dim_base, dim_date = tables["fact_trips"], tables["dim_base"], tables["dim_date"]
    failures = []

    def check(name, condition):
        if condition:
            logger.info("passed: %s", name)
        else:
            failures.append(name)
            logger.error("FAILED check: %s", name)

    check("fact_trips is not empty", len(fact) > 0)
    check("no null pickup_datetime", fact["pickup_datetime"].notna().all())
    check("no null lat/lon", fact[["lat", "lon"]].notna().all().all())
    check("no null base_id", fact["base_id"].notna().all())
    check("no null date_id", fact["date_id"].notna().all())
    check("lat within bounds", fact["lat"].between(LAT_MIN, LAT_MAX).all())
    check("lon within bounds", fact["lon"].between(LON_MIN, LON_MAX).all())
    check("hour between 0 and 23", fact["hour"].between(0, 23).all())
    check("all base_id values exist in dim_base", fact["base_id"].isin(dim_base["base_id"]).all())
    check("all date_id values exist in dim_date", fact["date_id"].isin(dim_date["date_id"]).all())
    check("trip_id is unique", fact["trip_id"].is_unique)
    check("base_id is unique in dim_base", dim_base["base_id"].is_unique)
    check("date_id is unique in dim_date", dim_date["date_id"].is_unique)

    logger.info("Data quality: %s/%s checks passed", 13 - len(failures), 13)
    if failures:
        raise DataQualityError(f"{len(failures)} data quality check(s) failed: {failures}")


# =====================================================================
# LOAD
# =====================================================================
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dim_base (
    base_id     INTEGER PRIMARY KEY,
    base_code   TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id       INTEGER PRIMARY KEY,
    trip_date     TEXT NOT NULL UNIQUE,
    year          INTEGER NOT NULL,
    month         INTEGER NOT NULL,
    month_name    TEXT NOT NULL,
    day           INTEGER NOT NULL,
    weekday_num   INTEGER NOT NULL,
    weekday_name  TEXT NOT NULL,
    is_weekend    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id       INTEGER PRIMARY KEY,
    pickup_datetime TEXT NOT NULL,
    date_id       INTEGER NOT NULL REFERENCES dim_date(date_id),
    base_id       INTEGER NOT NULL REFERENCES dim_base(base_id),
    lat           REAL NOT NULL,
    lon           REAL NOT NULL,
    hour          INTEGER NOT NULL,
    month         INTEGER NOT NULL,
    month_name    TEXT NOT NULL,
    weekday_num   INTEGER NOT NULL,
    weekday_name  TEXT NOT NULL,
    is_weekend    INTEGER NOT NULL,
    source_file   TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_trips_date ON fact_trips(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_trips_base ON fact_trips(base_id);
CREATE INDEX IF NOT EXISTS idx_fact_trips_hour ON fact_trips(hour);
"""


def create_schema(engine):
    with engine.begin() as conn:
        for statement in SCHEMA_SQL.strip().split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(text(statement))
    logger.info("Schema ensured in %s", DB_PATH)


def load(tables: dict):
    """Truncates and reloads each table -- idempotent, safe to re-run."""
    engine = create_engine(DB_URI)
    create_schema(engine)

    load_order = [
        (BASE_DIM_TABLE, tables["dim_base"]),
        (DATE_DIM_TABLE, tables["dim_date"]),
        (TRIPS_TABLE, tables["fact_trips"]),
    ]

    with engine.begin() as conn:
        for table_name, _ in load_order:
            conn.execute(text(f"DELETE FROM {table_name}"))
            logger.info("Cleared existing rows from %s", table_name)

    for table_name, df in load_order:
        chunksize = 50_000 if len(df) > 50_000 else None
        df.to_sql(table_name, engine, if_exists="append", index=False, chunksize=chunksize)
        logger.info("Loaded %s rows into %s", len(df), table_name)

    return engine


def verify_load(engine):
    with engine.connect() as conn:
        for table in (BASE_DIM_TABLE, DATE_DIM_TABLE, TRIPS_TABLE):
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            logger.info("%s: %s rows", table, count)


# =====================================================================
# ORCHESTRATION
# =====================================================================
def setup_logging(verbose: bool):
    log_path = os.path.join(LOG_DIR, "etl_run.log")
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_path)],
    )


def main():
    parser = argparse.ArgumentParser(description="Uber ETL pipeline (single-file version)")
    parser.add_argument("--skip-checks", action="store_true", help="Skip data quality checks")
    parser.add_argument("--verbose", action="store_true", help="Debug-level logging")
    args = parser.parse_args()

    setup_logging(args.verbose)
    start = time.time()
    logger.info("=" * 60)
    logger.info("Starting Uber ETL pipeline")
    logger.info("=" * 60)

    try:
        t0 = time.time()
        logger.info("STAGE 1/3: EXTRACT")
        raw_df = extract()
        logger.info("Extract completed in %.1fs (%s rows)", time.time() - t0, len(raw_df))

        t0 = time.time()
        logger.info("STAGE 2/3: TRANSFORM")
        tables = transform(raw_df)
        logger.info("Transform completed in %.1fs", time.time() - t0)

        if not args.skip_checks:
            logger.info("Running data quality checks...")
            run_checks(tables)
        else:
            logger.warning("Skipping data quality checks (--skip-checks passed)")

        t0 = time.time()
        logger.info("STAGE 3/3: LOAD")
        engine = load(tables)
        verify_load(engine)
        logger.info("Load completed in %.1fs", time.time() - t0)

        logger.info("=" * 60)
        logger.info("Pipeline SUCCEEDED in %.1fs. Warehouse: %s", time.time() - start, DB_PATH)
        logger.info("=" * 60)

    except DataQualityError as e:
        logger.error("Pipeline FAILED at data quality gate: %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Pipeline FAILED with an unexpected error")
        sys.exit(1)


if __name__ == "__main__":
    main()