"""
Load Milwaukee crash CSV -> SQLite, with optional geocoding cache.

Inputs:
  data/raw/trafficaccident.csv   (casenumber, casedate, crashloc)

Outputs:
  data/db/milwaukee_crashes.db
    - crashes          (final fact table incl. lat/lon)
    - geocode_cache    (crash_location -> lat/lon, for reuse)

Run:
  python src/load.py
"""

from pathlib import Path
import time
import requests
import pandas as pd
from sqlalchemy import create_engine, text
import os
from requests.adapters import HTTPAdapter, Retry


# ---------- Paths ----------
RAW_CSV = Path("data/raw/trafficaccident.csv")
DB_PATH = Path("data/db/milwaukee_crashes.db")

# ---------- Geocoding config ----------
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "MilwaukeeCrashInsights/1.0 (zachariya4@gmail.com)"  
REQUESTS_PER_SECOND = 1.0  
MAX_NEW_LOOKUPS = int(os.getenv("GEOCODE_MAX", "100"))
GEOCODE_MODE = os.getenv("GEOCODE_MODE", "limited")



def _retrying_session():
    s = requests.Session()
    retries = Retry(
        total=3, backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",)
    )
    s.headers.update({"User-Agent": USER_AGENT})
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s

SESSION = _retrying_session()


# ---------- Helpers ----------
def read_and_prepare() -> pd.DataFrame:
    if not RAW_CSV.exists():
        raise FileNotFoundError(f"Missing CSV at {RAW_CSV.resolve()}")

    # Read & normalize column names
    df = pd.read_csv(RAW_CSV)
    df.columns = [c.strip().lower() for c in df.columns]

    # Expected columns
    expected = {"casenumber", "casedate", "crashloc"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing expected columns: {missing}")

    # Rename to final names
    df = df.rename(
        columns={
            "casenumber": "case_number",
            "casedate": "crash_datetime",
            "crashloc": "crash_location",
        }
    )

    # Parse datetime (your format is ISO-like "YYYY-MM-DD HH:MM:SS")
    df["crash_datetime"] = pd.to_datetime(
        df["crash_datetime"], format="%Y-%m-%d %H:%M:%S", errors="coerce"
    )

    # Clean core fields
    df["case_number"] = df["case_number"].astype(str).str.strip()
    df["crash_location"] = df["crash_location"].astype(str).str.strip().str.upper()

    # Drop rows missing essentials
    df = df.dropna(subset=["case_number", "crash_datetime"])
    df = df[df["case_number"] != ""]

    # Derivatives for analysis
    df["year"] = df["crash_datetime"].dt.year.astype("Int64")
    df["month"] = df["crash_datetime"].dt.month.astype("Int64")
    df["day_of_week"] = df["crash_datetime"].dt.day_name()
    df["hour_of_day"] = df["crash_datetime"].dt.hour.astype("Int64")
    df["is_weekend"] = (df["crash_datetime"].dt.dayofweek >= 5).astype(int)  # 0/1

    # Deduplicate by case_number (keep newest record)
    df = (
        df.sort_values("crash_datetime")
          .drop_duplicates(subset=["case_number"], keep="last")
          .reset_index(drop=True)
    )

    return df


def create_schema(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS crashes;"))
        conn.execute(text("""
            CREATE TABLE crashes (
                case_number    TEXT PRIMARY KEY,
                crash_datetime TEXT,
                year           INTEGER,
                month          INTEGER,
                day_of_week    TEXT,
                hour_of_day    INTEGER,
                is_weekend     INTEGER,
                crash_location TEXT,
                lat            REAL,
                lon            REAL
            );
        """))
        # cache for geocoding
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS geocode_cache (
                crash_location TEXT PRIMARY KEY,
                latitude       REAL,
                longitude      REAL,
                ts             TEXT
            );
        """))


# ---------- Geocoding with cache ----------
def _fetch_cached_coords(engine, locations):
    """Return dict[location] -> (lat, lon) for those present in cache.
    Queries in batches to avoid SQLite's ~999-parameter limit.
    """
    out = {}
    if not locations:
        return out

    BATCH_SIZE = 900  # safely under SQLite's ~999 parameters per statement
    with engine.begin() as conn:
        for i in range(0, len(locations), BATCH_SIZE):
            batch = locations[i:i + BATCH_SIZE]
            placeholders = ",".join(["?"] * len(batch))
            sql = (
                "SELECT crash_location, latitude, longitude "
                f"FROM geocode_cache WHERE crash_location IN ({placeholders})"
            )
            for loc, lat, lon in conn.exec_driver_sql(sql, tuple(batch)):
                out[loc] = (lat, lon)
    return out



def _save_coords(engine, rows):
    """rows is list of tuples (crash_location, lat, lon)."""
    if not rows:
        return
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "INSERT OR REPLACE INTO geocode_cache "
            "(crash_location, latitude, longitude, ts) "
            "VALUES (?, ?, ?, datetime('now'))",
            rows
        )

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

def _geocode_location(text_loc):
    """Call Nominatim for a single location string."""
    params = {
        "q": f"{text_loc}, Milwaukee, Wisconsin, USA",
        "format": "json",
        "limit": 1,
    }
    r = SESSION.get(NOMINATIM_URL, params=params, timeout=6)
    r.raise_for_status()
    js = r.json()
    if not js:
        return None, None
    return float(js[0]["lat"]), float(js[0]["lon"])


def enrich_with_latlon(df: pd.DataFrame, engine) -> pd.DataFrame:
    """Add lat/lon columns using cached geocoding.
       Modes: 
          - 'cache_only' : do not call network; just merge cache
          - 'limited'    : geocode up to MAX_NEW_LOOKUPS new locations this run
          - 'all'        : geocode all missing locations
       """
    uniq_locs = df["crash_location"].dropna().unique().tolist()

    cached = _fetch_cached_coords(engine, uniq_locs)
    to_lookup_all = [loc for loc in uniq_locs if loc not in cached]

    print(f"[GEOCODE] unique locations: {len(uniq_locs):,} | cached: {len(cached):,} | missing: {len(to_lookup_all):,}")

    to_lookup = []
    if GEOCODE_MODE == "cache_only":
        print("[GEOCODE] cache_only mode: skipping all network lookups.")
        to_lookup = []
    elif GEOCODE_MODE == "limited":
        to_lookup = to_lookup_all[:MAX_NEW_LOOKUPS]
        if to_lookup:
            print(f"[GEOCODE] limited mode: will lookup up tp {len(to_lookup)} new locations this run.")
    elif GEOCODE_MODE  == 'all':
        to_lookup = to_lookup_all
        if to_lookup:
            print(f"[GEOCODE] all mode: will lookup {len(to_lookup)} as new locations.")
    else:
        print(f"[GEOCODE] unknown mode '{GEOCODE_MODE}', defaulting to 'limited'.")
        to_lookup = to_lookup_all[:MAX_NEW_LOOKUPS]

    new_rows = []
    if to_lookup:
        sleep_s = max(0.001, 1.0 / REQUESTS_PER_SECOND)
        for i, loc in enumerate(to_lookup, start=1):
            try:
                lat, lon = _geocode_location(loc)
            except Exception as e:
                lat, lon = None, None
            new_rows.append((loc, lat, lon))
            
            if i % 25 == 0 or i ==len(to_lookup):
                print(f"[GEOCODE] {i}/{len(to_lookup)} done")
            time.sleep(sleep_s)

        _save_coords(engine, new_rows)

    # Merge cache back into df
    with engine.begin() as conn:
        cache_df = pd.read_sql("SELECT crash_location, latitude, longitude FROM geocode_cache", conn)

    cache_df = cache_df.rename(columns={"latitude": "lat", "longitude": "lon"})
    df = df.merge(cache_df, on="crash_location", how="left")
    have = df[["lat", "lon"]].notna().all(axis=1).sum()
    print(f"[GEOCODE] rows with lat/lon after merge: {have:,}")

    return df


def load_to_db(df: pd.DataFrame) -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{DB_PATH}")

    # (Re)create target schema and cache table
    create_schema(engine)

    # Geocode + attach lat/lon
    df = enrich_with_latlon(df, engine)

    # Save to SQLite
    # Ensure datetime as ISO string for SQLite TEXT
    df_out = df.copy()
    df_out["crash_datetime"] = df_out["crash_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

    df_out.to_sql("crashes", con=engine, if_exists="append", index=False, method="multi", chunksize=2000)

    with engine.begin() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM crashes;")).scalar()
        uniques = conn.execute(text("SELECT COUNT(DISTINCT case_number) FROM crashes;")).scalar()
        with_latlon = conn.execute(text("SELECT COUNT(*) FROM crashes WHERE lat IS NOT NULL AND lon IS NOT NULL;")).scalar()

    print(f"[LOAD OK] Rows: {total:,} | Distinct case_number: {uniques:,} | With lat/lon: {with_latlon:,}")
    print(f"DB: {DB_PATH.resolve()}")


if __name__ == "__main__":
    df = read_and_prepare()
    load_to_db(df)
