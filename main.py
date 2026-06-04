"""
Vessel collision detection on Danish AIS data.

Pipeline:
  1. Load AIS CSVs with an explicit schema
  2. Filter to December 2021, bounding box around (55.225, 14.245)
  3. Refine to 50 nm radius via Haversine
  4. Drop invalid rows, stationary vessels, and GPS jumps
  5. Bucket rows in (1-min, ~1.1km lat/lon cells); emit 8-neighbor cells on one side
  6. Self-join on bucket key → candidate pairs
  7. Compute pairwise Haversine, filter to < CLOSE_THRESHOLD_M
  8. Require persistence (close across N consecutive minutes)
  9. Pick the pair with the smallest minimum distance
 10. Extract ±10 min trajectory, render Folium map, write results
"""

import argparse
import os
import sys
from datetime import timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# --- Constants -------------------------------------------------------------
CENTER_LAT = 55.225
CENTER_LON = 14.245
RADIUS_NM = 50
RADIUS_KM = RADIUS_NM * 1.852
EARTH_RADIUS_KM = 6371.0088

# Rough bounding box around the center — 1 degree latitude ≈ 111 km,
# 1 degree longitude at this latitude ≈ 63 km. 50 nm ≈ 93 km so we pad to ~1°.
BBOX_LAT_MIN, BBOX_LAT_MAX = 54.0, 56.5
BBOX_LON_MIN, BBOX_LON_MAX = 12.5, 16.0

# Cell size for spatial bucketing. 0.01° latitude ≈ 1.1 km — small enough that
# two ships in the same/adjacent cell within the same minute are realistic
# collision candidates, large enough not to over-fragment the join.
CELL_SIZE_DEG = 0.01

# Filters
MIN_SOG_KNOTS = 0.5         # below this we consider the vessel stationary
MAX_PLAUSIBLE_KNOTS = 60.0  # implied speed above this = GPS noise
STATIONARY_STATUSES = {"Moored", "At anchor", "Aground"}
EXCLUDED_MOBILE_TYPES = {"Base Station", "AtoN", "Search and Rescue Transponder"}

# Detection
CLOSE_THRESHOLD_M = 500     # candidate pair distance threshold for self-join
COLLISION_THRESHOLD_M = 100 # what we consider an actual collision
MIN_CONSECUTIVE_MINUTES = 2 # persistence requirement (anti-glitch)


# --- Spark expressions -----------------------------------------------------
def haversine_km(lat1, lon1, lat2, lon2):
    """Haversine distance in km, expressed as a column expression."""
    lat1_r = F.radians(lat1)
    lat2_r = F.radians(lat2)
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(lat1_r) * F.cos(lat2_r) * F.sin(dlon / 2) ** 2
    c = 2 * F.asin(F.sqrt(a))
    return EARTH_RADIUS_KM * c


def ais_schema():
    """Schema for Danish AIS CSV. Column names match the source files exactly."""
    return StructType([
        StructField("# Timestamp", StringType(), True),
        StructField("Type of mobile", StringType(), True),
        StructField("MMSI", IntegerType(), True),
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField("Navigational status", StringType(), True),
        StructField("ROT", DoubleType(), True),
        StructField("SOG", DoubleType(), True),
        StructField("COG", DoubleType(), True),
        StructField("Heading", IntegerType(), True),
        StructField("IMO", StringType(), True),
        StructField("Callsign", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("Ship type", StringType(), True),
        StructField("Cargo type", StringType(), True),
        StructField("Width", DoubleType(), True),
        StructField("Length", DoubleType(), True),
        StructField("Type of position fixing device", StringType(), True),
        StructField("Draught", DoubleType(), True),
        StructField("Destination", StringType(), True),
        StructField("ETA", StringType(), True),
        StructField("Data source type", StringType(), True),
        StructField("A", DoubleType(), True),
        StructField("B", DoubleType(), True),
        StructField("C", DoubleType(), True),
        StructField("D", DoubleType(), True),
    ])


# --- Pipeline stages -------------------------------------------------------
def load_and_filter(spark, input_path):
    """Load AIS data, parse timestamps, restrict to Dec 2021 + bounding box."""
    df = spark.read.csv(
        input_path, header=True, schema=ais_schema(), mode="DROPMALFORMED"
    )

    df = (
        df.select(
            F.to_timestamp("# Timestamp", "dd/MM/yyyy HH:mm:ss").alias("ts"),
            F.col("MMSI").alias("mmsi"),
            F.col("Latitude").alias("lat"),
            F.col("Longitude").alias("lon"),
            F.col("Navigational status").alias("nav_status"),
            F.col("SOG").alias("sog"),
            F.col("Type of mobile").alias("mobile_type"),
            F.col("Name").alias("name"),
            F.col("Ship type").alias("ship_type"),
            F.col("IMO").alias("imo"),
        )
        # Hard filters — invalid rows
        .filter(F.col("ts").isNotNull())
        .filter(F.col("mmsi").isNotNull())
        .filter(F.col("lat").between(BBOX_LAT_MIN, BBOX_LAT_MAX))
        .filter(F.col("lon").between(BBOX_LON_MIN, BBOX_LON_MAX))
        # December 2021 only
        .filter(F.col("ts") >= F.lit("2021-12-01 00:00:00").cast("timestamp"))
        .filter(F.col("ts") < F.lit("2022-01-01 00:00:00").cast("timestamp"))
    )

    return df


def refine_to_radius(df):
    """Drop rows outside 50 nm of the center via Haversine."""
    df = df.withColumn(
        "dist_from_center_km",
        haversine_km(F.col("lat"), F.col("lon"),
                     F.lit(CENTER_LAT), F.lit(CENTER_LON)),
    )
    return df.filter(F.col("dist_from_center_km") <= RADIUS_KM).drop("dist_from_center_km")


def filter_moving_vessels(df):
    """Exclude base stations, stationary vessels, and clearly non-moving rows."""
    return (
        df.filter(~F.col("mobile_type").isin(*EXCLUDED_MOBILE_TYPES))
          .filter(
              F.col("nav_status").isNull()
              | ~F.col("nav_status").isin(*STATIONARY_STATUSES)
          )
          .filter(F.col("sog").isNotNull() & (F.col("sog") >= MIN_SOG_KNOTS))
    )


def drop_gps_jumps(df):
    """Drop rows whose implied speed from the previous point exceeds physical plausibility."""
    w = Window.partitionBy("mmsi").orderBy("ts")
    df = (
        df.withColumn("prev_lat", F.lag("lat").over(w))
          .withColumn("prev_lon", F.lag("lon").over(w))
          .withColumn("prev_ts", F.lag("ts").over(w))
    )
    df = df.withColumn(
        "dt_hours",
        (F.col("ts").cast("long") - F.col("prev_ts").cast("long")) / 3600.0,
    ).withColumn(
        "step_km",
        haversine_km(F.col("prev_lat"), F.col("prev_lon"), F.col("lat"), F.col("lon")),
    ).withColumn(
        "implied_knots",
        F.when(F.col("dt_hours") > 0, (F.col("step_km") / F.col("dt_hours")) / 1.852)
         .otherwise(F.lit(0.0)),
    )
    # Keep the row if it's the first per MMSI (no previous point) or implied speed is plausible.
    return df.filter(
        F.col("prev_ts").isNull() | (F.col("implied_knots") <= MAX_PLAUSIBLE_KNOTS)
    ).drop("prev_lat", "prev_lon", "prev_ts", "dt_hours", "step_km", "implied_knots")


def find_candidate_pairs(df):
    """Spatial-temporal self-join to produce candidate (vessel A, vessel B) rows.

    Each row is bucketed into a (minute, lat_cell, lon_cell) key. The B-side
    additionally explodes into 9 cells (self + 8 neighbors) so pairs that
    straddle a cell boundary are still matched. The `mmsi_a < mmsi_b` filter
    ensures each unordered pair appears once.
    """
    df = (
        df.withColumn("t_min", (F.unix_timestamp("ts") / 60).cast("long"))
          .withColumn("lat_cell", F.floor(F.col("lat") / CELL_SIZE_DEG).cast("int"))
          .withColumn("lon_cell", F.floor(F.col("lon") / CELL_SIZE_DEG).cast("int"))
    )

    a = (
        df.select(
            F.col("mmsi").alias("mmsi_a"),
            F.col("ts").alias("ts_a"),
            F.col("lat").alias("lat_a"),
            F.col("lon").alias("lon_a"),
            F.col("sog").alias("sog_a"),
            "t_min",
            F.col("lat_cell").alias("cell_lat"),
            F.col("lon_cell").alias("cell_lon"),
        )
    )

    # B-side: explode into the 3x3 neighborhood
    neighbor_offsets = [(di, dj) for di in (-1, 0, 1) for dj in (-1, 0, 1)]
    offsets = df.sparkSession.createDataFrame(neighbor_offsets, ["dlat", "dlon"])

    b = (
        df.crossJoin(F.broadcast(offsets))
          .select(
              F.col("mmsi").alias("mmsi_b"),
              F.col("ts").alias("ts_b"),
              F.col("lat").alias("lat_b"),
              F.col("lon").alias("lon_b"),
              F.col("sog").alias("sog_b"),
              "t_min",
              (F.col("lat_cell") + F.col("dlat")).alias("cell_lat"),
              (F.col("lon_cell") + F.col("dlon")).alias("cell_lon"),
          )
    )

    pairs = (
        a.join(b, on=["t_min", "cell_lat", "cell_lon"])
         .filter(F.col("mmsi_a") < F.col("mmsi_b"))
    )

    pairs = pairs.withColumn(
        "dist_m",
        haversine_km(F.col("lat_a"), F.col("lon_a"),
                     F.col("lat_b"), F.col("lon_b")) * 1000.0,
    )
    return pairs.filter(F.col("dist_m") < CLOSE_THRESHOLD_M)


def select_collision_pair(close_pairs):
    """Pick the (mmsi_a, mmsi_b) with the smallest minimum distance and confirm persistence."""
    per_minute = (
        close_pairs.groupBy("mmsi_a", "mmsi_b", "t_min")
                   .agg(F.min("dist_m").alias("min_dist_m"))
    )

    summary = (
        per_minute.groupBy("mmsi_a", "mmsi_b")
                  .agg(
                      F.min("min_dist_m").alias("min_dist_m"),
                      F.count("*").alias("close_minutes"),
                  )
                  .filter(F.col("close_minutes") >= MIN_CONSECUTIVE_MINUTES)
                  .orderBy("min_dist_m")
    )

    top = summary.limit(1).collect()
    if not top:
        return None
    row = top[0]
    return {
        "mmsi_a": row["mmsi_a"],
        "mmsi_b": row["mmsi_b"],
        "min_dist_m": row["min_dist_m"],
        "close_minutes": row["close_minutes"],
    }


def find_collision_moment(close_pairs, mmsi_a, mmsi_b):
    """Find the exact AIS frame where the two vessels were closest."""
    pair_rows = close_pairs.filter(
        (F.col("mmsi_a") == mmsi_a) & (F.col("mmsi_b") == mmsi_b)
    )
    closest = pair_rows.orderBy("dist_m").limit(1).collect()
    if not closest:
        return None
    r = closest[0]
    # ts_a and ts_b are typically within a few seconds — use the midpoint
    mid_ts = r["ts_a"] + (r["ts_b"] - r["ts_a"]) / 2
    return {
        "timestamp": mid_ts,
        "ts_a": r["ts_a"],
        "ts_b": r["ts_b"],
        "lat_a": r["lat_a"], "lon_a": r["lon_a"],
        "lat_b": r["lat_b"], "lon_b": r["lon_b"],
        "dist_m": r["dist_m"],
    }


def resolve_vessel_names(df, mmsi_a, mmsi_b):
    """Look up the most-populated Name for each MMSI."""
    names = (
        df.filter(F.col("mmsi").isin(mmsi_a, mmsi_b) & F.col("name").isNotNull())
          .groupBy("mmsi", "name")
          .count()
          .orderBy(F.desc("count"))
          .collect()
    )
    out = {}
    for row in names:
        out.setdefault(row["mmsi"], row["name"])
    return out


def extract_trajectory(df, mmsi, collision_ts, window_minutes=10):
    """Return a Pandas DataFrame of one vessel's positions in ±window_minutes."""
    start = collision_ts - timedelta(minutes=window_minutes)
    end = collision_ts + timedelta(minutes=window_minutes)
    return (
        df.filter(F.col("mmsi") == mmsi)
          .filter(F.col("ts").between(F.lit(start), F.lit(end)))
          .orderBy("ts")
          .select("ts", "lat", "lon", "sog")
          .toPandas()
    )


def render_map(traj_a, traj_b, names, collision, output_path):
    """Render a Folium map with both trajectories and a collision marker."""
    import folium

    m = folium.Map(
        location=[collision["lat_a"], collision["lon_a"]],
        zoom_start=14,
        tiles="OpenStreetMap",
    )

    for traj, color, label in (
        (traj_a, "blue", f"{names.get(collision['mmsi_a'], collision['mmsi_a'])} (MMSI {collision['mmsi_a']})"),
        (traj_b, "red",  f"{names.get(collision['mmsi_b'], collision['mmsi_b'])} (MMSI {collision['mmsi_b']})"),
    ):
        if traj.empty:
            continue
        coords = list(zip(traj["lat"], traj["lon"]))
        folium.PolyLine(coords, color=color, weight=3, opacity=0.8, tooltip=label).add_to(m)
        folium.Marker(coords[0], icon=folium.Icon(color=color, icon="play"),
                      tooltip=f"{label} — start").add_to(m)
        folium.Marker(coords[-1], icon=folium.Icon(color=color, icon="stop"),
                      tooltip=f"{label} — end").add_to(m)

    folium.Marker(
        [(collision["lat_a"] + collision["lat_b"]) / 2,
         (collision["lon_a"] + collision["lon_b"]) / 2],
        icon=folium.Icon(color="black", icon="exclamation-sign"),
        tooltip=f"Collision @ {collision['timestamp']} ({collision['dist_m']:.0f} m)"
    ).add_to(m)

    m.save(output_path)


def main():
    parser = argparse.ArgumentParser(description="Detect vessel collisions in Danish AIS data")
    parser.add_argument("--input", default="/data",
                        help="AIS CSV file or directory")
    parser.add_argument("--output", default="/app/output",
                        help="Output directory")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    spark = (
    SparkSession.builder
        .appName("VesselCollisionDetection")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "400")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.maxResultSize", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[1/6] Loading and filtering AIS data from {args.input}")
    df = load_and_filter(spark, args.input)
    df = refine_to_radius(df)
    df = filter_moving_vessels(df)
    # Cache: we read this multiple times (GPS filter, candidate join, name lookup, trajectory).
    df.cache()
    print(f"      Rows after filter: {df.count():,}")

    print("[2/6] Removing GPS jumps")
    df_clean = drop_gps_jumps(df).cache()

    print("[3/6] Building candidate pairs via spatial-temporal bucket join")
    close = find_candidate_pairs(df_clean).cache()
    print(f"      Close-pair rows: {close.count():,}")

    print("[4/6] Selecting collision pair (with persistence check)")
    pair = select_collision_pair(close)
    if pair is None:
        print("No persistent close encounter found.", file=sys.stderr)
        sys.exit(1)
    print(f"      Pair: MMSI {pair['mmsi_a']} & {pair['mmsi_b']}, "
          f"min distance {pair['min_dist_m']:.1f} m across {pair['close_minutes']} min")

    print("[5/6] Locating collision moment + resolving names")
    moment = find_collision_moment(close, pair["mmsi_a"], pair["mmsi_b"])
    names = resolve_vessel_names(df, pair["mmsi_a"], pair["mmsi_b"])
    collision = {**moment, "mmsi_a": pair["mmsi_a"], "mmsi_b": pair["mmsi_b"]}

    print("[6/6] Extracting trajectories and rendering map")
    traj_a = extract_trajectory(df_clean, pair["mmsi_a"], moment["timestamp"])
    traj_b = extract_trajectory(df_clean, pair["mmsi_b"], moment["timestamp"])
    map_path = os.path.join(args.output, "collision_map.html")
    render_map(traj_a, traj_b, names, collision, map_path)

    # Result text
    name_a = names.get(pair["mmsi_a"], "UNKNOWN")
    name_b = names.get(pair["mmsi_b"], "UNKNOWN")
    result = (
        "=== Vessel Collision Detection — Result ===\n"
        f"Vessel A: {name_a} (MMSI {pair['mmsi_a']})\n"
        f"Vessel B: {name_b} (MMSI {pair['mmsi_b']})\n"
        f"Collision time (UTC): {moment['timestamp']}\n"
        f"Position A: ({moment['lat_a']:.6f}, {moment['lon_a']:.6f})\n"
        f"Position B: ({moment['lat_b']:.6f}, {moment['lon_b']:.6f})\n"
        f"Minimum distance: {moment['dist_m']:.1f} m\n"
        f"Close-approach duration: {pair['close_minutes']} minutes\n"
        f"Map: {map_path}\n"
    )
    print("\n" + result)
    with open(os.path.join(args.output, "result.txt"), "w") as f:
        f.write(result)

    # Save trajectories as CSV for reproducibility
    traj_a.to_csv(os.path.join(args.output, f"trajectory_{pair['mmsi_a']}.csv"), index=False)
    traj_b.to_csv(os.path.join(args.output, f"trajectory_{pair['mmsi_b']}.csv"), index=False)

    spark.stop()


if __name__ == "__main__":
    main()
