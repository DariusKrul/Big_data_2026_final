# Report — Vessel Collision Detection

## 1. Problem

Identify the pair of vessels that collided (or came closest to colliding) in December 2021 within 50 nautical miles of (55.225°N, 14.245°E) — a point in the southwest Baltic Sea — using Danish AIS data, then visualise their trajectories in a ±10 minute window around the collision.

## 2. Data

- **Source**: Danish Maritime Authority, https://web.ais.dk/aisdata/
- **Period**: 2021-12-01 to 2021-12-31
- **Volume**: ~_TBD_ GB extracted across 31 daily CSVs
- **Schema**: 26 columns. The pipeline uses an explicit `StructType` rather than letting Spark infer the schema — inference would require an extra pass over the data.

## 3. Filtering pipeline

The filters are applied in order of cost. Cheap predicates (column comparisons) run first; expensive ones (Haversine, window functions) run only on the surviving rows.

| Step | Filter | Rationale |
|------|--------|-----------|
| 1 | `timestamp` in December 2021 | Assignment timeframe |
| 2 | Latitude ∈ [54.0, 56.5], Longitude ∈ [12.5, 16.0] | Bounding-box pre-filter, ~1° padded around the center to fully cover the 50 nm radius |
| 3 | Non-null MMSI, lat, lon | Drop malformed rows |
| 4 | Haversine distance from (55.225, 14.245) ≤ 92.6 km | Exact 50 nm refinement |
| 5 | `Type of mobile` ∉ {Base Station, AtoN, SART} | Exclude shore stations & navigation aids |
| 6 | `Navigational status` ∉ {Moored, At anchor, Aground} | Exclude stationary vessels by declared status |
| 7 | `SOG` ≥ 0.5 knots | Numerical confirmation that the vessel is moving |
| 8 | Implied speed from previous AIS frame ≤ 60 knots | Drop GPS jumps that would otherwise look like teleportation |

## 4. Collision detection

The naïve approach — compare every vessel against every other at every timestamp — is O(n²) and infeasible. The pipeline instead uses a **spatial-temporal bucket self-join**:

1. Each row gets keys `(t_min, lat_cell, lon_cell)` where `t_min = floor(unix_ts / 60)` and `lat_cell / lon_cell` use a 0.01° grid (~1.1 km cells at this latitude).
2. The **left side** of the join emits one row per AIS message.
3. The **right side** emits 9 rows per AIS message: the original cell plus all 8 neighbour cells. This catches pairs that straddle a cell boundary.
4. The join is an **equi-join** on `(t_min, lat_cell, lon_cell)` — Spark handles this efficiently with a shuffle hash join.
5. We filter `mmsi_a < mmsi_b` to dedupe ordered pairs.
6. Only the surviving candidate pairs have their actual Haversine distance computed.

This approach reduces the comparison count from O(N²) to roughly O(N · k) where k is the average vessel density per cell-minute. In the Baltic in December, k is small — typically tens, not thousands — making the join tractable on a single Spark node.

## 5. False-positive defense

A single AIS frame showing two vessels in the same place can be caused by a GPS error. Two defenses:

- **Per-MMSI implied-speed filter** drops the bad frame upstream.
- **Persistence requirement**: the close-pair condition must hold across ≥ 2 consecutive 1-minute windows. A glitch shows up in one minute and then disappears; a real collision shows up in several.

## 6. Result

_To be filled in after first full run._

- **Vessel A**: _NAME_ (MMSI _XXXXXXXXX_)
- **Vessel B**: _NAME_ (MMSI _XXXXXXXXX_)
- **Collision time (UTC)**: _YYYY-MM-DD HH:MM:SS_
- **Position A**: _LAT_, _LON_
- **Position B**: _LAT_, _LON_
- **Minimum distance**: _D_ m
- **Close-approach duration**: _N_ minutes

See `output/collision_map.html` for the trajectory map.

## 7. Computational notes

- **Caching**: `df` (filtered) is cached because it's read three times — for the GPS-jump filter, the candidate join, and the name lookup.
- **`spark.sql.shuffle.partitions` = 200** is the Spark default and is fine for ~60 GB of input on a single beefy node. For a multi-node cluster, scale to 2-3× the total core count.
- **Driver memory** should be at least 4 GB (set via `SPARK_DRIVER_MEMORY` in docker-compose) — the final trajectory `.toPandas()` calls pull data to the driver, and the candidate-pair `.count()` can also fan out.
- **Broadcast hint** on the 9-row `offsets` DataFrame ensures Spark doesn't shuffle it.

## 8. Limitations and future work

- The 1.1 km cell + 8-neighbor approach catches all pairs within ~1.1 km, but a vessel exactly on a cell corner with a partner ~1.6 km away (diagonal corner of neighbor) might be missed. Since our collision threshold is 500 m, this isn't a practical issue.
- The implied-speed filter uses a fixed 60-knot ceiling; fast craft (RIBs, naval) can briefly exceed this. None are involved in commercial collisions of interest.
- The persistence threshold (≥ 2 minutes) could be tuned with more data.
