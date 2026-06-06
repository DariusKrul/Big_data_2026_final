# Report — Vessel Collision Detection

## 1. Problem

Identify the pair of vessels that collided (or came closest to colliding) in December 2021 within 50 nautical miles of (55.225°N, 14.245°E) — a point in the southwestern Baltic Sea, southeast of Bornholm — using Danish AIS data. Visualise the trajectories of both vessels in a ±10-minute window around the collision.

## 2. Data

- **Source**: Danish Maritime Authority — https://web.ais.dk/aisdata/
- **Period**: 2021-12-01 to 2021-12-31 (full month)
- **Volume**: ~57 GB extracted across 31 daily CSVs (`aisdk-2021-12-DD.csv`), 1.7-2.1 GB per day
- **Schema**: 26 columns. The pipeline uses an explicit `StructType` rather than letting Spark infer the schema — inference would require an additional pass over 57 GB of data.

## 3. Filtering pipeline

Filters are applied in order of cost: cheap predicates (column comparisons, range filters) run first; expensive ones (Haversine, window functions) run only on the surviving rows.

### Row-level filters (applied during initial load)

| # | Filter | Rationale |
|---|--------|-----------|
| 1 | `timestamp` in December 2021 | Assignment timeframe |
| 2 | Latitude ∈ [54.0, 56.5], Longitude ∈ [12.5, 16.0] | Bounding-box pre-filter, ~1° padded around centre to fully cover 50 nm radius |
| 3 | Non-null MMSI, latitude, longitude | Drop malformed rows |
| 4 | MMSI ∈ [200000000, 799999999] | Ship-station MMSIs use country MID prefixes 201–775; excludes SAR aircraft (111xxx), AIS-SART (970xxx), MOB devices (972xxx), EPIRB (974xxx), AtoN (98xxxx–99xxxx) |
| 5 | Haversine distance from (55.225°N, 14.245°E) ≤ 92.6 km | Exact 50 nm refinement |
| 6 | `Type of mobile` = `"Class A"` | Restricts to commercial vessels with mandatory transponders; excludes Class B pleasure craft, base stations, AtoN, SART |
| 7 | `Navigational status` ∉ {Moored, At anchor, Aground} | Excludes stationary vessels by declared status |
| 8 | `SOG` ≥ 0.5 knots | Numerical confirmation the vessel is moving |
| 9 | `Ship type` does not match regex `pilot\|tug\|search and rescue\|dredg\|tender\|anti-pollution\|law enforcement` | Excludes vessels whose declared type indicates routine operational close-quarters work |
| 10 | `Name` does not match regex `PILOT\|TUG\|RESCUE\|KBV \|SAR ` | Belt-and-suspenders: catches operational vessels whose `Ship type` is null or `"Undefined"` (e.g. DANPILOT PAPA in the dataset) |
| 11 | Implied speed from previous AIS frame ≤ 60 knots | Drop GPS jumps that would otherwise look like teleportation |

### Pair-level filters (applied to detected close encounters)

| # | Filter | Rationale |
|---|--------|-----------|
| 12 | Close-approach duration ≥ 2 minutes (consecutive) | Anti-glitch persistence requirement |
| 13 | Close-approach duration ≤ 30 minutes | Collisions are brief events; longer durations indicate docked/rafted vessels |
| 14 | Mean SOG ≥ 2.0 knots for both vessels during encounter | Confirms both vessels were actually underway when close |

## 4. Collision detection

The naïve approach — compare every vessel against every other at every timestamp — is O(N²) and infeasible at this data volume. The pipeline instead uses a **spatial-temporal bucket self-join**:

1. Each row gets keys `(t_min, lat_cell, lon_cell)` where `t_min = floor(unix_ts / 60)` and `lat_cell / lon_cell` use a 0.01° grid (~1.1 km cells at this latitude).
2. The **left side** of the join emits one row per AIS message.
3. The **right side** emits 9 rows per AIS message: the original cell plus all 8 neighbour cells. This catches pairs that straddle a cell boundary.
4. The join is an **equi-join** on `(t_min, lat_cell, lon_cell)` — Spark handles this efficiently with a shuffle hash join.
5. The `mmsi_a < mmsi_b` filter deduplicates ordered pairs.
6. Only the surviving candidate pairs have their actual Haversine distance computed; pairs with `dist_m < 500 m` are retained as "close encounters."

This reduces the comparison count from O(N²) to roughly O(N · k) where k is the average vessel density per cell-minute. In the Baltic in December, k is small — typically tens, not thousands — making the join tractable on a single Spark node.

Haversine distance is expressed as a Spark **column expression** rather than a Python UDF, avoiding per-row serialization overhead between the JVM and Python.

## 5. False-positive defense — iterative refinement

The southwest Baltic is a busy maritime area with many close-quarters operations that are *not* collisions: pilot transfers, tug operations, rescue work, dockings, fishing pairs. Ranking by minimum GPS distance is **structurally biased** toward these operations — a deliberate alongside operation produces 0.2–5 m of GPS separation, while an actual collision between two 50–90 m commercial vessels still has 30–60 m between their AIS antennas at impact.

Initial pipeline runs on the full month surfaced four distinct classes of false positive, each requiring an additional filter. This iterative refinement is the substantive engineering content of the project.

| Iteration | False positive surfaced | Why it passed earlier filters | Filter added |
|-----------|------------------------|------------------------------|--------------|
| **1** | **RESCUE SJOMANSHUSET / RESCUE PANTAMERA** — 0.2 m apart for 219 min at the Simrishamn SSRS station on Dec 16 | Both rescue boats emitted occasional SOG > 0.5 kn at the mooring due to GPS jitter and wave motion, passing the row-level moving filter | **Pair-level**: max encounter duration ≤ 30 min, mean SOG of both vessels ≥ 2.0 kn during encounter |
| **2** | **KBV 302 / SAR aircraft (MMSI 111219512)** — 0.5 m apart for 25 min east of Bornholm on Dec 13 at 08:45 UTC, ~5 hours after the real collision | The SAR aircraft (MMSI prefix 111) is not a ship station and has no meaningful `Ship type`; previous filters did not address non-vessel MMSI ranges | **MMSI range filter**: restrict to ship-station MMSIs in [200000000, 799999999], dropping SAR aircraft, AIS-SART, EPIRBs, AtoN signals |
| **3** | **DANPILOT PAPA / MV ICE POINT** — 2.1 m apart for 19 min on Dec 16, classic pilot-transfer signature | DANPILOT PAPA had its `Ship type` recorded as literally `"Undefined"` in the AIS data, evading a type-based exclusion based on enumerated values | **Name-pattern filter**: regex match for `PILOT\|TUG\|RESCUE\|KBV \|SAR ` in vessel name field |
| **4** | **SILLE BOB / JANNE** — 3.2 m apart for 6 min on Dec 29, two Danish pleasure craft (5 m × 2 m each) rafted together | Class B pleasure craft were not previously excluded; they passed all "commercial vessel" filters because the original `EXCLUDED_MOBILE_TYPES` was a blocklist rather than an allowlist | **Mobile type filter (allowlist)**: restrict to `Class A` only, eliminating Class B pleasure craft, base stations, AtoN, SART |

After all four filters were applied, the pipeline correctly identified the Scot Carrier / Karin Høj collision (Dec 13, 02:27 UTC) as the closest moving-vessel encounter in the area for the month — the actual maritime accident the assignment is designed around.

Each false positive corresponds to a structurally distinct class of close-quarters maritime activity, and each filter targets that specific class. A collision is, in fact, the *exception* among close GPS encounters in commercial waters; isolating it required identifying and excluding every category of routine intentional close-quarters work.

## 6. Result

- **Vessel A**: **KARIN HOEJ** (MMSI 219021240, Denmark)
- **Vessel B**: **MV SCOT CARRIER** (MMSI 232018267, United Kingdom)
- **Collision time (UTC)**: **2021-12-13 02:27:43.5**
- **Position A**: 55.223067°N, 14.243730°E
- **Position B**: 55.223092°N, 14.243683°E
- **Minimum distance**: **4.1 m**
- **Close-approach duration**: 3 minutes

The collision location is in the southwestern Baltic Sea, approximately 10 nautical miles south of Ystad, Sweden — corresponding to the publicly documented December 13, 2021 maritime incident in which the British cargo vessel Scot Carrier struck the Danish cargo vessel Karin Høj, which subsequently capsized.

The 3-minute close-approach duration observed in the data reflects that Karin Høj's AIS transmissions ceased shortly after the collision (consistent with vessel inversion), so the encounter was only observable in AIS data for the minutes immediately surrounding impact. This asymmetric AIS dropout is itself a strong corroborating signature of a genuine collision, distinguishing it from the routine close-quarters operations identified earlier as false positives — those involve both vessels continuing to transmit normally throughout and after the encounter.

See `output/collision_map.html` for the trajectory map (±10 minutes around the collision) and `output/result.txt` for the machine-generated result block.

## 7. Computational notes

- **Spark configuration**:
  - `spark.sql.shuffle.partitions = 400` (doubled from default 200 for the full-month workload)
  - `spark.sql.adaptive.enabled = true` and `spark.sql.adaptive.coalescePartitions.enabled = true` — Adaptive Query Execution automatically handles data skew on the candidate-pair join, which is critical because vessel density varies dramatically between cells (busy shipping lanes vs. open water)
  - `spark.driver.maxResultSize = 2g` to accommodate `.collect()` and `.count()` calls on cached intermediate DataFrames

- **Caching**: The filtered DataFrame is cached because it is read three times: for the GPS-jump window function, for the candidate-pair self-join, and for the name-resolution step.

- **Broadcast hint**: The 9-row neighbour-offsets DataFrame is broadcast (`F.broadcast(offsets)`) to avoid shuffling it across executors.

- **Driver memory**: 8 GB, set via `SPARK_DRIVER_MEMORY` environment variable in `docker-compose.yml`.

- **WSL2 memory tuning**: On Windows + Docker Desktop with the WSL2 backend, resource limits are managed via `~/.wslconfig` rather than the Docker UI. Development used `memory=12GB`.

- **Runtime**: The full-month pipeline runs end-to-end in approximately 30–60 minutes on a typical developer laptop. The dominant cost is the candidate-pair self-join + Haversine computation, which produces tens of millions of intermediate rows before filtering down to a few thousand persistent close-approach events.

## 8. Limitations and future work

- **Structural bias of "minimum distance"**: The closest GPS proximity in the data is structurally produced by deliberate alongside operations, not collisions (collisions involve vessels whose hulls touch but whose GPS antennas remain meters apart). The four false-positive filters address this empirically; a more principled approach would use **asymmetric AIS dropout** — at least one vessel typically loses transmission capability during a real collision (as Karin Høj did when capsizing). Implementing this would require analyzing per-MMSI signal continuity in the window following a close encounter and ranking candidates by dropout asymmetry.

- **Geographic generalisation**: The MMSI range, name-pattern, and ship-type filters are tuned to Danish-Baltic data conventions. Adapting to other geographies would require localising operational vessel name patterns (e.g. SSRS prefix `RESCUE`, Danish pilot service prefix `DANPILOT`, Swedish coast guard prefix `KBV`).

- **Spatial cell size**: The 0.01° / ~1.1 km cells with 8-neighbour explosion catch all pairs within ~1.1 km, but a vessel exactly on a cell corner with a partner 1.6 km away (diagonal corner of a neighbour cell) could be missed. Since the close-encounter threshold is 500 m, this is not a practical issue.

- **Implied-speed ceiling**: A fixed 60-knot ceiling for the GPS-jump filter is appropriate for commercial cargo vessels but would falsely drop fast craft (RIBs, naval, hydrofoils). None are involved in commercial collisions of interest.

- **Persistence threshold**: ≥ 2 consecutive minutes was sufficient for this dataset; a longer window (≥ 3–5 min) might be appropriate for areas with more GPS noise. The 3-minute close-approach duration of the actual collision was right at the lower bound of the persistence requirement, reflecting Karin Høj's AIS dropout.
