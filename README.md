# bigdata-task-final-krul

PySpark utility that detects the closest moving-vessel encounter (collision) in Danish AIS data for December 2021 within 50 nm of (55.225°N, 14.245°E), and renders the trajectories of both vessels in a ±10-minute window around the event.

## Result

The pipeline identifies the **Scot Carrier / Karin Høj collision** of December 13, 2021 — a publicly documented maritime accident in the southwestern Baltic Sea.

- **Vessel A**: KARIN HOEJ (MMSI 219021240, Denmark)
- **Vessel B**: MV SCOT CARRIER (MMSI 232018267, United Kingdom)
- **Collision time (UTC)**: 2021-12-13 02:27:43.5
- **Position**: 55.223°N, 14.244°E (approximately 10 nm south of Ystad, Sweden)
- **Minimum distance**: 4.1 m
- **Close-approach duration**: 3 minutes (Karin Høj's AIS ceased transmitting shortly after impact, consistent with the vessel capsizing)

See `report.md` for the full methodology and `output/collision_map.html` for the trajectory visualisation.

## What it does

Given a directory of Danish AIS CSV files, the tool outputs the following files to the `output/` directory:

- `result.txt` — MMSI numbers, vessel names, collision timestamp, coordinates, minimum distance
- `collision_map.html` — interactive Folium map with both trajectories and the collision marker
- `trajectory_<MMSI>.csv` — the ±10 min position track for each vessel

## Prerequisites

- Docker 24+ installed and running (for execution)
- Danish AIS data for December 2021, available at https://web.ais.dk/aisdata/

## Docker Hub image

```
kruel1/bigdata_task_final:latest
```

The image is built and pushed automatically via GitHub Actions on every push to `main`. The `latest` tag always tracks the most recent successful build.

## 1 — Download AIS data

Download the monthly archive from https://web.ais.dk/aisdata/ (file: `aisdk-2021-12.zip`, ~17 GB compressed, ~57 GB extracted across 31 daily CSVs).

Extract the CSVs to a directory of your choice (e.g. `D:\BIG-DATA-2026\` on Windows, or `./data/` on Linux/macOS).

## 2 — Build the image (local; optional — CI does this for you)

```bash
docker build -t kruel1/bigdata_task_final:1.0 .
```

## 3 — Run the container

Update `docker-compose.yml` so the data volume points to wherever your extracted CSVs live:

```yaml
volumes:
  - D:/BIG-DATA-2026:/data:ro      # Windows example
  - ./output:/app/output
```

Then:

```bash
docker compose up --build
```

Or with plain `docker run`:

```bash
docker run --rm \
    -e SPARK_DRIVER_MEMORY=8g \
    -v /path/to/ais/csvs:/data:ro \
    -v ${PWD}/output:/app/output \
    kruel1/bigdata_task_final:latest
```

Runtime is approximately 30–60 minutes on a typical developer laptop.

## Dockerfile

```dockerfile
FROM python:3.11-slim
LABEL maintainer="kruel1" \
      version="1.0" \
      description="Vessel collision detection on Danish AIS data using PySpark"
RUN apt-get update && apt-get install -y --no-install-recommends \
        default-jre-headless procps && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
VOLUME ["/data", "/app/output"]
ENTRYPOINT ["python", "/app/main.py"]
CMD ["--input", "/data", "--output", "/app/output"]
```

## GitHub Actions / Docker Hub setup

The CI workflow at `.github/workflows/docker-build.yml` builds and pushes the image to Docker Hub on every push to `main`. Required repository secrets:

- `DOCKERHUB_USERNAME` — `kruel1`
- `DOCKERHUB_TOKEN` — Docker Hub access token (Read/Write/Delete)

## Methodology — summary

See `report.md` for the full write-up. Briefly:

- **Data engineering**: 57 GB of CSVs loaded with an explicit schema; filtered in cost order (cheap predicates first, Haversine only on survivors).

- **Spatial-temporal bucket self-join** is the central computational optimisation, avoiding the O(N²) Cartesian product. Rows are bucketed into (1-minute, ~1.1 km lat/lon cell) keys, and one side of the join emits 8 neighbour cells so pairs straddling boundaries are still matched.

- **Eleven row-level filters** progressively reduce noise: bounding box → time → valid coords → MMSI range (excludes aircraft, SART, AtoN) → Haversine refinement → Class A mobile type → navigational status → moving (SOG ≥ 0.5 kn) → ship-type pattern (excludes pilot/tug/rescue/dredging) → name pattern (catches operational vessels with undefined ship-type) → GPS-jump filter (per-MMSI implied-speed check).

- **Three pair-level filters** target close encounters: persistence ≥ 2 minutes (anti-glitch), duration ≤ 30 minutes (excludes docked/rafted vessels), mean SOG ≥ 2 kn for both vessels (confirms underway during the encounter).

- **Iterative false-positive defense**: the full month surfaced four distinct classes of non-collision close encounter — rescue boats docked at a SSRS station, a SAR helicopter winching to a Coast Guard vessel, a pilot boat performing a transfer to a cargo ship, and two pleasure craft rafted together. Each surfaced a different category of operational maritime activity and required a different filter. After all four iterations, the pipeline correctly isolates the Scot Carrier / Karin Høj collision.

## Project structure

```
.
├── .github/workflows/docker-build.yml   # CI: builds + pushes to Docker Hub
├── data/                # raw AIS CSVs (gitignored)
├── output/              # results (gitignored)
├── Dockerfile
├── docker-compose.yml
├── download_data.sh
├── main.py              # PySpark pipeline
├── README.md
├── report.md            # written report
└── requirements.txt
```
