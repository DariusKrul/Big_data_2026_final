# big_data_2026_final

PySpark utility that detects the closest vessel encounter (collision) in Danish AIS data for December 2021 within 50 nm of (55.225°N, 14.245°E), and renders the trajectories of both vessels in a ±10-minute window around the event.

## What it does

Given a directory of Danish AIS CSV files, the tool outputs the following files to the `output/` directory:

- `result.txt` – MMSI numbers, vessel names, collision timestamp, coordinates, minimum distance
- `collision_map.html` – interactive Folium map with both trajectories and the collision marker
- `trajectory_<MMSI>.csv` – the ±10 min position track for each vessel

## Prerequisites

- Docker 24+ installed and running (for execution)
- Danish AIS data for December 2021, available at https://web.ais.dk/aisdata/

## Docker Hub image

```
kruel1/bigdata_task_final:1.0
```

The image is built and pushed automatically via GitHub Actions on every push to `main`. The `latest` tag always tracks the most recent successful build.

## 1 — Download AIS data

```bash
./download_data.sh                  # full December 2021 (~60 GB extracted)
./download_data.sh 2021-12-13       # single day, useful for fast iteration
```

The script writes CSVs into `./data/`. The full month is large; for development, start with just the day of the suspected collision.

## 2 — Build the image (local; optional — CI does this for you)

```bash
docker build -t kruel1/bigdata_task_final:1.0 .
```

## 3 — Run the container

```bash
docker run --rm \
    -v ${PWD}/data:/data:ro \
    -v ${PWD}/output:/app/output \
    kruel1/bigdata_task_final:1.0
```

This mounts the AIS CSVs (read-only) and writes the result files to a local `output/` directory.

Or with `docker-compose`:

```bash
docker compose up --build
```

## Dockerfile

```dockerfile
FROM python:3.11-slim
LABEL maintainer="kruel1" \
      version="1.0" \
      description="Vessel collision detection on Danish AIS data using PySpark"
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless procps && rm -rf /var/lib/apt/lists/*
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

To enable the CI workflow at `.github/workflows/docker-build.yml`:

1. Generate a Docker Hub access token: Docker Hub → Account Settings → Security → New Access Token (Read/Write/Delete).
2. In your GitHub repo: Settings → Secrets and variables → Actions → New repository secret. Add:
   - `DOCKERHUB_USERNAME` — your Docker Hub username (`kruel1`)
   - `DOCKERHUB_TOKEN` — the access token from step 1
3. Push to `main`. The Actions tab will show the build; the image will appear at `kruel1/bigdata_task_final:latest`.

## Methodology — summary

See `report.md` for the full write-up. Briefly:

- **Filtering** is done in this order (cheapest first): bounding-box → time → valid coords → Haversine refinement → mobile type → moving (SOG ≥ 0.5 kn) → nav status not in {Moored, At anchor, Aground}.
- **GPS noise** is removed per-MMSI by computing implied speed between consecutive AIS frames and dropping any frame implying > 60 knots from the previous one.
- **Collision detection** uses a spatial-temporal bucket self-join: each row is keyed by (1-minute window, ~1.1 km lat/lon cell). The right side of the join emits 8 neighbor cells, so any pair within ~1.1 km of each other in the same minute is a candidate. This avoids the O(n²) Cartesian product and is the central computational optimisation.
- **False-positive defense** comes from requiring the close encounter to persist across at least 2 consecutive minutes — a single-frame GPS jump won't satisfy this.

## Project structure

```
.
├── .github/workflows/docker-build.yml   # CI: builds + pushes to Docker Hub
├── data/                # raw AIS CSVs (gitignored, populated by download_data.sh)
├── output/              # results (gitignored)
├── Dockerfile
├── docker-compose.yml
├── download_data.sh
├── main.py              # PySpark pipeline
├── README.md
├── report.md            # written report
└── requirements.txt
```
