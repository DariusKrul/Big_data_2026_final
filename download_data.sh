#!/usr/bin/env bash
# Download Danish AIS daily CSV files into ./data/
# Usage:
#   ./download_data.sh                 # downloads all of December 2021
#   ./download_data.sh 2021-12-13      # downloads a single day
#   ./download_data.sh 2021-12-10 2021-12-15   # downloads a date range
#
# Each daily file is ~150-200 MB compressed, ~2-4 GB uncompressed.
# Source: Danish Maritime Authority — https://web.ais.dk/aisdata/
#
# NOTE: verify the URL pattern below by visiting https://web.ais.dk/aisdata/ in
# a browser. The host has changed in the past (web.ais.dk vs aisdata.ais.dk);
# adjust BASE_URL if downloads 404.

set -euo pipefail

BASE_URL="https://web.ais.dk/aisdata"
DATA_DIR="./data"

mkdir -p "$DATA_DIR"

# Default: full month of December 2021
START="${1:-2021-12-01}"
END="${2:-${1:-2021-12-31}}"

# Date arithmetic works on GNU date (Linux). On macOS install coreutils and use gdate.
date_cmd="date"
if [[ "$(uname)" == "Darwin" ]]; then
    if command -v gdate &> /dev/null; then
        date_cmd="gdate"
    else
        echo "On macOS, install coreutils: brew install coreutils"
        exit 1
    fi
fi

current="$START"
while [[ "$current" != "$($date_cmd -d "$END + 1 day" +%Y-%m-%d)" ]]; do
    filename="aisdk-${current}.zip"
    url="${BASE_URL}/${filename}"
    target="${DATA_DIR}/${filename}"

    if [[ -f "${target%.zip}.csv" ]]; then
        echo "[skip] ${current} — CSV already extracted"
    elif [[ -f "$target" ]]; then
        echo "[skip] ${current} — zip already present, extracting"
        unzip -o "$target" -d "$DATA_DIR"
        rm "$target"
    else
        echo "[get ] ${current}"
        curl -fL --retry 3 -o "$target" "$url"
        unzip -o "$target" -d "$DATA_DIR"
        rm "$target"
    fi

    current="$($date_cmd -d "$current + 1 day" +%Y-%m-%d)"
done

echo "Done. Files in $DATA_DIR:"
ls -lh "$DATA_DIR"
