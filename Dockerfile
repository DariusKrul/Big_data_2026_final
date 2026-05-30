FROM python:3.11-slim

LABEL maintainer="kruel1" \
      version="1.0" \
      description="Vessel collision detection on Danish AIS data using PySpark"

# Java is required for PySpark. procps gives us `ps` which Spark uses to manage workers.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps && \
    rm -rf /var/lib/apt/lists/*

# Debian creates this symlink automatically when default-jre is installed; we use it
# so the image works on both amd64 and arm64 without arch-specific paths.
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# /data: read-only mount for raw AIS CSVs (host provides them)
# /app/output: write target for results and the map HTML
VOLUME ["/data", "/app/output"]

ENTRYPOINT ["python", "/app/main.py"]
CMD ["--input", "/data", "--output", "/app/output"]
