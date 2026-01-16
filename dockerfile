# ---- build stage ----
FROM python:3.11-slim as builder
WORKDIR /wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc g++ \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps -w /wheels -r requirements.txt

# ---- runtime stage ----
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY --from=builder /wheels /wheels
RUN pip install --no-cache /wheels/* && rm -rf /wheels

# COPY UPDATED PATHS
COPY volguard/volguard.py /app/
COPY config/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh
# create non-root user
RUN groupadd -g 1000 volguard && useradd -u 1000 -g 1000 volguard
USER volguard
ENTRYPOINT ["/entrypoint.sh"]
