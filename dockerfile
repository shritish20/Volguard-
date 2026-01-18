# ---- build stage ----
FROM python:3.11-slim as builder
WORKDIR /wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps -w /wheels -r requirements.txt

# ---- runtime stage ----
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY --from=builder /wheels /wheels
# FIX: Only install .whl files, ignore requirements.txt
RUN pip install --no-cache /wheels/*.whl && rm -rf /wheels

# COPY UPDATED PATHS
COPY entrypoint.sh /entrypoint.sh
COPY volguard/volguard.py /app/

RUN chmod +x /entrypoint.sh
# create non-root user
RUN groupadd -g 1000 volguard && useradd -u 1000 -g 1000 volguard
USER volguard
ENTRYPOINT ["/entrypoint.sh"]
