FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src /app/src
COPY scripts /app/scripts
COPY README.md /app/README.md
COPY LICENSE /app/LICENSE

RUN chmod +x /app/scripts/compose_start.sh \
    && mkdir -p /app/data/clean /app/data/raw_sql /app/data/raw_parquet /app/data/raw /tmp/ojs_api_api_duckdb_tmp

EXPOSE 8000

CMD ["/app/scripts/compose_start.sh"]
