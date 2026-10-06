ARG PYTHON_IMAGE=python:3.12.15-slim-bookworm
FROM ${PYTHON_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

RUN groupadd --system app && useradd --system --gid app --home /app app

COPY --chown=app:app src /app/src
COPY --chown=app:app README.md /app/README.md
COPY --chown=app:app HOW_TO_CALL.md /app/HOW_TO_CALL.md
COPY --chown=app:app LICENSE /app/LICENSE

USER app

EXPOSE 8000

CMD ["python", "src/3_build_api.py", "--host", "0.0.0.0", "--port", "8000"]
