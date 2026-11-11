FROM apache/superset:latest

USER root

COPY requirements.txt /app/requirements.txt

RUN /usr/local/bin/pip install \
    --no-cache-dir \
    --target=/app/.venv/lib/python3.10/site-packages \
    -r /app/requirements.txt

USER superset