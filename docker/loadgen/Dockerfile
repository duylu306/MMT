FROM python:3.12.5-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /workspace

COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip==24.3.1 && \
    pip install -r /tmp/requirements.txt && \
    rm -f /tmp/requirements.txt

CMD ["python", "-m", "scripts.smoke_test"]
