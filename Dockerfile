# =========================
# Stage 1: Base
# =========================
FROM harbor.dell.com/devops-images/debian-12/python-3.12:latest as base

# Set working directory
WORKDIR /app

# Install build-time system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    postgresql-server-dev-all \
    ca-certificates \
  && update-ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# =========================
# Stage 2: Runtime
# =========================
FROM harbor.dell.com/devops-images/debian-12/python-3.12:latest

# Set working directory
WORKDIR /app

# Copy Python dependencies from base stage
COPY --from=base /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages

# Copy deployment helpers
# COPY deployment/get_config_v1.py ./deployment/
COPY deployment/get_config_v2.py ./deployment/
COPY deployment/get_certs.py ./deployment/
COPY database_scripts/ ./database_scripts/

ARG BUCKET_NAME
ARG ACCESS_KEY_ID
ARG SECRET_ACCESS_KEY
ARG ECS_ENDPOINT
ARG ENVIRONMENT_TAG
ARG VAULT_ROLE_ID
ARG VAULT_SECRET_ID
ARG VAULT_ADDR
ARG VAULT_NAMESPACE
ARG KOB_NAMESPACE

ENV BUCKET_NAME=${BUCKET_NAME}
ENV ACCESS_KEY_ID=${ACCESS_KEY_ID}
ENV SECRET_ACCESS_KEY=${SECRET_ACCESS_KEY}
ENV ECS_ENDPOINT=${ECS_ENDPOINT}
ENV ENVIRONMENT_TAG=${ENVIRONMENT_TAG}
ENV VAULT_ROLE_ID=${VAULT_ROLE_ID}
ENV VAULT_SECRET_ID=${VAULT_SECRET_ID}
ENV VAULT_ADDR=${VAULT_ADDR}
ENV VAULT_NAMESPACE=${VAULT_NAMESPACE}
ENV KOB_NAMESPACE=${KOB_NAMESPACE}

ENV PYTHONHTTPSVERIFY=0

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=en_US.UTF-8

RUN apt-get update && apt-get install -y --no-install-recommends \
    # Base runtime essentials
    ca-certificates \
    locales \
    # Tools
    software-properties-common \
    wget \
    curl \
    unzip \
    git \
    # Libraries needed by python
    libpq-dev \
    libcairo2-dev \
    # OCR and PDF utilities
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-por \
    tesseract-ocr-spa \
    tesseract-ocr-fra \
    tesseract-ocr-ita \
    tesseract-ocr-swe \
    tesseract-ocr-jpn \
    tesseract-ocr-kor \
    tesseract-ocr-all \
    poppler-utils \
    # Java
    default-jre \
    # LibreOffice
    libreoffice \
    libreoffice-writer \
    libreoffice-calc \
    libreoffice-impress \
    libreoffice-core \
    libreoffice-common \
    s3fs \
    fuse \
  && locale-gen en_US.UTF-8 \
  && update-ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Fetch certs
RUN python deployment/get_certs.py && update-ca-certificates

# Copy application code
COPY kafka_framework/ ./kafka_framework/
COPY services/ ./services/
COPY *.py ./
COPY *.ini ./
COPY core/ ./core/

# Create directories 
RUN mkdir -p /app/input /app/output \
            /app/ip_content_management/input /app/ip_content_management/output \
            /app/deployment/tmp \
            /app/deployment/logs \
    && chmod -R 777 /app

# Create non-root user and fix permissions before switching
RUN useradd -m -u 1001 appuser \
    && chown -R appuser:appuser /app \
    && chown appuser:appuser $(python -m certifi) \
    && chmod 755 /usr/local/lib/python3.12/site-packages/certifi

RUN mkdir -p /etc/passwd-s3fs \
    && chown -R appuser:appuser /etc/passwd-s3fs \
    && chmod -R 755 /etc/passwd-s3fs

# Entrypoint script
COPY --chmod=755 entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
# Switch to non-root
USER appuser

# Python environment
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["./entrypoint.sh"]