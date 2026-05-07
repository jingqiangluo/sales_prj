FROM python:3.11-slim

WORKDIR /usr/app

# === Install system dependencies for psycopg2 + general tools ===
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip

# Install dbt
RUN pip install --no-cache-dir \
    dbt-core \
    dbt-postgres

# Optional: extra packages
# COPY requirements.txt .
# RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

# Copy your dbt project
COPY . .

# Keep container running
CMD ["tail", "-f", "/dev/null"]