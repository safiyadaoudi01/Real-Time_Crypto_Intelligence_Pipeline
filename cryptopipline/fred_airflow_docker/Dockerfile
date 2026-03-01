FROM apache/airflow:2.8.1

USER root

# Dépendances système si besoin
RUN apt-get update && apt-get install -y \
    gcc \
    && apt-get clean

USER airflow

# Installer libs Python
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt