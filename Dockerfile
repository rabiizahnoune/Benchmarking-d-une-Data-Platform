FROM python:3.11

# Installer les dépendances système
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    openjdk-17-jre-headless \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Télécharger les JARs pour Iceberg et Delta
RUN mkdir -p /app/jars \
    && curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.3/iceberg-spark-runtime-3.4_2.12-1.4.3.jar -o /app/jars/iceberg-spark-runtime-3.4_2.12-1.4.3.jar \
    && curl -L https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar -o /app/jars/delta-core_2.12-2.4.0.jar



# Répertoire de travail
WORKDIR /app

# Installer les dépendances Python avec force-reinstall pour éviter les problèmes de cache
COPY requirements.txt .
RUN pip install --no-cache-dir --force-reinstall -r requirements.txt


EXPOSE 8888
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]