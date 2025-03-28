from pyspark.sql import SparkSession
import shutil
from pathlib import Path

# Initialiser Spark avec les configurations Delta
spark = SparkSession.builder \
    .appName("ConvertDeltaToSparkCompatible") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

# Base path et scale factors
base_path = "/app/data/"
scale_factors = ["sf10", "sf50", "sf100"]

# Convertir les fichiers Delta
for sf in scale_factors:
    original_delta_path = f"{base_path}{sf}/delta/"
    temp_delta_path = f"{base_path}{sf}/delta_temp/"
    parquet_path = f"{base_path}{sf}/parquet/data.parquet"

    print(f"Conversion de Delta pour {sf}...")
    if Path(parquet_path).exists():
        try:
            # Lire les données depuis Parquet (source fiable)
            df = spark.read.parquet(parquet_path)
            
            # Écrire en Delta dans un dossier temporaire
            df.write.format("delta").mode("overwrite").save(temp_delta_path)
            
            # Supprimer l’ancien dossier Delta s’il existe
            if Path(original_delta_path).exists():
                shutil.rmtree(original_delta_path)
            
            # Renommer le dossier temporaire
            shutil.move(temp_delta_path, original_delta_path)
            
            print(f"Delta pour {sf} converti avec succès.")
        except Exception as e:
            print(f"Erreur lors de la conversion de {sf} : {str(e)}")
    else:
        print(f"Fichier Parquet pour {sf} non trouvé à {parquet_path}")

# Arrêter Spark
spark.stop()
print("Conversion terminée !")