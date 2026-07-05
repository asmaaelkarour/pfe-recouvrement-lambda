"""
AWS Glue ETL Job – Bronze → Silver (TNR Recovery)

Transformations :
  - Lecture des fichiers JSON bruts depuis Bronze
  - Validation et nettoyage des champs
  - Normalisation des types (dates, montants, codes clients)
  - Dédoublonnage par record_id
  - Écriture en Parquet partitionné dans Silver
"""

import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import DropFields, Filter, Map
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, StringType, StructField, StructType, TimestampType
)

# ─────────────────────────────────────────────
# INIT
# ─────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "SOURCE_BUCKET",
    "TARGET_BUCKET",
    "DATABASE_NAME",
])

sc         = SparkContext()
glueContext = GlueContext(sc)
spark      = glueContext.spark_session
job        = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_BUCKET  = args["SOURCE_BUCKET"]
TARGET_BUCKET  = args["TARGET_BUCKET"]
DATABASE_NAME  = args["DATABASE_NAME"]
RUN_DATE       = datetime.utcnow().strftime("%Y-%m-%d")

print(f"[Bronze→Silver] Starting ETL run: {RUN_DATE}")
print(f"  Source : s3://{SOURCE_BUCKET}/incoming/")
print(f"  Target : s3://{TARGET_BUCKET}/transactions/")

# ─────────────────────────────────────────────
# LECTURE (Bronze)
# ─────────────────────────────────────────────
source_path = f"s3://{SOURCE_BUCKET}/incoming/"

# Lire les fichiers JSON arrays, puis exploser en lignes
raw_df = spark.read.option("multiline", "true").json(source_path)
raw_df = raw_df.select(F.explode(F.col("value")).alias("record"))
raw_df = raw_df.select("record.*")
print(f"[Bronze→Silver] Raw records read: {raw_df.count()}")

# ─────────────────────────────────────────────
# NETTOYAGE & VALIDATION
# ─────────────────────────────────────────────
import uuid as uuid_module

def clean_transactions(df):
    # Ajouter métadonnées manquantes
    df = df.withColumn("record_id", F.md5(F.concat_ws("||", F.col("*"))))
    df = df.withColumn("ingested_at", F.current_timestamp())
    df = df.withColumn("source", F.lit("api_externe"))

    # Sélectionner et typer les colonnes principales
    df = df.select(
        F.col("record_id"),
        F.col("ingested_at"),
        F.col("source"),
        F.col("id_client").cast(StringType()).alias("id_client"),
        F.col("nom_client").cast(StringType()).alias("nom_client"),
        F.col("montant").cast(DecimalType(15, 2)).alias("montant"),
        F.col("devise").cast(StringType()).alias("devise"),
        F.to_date(F.col("date_echeance"), "yyyy-MM-dd").alias("date_echeance"),
        F.col("statut").cast(StringType()).alias("statut"),
        F.col("categorie_client").cast(StringType()).alias("categorie_client"),
    )

    # Supprimer les lignes sans identifiant client
    df = df.filter(F.col("id_client").isNotNull() & (F.col("id_client") != ""))

    # Supprimer les montants négatifs invalides
    df = df.filter(F.col("montant") >= 0)

    # Normaliser la devise
    df = df.withColumn("devise", F.upper(F.col("devise")))
    df = df.fillna({"devise": "MAD", "statut": "EN_COURS"})

    # Ajouter colonnes de partition
    df = df.withColumn("year",  F.year(F.col("date_echeance")))
    df = df.withColumn("month", F.month(F.col("date_echeance")))
    df = df.withColumn("day",   F.dayofmonth(F.col("date_echeance")))

    # Dédoublonnage sur record_id
    df = df.dropDuplicates(["record_id"])

    return df


cleaned_df = clean_transactions(raw_df)
print(f"[Bronze→Silver] Clean records: {cleaned_df.count()}")

# ─────────────────────────────────────────────
# ÉCRITURE (Silver – Parquet partitionné)
# ─────────────────────────────────────────────
target_path = f"s3://{TARGET_BUCKET}/transactions/"

cleaned_df.write \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .option("compression", "snappy") \
    .parquet(target_path)

print(f"[Bronze→Silver] Written to: {target_path}")

# ─────────────────────────────────────────────
# MISE À JOUR DU DATA CATALOG
# ─────────────────────────────────────────────
silver_dyf = DynamicFrame.fromDF(cleaned_df, glueContext, "silver_transactions")

glueContext.write_dynamic_frame.from_catalog(
    frame         = silver_dyf,
    database      = DATABASE_NAME,
    table_name    = "silver_transactions",
    transformation_ctx = "write_silver",
)

print("[Bronze→Silver] ETL completed successfully.")
job.commit()
