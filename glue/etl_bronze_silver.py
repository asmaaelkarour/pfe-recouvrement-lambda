"""
AWS Glue ETL Job – Bronze → Silver (TNR Recovery)

Transformations :
  - Lecture des fichiers JSON bruts depuis Bronze (11 tables indépendantes)
  - Validation et nettoyage des champs par table
  - Normalisation des types (dates, montants, codes clients)
  - Dédoublonnage par record_id
  - Écriture en Parquet partitionné dans Silver
"""

import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ─────────────────────────────────────────────
# INIT
# ─────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "SOURCE_BUCKET",
    "TARGET_BUCKET",
    "DATABASE_NAME",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_BUCKET = args["SOURCE_BUCKET"]
TARGET_BUCKET = args["TARGET_BUCKET"]
DATABASE_NAME = args["DATABASE_NAME"]
RUN_DATE      = datetime.utcnow().strftime("%Y-%m-%d")

print(f"[Bronze→Silver] Starting ETL run: {RUN_DATE}")
print(f"  Source : s3://{SOURCE_BUCKET}/incoming/")
print(f"  Target : s3://{TARGET_BUCKET}/silver/")

# ─────────────────────────────────────────────
# TABLES À TRAITER
# ─────────────────────────────────────────────
TABLES = [
    "clients",
    "contrats",
    "dossiers_recouvrement",
    "echeanciers",
    "factures",
    "impayes",
    "litiges",
    "mouvements_financiers",
    "paiements",
    "relances",
    "suspensions_ligne",
]

# ─────────────────────────────────────────────
# NETTOYAGE GÉNÉRIQUE
# ─────────────────────────────────────────────
def clean_generic(df, table):
    """Nettoyage générique applicable à toutes les tables"""

    # Calculer record_id : MD5 de tous les champs (valide en PySpark)
    columns = [F.col(col).cast(StringType()) for col in df.columns]
    df = df.withColumn(
        "record_id",
        F.md5(F.concat_ws("||", *columns))
    )

    # Ajouter métadonnées
    df = df.withColumn("ingested_at", F.current_timestamp())
    df = df.withColumn("source", F.lit("api_externe"))
    df = df.withColumn("table_name", F.lit(table))

    # Ajouter colonnes de partition (basé sur ingested_at)
    df = df.withColumn("year",  F.year(F.col("ingested_at")))
    df = df.withColumn("month", F.month(F.col("ingested_at")))
    df = df.withColumn("day",   F.dayofmonth(F.col("ingested_at")))

    # Dédoublonnage
    df = df.dropDuplicates(["record_id"])

    return df


# ─────────────────────────────────────────────
# RÈGLES MÉTIER PAR TABLE (SIMPLIFIED - Add more as schema is validated)
# ─────────────────────────────────────────────
def apply_business_rules(df):
    """Règles de nettoyage métier basiques - version simplifiée"""

    # Normaliser certaines colonnes texte en minuscule si elles existent
    if "statut" in df.columns:
        df = df.withColumn("statut", F.lower(F.col("statut")))

    if "email" in df.columns:
        df = df.withColumn("email", F.lower(F.col("email")))

    # Filtrer les montants négatifs si colonne existe
    if "montant" in df.columns:
        df = df.filter(F.col("montant") >= 0)

    return df


# ─────────────────────────────────────────────
# TRAITEMENT PAR TABLE
# ─────────────────────────────────────────────
total_written = 0

for table in TABLES:
    try:
        print(f"\n[Bronze→Silver] Processing table: {table}")

        # Lire la table spécifique depuis Bronze (JSON array déjà parsé par Spark)
        source_path = f"s3://{SOURCE_BUCKET}/incoming/{table}/"
        raw_df = spark.read.option("multiline", "true").json(source_path)

        raw_count = raw_df.count()
        print(f"  → Raw records: {raw_count}")

        if raw_count == 0:
            print(f"  ⚠ No data for table {table}, skipping")
            continue

        # Nettoyage générique
        cleaned_df = clean_generic(raw_df, table)

        # Règles métier spécifiques par table
        cleaned_df = apply_business_rules(cleaned_df)

        cleaned_count = cleaned_df.count()
        print(f"  → Clean records: {cleaned_count}")

        # Écriture en Parquet partitionné (une table Silver par source table)
        target_path = f"s3://{TARGET_BUCKET}/silver/{table}/"

        cleaned_df.write \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .option("compression", "snappy") \
            .parquet(target_path)

        print(f"  ✓ Written to: {target_path}")
        total_written += cleaned_count

    except Exception as e:
        print(f"  ✗ Error processing table {table}: {str(e)}")
        raise

print(f"\n[Bronze→Silver] ETL completed successfully.")
print(f"  Total records written: {total_written}")

job.commit()
