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
# RÈGLES MÉTIER PAR TABLE
# ─────────────────────────────────────────────
def apply_business_rules(df, table):
    """Règles de nettoyage métier spécifiques par table"""

    if table == "clients":
        # Email en minuscule, supprimer clients sans ID
        if "email" in df.columns:
            df = df.withColumn("email", F.lower(F.col("email")))
        df = df.filter((F.col("id_client").isNotNull()) & (F.col("id_client") != ""))

    elif table == "factures":
        # Montants positifs, dates cohérentes
        if "montant" in df.columns:
            df = df.filter(F.col("montant") >= 0)
        if "date_facture" in df.columns and "date_echeance" in df.columns:
            df = df.filter(F.col("date_facture") <= F.col("date_echeance"))

    elif table == "paiements":
        # Montants positifs, date de paiement récente
        if "montant" in df.columns:
            df = df.filter(F.col("montant") > 0)
        if "date_paiement" in df.columns:
            df = df.filter(F.col("date_paiement") <= F.current_date())

    elif table == "impayes":
        # Montants > 0, montant_du >= montant_recouvre
        if "montant_du" in df.columns:
            df = df.filter(F.col("montant_du") > 0)
        if "montant_du" in df.columns and "montant_recouvre" in df.columns:
            df = df.filter(F.col("montant_du") >= F.col("montant_recouvre"))

    elif table == "relances":
        # Montant >= 0, statut valide
        if "montant" in df.columns:
            df = df.filter(F.col("montant") >= 0)
        if "statut" in df.columns:
            valid_statuts = ["en_cours", "resolue", "litigieuse"]
            df = df.filter(F.lower(F.col("statut")).isin(valid_statuts))

    elif table == "echeanciers":
        # Date d'échéance future ou actuelle, montant >= 0
        if "date_echeance" in df.columns:
            df = df.filter(F.col("date_echeance") >= F.current_date())
        if "montant" in df.columns:
            df = df.filter(F.col("montant") >= 0)

    elif table == "litiges":
        # Statut valide, date de litige récente
        if "statut" in df.columns:
            valid_statuts = ["ouvert", "clos", "en_cours"]
            df = df.filter(F.lower(F.col("statut")).isin(valid_statuts))
        if "date_litige" in df.columns:
            df = df.filter(F.col("date_litige") >= F.date_sub(F.current_date(), 730))

    elif table == "mouvements_financiers":
        # Montant != 0, date récente
        if "montant" in df.columns:
            df = df.filter(F.col("montant") != 0)
        if "date_mouvement" in df.columns:
            df = df.filter(F.col("date_mouvement") <= F.current_date())

    elif table == "contrats":
        # Date début <= date fin
        if "date_debut" in df.columns and "date_fin" in df.columns:
            df = df.filter(F.col("date_debut") <= F.col("date_fin"))

    elif table == "dossiers_recouvrement":
        # Montant >= 0, statut valide
        if "montant_du" in df.columns:
            df = df.filter(F.col("montant_du") >= 0)
        if "statut" in df.columns:
            valid_statuts = ["ouvert", "clos", "suspendu"]
            df = df.filter(F.lower(F.col("statut")).isin(valid_statuts))

    elif table == "suspensions_ligne":
        # Date de suspension <= aujourd'hui
        if "date_suspension" in df.columns:
            df = df.filter(F.col("date_suspension") <= F.current_date())

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
        cleaned_df = apply_business_rules(cleaned_df, table)

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
