"""
AWS Glue ETL Job – Silver → Gold (TNR Recovery)

Transformations :
  - Lecture des transactions nettoyées depuis Silver
  - Calcul des KPIs de recouvrement par client et par catégorie
  - Agrégation mensuelle des montants dus / recouvrés
  - Scoring de risque simplifié
  - Écriture en Parquet partitionné dans Gold
"""

import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# ─────────────────────────────────────────────
# INIT
# ─────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "SOURCE_BUCKET",
    "TARGET_BUCKET",
    "DATABASE_NAME",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_BUCKET = args["SOURCE_BUCKET"]
TARGET_BUCKET = args["TARGET_BUCKET"]
DATABASE_NAME = args["DATABASE_NAME"]
RUN_DATE = datetime.utcnow().strftime("%Y-%m-%d")

print(f"[Silver→Gold] Starting ETL run: {RUN_DATE}")

# ─────────────────────────────────────────────
# LECTURE (Silver)
# ─────────────────────────────────────────────
silver_path = f"s3://{SOURCE_BUCKET}/transactions/"
silver_df = spark.read.parquet(silver_path)
print(f"[Silver→Gold] Silver records: {silver_df.count()}")

# ─────────────────────────────────────────────
# AGRÉGATION PAR CLIENT
# ─────────────────────────────────────────────
client_agg = silver_df.groupBy(
    "id_client", "nom_client", "categorie_client", "statut", "devise"
).agg(
    F.count("record_id").alias("nb_transactions"),
    F.sum("montant").cast(DecimalType(18, 2)).alias("montant_du"),
    F.sum(
        F.when(F.col("statut") == "SOLDE", F.col("montant")).otherwise(F.lit(0))
    ).cast(DecimalType(18, 2)).alias("montant_recouvre"),
    F.max("date_echeance").alias("derniere_echeance"),
    F.min("date_echeance").alias("premiere_echeance"),
)

# Taux de recouvrement
client_agg = client_agg.withColumn(
    "taux_recouvrement",
    F.round(
        F.col("montant_recouvre") *
        100 /
        F.nullif(
            F.col("montant_du"),
            F.lit(0)),
        2).cast(
        DecimalType(
            5,
            2)))

# ─────────────────────────────────────────────
# SCORING DE RISQUE
# ─────────────────────────────────────────────


def compute_risk_score(df):
    days_overdue = F.datediff(F.current_date(), F.col("derniere_echeance"))
    df = df.withColumn("jours_retard", days_overdue)

    df = df.withColumn("score_risque", F.when(
        F.col("jours_retard") > 360, F.lit("CRITIQUE")
    ).when(
        F.col("jours_retard") > 180, F.lit("ELEVE")
    ).when(
        F.col("jours_retard") > 90, F.lit("MOYEN")
    ).when(
        F.col("jours_retard") > 30, F.lit("FAIBLE")
    ).otherwise(F.lit("SAIN")))

    return df


client_agg = compute_risk_score(client_agg)

# ─────────────────────────────────────────────
# AGRÉGATION MENSUELLE (pour dashboard)
# ─────────────────────────────────────────────
monthly_agg = silver_df.groupBy(
    "year",
    "month",
    "categorie_client").agg(
        F.count("record_id").alias("nb_dossiers"),
        F.sum("montant").cast(
            DecimalType(
                18,
                2)).alias("total_montant_du"),
    F.sum(
                    F.when(
                        F.col("statut") == "SOLDE",
                        F.col("montant")).otherwise(
                            F.lit(0))).cast(
                                DecimalType(
                                    18,
                                    2)).alias("total_montant_recouvre"),
)

monthly_agg = monthly_agg.withColumn(
    "taux_recouvrement_mensuel",
    F.round(
        F.col("total_montant_recouvre") *
        100 /
        F.nullif(
            F.col("total_montant_du"),
            F.lit(0)),
        2).cast(
        DecimalType(
            5,
            2)))

# ─────────────────────────────────────────────
# ÉCRITURE (Gold)
# ─────────────────────────────────────────────
gold_recouvrement_path = f"s3://{TARGET_BUCKET}/recouvrement/"
gold_monthly_path = f"s3://{TARGET_BUCKET}/recouvrement_mensuel/"

client_agg.write \
    .mode("overwrite") \
    .partitionBy("categorie_client") \
    .option("compression", "snappy") \
    .parquet(gold_recouvrement_path)

monthly_agg.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .option("compression", "snappy") \
    .parquet(gold_monthly_path)

print(f"[Silver→Gold] Written recouvrement to: {gold_recouvrement_path}")
print(f"[Silver→Gold] Written monthly stats to: {gold_monthly_path}")
print(
    f"[Silver→Gold] ETL completed. Client aggregations: {
        client_agg.count()}")

job.commit()
