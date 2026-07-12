"""
AWS Glue ETL Job – Silver → Gold (PFE Recouvrement)

Transformations :
  - Lecture des tables Silver réelles (clients, factures, paiements,
    impayes, dossiers_recouvrement)
  - Agrégation par client : montant facturé, payé, impayé, dette totale
  - Calcul du taux de recouvrement et d'un score de risque simplifié
  - Écriture en Parquet partitionné par categorie_client (segment) dans Gold
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

print("[Silver->Gold] Starting ETL run: " + RUN_DATE)

# ─────────────────────────────────────────────
# LECTURE DES TABLES SILVER REELLES
# ─────────────────────────────────────────────
def read_silver_table(table_name):
    path = "s3://" + SOURCE_BUCKET + "/silver/" + table_name + "/"
    df = spark.read.parquet(path)
    print("[Silver->Gold] " + table_name + ": " + str(df.count()) + " records")
    return df

clients_df = read_silver_table("clients")
factures_df = read_silver_table("factures")
paiements_df = read_silver_table("paiements")
impayes_df = read_silver_table("impayes")
dossiers_df = read_silver_table("dossiers_recouvrement")

# ─────────────────────────────────────────────
# CAST DES COLONNES NUMERIQUES (stockees en string depuis le JSON source)
# ─────────────────────────────────────────────
factures_df = factures_df.withColumn(
    "montant_total_num", F.col("montant_total").cast(DecimalType(18, 2))
)

paiements_df = paiements_df.withColumn(
    "montant_paye_num", F.col("montant_paye").cast(DecimalType(18, 2))
)

impayes_df = impayes_df.withColumn(
    "montant_impaye_num", F.col("montant_impaye").cast(DecimalType(18, 2))
).withColumn(
    "jours_retard_num", F.col("jours_retard").cast("int")
)

dossiers_df = dossiers_df.withColumn(
    "montant_total_dette_num", F.col("montant_total_dette").cast(DecimalType(18, 2))
)

# ─────────────────────────────────────────────
# AGREGATIONS PAR CLIENT
# ─────────────────────────────────────────────
factures_agg = factures_df.groupBy("client_id").agg(
    F.sum("montant_total_num").alias("montant_facture"),
    F.count("facture_id").alias("nb_factures"),
    F.max("date_echeance").alias("derniere_echeance"),
)

paiements_agg = paiements_df.groupBy("client_id").agg(
    F.sum("montant_paye_num").alias("montant_paye_total"),
)

impayes_agg = impayes_df.groupBy("client_id").agg(
    F.sum("montant_impaye_num").alias("montant_impaye_total"),
    F.max("jours_retard_num").alias("jours_retard_max"),
)

dossiers_agg = dossiers_df.groupBy("client_id").agg(
    F.sum("montant_total_dette_num").alias("montant_dette_total"),
    F.count("dossier_id").alias("nb_dossiers"),
)

# ─────────────────────────────────────────────
# JOINTURE AVEC LA TABLE CLIENTS (LEFT JOIN : garder tous les clients)
# ─────────────────────────────────────────────
gold_df = clients_df.select(
    "client_id", "nom", "prenom", "segment", "statut_client", "score_risque"
)

gold_df = gold_df.join(factures_agg, on="client_id", how="left")
gold_df = gold_df.join(paiements_agg, on="client_id", how="left")
gold_df = gold_df.join(impayes_agg, on="client_id", how="left")
gold_df = gold_df.join(dossiers_agg, on="client_id", how="left")

# Remplacer les valeurs manquantes (clients sans facture/paiement) par 0
gold_df = gold_df.fillna({
    "montant_facture": 0,
    "montant_paye_total": 0,
    "montant_impaye_total": 0,
    "montant_dette_total": 0,
    "nb_factures": 0,
    "nb_dossiers": 0,
    "jours_retard_max": 0,
})

# ─────────────────────────────────────────────
# RENOMMAGE POUR CORRESPONDRE AUX NAMED QUERIES ATHENA EXISTANTES
# (categorie_client, montant_du, montant_recouvre, id_client, nom_client)
# ─────────────────────────────────────────────
gold_df = gold_df.withColumnRenamed("client_id", "id_client")
gold_df = gold_df.withColumnRenamed("nom", "nom_client")
gold_df = gold_df.withColumnRenamed("segment", "categorie_client")
gold_df = gold_df.withColumnRenamed("montant_facture", "montant_du")
gold_df = gold_df.withColumnRenamed("montant_paye_total", "montant_recouvre")
gold_df = gold_df.withColumnRenamed("statut_client", "statut_recouvrement")
gold_df = gold_df.withColumnRenamed("derniere_echeance", "date_echeance")

# ─────────────────────────────────────────────
# TAUX DE RECOUVREMENT
# ─────────────────────────────────────────────
gold_df = gold_df.withColumn(
    "taux_recouvrement",
    F.round(
        F.when(F.col("montant_du") > 0,
               F.col("montant_recouvre") * 100 / F.col("montant_du")
               ).otherwise(F.lit(0)),
        2
    )
)

# ─────────────────────────────────────────────
# SCORE DE RISQUE SIMPLIFIE (base sur le retard max en jours)
# ─────────────────────────────────────────────
gold_df = gold_df.withColumn(
    "niveau_risque_calcule",
    F.when(F.col("jours_retard_max") > 360, F.lit("CRITIQUE"))
     .when(F.col("jours_retard_max") > 180, F.lit("ELEVE"))
     .when(F.col("jours_retard_max") > 90, F.lit("MOYEN"))
     .when(F.col("jours_retard_max") > 30, F.lit("FAIBLE"))
     .otherwise(F.lit("SAIN"))
)

# Ajouter eventdate pour tracabilite (date de generation Gold)
gold_df = gold_df.withColumn("eventdate", F.lit(RUN_DATE))

# ─────────────────────────────────────────────
# ECRITURE (Gold)
# ─────────────────────────────────────────────
gold_path = "s3://" + TARGET_BUCKET + "/gold_recouvrement/"

gold_df.write \
    .mode("overwrite") \
    .partitionBy("categorie_client") \
    .option("compression", "snappy") \
    .parquet(gold_path)

final_count = gold_df.count()
print("[Silver->Gold] Written to: " + gold_path)
print("[Silver->Gold] ETL completed. Client aggregations: " + str(final_count))

job.commit()
