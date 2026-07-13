"""
AWS Glue ETL Job - Silver -> Gold (PFE Recouvrement) - v3 COMPLETE

Produit 7 tables Gold optimisees pour une webapp de dashboard :
  1. gold_recouvrement       : vue client consolidee (montants, risque)
  2. gold_relances            : efficacite des relances par client
  3. gold_litiges             : litiges par client (statut, motif)
  4. gold_contrats            : statut contrats / churn par client
  5. gold_aging               : repartition des impayes par anciennete
  6. gold_agence_performance  : performance par agence de recouvrement
  7. gold_kpis_globaux        : resume 1 ligne pour cartes de dashboard
"""

import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# ---------------------------------------------
# INIT
# ---------------------------------------------
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


def read_silver_table(table_name):
    path = "s3://" + SOURCE_BUCKET + "/silver/" + table_name + "/"
    df = spark.read.parquet(path)
    print("[Silver->Gold] " + table_name + ": " + str(df.count()) + " records")
    return df


def write_gold_table(df, table_name, partition_col=None):
    path = "s3://" + TARGET_BUCKET + "/" + table_name + "/"
    writer = df.write.mode("overwrite").option("compression", "snappy")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.parquet(path)
    print("[Silver->Gold] " + table_name + " written: " + str(df.count()) + " rows -> " + path)


clients_df = read_silver_table("clients")
factures_df = read_silver_table("factures")
paiements_df = read_silver_table("paiements")
impayes_df = read_silver_table("impayes")
dossiers_df = read_silver_table("dossiers_recouvrement")
relances_df = read_silver_table("relances")
litiges_df = read_silver_table("litiges")
contrats_df = read_silver_table("contrats")

# =====================================================================
# CAST DES COLONNES NUMERIQUES
# =====================================================================
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

# =====================================================================
# TABLE 1 : gold_recouvrement (vue client consolidee)
# =====================================================================
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

gold_df = clients_df.select(
    "client_id", "nom", "prenom", "segment", "statut_client", "score_risque"
)
gold_df = gold_df.join(factures_agg, on="client_id", how="left")
gold_df = gold_df.join(paiements_agg, on="client_id", how="left")
gold_df = gold_df.join(impayes_agg, on="client_id", how="left")
gold_df = gold_df.join(dossiers_agg, on="client_id", how="left")

gold_df = gold_df.fillna({
    "montant_facture": 0, "montant_paye_total": 0, "montant_impaye_total": 0,
    "montant_dette_total": 0, "nb_factures": 0, "nb_dossiers": 0, "jours_retard_max": 0,
})

gold_df = gold_df.withColumnRenamed("client_id", "id_client")
gold_df = gold_df.withColumnRenamed("nom", "nom_client")
gold_df = gold_df.withColumnRenamed("segment", "categorie_client")
gold_df = gold_df.withColumnRenamed("montant_facture", "montant_du")
gold_df = gold_df.withColumnRenamed("montant_paye_total", "montant_recouvre")
gold_df = gold_df.withColumnRenamed("statut_client", "statut_recouvrement")
gold_df = gold_df.withColumnRenamed("derniere_echeance", "date_echeance")

gold_df = gold_df.withColumn(
    "taux_recouvrement",
    F.round(
        F.when(F.col("montant_du") > 0,
               F.col("montant_recouvre") * 100 / F.col("montant_du")
               ).otherwise(F.lit(0)), 2
    )
)
gold_df = gold_df.withColumn(
    "niveau_risque_calcule",
    F.when(F.col("jours_retard_max") > 360, F.lit("CRITIQUE"))
     .when(F.col("jours_retard_max") > 180, F.lit("ELEVE"))
     .when(F.col("jours_retard_max") > 90, F.lit("MOYEN"))
     .when(F.col("jours_retard_max") > 30, F.lit("FAIBLE"))
     .otherwise(F.lit("SAIN"))
)
gold_df = gold_df.withColumn("eventdate", F.lit(RUN_DATE))
gold_df = gold_df.cache()

write_gold_table(gold_df, "gold_recouvrement", "categorie_client")

# =====================================================================
# TABLE 2 : gold_relances (efficacite des relances)
# =====================================================================
relances_par_client = relances_df.groupBy("client_id").agg(
    F.count("relance_id").alias("nb_relances_total"),
    F.sum(F.when(F.col("resultat") == "Paiement effectue", 1).otherwise(0)).alias("nb_paiements_suite_relance"),
    F.sum(F.when(F.col("resultat") == "Promesse de paiement", 1).otherwise(0)).alias("nb_promesses"),
    F.sum(F.when(F.col("resultat") == "Sans suite", 1).otherwise(0)).alias("nb_sans_suite"),
    F.sum(F.when(F.col("resultat") == "Refus", 1).otherwise(0)).alias("nb_refus"),
)
relances_par_client = relances_par_client.withColumn(
    "taux_efficacite_relance",
    F.round(
        F.when(F.col("nb_relances_total") > 0,
               F.col("nb_paiements_suite_relance") * 100 / F.col("nb_relances_total")
               ).otherwise(F.lit(0)), 2
    )
)
relances_par_client = relances_par_client.withColumnRenamed("client_id", "id_client")
relances_par_client = relances_par_client.withColumn("eventdate", F.lit(RUN_DATE))
relances_par_client = relances_par_client.withColumn(
    "efficacite_categorie",
    F.when(F.col("taux_efficacite_relance") >= 50, F.lit("EFFICACE"))
     .when(F.col("taux_efficacite_relance") >= 20, F.lit("MOYENNE"))
     .otherwise(F.lit("FAIBLE"))
)

write_gold_table(relances_par_client, "gold_relances", "efficacite_categorie")

# =====================================================================
# TABLE 3 : gold_litiges (litiges par client)
# =====================================================================
litiges_agg = litiges_df.groupBy("client_id").agg(
    F.count("litige_id").alias("nb_litiges_total"),
    F.sum(F.when(F.col("statut_litige") == "Ouvert", 1).otherwise(0)).alias("nb_litiges_ouverts"),
    F.sum(F.when(F.col("statut_litige") == "En cours", 1).otherwise(0)).alias("nb_litiges_en_cours"),
    F.sum(F.when(F.col("statut_litige") == "Cloture", 1).otherwise(0)).alias("nb_litiges_clotures"),
)
litiges_agg = litiges_agg.withColumnRenamed("client_id", "id_client")
litiges_agg = litiges_agg.withColumn("eventdate", F.lit(RUN_DATE))
litiges_agg = litiges_agg.withColumn(
    "a_litige_actif",
    F.when((F.col("nb_litiges_ouverts") + F.col("nb_litiges_en_cours")) > 0, F.lit("OUI")).otherwise(F.lit("NON"))
)

write_gold_table(litiges_agg, "gold_litiges", "a_litige_actif")

# =====================================================================
# TABLE 4 : gold_contrats (statut contrats / churn)
# =====================================================================
contrats_agg = contrats_df.groupBy("client_id").agg(
    F.count("contrat_id").alias("nb_contrats_total"),
    F.sum(F.when(F.col("statut_contrat") == "Actif", 1).otherwise(0)).alias("nb_contrats_actifs"),
    F.sum(F.when(F.col("statut_contrat") == "Resilie", 1).otherwise(0)).alias("nb_contrats_resilies"),
    F.sum(F.when(F.col("statut_contrat") == "Suspendu", 1).otherwise(0)).alias("nb_contrats_suspendus"),
)
contrats_agg = contrats_agg.withColumn(
    "statut_global",
    F.when(F.col("nb_contrats_actifs") > 0, F.lit("CLIENT_ACTIF"))
     .when(F.col("nb_contrats_suspendus") > 0, F.lit("CLIENT_SUSPENDU"))
     .otherwise(F.lit("CLIENT_CHURN"))
)
contrats_agg = contrats_agg.withColumnRenamed("client_id", "id_client")
contrats_agg = contrats_agg.withColumn("eventdate", F.lit(RUN_DATE))
contrats_agg = contrats_agg.cache()

write_gold_table(contrats_agg, "gold_contrats", "statut_global")

# =====================================================================
# TABLE 5 : gold_aging (repartition des impayes par anciennete)
# =====================================================================
aging_agg = impayes_df.groupBy("bucket_anciennete").agg(
    F.count("impaye_id").alias("nb_impayes"),
    F.sum("montant_impaye_num").alias("montant_total"),
    F.countDistinct("client_id").alias("nb_clients_concernes"),
)
aging_agg = aging_agg.withColumn("eventdate", F.lit(RUN_DATE))

write_gold_table(aging_agg, "gold_aging", "bucket_anciennete")

# =====================================================================
# TABLE 6 : gold_agence_performance
# =====================================================================
client_recouvre = gold_df.select(
    F.col("id_client").alias("client_id"), "montant_recouvre"
)

agence_base = dossiers_df.join(client_recouvre, on="client_id", how="left")
agence_base = agence_base.fillna({"montant_recouvre": 0})

agence_agg = agence_base.groupBy("agence_recouvrement").agg(
    F.count("dossier_id").alias("nb_dossiers"),
    F.sum("montant_total_dette_num").alias("montant_dette_total"),
    F.sum("montant_recouvre").alias("montant_recouvre_total"),
    F.countDistinct("client_id").alias("nb_clients"),
)
agence_agg = agence_agg.withColumn(
    "taux_recouvrement_agence",
    F.round(
        F.when(F.col("montant_dette_total") > 0,
               F.col("montant_recouvre_total") * 100 / F.col("montant_dette_total")
               ).otherwise(F.lit(0)), 2
    )
)
agence_agg = agence_agg.withColumn("eventdate", F.lit(RUN_DATE))

write_gold_table(agence_agg, "gold_agence_performance", None)

# =====================================================================
# TABLE 7 : gold_kpis_globaux (resume 1 ligne pour dashboard)
# =====================================================================
kpis_row = gold_df.agg(
    F.count("id_client").alias("nb_clients_total"),
    F.sum("montant_du").alias("montant_du_total"),
    F.sum("montant_recouvre").alias("montant_recouvre_total"),
    F.sum(F.when(F.col("niveau_risque_calcule") == "CRITIQUE", 1).otherwise(0)).alias("nb_clients_critique"),
    F.sum(F.when(F.col("niveau_risque_calcule") == "ELEVE", 1).otherwise(0)).alias("nb_clients_eleve"),
)

nb_litiges_ouverts_total = litiges_agg.agg(
    F.sum("nb_litiges_ouverts").alias("total")
).collect()[0]["total"] or 0

nb_clients_churn = contrats_agg.filter(
    F.col("statut_global") == "CLIENT_CHURN"
).count()

kpis_row = kpis_row.withColumn("nb_litiges_ouverts_total", F.lit(nb_litiges_ouverts_total))
kpis_row = kpis_row.withColumn("nb_clients_churn", F.lit(nb_clients_churn))
kpis_row = kpis_row.withColumn(
    "taux_recouvrement_global",
    F.round(
        F.when(F.col("montant_du_total") > 0,
               F.col("montant_recouvre_total") * 100 / F.col("montant_du_total")
               ).otherwise(F.lit(0)), 2
    )
)
kpis_row = kpis_row.withColumn("eventdate", F.lit(RUN_DATE))
kpis_row = kpis_row.withColumn("date_generation", F.current_timestamp())

write_gold_table(kpis_row, "gold_kpis_globaux", None)

print("[Silver->Gold] ETL completed successfully - 7 tables written.")
job.commit()
