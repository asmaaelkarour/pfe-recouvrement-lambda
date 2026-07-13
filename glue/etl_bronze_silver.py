"""
AWS Glue ETL Job - Bronze -> Silver (PFE Recouvrement) - v3 NETTOYAGE COMPLET

Ajouts vs v2 :
  - Correction de l'encodage corrompu (mojibake UTF-8/Latin-1)
  - Suppression des suffixes parasites (#@!, etc.) en fin de valeur
  - Filtrage des valeurs sentinelles/test (client_id = 9999999)
"""

import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_BUCKET", "TARGET_BUCKET", "DATABASE_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_BUCKET = args["SOURCE_BUCKET"]
TARGET_BUCKET = args["TARGET_BUCKET"]

print("[ETL] Starting")

# Cles primaires metier par table (pour dedoublonnage fiable)
TABLE_PRIMARY_KEYS = {
    "clients": "client_id",
    "contrats": "contrat_id",
    "dossiers_recouvrement": "dossier_id",
    "echeanciers": "echeancier_id",
    "factures": "facture_id",
    "impayes": "impaye_id",
    "litiges": "litige_id",
    "mouvements_financiers": "mouvement_id",
    "paiements": "paiement_id",
    "relances": "relance_id",
    "suspensions_ligne": "suspension_id",
}

TABLES = ["clients", "contrats", "dossiers_recouvrement", "echeanciers", "factures",
          "impayes", "litiges", "mouvements_financiers", "paiements", "relances",
          "suspensions_ligne"]

# Valeurs sentinelles/test connues, a exclure quand elles apparaissent comme client_id
SENTINEL_CLIENT_IDS = ["9999999", "999999"]


# âââââââââââââââââââââââââââââââââââââââââââââ
# UDF : correction de l'encodage corrompu (mojibake)
# âââââââââââââââââââââââââââââââââââââââââââââ
def fix_mojibake(value):
    """
    Corrige le cas standard ou du texte UTF-8 a ete mal interprete comme
    Latin-1 puis re-encode (ex: "CafÃÂ©" au lieu de "CafÃ©").
    Si la conversion echoue (octet invalide / corruption non standard,
    donnee perdue a la source), la valeur d'origine est conservee telle
    quelle plutot que risquer une double-corruption.
    """
    if value is None:
        return None
    try:
        candidate = value.encode("latin-1").decode("utf-8")
        return candidate
    except (UnicodeDecodeError, UnicodeEncodeError):
        return value


fix_mojibake_udf = F.udf(fix_mojibake, StringType())


def clean_data_quality(df):
    """Nettoyage generique de qualite applique a TOUTES les tables"""

    string_fields = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]

    # 1. Correction de l'encodage corrompu (AVANT tout autre nettoyage)
    for col_name in string_fields:
        df = df.withColumn(col_name, fix_mojibake_udf(F.col(col_name)))

    # 2. TRIM des espaces en debut/fin
    for col_name in string_fields:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # 3. Suppression des suffixes parasites en fin de valeur (#@!, etc.)
    for col_name in string_fields:
        df = df.withColumn(col_name, F.regexp_replace(F.col(col_name), r"[#@!]+$", ""))
        df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # 4. Remplacer valeurs "vides deguisees" par NULL
    empty_values = ["", "null", "n/a", "na", "none", "-", "undefined"]
    for col_name in string_fields:
        condition = F.upper(F.col(col_name)).isin([v.upper() for v in empty_values])
        df = df.withColumn(col_name, F.when(condition, F.lit(None)).otherwise(F.col(col_name)))

    # 5. Dedupliquer espaces multiples a l'interieur du texte
    for col_name in string_fields:
        df = df.withColumn(col_name, F.regexp_replace(F.col(col_name), " +", " "))

    # 6. Normaliser casse pour colonnes categoriques/statut
    categorical_patterns = ["statut", "email", "categorie", "segment", "type", "genre", "etat", "mode"]
    for col_name in string_fields:
        if any(pattern in col_name.lower() for pattern in categorical_patterns):
            df = df.withColumn(col_name, F.lower(F.col(col_name)))

    return df


def clean_generic(df, table):
    """Nettoyage generique: qualite + dedoublonnage metier"""

    df = clean_data_quality(df)

    df = df.withColumn("ingested_at", F.current_timestamp())
    df = df.withColumn("source", F.lit("api_externe"))
    df = df.withColumn("table_name", F.lit(table))
    df = df.withColumn("eventdate", F.date_format(F.col("ingested_at"), "yyyy-MM-dd"))

    # Filtrage des valeurs sentinelles/test sur client_id
    if "client_id" in df.columns:
        df = df.filter(~F.col("client_id").isin(SENTINEL_CLIENT_IDS))

    cols_for_hash = [c for c in df.columns if c not in ["ingested_at", "source", "table_name", "eventdate"]]
    if cols_for_hash:
        cols_str = [F.col(c).cast("string") for c in cols_for_hash]
        df = df.withColumn("record_id", F.md5(F.concat_ws("||", *cols_str)))
    else:
        df = df.withColumn("record_id", F.lit("no_data"))

    pk = TABLE_PRIMARY_KEYS.get(table)
    if pk and pk in df.columns:
        window = Window.partitionBy(pk).orderBy(F.col("ingested_at").desc())
        df = df.withColumn("_row_num", F.row_number().over(window))
        df = df.filter(F.col("_row_num") == 1).drop("_row_num")
    else:
        print("  WARN: No primary key defined for " + table + ", using record_id dedup")
        if "record_id" in df.columns:
            df = df.dropDuplicates(["record_id"])

    return df


def apply_business_rules(df, table=None):
    """Regles metier specifiques (apres nettoyage generique)"""
    if "montant" in df.columns:
        df = df.filter(F.col("montant") >= 0)
    return df


total_written = 0

for table in TABLES:
    try:
        print("[" + table + "] Processing")

        source_path = "s3://" + SOURCE_BUCKET + "/incoming/" + table + "/"
        raw_df = spark.read.option("multiline", "true").json(source_path)

        raw_count = raw_df.count()
        if raw_count == 0:
            print("  No data, skipping")
            continue

        print("  Raw: " + str(raw_count) + " records")

        cols_to_keep = [c for c in raw_df.columns if c not in ["eventdate", "year", "month", "day"]]
        if cols_to_keep:
            raw_df = raw_df.select(*cols_to_keep)

        cleaned_df = clean_generic(raw_df, table)
        cleaned_df = apply_business_rules(cleaned_df, table)

        clean_count = cleaned_df.count()
        print("  Clean: " + str(clean_count) + " records")

        target_path = "s3://" + TARGET_BUCKET + "/silver/" + table + "/"
        cleaned_df.write.mode("overwrite").partitionBy("eventdate").option("compression", "snappy").parquet(target_path)

        print("  SUCCESS: Written to " + target_path)
        total_written += clean_count

    except Exception as error:
        print("  ERROR processing " + table + ": " + str(error))
        raise

print("\n[ETL] Complete. Total records written: " + str(total_written))
job.commit()
