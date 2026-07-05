"""
Lambda – Ingestion depuis API externe vers S3 Bronze.

Flux :
  1. Appel GET /api/data/<table>?page=X&limite=10000 sur l'API externe
  2. Accumulation de toutes les pages en mémoire
  3. Dépôt unique vers S3 Bronze en format JSON une fois toutes les pages reçues :
     incoming/<table>/eventdate=YYYY-MM-DD/<uuid>.json

Déclencheurs acceptés :
  - EventBridge Scheduler (event vide ou {"tables": [...]})
  - API Gateway POST /ingest  avec body optionnel {"tables": [...]}
  - Invocation directe

Variables d'environnement requises :
  API_BASE_URL  : URL racine de l'API externe
  BRONZE_BUCKET : nom du bucket S3 Bronze
"""

import io
import json
import logging
import os
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

API_BASE_URL    = os.environ["API_BASE_URL"].rstrip("/")
BRONZE_BUCKET   = os.environ["BRONZE_BUCKET"]
GLUE_JOB_NAME   = os.environ.get("GLUE_JOB_NAME", "")
LIMITE_PAR_PAGE = 10000

TABLES_DEFAUT = [
    "clients",
    "factures",
    "paiements",
    "contrats",
    "impayes",
    "relances",
    "dossiers_recouvrement",
    "litiges",
    "suspensions_ligne",
    "echeanciers",
    "mouvements_financiers",
]

HEADERS_API = {
    "Accept":                     "application/json",
    "User-Agent":                 "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "ngrok-skip-browser-warning": "true",
}


# ─────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────

def lambda_handler(event, context):
    logger.info("Événement reçu : %s", json.dumps(event))

    tables = _extraire_tables(event)
    logger.info("Tables à ingérer : %s", tables)

    resultats = []
    erreurs   = []

    for table in tables:
        try:
            resultat = _ingerer_table(table)
            resultats.append(resultat)
            logger.info("[%s] OK — %d lignes → s3://%s/%s",
                        table, resultat["lignes"], BRONZE_BUCKET, resultat["s3_key"])
        except urllib.error.HTTPError as exc:
            msg = f"HTTP {exc.code} : {exc.reason}"
            logger.error("[%s] %s", table, msg)
            erreurs.append({"table": table, "erreur": msg})
        except urllib.error.URLError as exc:
            msg = f"Connexion impossible : {exc.reason}"
            logger.error("[%s] %s", table, msg)
            erreurs.append({"table": table, "erreur": msg})
        except Exception as exc:  # noqa: BLE001
            logger.error("[%s] Erreur inattendue : %s", table, exc, exc_info=True)
            erreurs.append({"table": table, "erreur": str(exc)})

    if GLUE_JOB_NAME and resultats:
        _declencher_glue()

    statut    = "OK" if not erreurs else ("PARTIEL" if resultats else "ERREUR")
    code_http = 200 if not erreurs else 207

    return {
        "statusCode": code_http,
        "headers":    {"Content-Type": "application/json"},
        "body": json.dumps(
            {"statut": statut, "tables_traitees": resultats, "erreurs": erreurs},
            ensure_ascii=False,
        ),
    }


# ─────────────────────────────────────────────
# INGESTION D'UNE TABLE (pagination + upload S3)
#
# Accumulation de toutes les pages en mémoire.
# L'upload S3 est unique, à la fin, quand toutes les pages sont reçues,
# en format JSON (array de tous les enregistrements).
# ─────────────────────────────────────────────

def _ingerer_table(table: str) -> dict:
    now    = datetime.now(timezone.utc)
    s3_key = (
        f"incoming/{table}/"
        f"eventdate={now.strftime('%Y-%m-%d')}/"
        f"{uuid.uuid4()}.json"
    )

    donnees_totales = []
    total_lignes    = 0
    page            = 1

    while True:
        url = f"{API_BASE_URL}/api/data/{table}?page={page}&limite={LIMITE_PAR_PAGE}"
        req = urllib.request.Request(url, headers=HEADERS_API)

        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8")
            logger.info("[%s] page %d: HTTP %d, body length: %d", table, page, resp.status, len(body))
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                logger.error("[%s] Invalid JSON response. First 500 chars: %s", table, body[:500])
                raise

        donnees_totales.extend(payload.get("donnees", []))
        total_lignes += len(payload.get("donnees", []))
        total_pages   = int(payload.get("total_pages", 0))

        logger.info("[%s] page %d/%d — %d lignes au total",
                    table, page, total_pages, total_lignes)

        if not payload.get("a_page_suivante", False):
            break

        page += 1

    # Upload unique vers S3 Bronze en format JSON une fois toutes les pages reçues
    buffer = io.BytesIO()
    buffer.write(json.dumps(donnees_totales, ensure_ascii=False).encode("utf-8"))
    buffer.seek(0)

    s3.put_object(
        Bucket      = BRONZE_BUCKET,
        Key         = s3_key,
        Body        = buffer,
        ContentType = "application/json",
    )

    return {"table": table, "lignes": total_lignes, "s3_key": s3_key, "statut": "ok"}


# ─────────────────────────────────────────────
# DÉCLENCHEMENT GLUE (optionnel)
# ─────────────────────────────────────────────

def _declencher_glue():
    glue = boto3.client("glue")
    try:
        run = glue.start_job_run(JobName=GLUE_JOB_NAME)
        logger.info("Job Glue démarré : %s", run["JobRunId"])
    except Exception as exc:  # noqa: BLE001
        logger.warning("Impossible de démarrer le job Glue : %s", exc)


# ─────────────────────────────────────────────
# EXTRACTION DES TABLES DEPUIS L'ÉVÉNEMENT
# ─────────────────────────────────────────────

def _extraire_tables(event: dict) -> list:
    """
    Accepte trois formes d'appel :
      - {"tables": ["clients", "factures"]}  → invocation directe ou EventBridge
      - body JSON API Gateway avec "tables"  → POST /ingest
      - événement vide {}                    → toutes les tables
    """
    if isinstance(event, dict):
        if "tables" in event:
            return event["tables"]
        body_raw = event.get("body")
        if body_raw:
            try:
                body = json.loads(body_raw)
                if "tables" in body:
                    return body["tables"]
            except (json.JSONDecodeError, TypeError):
                pass
    return TABLES_DEFAUT
