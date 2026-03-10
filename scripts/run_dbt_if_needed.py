import os
import sys
import logging
import subprocess
import snowflake.connector

# ---------------------------------------------------------------------------
# Configuration du logging avec timestamps
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# ---------------------------------------------------------------------------
# Répertoire dbt — résolu par rapport à l'emplacement de ce script
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DBT_PROJECT_DIR = os.path.join(
    SCRIPT_DIR,
    "..",
    "Airbnb_proj_Stach_AWS_Snowflake_DBT"
)

# ---------------------------------------------------------------------------
# Connexion Snowflake
# ---------------------------------------------------------------------------
conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema="PIPELINE",
    autocommit=True,   # garantit que chaque UPDATE est immédiatement persisté
)

cur = conn.cursor()

try:
    # -----------------------------------------------------------------------
    # Remise à zéro des runs bloqués en RUNNING (crash runner précédent)
    # -----------------------------------------------------------------------
    cur.execute("""
        UPDATE RUN_DBT_FLAG
        SET dbt_status = 'PENDING'
        WHERE dbt_status = 'RUNNING'
    """)
    reset_count = cur.rowcount
    if reset_count > 0:
        logging.warning(f"{reset_count} run(s) bloqué(s) en RUNNING remis à PENDING")

    # -----------------------------------------------------------------------
    # 1. Chercher les runs non traités
    # -----------------------------------------------------------------------
    cur.execute("""
        SELECT run_id
        FROM RUN_DBT_FLAG
        WHERE processed = FALSE
        ORDER BY run_time
    """)
    rows = cur.fetchall()

    if not rows:
        logging.info("No new data → dbt not executed")
        sys.exit(0)   # exit propre et explicite, code 0 = pas d'erreur

    run_ids = [r[0] for r in rows]
    logging.info(f"New data detected — {len(run_ids)} run(s) à traiter → running dbt")

    # -----------------------------------------------------------------------
    # 2. Passer les runs en RUNNING (binding paramétré — pas d'injection SQL)
    # -----------------------------------------------------------------------
    placeholders = ",".join(["%s"] * len(run_ids))
    cur.execute(
        f"UPDATE RUN_DBT_FLAG SET dbt_status = 'RUNNING' WHERE run_id IN ({placeholders})",
        run_ids
    )

    # -----------------------------------------------------------------------
    # 3. Lancer dbt build avec le bon working directory
    # -----------------------------------------------------------------------
    subprocess.run(
        ["dbt", "build"],
        check=True,
        cwd=DBT_PROJECT_DIR
    )

    # -----------------------------------------------------------------------
    # 4. Succès → marquer les runs comme traités
    # -----------------------------------------------------------------------
    cur.execute(
        f"""
        UPDATE RUN_DBT_FLAG
        SET processed = TRUE,
            dbt_status = 'SUCCESS'
        WHERE run_id IN ({placeholders})
        """,
        run_ids
    )
    logging.info("dbt completed successfully")

except subprocess.CalledProcessError as e:
    # -----------------------------------------------------------------------
    # 5. Échec dbt → mettre à jour le statut ET faire échouer le job CI
    # -----------------------------------------------------------------------
    logging.error(f"dbt execution failed: {e}")
    try:
        placeholders = ",".join(["%s"] * len(run_ids))
        cur.execute(
            f"UPDATE RUN_DBT_FLAG SET dbt_status = 'FAILED' WHERE run_id IN ({placeholders})",
            run_ids
        )
    except Exception as update_err:
        logging.error(f"Impossible de mettre à jour le statut FAILED: {update_err}")
    sys.exit(1)   # le CI marquera le job en rouge

except Exception as e:
    logging.error(f"Erreur inattendue: {e}")
    sys.exit(1)

finally:
    cur.close()
    conn.close()
