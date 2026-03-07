# import des librairies
import snowflake.connector
import subprocess

conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema="PIPELINE"
)

cur = conn.cursor()

# 1️ chercher les runs non traités
cur.execute("""
SELECT run_id
FROM RUN_DBT_FLAG
WHERE processed = FALSE
ORDER BY run_time
""")

rows = cur.fetchall()

if not rows:
    print("No new data → dbt not executed")
    exit()

run_ids = [str(r[0]) for r in rows]
run_ids_sql = ",".join(run_ids)

print("New data detected → running dbt")

try:

    # 2️ passer les runs en RUNNING
    cur.execute(f"""
    UPDATE RUN_DBT_FLAG
    SET dbt_status = 'RUNNING'
    WHERE run_id IN ({run_ids_sql})
    """)

    # 3️ lancer dbt
    subprocess.run(["dbt", "build"], check=True)

    # 4️ si succès
    cur.execute(f"""
    UPDATE RUN_DBT_FLAG
    SET 
        processed = TRUE,
        dbt_status = 'SUCCESS'
    WHERE run_id IN ({run_ids_sql})
    """)

    print("dbt completed successfully")

except subprocess.CalledProcessError:

    # 5️ si échec
    cur.execute(f"""
    UPDATE RUN_DBT_FLAG
    SET dbt_status = 'FAILED'
    WHERE run_id IN ({run_ids_sql})
    """)

    print("dbt execution failed")

finally:
    cur.close()
    conn.close()

