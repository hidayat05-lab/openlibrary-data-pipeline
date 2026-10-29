from datetime import datetime
from uuid import uuid4

import psycopg2
import psycopg2.extras
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator


POSTGRES_CONFIG = {
    "host": "etl-postgres",
    "port": 5432,
    "database": "etl_database",
    "user": "etl_user",
    "password": "etl_password",
}


def create_raw_table():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    with open(
        "/usr/local/airflow/sql/raw/create_openlibrary_books.sql",
        "r",
        encoding="utf-8",
    ) as file:
        sql = file.read()

    cursor.execute(sql)

    conn.commit()

    cursor.close()
    conn.close()


def extract_and_load_raw(**context):
    url = "https://openlibrary.org/search.json"

    batch_id = str(uuid4())
    extracted_at = datetime.now().astimezone()

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO raw.openlibrary_books (
            batch_id,
            source,
            extracted_at,
            payload
        )
        VALUES (%s, %s, %s, %s);
    """

    total_loaded = 0

    for page in range(1, 11):
        params = {
            "q": "python",
            "page": page,
            "limit": 100,
        }

        response = requests.get(
            url,
            params=params,
            timeout=30,
        )

        response.raise_for_status()

        data = response.json()
        books = data.get("docs", [])

        if not books:
            break

        for book in books:
            cursor.execute(
                insert_sql,
                (
                    batch_id,
                    "openlibrary",
                    extracted_at,
                    psycopg2.extras.Json(book),
                ),
            )

        total_loaded += len(books)

        print(
            f"Page {page}: "
            f"{len(books)} books loaded"
        )

    conn.commit()

    cursor.close()
    conn.close()

    print(f"Batch ID: {batch_id}")
    print(f"Total books loaded: {total_loaded}")

    context["ti"].xcom_push(
    key="batch_id",
    value=batch_id
)

def get_pipeline_counts(**context):

    import psycopg2

    # Ambil batch_id dari task extract_and_load_raw
    batch_id = context["ti"].xcom_pull(
        task_ids="extract_and_load_raw",
        key="batch_id"
    )

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    # RAW: jumlah data yang masuk pada batch saat ini
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM raw.openlibrary_books
        WHERE batch_id = %s;
        """,
        (batch_id,)
    )
    raw_count = cursor.fetchone()[0]

    # STAGING: jumlah data dari batch saat ini
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM analytics.stg_openlibrary_books
        WHERE batch_id = %s;
        """,
        (batch_id,)
    )
    staging_count = cursor.fetchone()[0]

    # DATA MART: jumlah data yang tersedia saat ini
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM analytics.mart_books;
        """
    )
    mart_count = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    print(f"Batch ID: {batch_id}")
    print(f"RAW: {raw_count}")
    print(f"STAGING: {staging_count}")
    print(f"DATA MART: {mart_count}")

    return {
        "batch_id": batch_id,
        "raw_count": raw_count,
        "staging_count": staging_count,
        "mart_count": mart_count,
    }

with DAG(
    dag_id="openlibrary_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule= "@daily",
    catchup=False,
    tags=["openlibrary", "etl", "data-engineering"],
) as dag:

    create_raw = PythonOperator(
        task_id="create_raw_table",
        python_callable=create_raw_table,
    )

    extract_load_raw = PythonOperator(
        task_id="extract_and_load_raw",
        python_callable=extract_and_load_raw,
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command="""
            dbt run \
              --project-dir /usr/local/airflow/dbt/openlibrary_dbt \
              --profiles-dir /usr/local/airflow/dbt/openlibrary_dbt \
              --select stg_openlibrary_books

        """,
        env={
            "DBT_HOST": "etl-postgres",
            "DBT_PORT": "5432",
            "DBT_USER": "etl_user",
            "DBT_PASSWORD": "etl_password",
            "DBT_DBNAME": "etl_database",
            "DBT_SCHEMA": "analytics",
        }
    )

    dbt_mart = BashOperator(
        task_id="dbt_mart",
        bash_command="""
            dbt run \
              --project-dir /usr/local/airflow/dbt/openlibrary_dbt \
              --profiles-dir /usr/local/airflow/dbt/openlibrary_dbt \
              --select mart_books
        """,
         env={
            "DBT_HOST": "etl-postgres",
            "DBT_PORT": "5432",
            "DBT_USER": "etl_user",
            "DBT_PASSWORD": "etl_password",
            "DBT_DBNAME": "etl_database",
            "DBT_SCHEMA": "analytics",
        }
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
            dbt test \
              --project-dir /usr/local/airflow/dbt/openlibrary_dbt \
              --profiles-dir /usr/local/airflow/dbt/openlibrary_dbt \
              --select stg_openlibrary_books mart_books
        """,
        env={
            "DBT_HOST": "etl-postgres",
            "DBT_PORT": "5432",
            "DBT_USER": "etl_user",
            "DBT_PASSWORD": "etl_password",
            "DBT_DBNAME": "etl_database",
            "DBT_SCHEMA": "analytics",
        }
    )

    get_counts = PythonOperator(
        task_id="get_pipeline_counts",
        python_callable=get_pipeline_counts,
    )
    notification_gate = EmptyOperator(
        task_id="notification_gate",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    send_success_email = EmailOperator(
        task_id="send_success_email",
        to="kyyhidayat@gmail.com",
        subject="OpenLibrary Pipeline - SUCCESS",
        html_content="""
        <h2>OpenLibrary Pipeline - SUCCESS</h2>

        <p>Pipeline harian berhasil dijalankan.</p>

        <h3>Pipeline Summary</h3>

        <ul>
            <li>
                <b>RAW:</b>
                {{ ti.xcom_pull(task_ids='get_pipeline_counts')['raw_count'] }}
                records
            </li>

            <li>
                <b>STAGING:</b>
                {{ ti.xcom_pull(task_ids='get_pipeline_counts')['staging_count'] }}
                records
            </li>

            <li>
                <b>DATA MART:</b>
                {{ ti.xcom_pull(task_ids='get_pipeline_counts')['mart_count'] }}
                records
            </li>
        </ul>

        <p>
        <b>Batch ID:</b>
        {{ ti.xcom_pull(task_ids='get_pipeline_counts')['batch_id'] }}
        </p>

        <p>dbt STAGING: SUCCESS</p>
        <p>dbt DATA MART: SUCCESS</p>
        <p>dbt TEST: PASS</p>

        <p>Data pipeline selesai dan data siap digunakan untuk analisis.</p>
        """,
        conn_id="smtp_default",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    send_failure_email = EmailOperator(
        task_id="send_failure_email",
        to="EMAIL_GMAIL_KAMU@gmail.com",
        subject="OpenLibrary Pipeline - FAILED",
        html_content="""
        <h2>OpenLibrary Pipeline - FAILED</h2>

        <p>Pipeline OpenLibrary gagal dijalankan.</p>

        <p>
            Silakan cek Airflow untuk melihat task yang mengalami kegagalan.
        </p>

        <p>
            <b>DAG:</b> {{ dag.dag_id }}
        </p>

        <p>
            <b>Task:</b> {{ ti.task_id }}
        </p>
        """,
        conn_id="smtp_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    create_raw >> extract_load_raw >> dbt_staging >> dbt_mart >> dbt_test >> get_counts

    get_counts >> notification_gate

    notification_gate >> send_success_email
    notification_gate >> send_failure_email