from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json

with DAG(
    dag_id="nasa_apod_postgres",
    start_date=datetime.now() - timedelta(days=1),  
    schedule="@daily",
    catchup=False
):

    # Step 1: Create table if it doesn't exist
    @task
    def create_table():
        pg = PostgresHook(postgres_conn_id="my_postgres_connection")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        pg.run(create_table_query)

    # Step 2: Extract data from NASA API
    extract_apod = HttpOperator(
        task_id="extract_apod",
        http_conn_id="nasa_api",
        endpoint="planetary/apod",
        method="GET",
        # Use extra_dejson to read extras as JSON from the connection
        data={"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"},
        response_filter=lambda response: response.json(),
        log_response=True,      # helpful for debugging
        do_xcom_push=True       # ensure XCom push (should be default in recent versions)
    )

    # Step 3: Transform API response (TaskFlow)
    @task
    def transform_apod_data(response: dict):
        # Response should be a JSON-serializable dict (response_filter above)
        date_str = response.get("date") or None
        apod_data = {
            "title": response.get("title") or None,
            "explanation": response.get("explanation") or None,
            "url": response.get("url") or None,
            "date": date_str,
            "media_type": response.get("media_type") or None,
        }
        return apod_data

    # Step 4: Load into PostgreSQL (TaskFlow)
    @task
    def load_data_to_postgres(apod_data: dict):
        pg = PostgresHook(postgres_conn_id="my_postgres_connection")
        # Use to_date to safely cast string to date; if None passed, it becomes NULL
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, to_date(%s, 'YYYY-MM-DD'), %s);
        """
        params = (
            apod_data.get("title"),
            apod_data.get("explanation"),
            apod_data.get("url"),
            apod_data.get("date"),
            apod_data.get("media_type"),
        )
        pg.run(insert_query, parameters=params)

    # ---- Wiring tasks (explicit for clarity) ----
    t_create = create_table()
    t_create >> extract_apod

    api_response = extract_apod.output          # XComArg (the dict returned by response_filter)
    transformed = transform_apod_data(api_response)
    load_data_to_postgres(transformed)
