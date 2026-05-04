from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Add plugins and dags folders to path so we can import our custom logic
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from gemini_agent import generate_search_queries
from pipeline_functions import (
    extract_jobs,
    load_to_bigquery,
    transform_in_bigquery,
    export_to_sheets,
)

default_args = {
    'owner': 'michael_campbell',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ai_portfolio_job_search',
    default_args=default_args,
    description='End-to-end AI Job Search Pipeline',
    schedule=None, # Triggered manually
    catchup=False,
    tags=['portfolio', 'ai', 'elt'],
    # These params create a UI form when triggering the DAG!
    params={
        "resume_filename": Param(
            "Michael_Campbell_Healthcare_Analytics.md", 
            type="string", 
            description="The PDF resume to use for analysis and ranking"
        ),
        "search_location": Param(
            "Tampa, FL, USA", 
            type="string", 
            description="Geographic location to search within"
        ),
        "location_type": Param(
            "Any",
            type="string",
            enum=["Any", "Remote", "Hybrid", "On-site"],
            description="Preffered work type"
        ),
        "pages_per_query": Param(
            1,
            type="integer",
            minimum=1,
            maximum=3,
            description="Number of API pages to pull per query (1 page = 10 jobs)"
        ),
        "manual_queries": Param(
            None,
            type=["string", "null"],
            description="Optional: comma-separated job titles to search (e.g. 'Data Analyst, Data Engineer'). Overrides AI-generated queries when provided."
        )
    }
) as dag:

    def analyze_resume_step(**kwargs):
        """Task 1: AI Analyzer reads resume and generates search queries"""
        manual_queries = (kwargs['params'].get('manual_queries') or '').strip()
        if manual_queries:
            queries = [q.strip() for q in manual_queries.split(',') if q.strip()]
            print(f"Using manual queries: {queries}")
            kwargs['ti'].xcom_push(key='search_queries', value=queries)
            return

        resume_file = kwargs['params']['resume_filename']
        project_dir = os.path.join(os.path.dirname(__file__), '..')
        pdf_path = os.path.join(project_dir, 'data', 'resumes', resume_file)

        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"Resume not found at {pdf_path}. Please ensure it is in the data/resumes/ folder.")

        queries = generate_search_queries(pdf_path)
        kwargs['ti'].xcom_push(key='search_queries', value=queries)
        print(f"Pushed queries to XCom: {queries}")

    def extract_jobs_step(**kwargs):
        """Task 2: Pull raw data from JSearch API"""
        queries = kwargs['ti'].xcom_pull(task_ids='analyze_resume_task', key='search_queries')
        location = kwargs['params']['search_location']
        location_type = kwargs['params']['location_type']
        pages = kwargs['params']['pages_per_query']        
        
        all_raw_jobs = []

        for query in queries:
            if location_type == "Any":
                full_search_term = f"{query} in {location}"
            else:
                full_search_term = f"{query} {location_type} in {location}"
            print(f"Extracting: {full_search_term} (Pages: {pages})")
            
            try:
                data = extract_jobs(full_search_term, num_pages=pages)
                if data and 'data' in data:
                    data['search_query'] = full_search_term
                    all_raw_jobs.append(data)
            except Exception as e:
                print(f"Failed to extract {full_search_term}: {e}")

        kwargs['ti'].xcom_push(key='raw_jobs', value=all_raw_jobs)
        print(f"Extraction complete. Found {len(all_raw_jobs)} result blocks.")
        

    def load_to_bq_step(**kwargs):
        """Task 3: Load raw data to BigQuery Data Lake"""
        raw_jobs = kwargs['ti'].xcom_pull(task_ids='extract_jobs_task', key='raw_jobs')
        if raw_jobs:
            load_to_bigquery(raw_jobs, table_id="search_queries")
            print("Loaded to BigQuery successfully.")
        else:
            print("No raw jobs to load.")

    def transform_bq_step(**kwargs):
        """Task 4: Run SQL to flatten and deduplicate jobs"""
        transform_in_bigquery()
        print("Transformation complete.")

    def rank_jobs_step(**kwargs):
        """Task 5: Pull transformed jobs from BQ, rank with Gemini, save rankings"""
        import json
        from google.cloud import bigquery
        from gemini_agent import rank_jobs_with_strategist

        resume_file = kwargs['params']['resume_filename']
        project_dir = os.path.join(os.path.dirname(__file__), '..')
        pdf_path = os.path.join(project_dir, 'data', 'resumes', resume_file)

        client = bigquery.Client()
        from datetime import date, datetime as dt
        def serialize_row(row):
            return {k: v.isoformat() if isinstance(v, (date, dt)) else v for k, v in dict(row).items()}
        queries = kwargs['ti'].xcom_pull(task_ids='analyze_resume_task', key='search_queries') or []
        location = kwargs['params']['search_location']
        location_type = kwargs['params']['location_type']
        
        full_terms = []
        for q in queries:
            if location_type == "Any":
                full_terms.append(f"{q} in {location}")
            else:
                full_terms.append(f"{q} {location_type} in {location}")
                
        if not full_terms:
            print("No search queries found to rank.")
            return

        terms_sql = ", ".join([f"'{term}'" for term in full_terms])
        
        sql = f"""
            SELECT * FROM `job_data.raw_jobs` 
            WHERE search_query IN ({terms_sql})
            ORDER BY extraction_timestamp DESC 
            LIMIT 50
        """
        
        jobs_list = [serialize_row(row) for row in client.query(sql).result()]

        if not jobs_list:
            print("No jobs to rank.")
            return

        ranked_text = rank_jobs_with_strategist(jobs_list, pdf_path)
        print(f"Ranking complete. Result preview: {ranked_text[:200]}")

        try:
            ranked_jobs = json.loads(ranked_text.replace("```json", "").replace("```", "").strip())
            run_id = kwargs.get('run_id', 'unknown_run')
            if isinstance(ranked_jobs, list) and ranked_jobs:
                for job in ranked_jobs:
                    job['resume_used'] = resume_file
                    job['airflow_run_id'] = run_id
                load_to_bigquery(ranked_jobs, table_id="ranked_job_search")
                print(f"Saved {len(ranked_jobs)} ranked jobs to ranked_job_search for run {run_id}.")
        except (json.JSONDecodeError, Exception) as e:
            print(f"Could not parse ranking output as JSON: {e}")

    # ---------------------------------------------------------
    # OPERATOR DEFINITIONS
    # ---------------------------------------------------------
    analyze_task = PythonOperator(
        task_id='analyze_resume_task',
        python_callable=analyze_resume_step,
    )

    extract_task = PythonOperator(
        task_id='extract_jobs_task',
        python_callable=extract_jobs_step,
    )

    load_task = PythonOperator(
        task_id='load_to_bq_task',
        python_callable=load_to_bq_step,
    )

    transform_task = PythonOperator(
        task_id='transform_bq_task',
        python_callable=transform_bq_step,
    )

    rank_task = PythonOperator(
        task_id='rank_jobs_task',
        python_callable=rank_jobs_step,
    )

    export_task = PythonOperator(
        task_id='export_to_sheets_task',
        python_callable=export_to_sheets,
    )
    
    # DAG execution order
    analyze_task >> extract_task >> load_task >> transform_task >> rank_task >> export_task
