import sys, requests, json, logging, os
from datetime import datetime
from google.cloud import bigquery
from airflow.sdk import Variable



def extract_jobs(query: str, num_pages: int = 1):
    """Fetch job listings from the JSearch API for a given search query."""
    api_key = Variable.get("RAPIDAPI_KEY")
    headers = {
        'x-rapidapi-host': 'jsearch.p.rapidapi.com',
        'x-rapidapi-key': api_key,
    }
    params = {
        "query": query,
        "page": "1",
        "num_pages": str(num_pages),
        "country": "US",
        "language": "en",
        "date_posted": "3days",
    }
    response = requests.get('https://jsearch.p.rapidapi.com/search', headers=headers, params=params)
    response.raise_for_status()
    logging.info("JSearch OK: %s", query)
    return response.json()


def load_to_bigquery(raw_data, dataset_id="job_data", table_id="search_queries"):
    """
    Append raw API response records to a BigQuery table.
    Nested list/dict fields are serialized to JSON strings before loading
    to avoid PyArrow type conflicts.
    """
    client = bigquery.Client()
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    if isinstance(raw_data, dict):
        data_to_load = [raw_data]
    elif isinstance(raw_data, list) and raw_data:
        data_to_load = raw_data
    else:
        logging.warning("No valid data for BigQuery load — skipping.")
        return "No data to load"

    for record in data_to_load:
        record['extraction_timestamp'] = datetime.utcnow().isoformat()
        record['search_query'] = record.get('search_query', 'unknown')
        for key, value in list(record.items()):
            if isinstance(value, (list, dict)):
                record[key] = json.dumps(value)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        autodetect=True,
    )
    job = client.load_table_from_json(data_to_load, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {len(data_to_load)} records to {table_ref}")
    return f"Loaded {len(data_to_load)} records"


def transform_in_bigquery():
    """
    Flatten JSON job records from search_queries into the raw_jobs table.
    Deduplicates by job_id, keeping the most recent extraction.
    """
    client = bigquery.Client()

    table = client.get_table("job_data.search_queries")
    if not table.schema:
        print("search_queries has no schema yet — skipping transform.")
        return "Skipped - search_queries is empty"

    sql = """
    CREATE OR REPLACE TABLE `job_data.raw_jobs` AS
    WITH flattened AS (
        SELECT
            extraction_timestamp,
            search_query,
            JSON_VALUE(job, '$.job_id')           AS job_id,
            JSON_VALUE(job, '$.employer_name')    AS employer_name,
            JSON_VALUE(job, '$.employer_website') AS employer_website,
            JSON_VALUE(job, '$.job_title')        AS job_title,
            JSON_VALUE(job, '$.job_location')     AS job_location,
            JSON_VALUE(job, '$.job_is_remote')    AS job_is_remote,
            JSON_VALUE(job, '$.job_posted_at')    AS job_posted_at,
            JSON_VALUE(job, '$.job_apply_link')   AS job_apply_link,
            JSON_VALUE(job, '$.job_description')  AS job_description
        FROM `job_data.search_queries`,
        UNNEST(JSON_EXTRACT_ARRAY(data)) AS job
        WHERE job IS NOT NULL
    ),
    deduped AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY job_id
                ORDER BY extraction_timestamp DESC
            ) AS rn
        FROM flattened
        WHERE job_id IS NOT NULL
    )
    SELECT
        job_id, employer_name, employer_website, job_title, job_location,
        job_is_remote, job_posted_at, job_apply_link, job_description,
        extraction_timestamp, search_query
    FROM deduped
    WHERE rn = 1
    """
    client.query(sql).result()
    print("BigQuery transform complete")
    return "Transformed with deduplication"


def export_to_sheets(**kwargs):
    """
    Export the ranked_job_search table to Google Sheets.
    Reads the target spreadsheet ID from the GOOGLE_SHEET_ID Airflow Variable.
    """
    from google_sheets_manager import GoogleSheetsManager

    client = bigquery.Client()
    try:
        table = client.get_table("job_data.ranked_job_search")
        if not table.schema:
            print("ranked_job_search table exists but has no data yet — skipping export.")
            return
    except Exception:
        print("ranked_job_search table not found — skipping export.")
        return

    resume_file = kwargs.get('params', {}).get('resume_filename', '')
    worksheet_name = os.path.splitext(resume_file)[0] if resume_file else None

    run_id = kwargs.get('run_id')
    where_clause = f"WHERE airflow_run_id = '{run_id}'" if run_id else ""

    sql = f"""
    SELECT
        job_title                      AS `Job Title`,
        employer_name                  AS `Company`,
        employer_website               AS `Company Website`,
        job_location                   AS `Location`,
        job_is_remote                  AS `Work Location`,
        job_posted_at                  AS `Posted Date`,
        job_apply_link                 AS `Apply Link`,
        match_score                    AS `Match Score`,
        strategic_why                  AS `Why It Fits`
    FROM `job_data.ranked_job_search`
    {where_clause}
    ORDER BY match_score DESC
    LIMIT 100
    """
    final_jobs = [dict(row) for row in client.query(sql).result()]

    if not final_jobs:
        print("No ranked jobs to export.")
        return

    try:
        sheet_id = Variable.get("GOOGLE_SHEET_ID")
    except Exception:
        sheet_id = None
        


    manager = GoogleSheetsManager("AI Job Search", spreadsheet_id=sheet_id, worksheet_name=worksheet_name)
    manager.append_leads(final_jobs)
    print(f"Exported {len(final_jobs)} jobs to Google Sheets")
