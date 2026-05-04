"""
Unit tests for pipeline_functions.py
Run with: pytest tests/test_pipeline_functions.py

These tests use mocking so they run without live GCP or API credentials.
"""
import json
import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock, call


# ---------------------------------------------------------------------------
# load_to_bigquery
# ---------------------------------------------------------------------------

class TestLoadToBigquery:

    @patch('pipeline_functions.bigquery.Client')
    @patch('pipeline_functions.Variable.get', return_value='fake-key')
    def test_skips_on_empty_input(self, mock_var, mock_client):
        from dags.pipeline_functions import load_to_bigquery
        result = load_to_bigquery([])
        assert result == "No data to load"
        mock_client().load_table_from_json.assert_not_called()

    @patch('pipeline_functions.bigquery.Client')
    @patch('pipeline_functions.Variable.get', return_value='fake-key')
    def test_serializes_nested_fields(self, mock_var, mock_bq):
        """list and dict values must be JSON-serialized before BigQuery load."""
        from dags.pipeline_functions import load_to_bigquery

        mock_job = MagicMock()
        mock_bq().load_table_from_json.return_value = mock_job
        mock_bq().project = 'test-project'

        record = {'title': 'Data Engineer', 'skills': ['SQL', 'Python'], 'meta': {'level': 'mid'}}
        load_to_bigquery([record])

        loaded = mock_bq().load_table_from_json.call_args[0][0]
        assert isinstance(loaded[0]['skills'], str)
        assert isinstance(loaded[0]['meta'], str)
        assert json.loads(loaded[0]['skills']) == ['SQL', 'Python']

    @patch('pipeline_functions.bigquery.Client')
    @patch('pipeline_functions.Variable.get', return_value='fake-key')
    def test_adds_extraction_timestamp(self, mock_var, mock_bq):
        from dags.pipeline_functions import load_to_bigquery

        mock_bq().project = 'test-project'
        mock_bq().load_table_from_json.return_value = MagicMock()

        record = {'title': 'Analyst'}
        load_to_bigquery([record])

        loaded = mock_bq().load_table_from_json.call_args[0][0]
        assert 'extraction_timestamp' in loaded[0]

    @patch('pipeline_functions.bigquery.Client')
    @patch('pipeline_functions.Variable.get', return_value='fake-key')
    def test_wraps_single_dict_in_list(self, mock_var, mock_bq):
        from dags.pipeline_functions import load_to_bigquery

        mock_bq().project = 'test-project'
        mock_bq().load_table_from_json.return_value = MagicMock()

        load_to_bigquery({'title': 'Engineer'})

        loaded = mock_bq().load_table_from_json.call_args[0][0]
        assert isinstance(loaded, list)
        assert len(loaded) == 1


# ---------------------------------------------------------------------------
# extract_jobs
# ---------------------------------------------------------------------------

class TestExtractJobs:

    @patch('pipeline_functions.requests.get')
    @patch('pipeline_functions.Variable.get', return_value='test-api-key')
    def test_passes_query_and_pages(self, mock_var, mock_get):
        from dags.pipeline_functions import extract_jobs

        mock_resp = MagicMock()
        mock_resp.json.return_value = {'data': []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        extract_jobs("Data Engineer", num_pages=2)

        _, kwargs = mock_get.call_args
        assert kwargs['params']['query'] == "Data Engineer"
        assert kwargs['params']['num_pages'] == "2"

    @patch('pipeline_functions.requests.get')
    @patch('pipeline_functions.Variable.get', return_value='test-api-key')
    def test_raises_on_http_error(self, mock_var, mock_get):
        from dags.pipeline_functions import extract_jobs
        import requests

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("429")
        mock_get.return_value = mock_resp

        with pytest.raises(requests.HTTPError):
            extract_jobs("bad query")


# ---------------------------------------------------------------------------
# transform_in_bigquery
# ---------------------------------------------------------------------------

class TestTransformInBigquery:

    @patch('pipeline_functions.bigquery.Client')
    def test_skips_when_schema_empty(self, mock_bq):
        from dags.pipeline_functions import transform_in_bigquery

        mock_table = MagicMock()
        mock_table.schema = []
        mock_bq().get_table.return_value = mock_table

        result = transform_in_bigquery()
        assert "Skipped" in result
        mock_bq().query.assert_not_called()

    @patch('pipeline_functions.bigquery.Client')
    def test_runs_sql_when_schema_present(self, mock_bq):
        from dags.pipeline_functions import transform_in_bigquery

        mock_table = MagicMock()
        mock_table.schema = [MagicMock()]  # non-empty
        mock_bq().get_table.return_value = mock_table

        transform_in_bigquery()
        mock_bq().query.assert_called_once()
        sql = mock_bq().query.call_args[0][0]
        assert "raw_jobs" in sql
        assert "PARTITION BY job_id" in sql


# ---------------------------------------------------------------------------
# export_to_sheets
# ---------------------------------------------------------------------------

class TestExportToSheets:

    @patch('pipeline_functions.bigquery.Client')
    def test_skips_when_table_missing(self, mock_bq):
        from dags.pipeline_functions import export_to_sheets

        mock_bq().get_table.side_effect = Exception("Not found")
        export_to_sheets()
        mock_bq().query.assert_not_called()

    @patch('pipeline_functions.Variable.get', return_value='sheet-id-123')
    @patch('pipeline_functions.bigquery.Client')
    def test_calls_append_leads_with_jobs(self, mock_bq, mock_var):
        from dags.pipeline_functions import export_to_sheets

        mock_bq().get_table.return_value = MagicMock()
        mock_bq().query.return_value.result.return_value = [
            {'job_id': '1', 'job_title': 'Analyst', 'match_score': 8}
        ]

        with patch('pipeline_functions.GoogleSheetsManager') as mock_gsm:
            # need to make the import work inside the function
            with patch.dict('sys.modules', {'google_sheets_manager': MagicMock()}):
                export_to_sheets()
