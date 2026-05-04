import pytest
import os
from unittest.mock import patch
from dotenv import load_dotenv
from dags.pipeline_functions import extract_jobs

# Load API key from .env so we don't need the Airflow Database running
load_dotenv()
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")

# This tells pytest to skip these tests unless we explicitly want to run live API calls
# You can remove the skip decorator if you want to run it every time.
@pytest.mark.live
class TestLiveJSearchAPI:

    @patch('dags.pipeline_functions.Variable.get')
    def test_live_search_returns_results(self, mock_var):
        mock_var.return_value = RAPIDAPI_KEY
        
        """Test that a broad search actually returns data instead of an empty list."""
        
        # We'll use a broad query that is guaranteed to have jobs
        query = "Data Analyst in Tampa, FL, USA"
        
        print(f"\n[Live API Test] Fetching jobs for: {query}")
        result = extract_jobs(query, num_pages=1)
        
        # Verify the structure
        assert 'data' in result, "API response is missing the 'data' key"
        
        # Verify it actually found jobs! This catches the empty list `[]` bug
        jobs = result['data']
        assert len(jobs) > 0, f"Search returned 0 results for query: {query}"
        
        print(f"[Live API Test] Success! Found {len(jobs)} jobs.")

    @patch('dags.pipeline_functions.Variable.get')
    def test_live_search_remote_roles(self, mock_var):
        mock_var.return_value = RAPIDAPI_KEY
        
        """Test that adding 'Remote' to the query actually returns remote jobs."""
        
        query = "Data Engineer Remote in Tampa, FL, USA"
        
        print(f"\n[Live API Test] Fetching jobs for: {query}")
        result = extract_jobs(query, num_pages=1)
        
        jobs = result.get('data', [])
        assert len(jobs) > 0, f"Search returned 0 results for query: {query}"
        
        # Check if at least one job in the results is actually remote
        # The JSearch API usually returns a boolean 'job_is_remote' flag
        remote_jobs = [job for job in jobs if job.get('job_is_remote') == True]
        
        print(f"[Live API Test] Found {len(jobs)} total jobs, {len(remote_jobs)} are strictly remote.")
        
        # We assert that we got at least one remote job back
        assert len(remote_jobs) > 0, "No remote jobs found despite searching for remote roles!"
