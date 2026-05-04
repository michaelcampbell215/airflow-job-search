import os
import json
import time
import PyPDF2
from google import genai
from google.genai import types


def extract_text_from_pdf(pdf_path):
    md_path = pdf_path.replace('.pdf', '.md')
    if os.path.exists(md_path):
        with open(md_path, 'r', encoding='utf-8') as f:
            return f.read()
    text = ""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                text += page.extract_text() + "\n"
    except Exception as e:
        print(f"Error reading PDF {pdf_path}: {e}")
    return text


def call_llm(system_prompt, user_input):
    from airflow.sdk import Variable
    try:
        api_key = Variable.get("GEMINI_API_KEY")
    except Exception:
        raise ValueError("GEMINI_API_KEY not found. Add it in Airflow Admin → Variables.")
    client = genai.Client(api_key=api_key)
    for attempt in range(3):
        time.sleep(2)
        try:
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=str(user_input),
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt
                )
            )
            return response.text
        except Exception as e:
            if '429' in str(e) and attempt < 2:
                wait = 30 * (attempt + 1)
                print(f"Rate limited, retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def generate_search_queries(resume_pdf_path):
    prompt_path = os.path.join(os.path.dirname(__file__), '..', 'include', 'agents', 'analyzer.md')
    with open(prompt_path, 'r') as f:
        analyzer_template = f.read()

    resume_text = extract_text_from_pdf(resume_pdf_path)
    system_prompt = analyzer_template.replace("{resume_text}", resume_text)

    print(f"🤖 Analyzer is evaluating resume: {os.path.basename(resume_pdf_path)}...")
    json_output = call_llm(system_prompt, "Generate search queries.")

    clean_json = json_output.replace("```json", "").replace("```", "").strip()
    try:
        queries = json.loads(clean_json)
        print(f"✅ Generated queries: {queries}")
        return queries
    except json.JSONDecodeError as e:
        print(f"Error parsing generated queries: {e}")
        print(f"Raw LLM output: {json_output}")
        return ["Data Analyst", "Data Engineer"]


def rank_jobs_with_strategist(job_data, resume_pdf_path):
    prompt_path = os.path.join(os.path.dirname(__file__), '..', 'include', 'agents', 'strategist.md')
    with open(prompt_path, 'r') as f:
        strategist_template = f.read()

    resume_text = extract_text_from_pdf(resume_pdf_path)
    system_prompt = strategist_template.replace("{resume_text}", resume_text)

    print(f"🧠 Strategist is ranking {len(job_data)} jobs using resume: {os.path.basename(resume_pdf_path)}")
    ranked_text = call_llm(system_prompt, json.dumps(job_data))
    return ranked_text


def format_jobs_with_archivist(ranked_text):
    prompt_path = os.path.join(os.path.dirname(__file__), '..', 'include', 'agents', 'archivist.md')
    with open(prompt_path, 'r') as f:
        archivist_prompt = f.read()

    print("🗄️ Archivist is formatting data for Notion...")
    json_output = call_llm(archivist_prompt, ranked_text)

    clean_json = json_output.replace("```json", "").replace("```", "").strip()
    try:
        parsed_data = json.loads(clean_json)
        return parsed_data
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from Archivist: {e}")
        return []
