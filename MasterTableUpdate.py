import pandas as pd
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from googleapiclient.discovery import build
import gspread
import re

# Set up credentials and API clients
SERVICE_ACCOUNT_FILE = '/Users/keithjohnson/Desktop/GospelStatsBrandReports/rich-stratum-367618-60ffa4f70aea.json'
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform"
]

credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
gc = gspread.authorize(credentials)
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# BigQuery setup
dataset_id = 'rich-stratum-367618.NewGospelBrandReports'
master_table_id = 'MasterTable'
log_table_id = 'ProcessedSheetsLog'

# Define schema
master_schema = [
    bigquery.SchemaField("Channel title", "STRING"),
    bigquery.SchemaField("Brands", "STRING"),
    bigquery.SchemaField("Views", "INTEGER"),
    bigquery.SchemaField("Video title", "STRING"),
    bigquery.SchemaField("Video URL", "STRING"),
    bigquery.SchemaField("Duration in seconds", "INTEGER"),
    bigquery.SchemaField("Country", "STRING"),
    bigquery.SchemaField("Language", "STRING"),
    bigquery.SchemaField("Date published", "DATE"),
    bigquery.SchemaField("Category", "STRING"),
    bigquery.SchemaField("YouTube URL", "STRING"),
    bigquery.SchemaField("All time views", "INTEGER"),
    bigquery.SchemaField("All time subs", "INTEGER"),
    bigquery.SchemaField("30 day views", "INTEGER"),
    bigquery.SchemaField("30 Day Subs", "INTEGER"),
    bigquery.SchemaField("Made_for_kids", "STRING"),
    bigquery.SchemaField("date", "DATE")
]

log_schema = [bigquery.SchemaField("sheet_name", "STRING")]

# Create tables if they don't exist
def create_table_if_needed(client, dataset, table_name, schema):
    table_ref = f"{dataset}.{table_name}"
    try:
        client.get_table(table_ref)
    except:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)

create_table_if_needed(bq_client, dataset_id, master_table_id, master_schema)
create_table_if_needed(bq_client, dataset_id, log_table_id, log_schema)

# Fetch processed sheets
processed_sheets = bq_client.query(
    f"SELECT sheet_name FROM `{dataset_id}.{log_table_id}`"
).to_dataframe()
processed_list = processed_sheets['sheet_name'].tolist()

# Initialize Drive API
drive_service = build('drive', 'v3', credentials=credentials)
folder_id = '1JTDdz7v3ohW1CC8AzUBd5xADI9dOAycu'

# List all spreadsheet files in the folder
query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
response = drive_service.files().list(q=query, fields='files(id, name)').execute()
folder_files = response.get('files', [])

EXPECTED_HEADERS = [
    "Channel title",
    "Brands",
    "Views",
    "Video title",
    "Video URL",
    "Duration in seconds",
    "Country",
    "Language",
    "Date published",
    "Category",
    "YouTube URL",
    "All time views",
    "All time subs",
    "30 day views",
    "30 Day Subs",
    "Made_for_kids"
]

for sheet_file in folder_files:
    sheet_name = sheet_file['name']

    # Skip if we've already processed it
    if sheet_name in processed_list:
        print(f"Skipping already processed sheet: {sheet_name}")
        continue

    # Look for a date in the format "YYYY MM DD"
    match = re.search(r'(\d{4}\s\d{2}\s\d{2})', sheet_name)
    if not match:
        print(f"Date not found in sheet name: {sheet_name}, skipping.")
        continue

    # Parse the matched date string
    date_str = match.group(1)
    date = pd.to_datetime(date_str, format='%Y %m %d').date()

    # Open sheet by ID
    sheet = gc.open_by_key(sheet_file['id']).sheet1

    # Figure out which headers exist
    sheet_headers = sheet.row_values(1)
    valid_expected = [hdr for hdr in EXPECTED_HEADERS if hdr in sheet_headers]

    # Read data into DataFrame
    df = pd.DataFrame(sheet.get_all_records(expected_headers=valid_expected))

    # Add any missing columns
    missing_cols = set(EXPECTED_HEADERS) - set(df.columns)
    for col in missing_cols:
        df[col] = None

    # Add your "date" column from the sheet name
    df['date'] = date

    # Convert columns to correct data types based on your BigQuery schema.

    ## 1. Convert integer fields
    int_fields = [
        "Views",
        "Duration in seconds",
        "All time views",
        "All time subs",
        "30 day views",
        "30 Day Subs"
    ]
    for col in int_fields:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    ## 2. Convert date fields
    date_fields = ["Date published", "date"]
    for col in date_fields:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    ## 3. Convert string fields (based on schema)
    for field in master_schema:
        if field.field_type == "STRING" and field.name in df.columns:
            # Force any type to string to avoid ArrowTypeError
            df[field.name] = df[field.name].fillna("").astype(str)

    # Trim columns to match the BigQuery schema order and presence
    df = df[[field.name for field in master_schema if field.name in df.columns]]

    # Configure the load job
    job_config = bigquery.LoadJobConfig()
    job_config.schema = master_schema
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    # Load to BigQuery
    bq_client.load_table_from_dataframe(
        df,
        f"{dataset_id}.{master_table_id}",
        job_config=job_config
    ).result()

    # Log the processed sheet
    log_df = pd.DataFrame({'sheet_name': [sheet_name]})
    log_job_config = bigquery.LoadJobConfig()
    log_job_config.schema = log_schema
    log_job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    bq_client.load_table_from_dataframe(
        log_df,
        f"{dataset_id}.{log_table_id}",
        job_config=log_job_config
    ).result()

    print(f"Processed and logged: {sheet_name}")

# Deduplicate master table
deduplicate_query = f"""
CREATE OR REPLACE TABLE `{dataset_id}.{master_table_id}` AS
SELECT DISTINCT *
FROM `{dataset_id}.{master_table_id}`
"""
bq_client.query(deduplicate_query).result()

print("All sheets processed, logged, and deduplicated successfully.")
