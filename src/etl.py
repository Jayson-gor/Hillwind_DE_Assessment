import pandas as pd
import json
import os
from datetime import datetime
import time
import logging

# --- Configuration ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Project root
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Logging Configuration ---
LOG_FILE = os.path.join(OUTPUT_DIR, "etl_log.txt")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Input files
EMPLOYEES_PATH = os.path.join(DATA_DIR, "employees_raw.csv")
PLANS_PATH = os.path.join(DATA_DIR, "plans_raw.csv")
CLAIMS_PATH = os.path.join(DATA_DIR, "claims_raw.csv")
COMPANY_LOOKUP_PATH = os.path.join(DATA_DIR, "company_lookup.json")
API_MOCK_PATH = os.path.join(DATA_DIR, "api_mock.json")

# Output files
VALIDATION_ERRORS_PATH = os.path.join(OUTPUT_DIR, "validation_errors.csv")
CLEAN_DATA_PATH = os.path.join(OUTPUT_DIR, "clean_data.parquet")

# High-water mark file
WATERMARK_PATH = os.path.join(OUTPUT_DIR, "watermark.txt")

# --- Main ETL Function ---
def run_etl():
    """Main function to run the complete ETL process."""
    logger.info("="*60)
    logger.info("Starting ETL process...")
    start_time = datetime.now()
    
    # Initialize metrics
    metrics = {
        'start_time': start_time,
        'rows_in': 0,
        'rows_out': 0,
        'errors': 0,
        'duplicates_removed': 0,
        'nulls_filled': 0,
        'eins_inferred': 0
    }

    # 1. Incrementalism: Get high-water mark
    last_run_timestamp = get_high_water_mark()
    logger.info(f"Processing data updated since: {last_run_timestamp}")

    # 2. Ingest Data
    employees_df, plans_df, claims_df = ingest_data(last_run_timestamp)
    metrics['rows_in'] = len(employees_df)
    
    # If no new data, ensure output files exist with proper structure
    if employees_df.empty and claims_df.empty and plans_df.empty:
        logger.info("No new data to process.")
        # Ensure validation errors file exists with headers
        if not os.path.exists(VALIDATION_ERRORS_PATH):
            pd.DataFrame(columns=['row_id', 'field', 'error_reason']).to_csv(VALIDATION_ERRORS_PATH, index=False)
        # Don't update watermark if no data processed
        logger.info(f"Clean data file: {CLEAN_DATA_PATH}")
        logger.info(f"Validation errors file: {VALIDATION_ERRORS_PATH}")
        log_summary(metrics, start_time)
        return

    # 3. Clean Data
    # (For this exercise, we'll merge and clean the employee data primarily)
    clean_df, errors_df, clean_metrics = clean_data(employees_df)
    metrics.update(clean_metrics)
    metrics['errors'] = len(errors_df)
    logger.info(f"Found {len(errors_df)} validation errors during cleaning.")

    # 4. Enrich Data
    enriched_df, enrich_metrics = enrich_data(clean_df)
    metrics.update(enrich_metrics)
    logger.info("Data enrichment complete.")

    # 5. Save Outputs
    save_metrics = save_outputs(enriched_df, errors_df)
    metrics.update(save_metrics)

    # 6. Update High-Water Mark
    update_high_water_mark()

    logger.info("ETL process finished successfully.")
    logger.info(f"Clean data saved to: {CLEAN_DATA_PATH}")
    logger.info(f"Validation errors saved to: {VALIDATION_ERRORS_PATH}")
    
    # 7. Log Summary Metrics
    log_summary(metrics, start_time)

# --- Helper Functions (Stubs) ---

def get_high_water_mark():
    """Reads the high-water mark from a file."""
    if not os.path.exists(WATERMARK_PATH):
        # Use a realistic minimum date that pandas can handle
        return '1900-01-01T00:00:00'
    with open(WATERMARK_PATH, 'r') as f:
        return f.read().strip()

def update_high_water_mark():
    """Updates the high-water mark to the current time."""
    with open(WATERMARK_PATH, 'w') as f:
        f.write(datetime.now().isoformat())

def ingest_data(last_run_timestamp):
    """Ingests data from CSVs, filtering for new/updated rows."""
    logger.info("Ingesting data...")
    
    # Use the appropriate date columns for incremental filtering.
    employees_df = pd.read_csv(EMPLOYEES_PATH, header=0, parse_dates=['start_date'])
    claims_df = pd.read_csv(CLAIMS_PATH, header=0, parse_dates=['service_date'])
    plans_df = pd.read_csv(PLANS_PATH, header=0)

    # Filter based on high-water mark using the correct date columns
    employees_df = employees_df[employees_df['start_date'] > pd.to_datetime(last_run_timestamp)]
    # For claims, we'll also use its date column.
    claims_df = claims_df[claims_df['service_date'] > pd.to_datetime(last_run_timestamp)]

    logger.info(f"Ingested {len(employees_df)} new/updated employee rows based on 'start_date'.")
    logger.info(f"Ingested {len(claims_df)} new/updated claim rows based on 'service_date'.")
    
    # In a real scenario, you'd merge claims and plans into the employee data.
    # For this exercise, we will focus on cleaning and enriching the employee data.
    return employees_df, plans_df, claims_df

def clean_data(df):
    """
    Cleans and validates the dataframe by performing the following steps:
    - Deduplicates rows.
    - Validates email formats and drops rows with invalid emails.
    - Coerces date fields, logging errors for invalid formats.
    - Carries forward missing 'title' values for each employee.
    
    Returns a tuple containing the cleaned DataFrame, validation errors DataFrame, and metrics dict.
    """
    logger.info("Cleaning and validating data...")
    metrics = {'duplicates_removed': 0, 'nulls_filled': 0}
    
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), metrics

    errors = []
    clean_df = df.copy()
    # Use original index as row_id for traceability
    clean_df['row_id'] = clean_df.index

    # 1. Deduplication
    initial_rows = len(clean_df)
    # The 'last_updated' column is not in employees_raw.csv, so we dedupe on available keys.
    clean_df.drop_duplicates(subset=['person_id', 'email', 'start_date'], inplace=True)
    duplicates_removed = initial_rows - len(clean_df)
    metrics['duplicates_removed'] = duplicates_removed
    logger.info(f"Removed {duplicates_removed} duplicate rows.")

    # 2. Email Validation
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    valid_emails = clean_df['email'].str.match(email_regex, na=True)
    
    invalid_email_rows = clean_df[~valid_emails]
    for index, row in invalid_email_rows.iterrows():
        errors.append({
            'row_id': row['row_id'],
            'field': 'email',
            'error_reason': f"Invalid email format: {row['email']}"
        })
    
    # Filter out invalid rows
    clean_df = clean_df[valid_emails]

    # 3. Date Coercion/Validation
    # The ingest step already tried parsing, but we can check for NaT (Not a Time) values
    # that result from failed parsing.
    date_cols = ['start_date', 'end_date', 'last_updated']
    for col in date_cols:
        if col in clean_df.columns:
            invalid_dates = clean_df[pd.to_datetime(clean_df[col], errors='coerce').isna()]
            for index, row in invalid_dates.iterrows():
                 errors.append({
                    'row_id': row['row_id'],
                    'field': col,
                    'error_reason': f"Invalid date format: {row[col]}"
                })
            # Coerce to datetime, turning failures into NaT
            clean_df[col] = pd.to_datetime(clean_df[col], errors='coerce')
            # Drop rows where key dates are invalid
            if col in ['start_date', 'last_updated']:
                 clean_df.dropna(subset=[col], inplace=True)

    # 4. Carry forward titles where possible
    # Sort by employee and date to ensure logical forward fill
    clean_df.sort_values(['person_id', 'start_date'], inplace=True)
    nulls_before = clean_df['title'].isnull().sum()
    clean_df['title'] = clean_df.groupby('person_id')['title'].transform(lambda x: x.ffill().bfill())
    nulls_after = clean_df['title'].isnull().sum()
    metrics['nulls_filled'] = nulls_before - nulls_after
    
    # If still null, flag as validation error and fill with 'Unknown'
    remaining_nulls = clean_df[clean_df['title'].isnull()]
    for index, row in remaining_nulls.iterrows():
        errors.append({
            'row_id': row['row_id'],
            'field': 'title',
            'error_reason': 'Missing title - no value to forward/backward fill'
        })
    clean_df['title'].fillna('Unknown', inplace=True)
    logger.info(f"Filled {metrics['nulls_filled']} null titles via forward/backward fill. {nulls_after} remain as 'Unknown'.")

    errors_df = pd.DataFrame(errors)
    return clean_df, errors_df, metrics

def enrich_data(df):
    """
    Enriches the dataframe by:
    1. Inferring missing EINs from email domains using company_lookup.json.
    2. Simulating an API call to an enrichment service (using api_mock.json)
       with caching and retry logic.
    """
    logger.info("Enriching data...")
    metrics = {'eins_inferred': 0}
    
    if df.empty:
        return df, metrics

    enriched_df = df.copy()

    # 1. Infer missing EINs
    with open(COMPANY_LOOKUP_PATH, 'r') as f:
        company_lookup = json.load(f)
    
    # Create a mapping from email domain to EIN
    domain_to_ein = {domain: ein for domain, ein in company_lookup.items()}

    # Track nulls before inference
    nulls_before = enriched_df['company_ein'].isnull().sum()

    # Function to apply to each row
    def get_ein(row):
        if pd.notna(row['company_ein']):
            return row['company_ein']
        # Safety check for email extraction
        if pd.isna(row['email']) or '@' not in str(row['email']):
            logger.warning(f"Invalid email for person_id {row['person_id']}: {row['email']}")
            return None
        try:
            domain = row['email'].split('@')[1]
            inferred = domain_to_ein.get(domain)
            if inferred:
                return inferred
            # If domain not in lookup, try to extract from notes or use fallback
            logger.warning(f"Could not infer EIN for person_id {row['person_id']} with email domain {domain}")
            return None
        except (IndexError, AttributeError):
            logger.warning(f"Could not extract domain from email for person_id {row['person_id']}")
            return None

    enriched_df['company_ein'] = enriched_df.apply(get_ein, axis=1)
    nulls_after = enriched_df['company_ein'].isnull().sum()
    metrics['eins_inferred'] = nulls_before - nulls_after
    logger.info(f"Inferred {metrics['eins_inferred']} missing EINs. {nulls_after} remain null.")

    # 2. Simulate API enrichment
    with open(API_MOCK_PATH, 'r') as f:
        api_mock_data = json.load(f)

    # Simple cache for the mock API
    api_cache = {}

    def get_enrichment_data(person_id, retries=3, backoff_factor=0.5):
        """Simulates a cached, retrying API call."""
        if person_id in api_cache:
            return api_cache[person_id]
        
        # Simulate network call
        for i in range(retries):
            try:
                # In a real scenario, this would be a requests.get() call
                # Here we just look up the data from our mock file
                if person_id in api_mock_data:
                    # Simulate success
                    result = api_mock_data[person_id]
                    api_cache[person_id] = result # Cache the result
                    time.sleep(0.05) # Simulate latency
                    return result
                else:
                    # Simulate a 404 Not Found
                    return {'personal_email': None, 'phone_number': None}
            except Exception as e:
                # Simulate a server error
                print(f"API call failed for {person_id} on attempt {i+1}. Retrying...")
                time.sleep(backoff_factor * (2 ** i)) # Exponential backoff
        
        return {'personal_email': None, 'phone_number': None} # Return empty if all retries fail

    # Apply the enrichment function
    enrichment_results = enriched_df['person_id'].apply(get_enrichment_data)
    enrichment_df = pd.json_normalize(enrichment_results)

    # Merge the new fields back into the dataframe
    final_df = pd.concat([enriched_df.reset_index(drop=True), enrichment_df], axis=1)
    logger.info(f"Enriched {len(final_df)} rows with API data (cached: {len(api_cache)} calls).")
    
    # Define consistent column order for schema consistency across runs
    FINAL_COLUMNS = [
        'person_id', 'full_name', 'title', 'email', 'company_ein', 'start_date', 
        'notes', 'row_id', 'personal_email', 'phone_number'
    ]
    final_df = final_df.reindex(columns=[c for c in FINAL_COLUMNS if c in final_df.columns])
    
    return final_df, metrics

def save_outputs(clean_df, errors_df):
    """Saves the clean data and validation errors."""
    metrics = {'rows_out': 0}
    
    # Append errors to the validation file
    if not errors_df.empty:
        errors_df.to_csv(VALIDATION_ERRORS_PATH, mode='a', header=not os.path.exists(VALIDATION_ERRORS_PATH), index=False)
    elif not os.path.exists(VALIDATION_ERRORS_PATH):
        # Create empty CSV with headers if no errors exist
        pd.DataFrame(columns=['row_id', 'field', 'error_reason']).to_csv(VALIDATION_ERRORS_PATH, index=False)
    
    # Merge with existing clean data if it exists
    if os.path.exists(CLEAN_DATA_PATH) and not clean_df.empty:
        existing_df = pd.read_parquet(CLEAN_DATA_PATH)
        
        # Ensure schema compatibility - align columns between old and new data
        all_columns = list(set(existing_df.columns) | set(clean_df.columns))
        for col in all_columns:
            if col not in existing_df.columns:
                existing_df[col] = None
            if col not in clean_df.columns:
                clean_df[col] = None
        
        # Define canonical column order for consistent schema
        CANONICAL_COLUMNS = [
            'person_id', 'full_name', 'title', 'email', 'company_ein',
            'start_date', 'notes', 'row_id', 'personal_email', 'phone_number'
        ]
        # Combine old and new data with consistent column order
        merged_df = pd.concat([existing_df, clean_df], ignore_index=True)
        # Reindex with canonical order, add any extra columns at the end
        final_columns = CANONICAL_COLUMNS + [c for c in all_columns if c not in CANONICAL_COLUMNS]
        merged_df = merged_df.reindex(columns=[c for c in final_columns if c in merged_df.columns])
        merged_df.drop_duplicates(subset=['person_id', 'start_date'], keep='last', inplace=True)
        merged_df.to_parquet(CLEAN_DATA_PATH, index=False)
        metrics['rows_out'] = len(merged_df)
        logger.info(f"Merged {len(clean_df)} new rows with {len(existing_df)} existing rows. Total: {len(merged_df)}")
    elif not clean_df.empty:
        # First run or no existing data
        clean_df.to_parquet(CLEAN_DATA_PATH, index=False)
        metrics['rows_out'] = len(clean_df)
        logger.info(f"Saved {len(clean_df)} rows to clean data.")
    elif os.path.exists(CLEAN_DATA_PATH):
        # No new data, keep existing
        existing_df = pd.read_parquet(CLEAN_DATA_PATH)
        metrics['rows_out'] = len(existing_df)
        logger.info("No new data to save. Existing clean data preserved.")
    else:
        # No data at all, create empty parquet with schema
        clean_df.to_parquet(CLEAN_DATA_PATH, index=False)
        metrics['rows_out'] = 0
    
    return metrics

def log_summary(metrics, start_time):
    """Logs comprehensive summary metrics for the ETL run."""
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("="*60)
    logger.info("ETL RUN SUMMARY")
    logger.info("="*60)
    logger.info(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Duration: {duration:.2f} seconds")
    logger.info(f"")
    logger.info(f"DATA FLOW:")
    logger.info(f"  Rows In:              {metrics['rows_in']}")
    logger.info(f"  Rows Out:             {metrics['rows_out']}")
    logger.info(f"  Validation Errors:    {metrics['errors']}")
    logger.info(f"")
    logger.info(f"DATA QUALITY:")
    logger.info(f"  Duplicates Removed:   {metrics['duplicates_removed']}")
    logger.info(f"  Null Titles Filled:   {metrics['nulls_filled']}")
    logger.info(f"  EINs Inferred:        {metrics['eins_inferred']}")
    logger.info(f"")
    logger.info(f"OUTPUT FILES:")
    logger.info(f"  Clean Data:           {CLEAN_DATA_PATH}")
    logger.info(f"  Validation Errors:    {VALIDATION_ERRORS_PATH}")
    logger.info(f"  Log File:             {LOG_FILE}")
    logger.info("="*60)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run ETL pipeline')
    parser.add_argument('--full-refresh', action='store_true', 
                        help='Force full data reload by clearing the watermark')
    args = parser.parse_args()
    
    if args.full_refresh:
        if os.path.exists(WATERMARK_PATH):
            os.remove(WATERMARK_PATH)
            print("Watermark cleared. Running full refresh...")
    
    run_etl()
