import duckdb
import os

# Define file paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Project root
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# File paths for raw data
EMPLOYEES_RAW_PATH = os.path.join(DATA_DIR, "employees_raw.csv")
PLANS_RAW_PATH = os.path.join(DATA_DIR, "plans_raw.csv")
CLAIMS_RAW_PATH = os.path.join(DATA_DIR, "claims_raw.csv")
COMPANY_LOOKUP_PATH = os.path.join(DATA_DIR, "company_lookup.json")

# DuckDB database file
DUCKDB_PATH = os.path.join(OUTPUT_DIR, "analytics.duckdb")

# File paths for SQL results
SQL_GAPS_PATH = os.path.join(OUTPUT_DIR, "sql_gaps.csv")
SQL_SPIKES_PATH = os.path.join(OUTPUT_DIR, "sql_spikes.csv")
SQL_ROSTER_PATH = os.path.join(OUTPUT_DIR, "sql_roster.csv")

def run_sql_analysis():
    """
    Runs all SQL analysis tasks using DuckDB.
    """
    # Connect to a persistent DuckDB file
    con = duckdb.connect(database=DUCKDB_PATH, read_only=False)
    print(f"Connected to DuckDB at: {DUCKDB_PATH}")

    # --- Load Data ---
    # Use CREATE OR REPLACE VIEW for instant loading (zero copy, idempotent)
    con.execute(f"CREATE OR REPLACE VIEW employees_raw AS SELECT * FROM read_csv_auto('{EMPLOYEES_RAW_PATH}', header=true);")
    con.execute(f"CREATE OR REPLACE VIEW plans_raw AS SELECT * FROM read_csv_auto('{PLANS_RAW_PATH}', header=true);")
    con.execute(f"CREATE OR REPLACE VIEW claims_raw AS SELECT * FROM read_csv_auto('{CLAIMS_RAW_PATH}', header=true);")
    
    # --- Define a common mapping for Company EIN to Company Name ---
    company_name_mapping_cte = """
    company_name_mapping AS (
        SELECT * FROM (VALUES
            ('11-1111111', 'Hillwinds Health'),
            ('22-2222222', 'Summit Solutions'),
            ('33-3333333', 'Peak Performers')
        ) AS t (company_ein, company_name)
    )
    """

    # --- Task 1: Plan Gap Detection (CORRECTED) ---
    plan_gaps_query = f"""
    WITH {company_name_mapping_cte},
    
    plans_clean AS (
        SELECT 
            cnm.company_name,
            p.plan_type,
            p.carrier_name AS carrier,
            CAST(p.start_date AS DATE) AS start_date,
            CAST(p.end_date AS DATE) AS end_date
        FROM plans_raw p
        JOIN company_name_mapping cnm ON p.company_ein = cnm.company_ein
        WHERE start_date IS NOT NULL
    ),

    -- Classic gaps-and-islands with proper overlap handling
    deduped AS (
        SELECT *,
               SUM(new_group) OVER (PARTITION BY company_name, plan_type ORDER BY start_date, end_date) AS grp
        FROM (
            SELECT *,
                   CASE WHEN start_date <= 
                             COALESCE(LAG(end_date) OVER (PARTITION BY company_name, plan_type ORDER BY start_date) + INTERVAL 1 DAY, start_date)
                        THEN 0 ELSE 1 END AS new_group
            FROM plans_clean
        )
    ),

    islands AS (
        SELECT 
            company_name,
            plan_type,
            MIN(start_date) AS island_start,
            MAX(end_date)   AS island_end,
            MIN(carrier)    AS carrier_during_island
        FROM deduped
        GROUP BY company_name, plan_type, grp
    ),

    with_next AS (
        SELECT *,
               LEAD(island_start) OVER (PARTITION BY company_name, plan_type ORDER BY island_start) AS next_start,
               LEAD(carrier_during_island) OVER (PARTITION BY company_name, plan_type ORDER BY island_start) AS next_carrier
        FROM islands
    )

    SELECT 
        company_name,
        island_end + INTERVAL 1 DAY AS gap_start,
        next_start - INTERVAL 1 DAY AS gap_end,
        DATEDIFF('day', island_end + INTERVAL 1 DAY, next_start - INTERVAL 1 DAY) AS gap_length_days,
        carrier_during_island AS previous_carrier,
        next_carrier
    FROM with_next
    WHERE next_start IS NOT NULL
      AND DATEDIFF('day', island_end, next_start) > 7
    ORDER BY company_name, gap_start;
    """
    con.execute(plan_gaps_query).fetchdf().to_csv(SQL_GAPS_PATH, index=False)
    print(f"Plan Gap Detection analysis complete. Output at {SQL_GAPS_PATH}")

    # --- Task 2: Claims Cost Spike ---
    claims_spike_query = f"""
    WITH {company_name_mapping_cte},

    claims_with_company AS (
        -- Join claims to the name mapping using EIN
        SELECT
            cnm.company_name,
            c.service_date,
            c.amount AS claim_cost
        FROM claims_raw c
        JOIN company_name_mapping cnm ON c.company_ein = cnm.company_ein
    ),

    daily_costs AS (
        -- Aggregate claim costs by company and service date
        SELECT
            company_name,
            CAST(service_date AS DATE) AS service_date,
            SUM(claim_cost) AS total_daily_cost
        FROM claims_with_company
        GROUP BY 1, 2
    ),

    rolling_90d AS (
        -- Calculate rolling 90-day cost for each date
        SELECT
            company_name,
            service_date,
            SUM(total_daily_cost) OVER (
                PARTITION BY company_name 
                ORDER BY service_date 
                ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
            ) AS rolling_90d_cost
        FROM daily_costs
    ),
    
    rolling_windows AS (
        -- Compare current 90-day window to previous period (1 day lag)
        SELECT
            company_name,
            service_date AS window_end,
            (service_date - INTERVAL '89' DAY) AS window_start,
            rolling_90d_cost AS current_90d_cost,
            LAG(rolling_90d_cost, 1) OVER (
                PARTITION BY company_name 
                ORDER BY service_date
            ) AS prev_90d_cost
        FROM rolling_90d
    )

    -- Calculate the percentage change and filter for spikes > 200%
    SELECT
        company_name,
        window_start,
        window_end,
        prev_90d_cost,
        current_90d_cost,
        ROUND(CASE 
            WHEN prev_90d_cost = 0 THEN NULL 
            ELSE (current_90d_cost - prev_90d_cost) / prev_90d_cost * 100 
        END, 2) AS pct_change
    FROM rolling_windows
    WHERE prev_90d_cost IS NOT NULL AND prev_90d_cost > 0 AND (current_90d_cost - prev_90d_cost) / prev_90d_cost > 2.0
    ORDER BY company_name, window_end;
    """
    con.execute(claims_spike_query).fetchdf().to_csv(SQL_SPIKES_PATH, index=False)
    print(f"Claims Cost Spike analysis complete. Output at {SQL_SPIKES_PATH}")

    # --- Task 3: Employee Roster Mismatch ---
    roster_mismatch_query = f"""
    WITH {company_name_mapping_cte},
    
    company_expected_counts AS (
        -- Use the mapping for expected counts as well
        SELECT company_name, expected_employees
        FROM (VALUES
            ('Hillwinds Health', 60),
            ('Summit Solutions', 45),
            ('Peak Performers', 40)
        ) AS t (company_name, expected_employees)
    ),

    observed_employee_counts AS (
        -- Calculate the observed number of unique employees per company
        SELECT
            cnm.company_name,
            COUNT(DISTINCT e.person_id) AS observed_employees
        FROM employees_raw e
        JOIN company_name_mapping cnm ON e.company_ein = cnm.company_ein
        GROUP BY cnm.company_name
    ),

    comparison AS (
        -- Join the expected and observed counts
        SELECT
            cec.company_name,
            cec.expected_employees AS expected,
            oec.observed_employees AS observed,
            ROUND(ABS(oec.observed_employees - cec.expected_employees) * 100.0 / cec.expected_employees, 2) AS pct_diff
        FROM company_expected_counts cec
        JOIN observed_employee_counts oec ON cec.company_name = oec.company_name
    )

    -- Assign a severity level based on the percentage difference
    SELECT
        company_name,
        expected,
        observed,
        pct_diff,
        CASE
            WHEN pct_diff > 100 THEN 'Critical'
            WHEN pct_diff > 50 THEN 'High'
            WHEN pct_diff > 20 THEN 'Medium'
            ELSE 'Low'
        END AS severity
    FROM comparison
    ORDER BY pct_diff DESC;
    """
    con.execute(roster_mismatch_query).fetchdf().to_csv(SQL_ROSTER_PATH, index=False)
    print(f"Employee Roster Mismatch analysis complete. Output at {SQL_ROSTER_PATH}")

    # Close the connection
    con.close()
    print(f"SQL analysis complete. Outputs saved in {OUTPUT_DIR}")
    print(f"DuckDB database available at: {DUCKDB_PATH}")

if __name__ == "__main__":
    run_sql_analysis()
