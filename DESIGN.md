# dbt System Design

This document outlines a proposed dbt structure for the data pipeline, covering source configuration, model layers, incremental strategies, testing, metadata, and operational best practices.

---

## 1. Source Configuration (`sources.yml`)

Before building models, I define my sources with freshness checks:

```yaml
# models/staging/sources.yml
version: 2
sources:
  - name: raw_data
    database: analytics
    schema: raw
    tables:
      - name: employees_raw
        loaded_at_field: start_date  # Best available timestamp
        freshness:
          warn_after: {count: 24, period: hour}
          error_after: {count: 48, period: hour}
      - name: plans_raw
        loaded_at_field: start_date
      - name: claims_raw
        loaded_at_field: service_date
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}
  
  - name: enrichment
    description: "Output from Python ETL pipeline"
    tables:
      - name: clean_data
        external:
          location: "outputs/clean_data.parquet"
        loaded_at_field: start_date
        freshness:
          warn_after: {count: 24, period: hour}
```

---

## 2. Model Structure (Staging → Intermediate → Marts)

### Staging Models (`staging/`)

Staging models are **views** (not incremental) since raw CSVs lack reliable `last_updated` timestamps. They perform light cleaning and type casting only.

```sql
-- models/staging/stg_employees.sql
{{ config(materialized='view') }}

SELECT
    person_id::INT AS employee_id,
    TRIM(full_name) AS full_name,
    LOWER(TRIM(email)) AS email,
    TRIM(title) AS title,
    company_ein,
    CAST(start_date AS DATE) AS start_date,
    notes,
    CURRENT_TIMESTAMP AS _dbt_loaded_at
FROM {{ source('raw_data', 'employees_raw') }}
```

-   `stg_employees`: View over `employees_raw`. Casts types, trims strings, standardizes email to lowercase.
-   `stg_plans`: View over `plans_raw`. Casts date fields, validates plan_type values.
-   `stg_claims`: View over `claims_raw`. Casts amount to decimal, validates claim_id uniqueness.
-   `stg_enrichment`: View over Python ETL output (`clean_data.parquet`). Declared as external source with freshness monitoring.

### Intermediate Models (`intermediate/`)

Business logic and joins. Ephemeral or table materialization depending on reuse.

-   `int_employee_plans`: Joins `stg_employees` with `stg_plans` on `company_ein` with date-range overlap logic to associate employees with their active plans.
-   `int_claims_enriched`: Joins `stg_claims` with plan and company context for downstream analytics.
-   `int_employee_deduplicated`: Deduplicates employees using `ROW_NUMBER()` partitioned by `employee_id`, ordered by `start_date DESC` to keep most recent.

### Fact & Dimension Models (`marts/`)

**`dim_employees`** (Type 2 SCD):
```sql
-- models/marts/dim_employees.sql
{{ config(
    materialized='incremental',
    unique_key='employee_surrogate_key',
    incremental_strategy='merge',
    merge_update_columns=['is_current', '_valid_to']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['employee_id', 'start_date']) }} AS employee_surrogate_key,
    employee_id,
    full_name,
    email,
    title,
    company_ein,
    start_date AS _valid_from,
    LEAD(start_date) OVER (PARTITION BY employee_id ORDER BY start_date) AS _valid_to,
    CASE WHEN _valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,
    CURRENT_TIMESTAMP AS _dbt_updated_at
FROM {{ ref('int_employee_deduplicated') }}
```

-   `dim_plans`: Plan dimension with surrogate key, plan_type validation, active flag.
-   `dim_companies`: Company dimension derived from distinct company_ein values.
-   `dim_date`: Standard date dimension (generated via dbt_date_spine or seed).

**`fct_claims`** (Incremental):
```sql
-- models/marts/fct_claims.sql
{{ config(
    materialized='incremental',
    unique_key='claim_id',
    incremental_strategy='merge'
) }}

SELECT
    claim_id,
    employee_surrogate_key,
    plan_surrogate_key,
    company_ein,
    service_date,
    amount,
    claim_type,
    CURRENT_TIMESTAMP AS _dbt_updated_at
FROM {{ ref('int_claims_enriched') }}
{% if is_incremental() %}
WHERE service_date > (SELECT MAX(service_date) FROM {{ this }})
{% endif %}
```

-   `fct_employee_plan_coverage`: Accumulating snapshot tracking coverage periods per employee-plan combination.

---

## 3. Incremental Strategy Summary

| Model | Materialization | Unique Key | Incremental Filter |
|-------|-----------------|------------|-------------------|
| `stg_*` | `view` | N/A | N/A (full refresh) |
| `dim_employees` | `incremental` (merge) | `employee_surrogate_key` | `_valid_from > max(_valid_from)` |
| `dim_plans` | `incremental` (merge) | `plan_surrogate_key` | `start_date > max(start_date)` |
| `fct_claims` | `incremental` (merge) | `claim_id` | `service_date > max(service_date)` |
| `fct_employee_plan_coverage` | `incremental` (merge) | `employee_id + plan_id` | `updated_at > max(updated_at)` |

**Key Design Decision:** Staging models are views (not incremental) because raw CSV sources lack reliable `last_updated` timestamps. Incrementalism starts at the mart layer where I control the grain and have proper audit columns.

---

## 4. Schema and Column Tests

```yaml
# models/marts/schema.yml
version: 2
models:
  - name: dim_employees
    columns:
      - name: employee_surrogate_key
        tests:
          - unique
          - not_null
      - name: employee_id
        tests:
          - not_null
      - name: email
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
      - name: _valid_from
        tests:
          - not_null
      - name: _valid_to
        tests:
          - dbt_utils.expression_is_true:
              expression: "_valid_to IS NULL OR _valid_to > _valid_from"

  - name: fct_claims
    columns:
      - name: claim_id
        tests:
          - unique
          - not_null
      - name: employee_surrogate_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_employees')
              field: employee_surrogate_key
      - name: amount
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1000000  # Flag outliers

  - name: dim_plans
    columns:
      - name: plan_type
        tests:
          - accepted_values:
              values: ['Medical', 'Dental', 'Vision']
      - name: end_date
        tests:
          - dbt_utils.expression_is_true:
              expression: "end_date IS NULL OR end_date >= start_date"
```

---

## 5. Data Quality and Monitoring

### Source Freshness
```bash
dbt source freshness --select source:raw_data
```
Alerts via Slack/PagerDuty if sources exceed thresholds defined in `sources.yml`.

### Anomaly Detection (using `dbt_expectations`)
```yaml
# Row count anomaly detection
- name: fct_claims
  tests:
    - dbt_expectations.expect_table_row_count_to_be_between:
        min_value: "{{ var('min_daily_claims', 100) }}"
        max_value: "{{ var('max_daily_claims', 50000) }}"
    - dbt_expectations.expect_column_mean_to_be_between:
        column_name: amount
        min_value: 50
        max_value: 5000  # Flag if average claim cost is abnormal
```

### Custom Data Quality Macro
```sql
-- macros/test_no_orphaned_foreign_keys.sql
{% test no_orphaned_fks(model, column_name, parent_model, parent_column) %}
SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} NOT IN (SELECT {{ parent_column }} FROM {{ parent_model }})
{% endtest %}
```

---

## 6. Metadata Columns

Every mart model includes standard audit columns:

| Column | Description | Example |
|--------|-------------|---------|
| `_dbt_updated_at` | Timestamp of last dbt run that touched this row | `CURRENT_TIMESTAMP` |
| `_dbt_valid_from` | SCD2: When this version became active | `2024-01-15` |
| `_dbt_valid_to` | SCD2: When this version was superseded | `2024-03-01` or `NULL` |
| `_is_current` | SCD2: Boolean flag for current record | `TRUE` |
| `_source_file` | Origin file for traceability | `'employees_raw.csv'` |

---

## 7. Model Deprecation Strategy

Safe deprecation follows a 4-phase process:

**Phase 1 — Mark Deprecated** (Week 1):
```yaml
# schema.yml
- name: old_dim_employees
  config:
    meta:
      deprecated: true
      deprecation_date: '2024-04-01'
      replacement: 'dim_employees_v2'
  description: "⚠️ DEPRECATED: Use dim_employees_v2 instead. Will be removed 2024-05-01."
```

**Phase 2 — Notify Stakeholders** (Week 1-2):
- Slack announcement to data consumers
- Update data catalog/documentation
- Add `WARNING` log in model using `{{ log("DEPRECATED: old_dim_employees", info=True) }}`

**Phase 3 — Monitor Usage** (Week 2-4):
```sql
-- Query warehouse audit logs
SELECT query_text, user_name, COUNT(*) 
FROM query_history 
WHERE query_text ILIKE '%old_dim_employees%'
GROUP BY 1, 2;
```

**Phase 4 — Archive & Remove** (Week 4+):
- Move to `models/_archived/` folder
- Remove from production after confirming zero queries
- Keep in version control for 90 days before deletion

---

# Deep Debugging & Incident Response

### Scenario Recap
A new upstream feed starts delivering ~20M rows/day → warehouse costs triple, dbt model runtime jumps from 4 → 45+ minutes, **final mart outputs are materially wrong even though all dbt tests pass**.

---

### 1. Most Likely Root Causes (in order of real-world probability)

| Rank | Cause | Why It Explains All Symptoms |
|------|-------|------------------------------|
| 1 | **Exploding join / fanout** | One-to-many or many-to-many join on a key that suddenly has duplicates or very low cardinality (e.g., new feed introduces duplicate `person_id`, `company_ein`, or nulls) → billions of rows in a CTE → cost & slowness + wrong aggregates |
| 2 | **Duplicate primary keys in source** | New feed violates uniqueness assumptions → `unique` tests still pass downstream because we only test surrogate keys, not business keys |
| 3 | **Missing / wrong partition pruning** | New data has `service_date` in the future or null → incremental models fall back to full-table scans |
| 4 | **Broadcast join of huge table** | A large dimension (e.g., enriched employees) is no longer small → gets broadcasted everywhere |

---

### 2. First 30-Minute War-Room Plan (exact timed actions)

| Time | Action | Goal |
|------|--------|------|
| 0–2 min | **Kill / pause the dbt run** + disable downstream jobs (Looker, reverse-ETL, Airflow DAGs) | Stop the bleeding |
| 2–7 min | Open warehouse query history → sort by **bytes processed** → find the most expensive query in the last failed run | Immediate suspect |
| 7–15 min | Pull the compiled SQL of that model → run `EXPLAIN (ANALYZE, DIST)` or equivalent (Snowflake: `EXPLAIN`, BigQuery: query plan) → look for **"Table Scan"** or **"Join with huge row multiplication"** | Confirm fanout vs partition issue |
| 15–22 min | In the same compiled SQL, progressively comment out CTEs/joins and replace with `SELECT COUNT(*) FROM cte_x` until row count jumps from millions → billions → **found the exact exploding CTE** | Pinpoint the regressive join |
| 22–30 min | Run: `SELECT join_key, COUNT(*) c FROM <exploding_table> GROUP BY 1 ORDER BY c DESC LIMIT 20` and same on the other side of the join | Identify duplicate or null keys causing the explosion |



---

### 3. How We Detect Logic Bugs That Slip Past dbt Tests

Add **data monitoring tests** (via dbt + Elementary or Monte Carlo):

```yaml
# models/marts/schema.yml
- name: fct_claims
  tests:
    - elementary.volume_anomalies
    - elementary.freshness_anomalies
    - dbt_expectations.expect_table_row_count_to_be_between:
        min_value: 500000
        max_value: 30000000   # would have caught 20M → 2B row explosion
```

Add metric continuity tests in production:

```sql
-- Example: total claim cost should not change >30% day-over-day
elementary.metric_anomalies(metric_name='total_claim_cost', sensitivity=0.3)
```

---

### 4. Concrete Guardrails to Prevent Recurrence

| Guardrail | Implementation | Why It Would Have Blocked This Incident |
|-----------|----------------|----------------------------------------|
| Source row-count + freshness alerts | `sources.yml` freshness + Elementary volume anomalies | 20M rows/day would trigger immediate Slack alert |
| Pre-merge production-sample dry runs | CI job runs `dbt compile && dbt run --select +problematic_model --full-refresh` on a 1% sample of prod data in a dev warehouse | Catches fanout before merge |
| Mandatory unique + not_null on business keys | Test on staging sources: `person_id`, `claim_id`, `company_ein` | Would have failed the moment duplicates appeared |
| Warehouse cost + query bytes alerts | Cloud provider billing alerts + query bytes processed monitoring | Tripling cost triggers PagerDuty within minutes |
| Join cardinality monitoring | Elementary `dimension_anomalies` on join keys or custom monitor: `SELECT COUNT(*) / COUNT(DISTINCT person_id) > 1.1` | Detects fanout automatically |

---

### Summary

This incident is almost always a **join-key integrity regression**. The 30-minute plan above finds it surgically. The guardrails above make it virtually impossible to merge such a regression again.
