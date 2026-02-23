# Module 4 Homework - Analytics Engineering with dbt

## Setup

### Prerequisites

1. Set up your dbt project following the [setup guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/setup)
2. Load the Green and Yellow taxi data for 2019-2020 into your warehouse
3. Run `dbt build --target prod` to create all models and run tests

## Quiz Questions & Answers

### Question 1. dbt Lineage and Execution

**Question:** Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

- `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies)
- Any model with upstream and downstream dependencies to `int_trips_unioned`
- `int_trips_unioned` only
- `int_trips_unioned`, `int_trips`, and `fct_trips` (downstream dependencies)

**Answer:** `int_trips_unioned` only

---

### Question 2. dbt Tests

**Question:** You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

**Answer:** dbt will fail the test, returning a non-zero exit code

---

### Question 3. Counting Records in `fct_monthly_zone_revenue`

**Question:** After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

**Answer:** `12,184`

SQL query used:
```sql
SELECT COUNT(*) FROM `[PROJECT_ID].[DATABASE].fct_monthly_zone_revenue`;
```

---

### Question 4. Best Performing Zone for Green Taxis (2020)

**Question:** Using the `fct_monthly_zone_revenue` table, find the pickup zone with the highest total revenue (`revenue_monthly_total_amount`) for Green taxi trips in 2020.

Which zone had the highest revenue?

**Answer:** `East Harlem North`

SQL query used:
```sql
SELECT 
    pickup_zone,
    SUM(revenue_monthly_total_amount) AS total_revenue
FROM `[PROJECT_ID].[DATABASE].fct_monthly_zone_revenue`
WHERE service_type = 'Green'
AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;
```

---

### Question 5. Green Taxi Trip Counts (October 2019)

**Question:** Using the `fct_monthly_zone_revenue` table, what is the total number of trips (`total_monthly_trips`) for Green taxis in October 2019?


**Answer:** `384,624`

SQL query used:
```sql
SELECT 
    SUM(total_monthly_trips) AS total_trips
FROM `[PROJECT_ID].[DATABASE].fct_monthly_zone_revenue`
WHERE service_type = 'Green'
AND revenue_month = '2019-10-01';
```

---

### Question 6. Build a Staging Model for FHV Data

**Question:** Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.


What is the count of records in `stg_fhv_tripdata`?

**Answer:** `22,998,722`

---

## Submission

Form for submitting: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw4
