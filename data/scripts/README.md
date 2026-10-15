# Operator scripts

## `aurora_sql.py`: read-only SQL on Aurora

Aurora PostgreSQL is private to the AWS VPC (`PubliclyAccessible=False`), so `psql` cannot connect to it from the
WorkSpace or a laptop. This script sends one query to the `acme-perf-etl` Lambda, which runs inside the VPC,
executes it **read-only**, and returns the rows. The result is printed as a table.

### Usage

Real-AWS terminal, from the repo root, after `./bin/setup-participant.sh`:

```bash
export ETL_FUNCTION=coding-workshop-acme-perf-etl-74cbf3a2

python data/scripts/aurora_sql.py "SELECT count(*) FROM employees"
python data/scripts/aurora_sql.py "SELECT department, count(*) FROM employees GROUP BY department ORDER BY 2 DESC"
python data/scripts/aurora_sql.py "SELECT review_period, round(avg(rating), 2) FROM performance_reviews GROUP BY 1"
python data/scripts/aurora_sql.py "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
```

Example output:

```
employees | reviews
----------+--------
2000      | 4000
(1 rows)
```

### Safeguards

| Rule | Enforced by |
|---|---|
| Only callers with AWS IAM credentials | `aws lambda invoke` is IAM-signed; the `sql` action is refused (403) over the public URL |
| One statement, starting with `SELECT`, `WITH`, `SHOW` or `EXPLAIN` | `lib/admin_sql.py` `check_query` |
| No writes, even if a query tries | Database session set to READ ONLY, and every transaction is rolled back |
| Bounded cost | 20 s statement timeout, at most 200 rows returned |

`INSERT`, `UPDATE`, `DELETE`, `DROP` and multiple statements are rejected with an error. Tested in
`backend/acme-perf-etl/tests/test_admin_sql.py`.

### Settings

| Variable | Default | Purpose |
|---|---|---|
| `ETL_FUNCTION` | `coding-workshop-acme-perf-etl-$PARTICIPANT_ID` | Lambda function name |
| `AWS_REGION` | `ap-south-1` | Region of the function |

For browsing tables without SQL, use **Explore the tables** on the live dashboard:
https://d2mx3l9n7j229p.cloudfront.net/dashboard.html