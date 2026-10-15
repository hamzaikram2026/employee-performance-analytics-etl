# Evidence

Captured output that proves the pipeline works locally and in real AWS. Every file is the raw output of a command,
saved unchanged. Secrets (passwords, participant code, credentials) are never captured.

## Testing and performance (local)

| File | What it proves |
|---|---|
| `test_results.txt` | Full test suite with per-module coverage (~97%), against the guide's coverage targets |
| `performance_results.txt` | Benchmark at 1× / 5× / 10× volume: time, throughput, peak memory, 120 s SLA met |
| `memory_by_stage.txt` | Memory used by Bronze, Silver and Gold, which sized the Lambda at 512 MB |

## Pipeline runs (local, LocalStack)

| File | What it proves |
|---|---|
| `pipeline_run.log` | A full Bronze → Silver → Gold run: structured JSON logs, rows read / written / quarantined per step |
| `lake_layout.txt` | The resulting data lake on S3: landing, bronze, silver, quarantine, gold and `_audit` paths |
| `lambda_*` | The service deployed and invoked as a Lambda on LocalStack |

## Real AWS deployment

| File | What it proves |
|---|---|
| `deploy_tail.txt` | `./bin/deploy-backend.sh aws` finished successfully (website URL, buckets) |
| `aws_lambda_function.txt` | Deployed function configuration: runtime, handler, 512 MB memory, timeout, VPC |
| `aws_aurora_cluster.txt` | Aurora PostgreSQL cluster: engine version, status, private (not publicly accessible) |
| `aws_lambda_full_refresh.json` | Full-refresh run in AWS: all 25 steps SUCCESS, row counts identical to local |
| `aws_lambda_reports.txt` | CloudWatch REPORT lines: duration (~80 s) and peak memory (210 MB) |
| `aws_end_to_end_run.json` | Live end-to-end run `8d92a2cb47be`: SUCCESS, 25 steps |
| `aws_end_to_end_layers.txt` | Bronze, Silver, Gold and `_audit` files written by that run, with timestamps (02:03:40 → 02:04:30) |

## How the end-to-end proof fits together

1. `aws_end_to_end_run.json`: the run id `8d92a2cb47be` and its 25 successful steps
2. `aws_end_to_end_layers.txt`: each layer written in order by that run, and `_audit/runs/…/8d92a2cb47be.json`
3. Aurora query (`data/scripts/aurora_sql.py`): 2,000 employees, 4,000 reviews, matching the pipeline's read counts
4. The live dashboard (https://d2mx3l9n7j229p.cloudfront.net/dashboard.html) shows the same run id in its header

## Regenerate

```bash
# Tests (LocalStack terminal)
cd backend/acme-perf-etl
python -m pytest --cov=lib --cov-report=term-missing > ../../data/evidence/test_results.txt 2>&1
python -m pytest -m performance -s > ../../data/evidence/performance_results.txt 2>&1

# End-to-end AWS run (real-AWS terminal, repo root)
aws lambda invoke --function-name coding-workshop-acme-perf-etl-74cbf3a2 --cli-read-timeout 300 \
  --cli-binary-format raw-in-base64-out --payload '{"stage":"all"}' data/evidence/aws_end_to_end_run.json
```