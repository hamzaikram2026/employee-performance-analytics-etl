# Live dashboard

**Public URL:** https://d2mx3l9n7j229p.cloudfront.net/dashboard.html

A single HTML page that shows the Gold layer live. Every load (and every **Refresh** click) calls the
`acme-perf-etl` Lambda, which reads the current Gold tables from S3, so the page always reflects the latest pipeline run.
The header shows the Gold build time and the pipeline **run id**, which matches the run's audit record in `_audit/runs/`.

## What it shows

| Section | Content | Business question |
|---|---|---|
| KPI tiles | Employees, improving, declining, critical skill gaps, ready now (high potential), high attrition risk, no recent learning | Q1–Q5 |
| Performance trend | Employees per trend (improving / stable / declining / single review / no reviews) | Q1 |
| Attrition risk | Low / Medium / High from the explainable risk score | Q5 |
| Promotion readiness | Promotion status, plus agreement with the talent team's labels | Q3 |
| Top critical competencies | Competencies with the most critical gaps (2+ levels below required) | Q2, Q7 |
| Departments | Average rating and % with a critical gap per department | Q1, Q2, Q7 |
| Team rating vs project outcome | Scatter per team, with the correlation | Q6 |
| Find employees | Filter by department, level, trend, risk, promotion status, rating, critical gap, name; sort and paginate | Q1–Q5 |
| Explore the tables | Preview any whitelisted table from **Aurora (live)**, Bronze, Silver or Gold | Lineage across layers |

## How it works

```
Browser ──► CloudFront /dashboard.html            (static page from the website S3 bucket)
        └─► CloudFront /api/acme-perf-etl?action=…  ──► Lambda acme-perf-etl ──► Gold Parquet on S3
                                                                         └──► Aurora (table preview, inside the VPC)
```

The page and the API share one domain, so there are no cross-site requests. Charts are drawn with Chart.js 4.4.1
(cdnjs). Light and dark mode follow the device setting, and the layout works on phones.

API actions used: `dashboard`, `search`, `table`. All are **read-only**: whitelisted filters and tables only, nothing is
written, and email addresses are never returned. See the main README, §4 "Read-only API and live dashboard".

## Publish or update

From the repo root, in the real-AWS terminal (after `./bin/setup-participant.sh`):

```bash
aws s3 cp data/dashboard/dashboard.html s3://coding-workshop-website-74cbf3a2/dashboard.html --content-type "text/html"
aws cloudfront create-invalidation --distribution-id EGARYEY01HZMY --paths "/*"
```

The page needs no rebuild when data changes. Run the pipeline and click **Refresh**.

## Preview against another API

Add `?api=<url>` to point the page at a different endpoint, for example the Lambda Function URL:
`dashboard.html?api=https://<function-url>.lambda-url.ap-south-1.on.aws/`

## Notes

* The first load after a quiet period can take 15–30 s while Aurora or the Lambda wakes up. Click **Refresh** if a
  "waking up" message appears.
* The page has no login. It shows aggregated figures and non-contact employee fields only.