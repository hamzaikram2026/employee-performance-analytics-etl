# Insights notebook (Gold layer, Matplotlib)

Answers the seven ACME business questions from the **Gold** tables, with 15 Matplotlib charts and summary tables.

| File | What it is |
|---|---|
| `performance_insights.ipynb` | The notebook (code + outputs) |
| `performance_insights.html` | Same notebook exported to HTML, with code |
| `performance_report.html` | Code-free report (charts and tables only), published at **https://d2mx3l9n7j229p.cloudfront.net/** |
| `charts/` | The 15 charts as PNG files (for slides and the README) |
| `build_notebook.py` | Generates the notebook from code, so it is reproducible and reviewable in git |

## Questions covered

| # | Question | Gold tables used |
|---|---|---|
| Q1 | Performance ratings and review history | `gold_review_history`, `gold_employee_performance` |
| Q2 | Critical skill gaps | `gold_skill_gaps` |
| Q3 | High potentials ready for promotion | `gold_promotion_candidates`, `gold_employee_profile` |
| Q4 | Training completed | `gold_training_summary` |
| Q5 | Attrition risk | `gold_attrition_risk`, `gold_employee_profile` |
| Q6 | Individual performance vs team / project outcomes | `gold_team_performance` |
| Q7 | Skill distribution | `gold_skill_distribution` |

## Run it

The notebook reads Gold from the data lake, so the pipeline must have run first. From the repo root:

```bash
# Locally (LocalStack): same environment variables as the pipeline (see the main README, §3)
python data/notebooks/build_notebook.py
python -m jupyter nbconvert --to notebook --execute --inplace data/notebooks/performance_insights.ipynb

# HTML versions
python -m jupyter nbconvert --to html data/notebooks/performance_insights.ipynb
python -m jupyter nbconvert --to html --no-input --no-prompt data/notebooks/performance_insights.ipynb --output performance_report
```

**Against real AWS** (reads the cloud Gold layer; the first cell prints "Connected: cloud mode"):

```bash
IS_LOCAL=false DATA_LAKE_BUCKET=coding-workshop-datalake-74cbf3a2 python -m jupyter lab data/notebooks/performance_insights.ipynb
```

## Publish the report

```bash
aws s3 cp data/notebooks/performance_report.html s3://coding-workshop-website-74cbf3a2/index.html --content-type "text/html"
aws cloudfront create-invalidation --distribution-id EGARYEY01HZMY --paths "/*"
```

The report is a snapshot of the Gold layer at export time. For live figures, use the dashboard:
**https://d2mx3l9n7j229p.cloudfront.net/dashboard.html** (`data/dashboard/`).