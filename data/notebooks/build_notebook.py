"""Generates data/notebooks/performance_insights.ipynb from the cells below.

Usage (from the repo root):
    python data/notebooks/build_notebook.py
    python -m jupyter nbconvert --to notebook --execute --inplace data/notebooks/performance_insights.ipynb
"""
from pathlib import Path

import nbformat as nbf

cells = []


def md(text):
    cells.append(nbf.v4.new_markdown_cell(text.strip()))


def code(text):
    cells.append(nbf.v4.new_code_cell(text.strip()))


# --------------------------------------------------------------------- intro
md('''
# ACME Inc. — Performance & Talent Insights

**Audience:** HR leadership and technical reviewers.

This notebook reads the **Gold layer** of the ACME data lake and answers the seven business questions.
Every number comes from curated tables produced by the `acme-perf-etl` pipeline:

`PostgreSQL (HR tables) + S3 (learning events)  →  Bronze (raw)  →  Silver (validated)  →  Gold (business tables)`

**How to run:** with the virtual environment active and the pipeline already run
(`python backend/acme-perf-etl/function.py all`), open this notebook and choose *Run All*.
Connection settings come from environment variables (`IS_LOCAL`, `POSTGRES_*`, `DATA_LAKE_BUCKET`, `AWS_ENDPOINT_URL`),
so the same notebook works locally (LocalStack) or against AWS.

**Contents:** 0. Setup · 1. At a glance · 2. Data quality · Q1–Q7 · Limitations & recommendations
''')

# --------------------------------------------------------------------- setup
md('## 0. Setup')
code('''
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / "backend" / "acme-perf-etl").exists())
sys.path.insert(0, str(ROOT / "backend" / "acme-perf-etl"))

from lib.bronze import partition_of
from lib.config import Config
from lib.storage import DataLake

cfg = Config.from_env()
lake = DataLake(cfg)
CHARTS = ROOT / "data" / "notebooks" / "charts"
CHARTS.mkdir(parents=True, exist_ok=True)

plt.rcParams.update({"figure.figsize": (9, 4.5), "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.alpha": 0.3})
pd.set_option("display.max_columns", 20)
pd.set_option("display.width", 160)


def gold(name):
    """Load one Gold table from the data lake (dropping technical lineage columns)."""
    return lake.read_parquet(f"gold/{name}/data.parquet").drop(columns=["_gold_built_at", "_run_id"], errors="ignore")


def finish(fig, name):
    """Tidy, save the chart as PNG for the presentation, and display it."""
    fig.tight_layout()
    fig.savefig(CHARTS / f"{name}.png", dpi=120)
    plt.show()


def heatmap(df, title, name, fmt="{:.0f}"):
    fig, ax = plt.subplots(figsize=(1.7 * len(df.columns) + 3, 0.5 * len(df) + 2))
    im = ax.imshow(df.values.astype(float), cmap="Blues", aspect="auto")
    ax.set_xticks(range(len(df.columns)), df.columns, rotation=25, ha="right")
    ax.set_yticks(range(len(df.index)), df.index)
    for i in range(df.shape[0]):
        for j in range(df.shape[1]):
            ax.text(j, i, fmt.format(df.values[i, j]), ha="center", va="center", fontsize=9)
    ax.set_title(title)
    ax.grid(False)
    fig.colorbar(im, ax=ax, shrink=0.8)
    finish(fig, name)


TABLES = ["gold_review_history", "gold_employee_performance", "gold_skill_gaps", "gold_skill_distribution",
          "gold_training_summary", "gold_attrition_risk", "gold_promotion_candidates",
          "gold_team_performance", "gold_employee_profile"]
G = {t: gold(t) for t in TABLES}
print("Connected:", cfg.describe()["mode"], "mode, bucket", cfg.bucket)
pd.DataFrame({"gold_table": TABLES, "rows": [len(G[t]) for t in TABLES]})
''')

# ---------------------------------------------------------------- at a glance
md('## 1. At a glance')
code('''
profile = G["gold_employee_profile"]
training = G["gold_training_summary"]
EVENT_COLS = ["enrollments", "modules_started", "modules_completed", "quizzes_attempted",
              "quizzes_passed", "courses_completed", "certifications_earned"]

kpis = {
    "Employees": len(profile),
    "Improving performance": int((profile["performance_trend"] == "improving").sum()),
    "Declining performance": int((profile["performance_trend"] == "declining").sum()),
    "With at least one critical skill gap": int((profile["critical_gap_count"] > 0).sum()),
    "Ready for promotion now (high potential)": int((profile["promotion_status"] == "Ready now - high potential").sum()),
    "High attrition risk (data-driven)": int((profile["derived_attrition_risk"] == "High").sum()),
    "No recorded learning activity": int((training[EVENT_COLS].sum(axis=1) == 0).sum()),
}
pd.Series(kpis, name="employees").to_frame()
''')

# --------------------------------------------------------------- data quality
md('''
## 2. Data quality: what was rejected and why

Before trusting the answers, it matters what the pipeline **removed**. Silver validates every row against a schema
contract and business rules; failures are not deleted silently but moved to a **quarantine** zone with a reason.

Findings from investigating the rejected data:
* **Events before hire date** were up to ~3 years early (median ~320 days) and include certifications,
  so they were judged invalid rather than "pre-boarding" activity.
* **Invalid event dates** were free text (`"yesterday"`, `"N/A"`); dates were not guessed, to avoid distorting timelines.
* **Schema drift:** 39 events used a `notes` field instead of `text`; these were mapped, not rejected.
''')
code('''
def latest_quarantine(name):
    parts = sorted({partition_of(k) for k in lake.list_keys(f"quarantine/{name}/") if k.endswith(".parquet")})
    return lake.read_parquet_prefix(f"quarantine/{name}/{parts[-1]}/") if parts else pd.DataFrame()

summary, reasons = [], []
for name in ["teams", "competencies", "employees", "projects", "performance_reviews",
             "competency_assessments", "talent_review", "learning_activity"]:
    kept = len(lake.read_parquet_prefix(f"silver/{name}/"))
    q = latest_quarantine(name)
    summary.append({"dataset": name, "kept_in_silver": kept, "quarantined": len(q),
                    "quarantined_pct": round(100 * len(q) / (kept + len(q)), 1)})
    if len(q):
        for reason, n in q["_reject_reason"].value_counts().items():
            reasons.append({"dataset": name, "reason": reason, "rows": int(n)})

reasons = pd.DataFrame(reasons).sort_values("rows")
fig, ax = plt.subplots(figsize=(9, 4))
ax.barh(reasons["dataset"] + ": " + reasons["reason"], reasons["rows"], color="#c62828")
ax.set(title="Quarantined rows by reason", xlabel="Rows")
finish(fig, "dq_quarantine_reasons")
pd.DataFrame(summary)
''')

# ------------------------------------------------------------------------ Q1
md('''
## Q1. What are the performance ratings and review history for each employee?

**Source:** `gold_review_history` (one row per employee per review period) and `gold_employee_performance` (one row per employee).
''')
code('''
history = G["gold_review_history"]
perf = G["gold_employee_performance"]

dist = pd.crosstab(history["rating"], history["review_period"])
fig, ax = plt.subplots()
dist.plot.bar(ax=ax, rot=0)
ax.set(title="Q1 · Rating distribution by review period", xlabel="Rating (1 = low, 5 = high)", ylabel="Employees")
finish(fig, "q1_rating_distribution")

order = ["improving", "stable", "declining", "single review", "no reviews"]
trend = perf["performance_trend"].value_counts().reindex(order, fill_value=0)
fig, ax = plt.subplots()
ax.bar(trend.index, trend.values, color=["#2e7d32", "#90a4ae", "#c62828", "#b0bec5", "#cfd8dc"])
for i, v in enumerate(trend.values):
    ax.text(i, v, f"{v:,}", ha="center", va="bottom")
ax.set(title="Q1 · Performance trend between the two review periods", ylabel="Employees")
finish(fig, "q1_performance_trend")
''')
code('''
# Review history for one example employee whose performance declined
example = perf.loc[perf["performance_trend"] == "declining", "employee_id"].iloc[0]
history.loc[history["employee_id"] == example,
            ["employee_name", "review_period", "rating", "previous_rating", "rating_change",
             "comment_sentiment", "manager_comment"]]
''')
code('''
# Per-employee summary (first 10 alphabetically); the full table is in gold_employee_performance
perf[["employee_name", "department", "latest_period", "latest_rating", "avg_rating",
      "rating_change", "performance_trend"]].sort_values("employee_name").head(10)
''')
md('''
**Findings**
* Most ratings sit at 3–4; about **40% of employees improved** and about **10% declined** between 2024-H2 and 2025-H1.
* **105 employees have no valid review**: their reviews were dated before their hire date and were quarantined.
  They need a fresh review, not to be treated as low performers.
''')

# ------------------------------------------------------------------------ Q2
md('''
## Q2. Which employees have critical skill gaps in key competencies?

**Rule:** a gap is *critical* when proficiency is **2 or more levels below** the level the role requires.
**Source:** `gold_skill_gaps` (one row per employee per assessed competency).
''')
code('''
gaps = G["gold_skill_gaps"]
critical = gaps[gaps["is_critical_gap"]]

top = critical["competency_name"].value_counts().head(10).sort_values()
fig, ax = plt.subplots()
ax.barh(top.index, top.values, color="#ef6c00")
ax.set(title="Q2 · Competencies with the most critical gaps", xlabel="Employees with a critical gap")
finish(fig, "q2_top_critical_competencies")

by_dept = (profile.assign(has_gap=profile["critical_gap_count"] > 0)
           .groupby("department")["has_gap"].mean().mul(100).sort_values())
fig, ax = plt.subplots()
ax.barh(by_dept.index, by_dept.values, color="#ef6c00")
ax.set(title="Q2 · Share of employees with a critical gap, by department", xlabel="% of employees")
finish(fig, "q2_critical_gap_by_department")

print("Employees with at least one critical gap:", critical["employee_id"].nunique(), "of", len(profile))
print(gaps["gap_severity"].value_counts().to_string())
''')
code('''
# Largest individual gaps
critical.sort_values("gap", ascending=False)[
    ["employee_name", "department", "job_level", "competency_name", "competency_category",
     "proficiency_level", "required_level", "gap"]].head(15)
''')
md('''
**Findings**
* **299 employees (15%)** are at least two levels below the requirement in one or more competencies.
* The bar charts show *where* to focus training budget: the most common critical competencies and the most affected departments.
* Caveat: each employee was assessed on only **two** competencies, so "no gap recorded" does not mean "no gap".
''')

# ------------------------------------------------------------------------ Q3
md('''
## Q3. Who are the high-potential employees ready for promotion?

**Data-driven readiness rule:** latest rating ≥ 4, average rating ≥ 3.5, no critical skill gaps, and performance not declining.
**High potential** comes from the talent review. **Source:** `gold_promotion_candidates`.
''')
code('''
promo = G["gold_promotion_candidates"]
order = ["Ready now - high potential", "Ready now", "High potential - develop", "Not yet"]
status = promo["promotion_status"].value_counts().reindex(order, fill_value=0)
fig, ax = plt.subplots()
ax.bar(status.index, status.values, color=["#1b5e20", "#66bb6a", "#42a5f5", "#cfd8dc"])
for i, v in enumerate(status.values):
    ax.text(i, v, f"{v:,}", ha="center", va="bottom")
ax.set(title="Q3 · Promotion readiness", ylabel="Employees")
finish(fig, "q3_promotion_status")

agreement = pd.crosstab(
    promo["derived_promotion_ready"].map({True: "Data: ready", False: "Data: not ready"}),
    promo["talent_promotion_readiness"])
heatmap(agreement, "Q3 · Data-driven readiness vs talent-review label", "q3_readiness_vs_label")
print(f"Talent 'Ready Now' label agrees with the data for {promo['readiness_label_agrees'].mean():.0%} of employees")
print("Contradictory labels (Low potential but 'Ready Now'):", int(promo["is_label_inconsistent"].sum()))
''')
code('''
# Top promotion candidates: ready now and high potential
promo[promo["promotion_status"] == "Ready now - high potential"].sort_values(
    ["avg_rating", "certifications_earned"], ascending=False)[
    ["employee_name", "department", "job_level", "latest_rating", "avg_rating",
     "certifications_earned", "talent_promotion_readiness"]].head(15)
''')
md('''
**Findings**
* **212 employees** are both high potential and ready now based on their data.
* The talent team's "Ready Now" label matches the data only about **half the time**, and some employees are labelled
  "Low potential" yet "Ready Now". **Recommendation:** run a calibration session before using these labels for succession planning.
''')

# ------------------------------------------------------------------------ Q4
md('''
## Q4. What training and development activities has each employee completed?

**Source:** `gold_training_summary` (one row per employee), built from the S3 learning-activity events.
''')
code('''
totals = training[EVENT_COLS].sum().sort_values()
fig, ax = plt.subplots()
ax.barh(totals.index, totals.values, color="#5e35b1")
ax.set(title="Q4 · Learning activity across the organisation", xlabel="Events")
finish(fig, "q4_activity_totals")

fig, ax = plt.subplots()
ax.hist(training["courses_completed"], bins=range(0, int(training["courses_completed"].max()) + 2),
        color="#5e35b1", edgecolor="white")
ax.set(title="Q4 · Courses completed per employee", xlabel="Courses completed", ylabel="Employees")
finish(fig, "q4_courses_per_employee")

no_activity = int((training[EVENT_COLS].sum(axis=1) == 0).sum())
print("Employees with no recorded learning activity:", no_activity)
training.groupby("department")[["courses_completed", "certifications_earned", "events_last_180d"]].mean().round(2)
''')
code('''
# Per-employee training record (first 10); the full table is in gold_training_summary
training[["employee_name", "department", "enrollments", "courses_completed", "certifications_earned",
          "quizzes_passed", "avg_quiz_score", "last_activity_date"]].sort_values("employee_name").head(10)
''')
md('''
**Findings**
* The pipeline gives a complete per-employee training record: enrolments, completions, certifications, and quiz results.
* A group of employees has **no recorded learning activity**; they are a natural target for development plans.
* Totals are **conservative**: events dated before an employee's hire date were quarantined.
''')

# ------------------------------------------------------------------------ Q5
md('''
## Q5. Which employees are at risk of attrition based on performance trends?

**Explainable risk score** (points): rating dropped 2+ levels **+3**, dropped 1 level **+2**, latest rating ≤ 2 **+2**,
negative manager comment **+1**, no learning activity in the last 180 days **+1**.
Score ≥ 4 = **High**, 2–3 = **Medium**, otherwise **Low**. **Source:** `gold_attrition_risk`.
''')
code('''
risk = G["gold_attrition_risk"]
levels = ["Low", "Medium", "High"]
counts = risk["derived_attrition_risk"].value_counts().reindex(levels, fill_value=0)
fig, ax = plt.subplots()
ax.bar(counts.index, counts.values, color=["#90a4ae", "#ffb300", "#c62828"])
for i, v in enumerate(counts.values):
    ax.text(i, v, f"{v:,}", ha="center", va="bottom")
ax.set(title="Q5 · Attrition risk (data-driven score)", ylabel="Employees")
finish(fig, "q5_attrition_risk")

by_trend = pd.crosstab(risk["performance_trend"], risk["derived_attrition_risk"], normalize="index").mul(100)
heatmap(by_trend.reindex(columns=levels), "Q5 · Attrition risk by performance trend (% of row)",
        "q5_risk_by_trend", fmt="{:.0f}%")

vs_label = pd.crosstab(risk["derived_attrition_risk"], risk["talent_attrition_risk"]).reindex(index=levels, columns=levels)
heatmap(vs_label, "Q5 · Data-driven risk vs talent-review label", "q5_risk_vs_label")
print(f"Exact agreement with the talent label: {risk['attrition_label_agrees'].mean():.0%}")
''')
code('''
# Highest-risk employees
risk[["employee_name", "department", "latest_rating", "rating_change", "latest_sentiment",
      "events_last_180d", "attrition_risk_score", "talent_attrition_risk"]].head(15)
''')
md('''
**Findings**
* **173 employees** score High risk; risk is concentrated among employees whose performance **declined**.
* The score is deliberately transparent: managers can see exactly *why* someone is flagged.
* It is stricter than the talent label, and the two agree on about half of employees. They are best used **together**:
  the talent label captures context the data cannot, while the score is consistent and auditable.
''')

# ------------------------------------------------------------------------ Q6
md('''
## Q6. How does employee performance correlate with team and project outcomes?

**Source:** `gold_team_performance` (one row per team). Projects are linked to **teams**, not individuals.
''')
code('''
teams = G["gold_team_performance"]
with_projects = teams[teams["has_projects"]]
r = with_projects["avg_latest_rating"].corr(with_projects["avg_outcome_score"])

fig, ax = plt.subplots()
ax.scatter(with_projects["avg_latest_rating"], with_projects["avg_outcome_score"], s=60, color="#00838f")
slope, intercept = np.polyfit(with_projects["avg_latest_rating"], with_projects["avg_outcome_score"], 1)
xs = np.linspace(with_projects["avg_latest_rating"].min(), with_projects["avg_latest_rating"].max(), 50)
ax.plot(xs, slope * xs + intercept, "--", color="#555")
ax.set(title=f"Q6 · Team rating vs project outcome  (r = {r:.2f}, n = {len(with_projects)} teams)",
       xlabel="Average latest employee rating", ylabel="Average project outcome score")
finish(fig, "q6_rating_vs_outcome")

print(f"Teams with projects: {len(with_projects)} of {len(teams)}  |  correlation r = {r:.2f}")
with_projects.assign(
    delivery=np.where(with_projects["pct_projects_on_time"] >= 0.5, "Mostly on-time", "Mostly delayed")
).groupby("delivery")[["avg_latest_rating", "pct_high_performers", "pct_high_attrition_risk"]].mean().round(3)
''')
md('''
**Findings**
* There is a **weak positive** relationship (r ≈ 0.17): teams with higher ratings tend to score slightly better on projects.
* Only **27 of 100 teams** have projects (33 projects in total), so this is **not conclusive**.
* **Recommendation:** record which employees work on each project, so outcomes can be linked to individuals, not just teams.
''')

# ------------------------------------------------------------------------ Q7
md('''
## Q7. What is the distribution of skills across the organisation?

**Source:** `gold_skill_distribution` (assessments by department × competency × proficiency level) and `gold_skill_gaps`.
''')
code('''
dist = G["gold_skill_distribution"]
by_dept = dist.pivot_table(index="department", columns="competency_category", values="employee_count",
                           aggfunc="sum", fill_value=0)
heatmap(by_dept, "Q7 · Assessments by department and competency category", "q7_skills_by_department")

levels = dist.pivot_table(index="proficiency_level", columns="competency_category", values="employee_count",
                          aggfunc="sum", fill_value=0)
fig, ax = plt.subplots()
levels.plot.bar(ax=ax, rot=0)
ax.set(title="Q7 · Proficiency levels by competency category", xlabel="Proficiency level (1-5)", ylabel="Assessments")
finish(fig, "q7_proficiency_by_category")

strength = gaps.groupby("competency_name")["proficiency_level"].mean().sort_values()
pd.DataFrame({"weakest competencies (avg proficiency)": strength.head(5).round(2).index,
              "weakest avg": strength.head(5).round(2).values,
              "strongest competencies (avg proficiency)": strength.tail(5).round(2).index[::-1],
              "strongest avg": strength.tail(5).round(2).values[::-1]})
''')
md('''
**Findings**
* **Domain** competencies are assessed most often, followed by **Technical** and **Leadership**.
* The heatmap shows which departments are assessed on which skill families, and the proficiency chart shows how skills are spread
  across levels. The table lists the organisation's weakest and strongest competencies.
* Counts are **assessments**, not unique people: each employee has two assessments.
''')

# --------------------------------------------------------------- limitations
md('''
## Limitations, assumptions and recommendations

**Data limitations**
* The business problem mentions **development plans and employee goals**, but the dataset contains neither.
  A `development_plans` table (employee, goal, target date, status) would be needed to track them.
* Only **two review periods** exist, so a "trend" is a single change between them.
* Each employee has only **two competency assessments**.
* Projects are linked to **teams**, not employees, which limits the Q6 analysis.

**Assumptions (documented in code and adjustable)**
* Critical gap = 2+ levels below required; recent training = last 180 days relative to the latest event.
* Attrition and promotion rules use simple, explainable thresholds; manager-comment sentiment uses keywords.

**Recommendations for ACME**
1. Use the critical-gap list (Q2) to target training budget at the most affected competencies and departments.
2. Hold a calibration session on talent labels: they agree with performance data only about half the time (Q3, Q5).
3. Re-review the 105 employees whose reviews predate their hire date (Q1).
4. Start capturing development plans, goals, and employee-level project assignments.
''')

nb = nbf.v4.new_notebook()
nb["cells"] = cells
nb["metadata"]["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
out = Path(__file__).resolve().parent / "performance_insights.ipynb"
nbf.write(nb, out)
print(f"Wrote {out} with {len(cells)} cells")