"""Schema contracts for every Silver dataset: the explicit data-quality rules per column."""
from __future__ import annotations

from .validation import ColumnSpec as Col
from .validation import TableContract

JOB_LEVELS = frozenset({"IC1", "IC2", "IC3", "Senior IC", "Manager", "Senior Manager", "Director"})
LEVELS_3 = frozenset({"Low", "Medium", "High"})
EVENT_TYPES = frozenset({
    "enrollment", "module_started", "module_completed", "quiz_attempted",
    "quiz_passed", "course_completed", "certification_earned",
})

CONTRACTS: dict[str, TableContract] = {
    "teams": TableContract(
        "teams",
        (Col("team_id", "int"), Col("name", "str"), Col("department", "str")),
        primary_key=("team_id",),
    ),
    "competencies": TableContract(
        "competencies",
        (
            Col("competency_id", "int"),
            Col("name", "str"),
            Col("category", "str", allowed=frozenset({"Technical", "Leadership", "Domain"})),
        ),
        primary_key=("competency_id",),
    ),
    "employees": TableContract(
        "employees",
        (
            Col("employee_id", "int"),
            Col("first_name", "str"),
            Col("last_name", "str"),
            Col("email", "str"),
            Col("team_id", "int"),
            Col("job_level", "str", allowed=JOB_LEVELS),
            Col("hire_date", "date"),
        ),
        primary_key=("employee_id",),
    ),
    "projects": TableContract(
        "projects",
        (
            Col("project_id", "int"),
            Col("name", "str"),
            Col("team_id", "int"),
            Col("outcome_status", "str",
                allowed=frozenset({"on-time", "delayed", "at-risk", "cancelled"})),
            Col("outcome_score", "float", min_value=0, max_value=100),
        ),
        primary_key=("project_id",),
    ),
    "performance_reviews": TableContract(
        "performance_reviews",
        (
            Col("review_id", "int"),
            Col("employee_id", "int"),
            Col("review_period", "str"),
            Col("rating", "int", min_value=1, max_value=5),
            Col("manager_comment", "str", nullable=True),
            Col("submitted_date", "date"),
        ),
        primary_key=("review_id",),
    ),
    "competency_assessments": TableContract(
        "competency_assessments",
        (
            Col("assessment_id", "int"),
            Col("employee_id", "int"),
            Col("competency_id", "int"),
            Col("proficiency_level", "int", min_value=1, max_value=5),
            Col("required_level", "int", min_value=1, max_value=5),
            Col("assessed_date", "date"),
        ),
        primary_key=("assessment_id",),
    ),
    "talent_review": TableContract(
        "talent_review",
        (
            Col("employee_id", "int"),
            Col("review_cycle", "str"),
            Col("potential_rating", "str", allowed=LEVELS_3),
            Col("promotion_readiness", "str",
                allowed=frozenset({"Ready Now", "1-2 Years", "Not Ready"})),
            Col("attrition_risk", "str", allowed=LEVELS_3),
        ),
        primary_key=("employee_id",),
    ),
    "learning_activity": TableContract(
        "learning_activity",
        (
            Col("event_id", "str"),
            Col("employee_id", "int"),
            Col("event_date", "datetime"),
            Col("event_type", "str", allowed=EVENT_TYPES),
            Col("course_name", "str"),
            Col("text", "str", nullable=True),
            Col("score", "float", nullable=True, min_value=0, max_value=100),
        ),
        primary_key=("event_id",),
        aliases={"notes": "text"},  # schema drift seen in 39 source records
    ),
}