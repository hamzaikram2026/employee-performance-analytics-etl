"""Run a read-only SQL query on Aurora through the deployed Lambda and print the result.

Aurora is private to the AWS VPC, so psql cannot reach it from the WorkSpace. This tool
sends the query to the Lambda with an IAM-signed invoke; the Lambda runs it read-only.

Usage (real-AWS terminal, after ./bin/setup-participant.sh):
    python data/scripts/aurora_sql.py "SELECT count(*) FROM employees"
"""
import json
import os
import sys

import boto3
from botocore.config import Config


def print_table(columns, rows):
    cells = [[("NULL" if v is None else v) for v in row] for row in rows]
    widths = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(columns)]
    print(" | ".join(c.ljust(w) for c, w in zip(columns, widths)))
    print("-+-".join("-" * w for w in widths))
    for row in cells:
        print(" | ".join(v.ljust(w) for v, w in zip(row, widths)))


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    function = os.getenv("ETL_FUNCTION") or f"coding-workshop-acme-perf-etl-{os.getenv('PARTICIPANT_ID', '')}"
    client = boto3.client("lambda", region_name=os.getenv("AWS_REGION", "ap-south-1"),
                          config=Config(read_timeout=120, retries={"max_attempts": 1}))
    result = client.invoke(FunctionName=function,
                           Payload=json.dumps({"action": "sql", "query": sys.argv[1]}).encode())
    outer = json.loads(result["Payload"].read())
    body = json.loads(outer["body"]) if "body" in outer else outer
    if outer.get("statusCode") != 200:
        print("ERROR:", body.get("error") or body.get("errorMessage") or body)
        return 1
    if body["columns"]:
        print_table(body["columns"], body["rows"])
    print(f"({body['row_count']} rows{', truncated' if body['truncated'] else ''})")
    return 0


if __name__ == "__main__":
    sys.exit(main())