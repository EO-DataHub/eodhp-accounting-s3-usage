import time

import boto3


def run_athena_query(query, database, output_bucket) -> float:
    athena = boto3.client("athena")

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": f"s3://{output_bucket}/athena-results/"},
    )

    query_execution_id = response["QueryExecutionId"]

    # Wait until query completes
    while True:
        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status["QueryExecution"]["Status"]["State"]

        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(1)

    if status != "SUCCEEDED":
        raise Exception(f"Athena query failed: {status}")

    result = athena.get_query_results(QueryExecutionId=query_execution_id)

    rows = result["ResultSet"]["Rows"]
    if len(rows) < 2 or len(rows[1]["Data"]) < 1:
        return 0
    value = rows[1]["Data"][0].get("VarCharValue", "0")
    return float(value)
