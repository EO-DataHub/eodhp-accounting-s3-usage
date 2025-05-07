import time
from typing import Generator

import boto3


def _run_athena_query(athena, query, database, output_bucket) -> str:
    """Runs an AWS Athena query and returns its execution ID."""
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

        if status == "SUCCEEDED":
            return query_execution_id

        if status not in {"RUNNING", "QUEUED"}:
            raise Exception(f"Athena query {query} failed: {status} {query_status=}")

        time.sleep(1)


def run_single_result_athena_query(query, database, output_bucket) -> float:
    athena = boto3.client("athena")
    query_execution_id = _run_athena_query(athena, query, database, output_bucket)

    result = athena.get_query_results(QueryExecutionId=query_execution_id)

    rows = result["ResultSet"]["Rows"]

    # The first row will be a header, eg [{'VarCharValue': 'total_gb_transferred'}]
    if len(rows) < 2 or len(rows[1]["Data"]) < 1:
        return 0

    value = rows[1]["Data"][0].get("VarCharValue", "0")
    return float(value)


def run_long_result_athena_query(query, database, output_bucket, page_size=100) -> Generator[tuple]:
    athena = boto3.client("athena")
    query_execution_id = _run_athena_query(athena, query, database, output_bucket)

    paginator = athena.get_paginator("get_query_results")
    page_iterator = paginator.paginate(
        QueryExecutionId=query_execution_id, PaginationConfig={"PageSize": page_size}
    )

    first = True

    for page in page_iterator:
        rows = page["ResultSet"]["Rows"]

        for row in rows:
            if first:
                # The first row will be a header, eg
                # {'Data': [{'VarCharValue': 'remoteip'}, {'VarCharValue': 'total_gb_transferred'}]}
                first = False
            else:
                # Each row is of this form:
                #   {'Data': [{'VarCharValue': '-'}, {'VarCharValue': '0.008852269500494003'}]}
                # where 'Data' and 'VarCharValue' are fixed boilerplate.
                #
                # Null values (or '-' in the logs, such as for bytessent with PUTs) come out as
                # '{}'. For example:
                #   {'Data': [{'VarCharValue': '-'}, {}]}
                # We ignore rows like this.
                result_row = tuple(map(lambda d: d.get("VarCharValue"), row["Data"]))
                if all(map(lambda d: d is not None, result_row)):
                    yield result_row
