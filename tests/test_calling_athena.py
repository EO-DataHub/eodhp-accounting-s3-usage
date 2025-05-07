from unittest import mock

from accounting_s3_usage.sampler.athena_utils import (
    run_long_result_athena_query,
    run_single_result_athena_query,
)


def test_running_single_result_athena_query_produces_correct_value():
    with (
        mock.patch("accounting_s3_usage.sampler.athena_utils.boto3") as botomock,
        mock.patch("accounting_s3_usage.sampler.athena_utils.time"),
    ):
        athenamock = botomock.client("athena")
        athenamock.start_query_execution.return_value = {"QueryExecutionId": 123}
        athenamock.get_query_execution.side_effect = [
            {"QueryExecution": {"Status": {"State": "QUEUED"}}},
            {"QueryExecution": {"Status": {"State": "RUNNING"}}},
            {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
        ]

        # This is a real-life API result from Athena.
        athenamock.get_query_results.return_value = {
            "UpdateCount": 0,
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "total_api_calls"}]},
                    {"Data": [{"VarCharValue": "166"}]},
                ],
                "ResultSetMetadata": {
                    "ColumnInfo": [
                        {
                            "CatalogName": "hive",
                            "SchemaName": "",
                            "TableName": "",
                            "Name": "total_api_calls",
                            "Label": "total_api_calls",
                            "Type": "bigint",
                            "Precision": 19,
                            "Scale": 0,
                            "Nullable": "UNKNOWN",
                            "CaseSensitive": False,
                        }
                    ]
                },
            },
            "ResponseMetadata": {
                "RequestId": "906106d7-ea38-4429-9551-7d2835eebc2f",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "date": "Wed, 07 May 2025 09:45:17 GMT",
                    "content-type": "application/x-amz-json-1.1",
                    "content-length": "379",
                    "connection": "keep-alive",
                    "x-amzn-requestid": "906106d7-ea38-4429-9551-7d2835eebc2f",
                },
                "RetryAttempts": 0,
            },
        }

        result = run_single_result_athena_query("test query", "ATHENA_DB", "ATHENA_OUTPUT_BUCKET")
        assert result == 166


def test_running_single_result_athena_query_with_no_matching_rows_produces_correct_value():
    with (
        mock.patch("accounting_s3_usage.sampler.athena_utils.boto3") as botomock,
        mock.patch("accounting_s3_usage.sampler.athena_utils.time"),
    ):
        athenamock = botomock.client("athena")
        athenamock.start_query_execution.return_value = {"QueryExecutionId": 123}
        athenamock.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }

        # This is a real-life API result from Athena.
        athenamock.get_query_results.return_value = {
            "UpdateCount": 0,
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "total_api_calls"}]},
                    {"Data": [{"VarCharValue": "0"}]},
                ],
                "ResultSetMetadata": {
                    "ColumnInfo": [
                        {
                            "CatalogName": "hive",
                            "SchemaName": "",
                            "TableName": "",
                            "Name": "total_api_calls",
                            "Label": "total_api_calls",
                            "Type": "bigint",
                            "Precision": 19,
                            "Scale": 0,
                            "Nullable": "UNKNOWN",
                            "CaseSensitive": False,
                        }
                    ]
                },
            },
            "ResponseMetadata": {
                "RequestId": "ea4ebbd1-58bb-4cd8-959f-e89479697bf3",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "date": "Wed, 07 May 2025 09:55:42 GMT",
                    "content-type": "application/x-amz-json-1.1",
                    "content-length": "377",
                    "connection": "keep-alive",
                    "x-amzn-requestid": "ea4ebbd1-58bb-4cd8-959f-e89479697bf3",
                },
                "RetryAttempts": 0,
            },
        }

        result = run_single_result_athena_query("test query", "ATHENA_DB", "ATHENA_OUTPUT_BUCKET")
        assert result == 0.0


def test_running_long_result_athena_query_with_multiple_pages_produces_correct_values():
    with (
        mock.patch("accounting_s3_usage.sampler.athena_utils.boto3") as botomock,
        mock.patch("accounting_s3_usage.sampler.athena_utils.time"),
    ):
        athenamock = botomock.client("athena")
        athenamock.start_query_execution.return_value = {"QueryExecutionId": 123}
        athenamock.get_query_execution.side_effect = [
            {"QueryExecution": {"Status": {"State": "QUEUED"}}},
            {"QueryExecution": {"Status": {"State": "RUNNING"}}},
            {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
        ]

        # These are real-life API result from Athena with a page size of 1.
        athenamock.get_paginator("get_query_results").paginate.return_value = [
            {
                "UpdateCount": 0,
                "ResultSet": {
                    "Rows": [
                        {
                            "Data": [
                                {"VarCharValue": "remoteip"},
                                {"VarCharValue": "total_gb_transferred"},
                            ]
                        }
                    ],
                    "ResultSetMetadata": {
                        "ColumnInfo": [
                            {
                                "CatalogName": "hive",
                                "SchemaName": "",
                                "TableName": "",
                                "Name": "remoteip",
                                "Label": "remoteip",
                                "Type": "varchar",
                                "Precision": 2147483647,
                                "Scale": 0,
                                "Nullable": "UNKNOWN",
                                "CaseSensitive": True,
                            },
                            {
                                "CatalogName": "hive",
                                "SchemaName": "",
                                "TableName": "",
                                "Name": "total_gb_transferred",
                                "Label": "total_gb_transferred",
                                "Type": "double",
                                "Precision": 17,
                                "Scale": 0,
                                "Nullable": "UNKNOWN",
                                "CaseSensitive": False,
                            },
                        ]
                    },
                },
                "NextToken": "AS/95lHeUo3ZoGRNv2e0WKmm5aFq2y+chQcaiWtIT3QJtJH3rQEMWWE6GUA931wcmNM+RkSfn8MmrLRhDTk+avHM9/K/mluj7A==",  # noqa
                "ResponseMetadata": {
                    "RequestId": "bdfff102-771f-402a-96e4-1772d487175d",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Wed, 07 May 2025 09:49:48 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "686",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "bdfff102-771f-402a-96e4-1772d487175d",
                    },
                    "RetryAttempts": 0,
                },
            },
            {
                "UpdateCount": 0,
                "ResultSet": {
                    "Rows": [
                        {"Data": [{"VarCharValue": "-"}, {"VarCharValue": "0.008852269500494003"}]}
                    ],
                    "ResultSetMetadata": {
                        "ColumnInfo": [
                            {
                                "CatalogName": "hive",
                                "SchemaName": "",
                                "TableName": "",
                                "Name": "remoteip",
                                "Label": "remoteip",
                                "Type": "varchar",
                                "Precision": 2147483647,
                                "Scale": 0,
                                "Nullable": "UNKNOWN",
                                "CaseSensitive": True,
                            },
                            {
                                "CatalogName": "hive",
                                "SchemaName": "",
                                "TableName": "",
                                "Name": "total_gb_transferred",
                                "Label": "total_gb_transferred",
                                "Type": "double",
                                "Precision": 17,
                                "Scale": 0,
                                "Nullable": "UNKNOWN",
                                "CaseSensitive": False,
                            },
                        ]
                    },
                },
                "NextToken": "AS/95lHeUo3Z1mobYWkLkO3zxurrJdRA2z4MvOlzPzDuIp8yUcvvIQnRC0Y9ICZH6RJZOPFf+4mFtFHEzkoANFiXz7dsTL43Dg==",  # noqa
                "ResponseMetadata": {
                    "RequestId": "9759ed8e-40b8-49a8-939d-7bbb83636c90",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Wed, 07 May 2025 09:49:48 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "679",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "9759ed8e-40b8-49a8-939d-7bbb83636c90",
                    },
                    "RetryAttempts": 0,
                },
            },
            {
                "UpdateCount": 0,
                "ResultSet": {
                    "Rows": [
                        {
                            "Data": [
                                {"VarCharValue": "18.175.49.181"},
                                {"VarCharValue": "0.02212107926607132"},
                            ]
                        }
                    ],
                    "ResultSetMetadata": {
                        "ColumnInfo": [
                            {
                                "CatalogName": "hive",
                                "SchemaName": "",
                                "TableName": "",
                                "Name": "remoteip",
                                "Label": "remoteip",
                                "Type": "varchar",
                                "Precision": 2147483647,
                                "Scale": 0,
                                "Nullable": "UNKNOWN",
                                "CaseSensitive": True,
                            },
                            {
                                "CatalogName": "hive",
                                "SchemaName": "",
                                "TableName": "",
                                "Name": "total_gb_transferred",
                                "Label": "total_gb_transferred",
                                "Type": "double",
                                "Precision": 17,
                                "Scale": 0,
                                "Nullable": "UNKNOWN",
                                "CaseSensitive": False,
                            },
                        ]
                    },
                },
                "NextToken": "AS/95lHeUo3Z09TMJQSraAXTrWP2sCaTVFj6KVXyCjrexO4axWiyGPeP1/nVTwaXww6gtiiOnSE6VCNBv4JChmXJ35oi5mIDuA==",  # noqa
                "ResponseMetadata": {
                    "RequestId": "e72819fd-f323-4919-b1d3-9cf217173c18",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Wed, 07 May 2025 09:49:48 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "690",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "e72819fd-f323-4919-b1d3-9cf217173c18",
                    },
                    "RetryAttempts": 0,
                },
            },
            {
                "UpdateCount": 0,
                "ResultSet": {
                    "Rows": [],
                    "ResultSetMetadata": {
                        "ColumnInfo": [
                            {
                                "CatalogName": "hive",
                                "SchemaName": "",
                                "TableName": "",
                                "Name": "remoteip",
                                "Label": "remoteip",
                                "Type": "varchar",
                                "Precision": 2147483647,
                                "Scale": 0,
                                "Nullable": "UNKNOWN",
                                "CaseSensitive": True,
                            },
                            {
                                "CatalogName": "hive",
                                "SchemaName": "",
                                "TableName": "",
                                "Name": "total_gb_transferred",
                                "Label": "total_gb_transferred",
                                "Type": "double",
                                "Precision": 17,
                                "Scale": 0,
                                "Nullable": "UNKNOWN",
                                "CaseSensitive": False,
                            },
                        ]
                    },
                },
                "ResponseMetadata": {
                    "RequestId": "f6bb4759-5d03-411c-8a11-aa6f1a984c9c",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Wed, 07 May 2025 09:49:48 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "493",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "f6bb4759-5d03-411c-8a11-aa6f1a984c9c",
                    },
                    "RetryAttempts": 0,
                },
            },
        ]

        results_it = run_long_result_athena_query("test query", "ATHENA_DB", "ATHENA_OUTPUT_BUCKET")
        results = list(results_it)

        assert results == [("-", "0.008852269500494003"), ("18.175.49.181", "0.02212107926607132")]


def test_running_long_result_athena_query_with_no_result_rows_produces_correct_values():
    with (
        mock.patch("accounting_s3_usage.sampler.athena_utils.boto3") as botomock,
        mock.patch("accounting_s3_usage.sampler.athena_utils.time"),
    ):
        athenamock = botomock.client("athena")
        athenamock.start_query_execution.return_value = {"QueryExecutionId": 123}
        athenamock.get_query_execution.side_effect = [
            {"QueryExecution": {"Status": {"State": "QUEUED"}}},
            {"QueryExecution": {"Status": {"State": "RUNNING"}}},
            {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
        ]

        # These are real-life API result from Athena with a page size of 1.
        athenamock.get_paginator("get_query_results").paginate.return_value = [
            {
                "UpdateCount": 0,
                "ResultSet": {
                    "Rows": [
                        {
                            "Data": [
                                {"VarCharValue": "remoteip"},
                                {"VarCharValue": "total_gb_transferred"},
                            ]
                        }
                    ],
                    "ResultSetMetadata": {
                        "ColumnInfo": [
                            {
                                "CatalogName": "hive",
                                "SchemaName": "",
                                "TableName": "",
                                "Name": "remoteip",
                                "Label": "remoteip",
                                "Type": "varchar",
                                "Precision": 2147483647,
                                "Scale": 0,
                                "Nullable": "UNKNOWN",
                                "CaseSensitive": True,
                            },
                            {
                                "CatalogName": "hive",
                                "SchemaName": "",
                                "TableName": "",
                                "Name": "total_gb_transferred",
                                "Label": "total_gb_transferred",
                                "Type": "double",
                                "Precision": 17,
                                "Scale": 0,
                                "Nullable": "UNKNOWN",
                                "CaseSensitive": False,
                            },
                        ]
                    },
                },
                "ResponseMetadata": {
                    "RequestId": "638d3e26-aebd-4a1d-a116-a48a2310e320",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {
                        "date": "Wed, 07 May 2025 09:59:41 GMT",
                        "content-type": "application/x-amz-json-1.1",
                        "content-length": "571",
                        "connection": "keep-alive",
                        "x-amzn-requestid": "638d3e26-aebd-4a1d-a116-a48a2310e320",
                    },
                    "RetryAttempts": 0,
                },
            }
        ]

        results_it = run_long_result_athena_query("test query", "ATHENA_DB", "ATHENA_OUTPUT_BUCKET")
        results = list(results_it)

        assert results == []
