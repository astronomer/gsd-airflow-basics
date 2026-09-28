"""
Example Airflow Dag demonstrating `template_searchpath`: instead of inlining SQL
as Python strings, each task's `sql=` argument is just a filename, and Airflow
resolves it against the folders listed in `template_searchpath` (here,
`include/sql/`). The files themselves are still rendered through Jinja, so
`{{ run_id }}` works exactly as it would in an inline string.

`insert_mission_log_entry` shows the other way to get values into a query:
`?` placeholders in the .sql file, filled in through the operator's
`parameters=` argument rather than interpolated into the SQL text. The values
in `parameters` are still Jinja-templated (for example `{{ params.mission }}`);
only the SQL string itself is no longer built by string interpolation.

Uses SQLite (through `apache-airflow-providers-sqlite`, added to
requirements.txt) purely because it needs no external database to run: the
connection points at a file under `include/sqlite/`, created automatically.
"""

from __future__ import annotations

import pendulum

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG

SQLITE_CONN_ID = "sqlite_mission_log"

with DAG(
    dag_id="example_sql_templates",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    default_args={"retries": 2},
    # Every relative path in a templated field (like the `sql=` filenames
    # below) is resolved against each folder in this list, in order.
    template_searchpath=["/usr/local/airflow/include/sql"],
    params={"mission": "ISS"},
    doc_md=__doc__,
) as dag:
    create_mission_log_table = SQLExecuteQueryOperator(
        task_id="create_mission_log_table",
        conn_id=SQLITE_CONN_ID,
        sql="create_mission_log_table.sql",
    )

    # parameters= binds values to the `?` placeholders in the .sql file --
    # the database driver escapes them, rather than Jinja splicing them
    # into the SQL string directly.
    insert_mission_log_entry = SQLExecuteQueryOperator(
        task_id="insert_mission_log_entry",
        conn_id=SQLITE_CONN_ID,
        sql="insert_mission_log_entry.sql",
        parameters=["{{ run_id }}", "{{ params.mission }}", "Dag run completed successfully"],
    )

    select_mission_log = SQLExecuteQueryOperator(
        task_id="select_mission_log",
        conn_id=SQLITE_CONN_ID,
        sql="select_mission_log.sql",
        show_return_value_in_logs=True,
    )

    create_mission_log_table >> insert_mission_log_entry >> select_mission_log
