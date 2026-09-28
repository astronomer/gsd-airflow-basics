"""
Example Airflow Dag with the fewest moving parts needed to show how a Dag
works. Start here if you're new to Airflow; `dags/complex.py` builds on every
concept introduced in this file.

A Dag (directed acyclic graph) is just a Python object that describes a set
of tasks and the order they should run in. Airflow reads this file, builds
that graph, and schedules/runs the tasks; it never runs the rest of this
module's code directly.
"""

from __future__ import annotations

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="example_basic",
    # A standard cron-like schedule: this Dag is meant to run once a day.
    # Compare with example_complex's schedule=None (manual trigger only) and
    # example_asset_consumer's Asset-based schedule.
    schedule="@daily",
    # Every Dag needs a start_date: the earliest logical date Airflow will
    # consider scheduling a run for.
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    default_args={"retries": 2},
    doc_md=__doc__,
) as dag:
    # A task is one unit of work. BashOperator is the simplest kind: it just
    # runs a shell command.
    say_hello = BashOperator(task_id="say_hello", bash_command="echo 'Hello, Airflow!'")

    say_goodbye = BashOperator(task_id="say_goodbye", bash_command="echo 'Goodbye, Airflow!'")

    # The `>>` operator sets a dependency: say_hello must succeed before
    # say_goodbye starts. say_hello is "upstream" of say_goodbye; say_goodbye
    # is "downstream" of say_hello.
    say_hello >> say_goodbye
