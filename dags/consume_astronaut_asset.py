"""
Example Airflow Dag demonstrating Asset-based (data-aware) scheduling.

`example_complex`'s crew_roster.get_astronauts task declares
`outlets=[Asset("current_astronauts")]`. Rather than scheduling this Dag on a
cron interval, `schedule=[Asset(...)]` below tells Airflow to run it
automatically every time that asset is updated: every time
`example_complex` successfully produces a new astronaut roster.
"""

from __future__ import annotations

import pendulum

from airflow.sdk import DAG, Asset, task

with DAG(
    dag_id="example_asset_consumer",
    schedule=[Asset("current_astronauts")],
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    default_args={"retries": 2},
    # Renders the module docstring above as the Dag's docs in the Airflow UI.
    doc_md=__doc__,
) as dag:

    @task
    def celebrate_new_crew_data() -> None:
        print("New astronaut roster is available! Downstream analytics can now run.")

    celebrate_new_crew_data()
