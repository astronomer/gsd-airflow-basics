#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example Airflow Dag demonstrating Asset-based (data-aware) scheduling.

`example_complex`'s crew_roster.get_astronauts task declares
`outlets=[Asset("current_astronauts")]`. Rather than scheduling this Dag on a
cron interval, `schedule=[Asset(...)]` below tells Airflow to run it
automatically every time that asset is updated -- that is, every time
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
