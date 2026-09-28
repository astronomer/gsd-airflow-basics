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
Example Airflow Dag, space-mission themed, built as a teaching tour of core
Airflow concepts:

- TaskGroup: visually and logically groups related tasks in the UI.
- Parallel tasks: tasks with no dependency between them run concurrently.
- chain(): a shorthand for wiring up a sequence of dependencies at once,
  including list-to-single (fan-in) and single-to-list (fan-out) shapes.
- The `>>` operator with a plain list (square brackets): the classic,
  explicit way to write a fan-out dependency, as an alternative to chain().
- depends_on_past: a per-task setting (not necessarily a whole-Dag one) that
  blocks a task instance until the *same* task succeeded on the previous
  Dag run. Useful for tasks that must not overlap or run out of order
  across runs.
- Retries: BashOperator tasks below randomly fail about half the time
  (`exit $((RANDOM % 2))`) so you can watch Airflow retry them.
- execution_timeout: a hard per-task wall-clock limit, independent of retries.
- TaskFlow API (`@task`) and dynamic task mapping (`.expand()`/`.partial()`):
  the "crew_roster" group follows the pattern from Astronomer's
  `example_astronauts` tutorial Dag. It calls a real API to get a list of
  astronauts currently in space, then maps a task over that list at *runtime*
  -- the number of mapped task instances isn't known until the Dag actually
  runs.
- Asset outlets and XCom: `get_astronauts` declares an Asset outlet (so other
  Dags could schedule off of it -- see consume_astronaut_asset.py) and pushes
  a value to XCom for inspection.
- Params: `include_crew_roster` lets you toggle a branch from the Trigger Dag
  UI form without editing code.
- Branching (`@task.branch`): picks one of several downstream paths at
  runtime based on a Param.
- short_circuit (`@task.short_circuit`): stops an entire downstream path
  outright (rather than choosing between paths) based on a runtime check.
- Sensors (`@task.sensor`): polls a condition on an interval, in
  `mode="reschedule"` so it frees its worker slot between pokes instead of
  blocking a worker while it waits.
- Trigger rules: `decommission_iss_mission` runs even if its upstream branch
  was skipped (NONE_FAILED_MIN_ONE_SUCCESS); `post_mission_report` always
  runs, success or failure (ALL_DONE).
- on_failure_callback: a lightweight alert hook, independent of retries/SLAs.
- Pools: the three launch tasks share a small pool to cap how many run at
  once, regardless of overall worker capacity.
- Operator diversity: the ISS and Tiangong lines use the classic
  BashOperator; the Crew Dragon line instead runs through @task.bash (the
  TaskFlow decorator for shell commands), @task (TaskFlow Python), and a
  classic PythonOperator, to show that these are all interchangeable
  building blocks you can mix within one Dag.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
import requests

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset, Param, TaskGroup, chain, task
from airflow.task.trigger_rule import TriggerRule


def alert_mission_control(context: dict) -> None:
    """on_failure_callback: fires whenever the task it's attached to fails."""
    ti = context["task_instance"]
    print(f"ALERT: {ti.task_id} failed on Dag run {context['dag_run'].run_id} -- paging mission control.")


with DAG(
    dag_id="example_complex",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "example2", "example3"],
    # Renders the module docstring above as the Dag's docs in the Airflow UI.
    doc_md=__doc__,
    # default_args apply to every task in the Dag unless a task overrides them.
    # Note depends_on_past is *not* set here -- see decommission_iss_mission
    # below for why it's better scoped to a single task in this example.
    # retry_delay is short so the flaky tasks below retry in seconds rather
    # than the 5-minute Airflow default -- handy for demoing retries live.
    default_args={"owner": "airflow", "retries": 2, "retry_delay": timedelta(seconds=5)},
    # Only one run of this Dag may be active at a time.
    max_active_runs=1,
    # Params surface as a form in the UI's "Trigger Dag w/ config" dialog and
    # are available in any task through `context["params"]`.
    params={
        "include_crew_roster": Param(
            True, type="boolean", description="Whether to run the crew roster branch."
        ),
    },
) as dag:
    # --- Sensor: poll a "launch window" condition every 2s, up to 20s, before
    # doing anything else. mode="reschedule" releases the worker slot between
    # pokes instead of occupying it the whole time (contrast with the default
    # mode="poke").
    @task.sensor(poke_interval=2, timeout=20, mode="reschedule")
    def wait_for_launch_window() -> bool:
        import random

        return random.random() > 0.3

    # --- Launch: three independent missions, no dependencies between them,
    # so Airflow schedules and runs all three in parallel -- though the
    # `pool` below caps how many run at once regardless.
    with TaskGroup("launch") as launch_group:
        launch_iss_mission = BashOperator(
            task_id="launch_iss_mission", bash_command="echo launch_iss_mission", pool="launch_pad"
        )
        launch_tiangong_mission = BashOperator(
            task_id="launch_tiangong_mission",
            bash_command="echo launch_tiangong_mission",
            pool="launch_pad",
        )

        # @task.bash is the TaskFlow-decorator equivalent of BashOperator: the
        # function returns the command to run instead of passing it as a
        # bash_command= argument.
        @task.bash(task_id="launch_crew_dragon_mission", pool="launch_pad")
        def _launch_crew_dragon_mission() -> str:
            return "echo launch_crew_dragon_mission"

        launch_crew_dragon_mission = _launch_crew_dragon_mission()

    # --- Track: one tracking task per mission. track_tiangong is flaky on
    # purpose (~50% failure rate) with retries=3, so Airflow will retry it
    # automatically instead of failing the whole Dag on the first bad run.
    # execution_timeout caps how long any single attempt may run, independent
    # of how many retries are left.
    with TaskGroup("track") as track_group:
        track_iss = BashOperator(task_id="track_iss", bash_command="echo track_iss")
        track_tiangong = BashOperator(
            task_id="track_tiangong",
            bash_command="echo track_tiangong && exit $((RANDOM % 2))",
            retries=3,
            execution_timeout=timedelta(seconds=30),
        )
        # Plain @task (TaskFlow Python) alongside its BashOperator siblings --
        # a TaskGroup doesn't care what operator type each task uses.
        @task(task_id="track_crew_dragon")
        def _track_crew_dragon() -> None:
            print("track_crew_dragon")

        track_crew_dragon = _track_crew_dragon()

    # --- Mission control: a single flaky task (also with retries) that every
    # launch must finish before it runs (fan-in, below) and that every
    # decommission task waits on (fan-out, below). on_failure_callback fires
    # an alert on top of (not instead of) the normal retry behavior.
    with TaskGroup("mission_control") as mission_control_group:
        establish_comms = BashOperator(
            task_id="establish_comms",
            bash_command="echo establish_comms && exit $((RANDOM % 2))",
            retries=3,
            on_failure_callback=alert_mission_control,
        )

    # --- Branch: decide, from a Param rather than a hardcoded value, whether
    # to run the crew roster group at all. @task.branch returns the task_id
    # (or list of task_ids) of the downstream path to take; every other
    # immediate downstream task is marked skipped.
    @task.branch
    def decide_crew_roster_branch(**context) -> str:
        # Reference the real task objects' task_ids (resolved after the whole
        # Dag has parsed, since this body only runs at task execution time)
        # rather than hardcoded strings, so a rename here is caught instead
        # of silently breaking the branch.
        if context["params"]["include_crew_roster"]:
            return astronauts.operator.task_id
        return skip_crew_roster.task_id

    skip_crew_roster = EmptyOperator(task_id="skip_crew_roster")

    # --- Crew roster: TaskFlow API + dynamic task mapping, following the
    # pattern from Astronomer's `example_astronauts` tutorial Dag.
    with TaskGroup("crew_roster") as crew_roster_group:
        # @task turns a plain Python function into an Airflow task (the
        # TaskFlow API). Its return value is automatically pushed to XCom and
        # can be passed to downstream tasks just by calling them with it.
        @task(outlets=[Asset("current_astronauts")])
        def get_astronauts(**context) -> list[dict]:
            """Fetch astronauts currently in space from the Open Notify API.

            Falls back to hardcoded data if the API is unreachable, so the
            Dag stays runnable offline. The returned list's length isn't
            known ahead of time -- that's what makes the mapping below
            "dynamic": Airflow decides how many task instances to create only
            after this task has actually run.
            """
            try:
                response = requests.get("http://api.open-notify.org/astros.json")
                response.raise_for_status()
                number_of_people_in_space = response.json()["number"]
                list_of_people_in_space = response.json()["people"]
            except Exception:
                print("API currently not available, using hardcoded data instead.")
                number_of_people_in_space = 3
                list_of_people_in_space = [
                    {"craft": "ISS", "name": "Oleg Kononenko"},
                    {"craft": "ISS", "name": "Nikolai Chub"},
                    {"craft": "Tiangong", "name": "Li Guangsu"},
                ]

            # Push an extra, explicitly-keyed value to XCom in addition to the
            # task's own return value (which is pushed automatically).
            context["ti"].xcom_push(
                key="number_of_people_in_space", value=number_of_people_in_space
            )
            return list_of_people_in_space

        # --- short_circuit: unlike branch (pick one of several *named*
        # paths), this stops everything downstream outright when it returns
        # False, without failing the Dag. Here it gates the whole mapped
        # greeting step on there actually being astronauts to greet.
        @task.short_circuit
        def any_astronauts_in_space(people: list[dict]) -> bool:
            return len(people) > 0

        @task
        def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
            """Runs once per astronaut returned by get_astronauts()."""
            craft = person_in_space["craft"]
            name = person_in_space["name"]
            print(f"{name} is currently in space flying on the {craft}! {greeting}")

        astronauts = get_astronauts()
        any_astronauts_in_space(astronauts) >> print_astronaut_craft.partial(
            greeting="Hello! :)"
        ).expand(person_in_space=astronauts)

    # --- Decommission: one task per mission. Only the ISS task carries
    # depends_on_past=True and trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS, so
    # only *that* task instance has to wait for its own previous run to
    # succeed before it can run again, and only it tolerates the crew_roster
    # branch above being skipped -- the other missions use plain defaults.
    with TaskGroup("decommission") as decommission_group:
        decommission_iss_mission = BashOperator(
            task_id="decommission_iss_mission",
            bash_command="echo decommission_iss_mission",
            depends_on_past=True,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        decommission_tiangong_mission = BashOperator(
            task_id="decommission_tiangong_mission", bash_command="echo decommission_tiangong_mission"
        )

        # Classic PythonOperator (pre-TaskFlow style): a python_callable
        # passed in directly, rather than a decorated function.
        def _decommission_crew_dragon_mission() -> None:
            print("decommission_crew_dragon_mission")

        decommission_crew_dragon_mission = PythonOperator(
            task_id="decommission_crew_dragon_mission",
            python_callable=_decommission_crew_dragon_mission,
        )

    # --- Final report: TriggerRule.ALL_DONE means this runs no matter what
    # happened upstream -- success, failure, or skip -- unlike every other
    # task in this Dag which uses the default ALL_SUCCESS.
    post_mission_report = BashOperator(
        task_id="post_mission_report",
        bash_command="echo post_mission_report",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # The sensor gates the very start of the pipeline.
    wait_for_launch_window() >> launch_group

    # Each launch feeds its matching tracking task -- three independent,
    # parallel branches (launch -> track) running side by side.
    launch_iss_mission >> track_iss
    launch_tiangong_mission >> track_tiangong
    launch_crew_dragon_mission >> track_crew_dragon

    # Fan-in through chain(): passing a TaskGroup to chain() connects *every* task
    # inside it to what comes next, so all three launches must finish before
    # mission control opens comms.
    chain(launch_group, establish_comms)

    # Fan-out through the `>>` operator and a plain list (square brackets): the
    # explicit alternative to chain() for "one task feeds many". Every
    # decommission task waits on mission control.
    establish_comms >> [
        decommission_iss_mission,
        decommission_tiangong_mission,
        decommission_crew_dragon_mission,
    ]

    # The ISS branch alone routes through the crew-roster decision: track_iss
    # feeds the branch task, which sends execution to exactly one of
    # crew_roster_group or skip_crew_roster, and both rejoin at
    # decommission_iss_mission.
    track_iss >> decide_crew_roster_branch() >> [crew_roster_group, skip_crew_roster]
    crew_roster_group >> decommission_iss_mission
    skip_crew_roster >> decommission_iss_mission

    track_tiangong >> decommission_tiangong_mission
    track_crew_dragon >> decommission_crew_dragon_mission

    # Fan-in through chain() again, this time into a plain task rather than a
    # TaskGroup, to close out the Dag.
    chain(decommission_group, post_mission_report)
