Overview
========

This project is a learning reference for Apache Airflow 3, built with the Astro CLI. Start with
`dags/simple_example.py`, then work through the rest of the Dags in the order listed.

Dag contents
============

- `dags/simple_example.py` (`example_basic`): start here. The fewest moving parts needed to show
  how a Dag works: two tasks, a `>>` dependency, and a cron schedule.
- `dags/example_dag.py` (`example_astronauts`): the default Dag Astro CLI generates for new
  projects. TaskFlow API and dynamic task mapping over a real API call.
- `dags/complex.py` (`example_complex`): the main teaching example, a space-mission-themed tour
  of most other Airflow features. Read the module docstring at
  the top of the file for a full explanation of each pattern. Covers:
  - TaskGroups
  - Parallel tasks
  - `chain()`
  - Fan-in/fan-out with the `>>` operator
  - `depends_on_past`
  - Retries
  - `execution_timeout`
  - The TaskFlow API (`@task`)
  - Dynamic task mapping (`.expand()`/`.partial()`)
  - Asset outlets
  - Params
  - Branching (`@task.branch`)
  - `short_circuit`
  - Sensors (`@task.sensor`)
  - Trigger rules
  - `on_failure_callback`
  - Pools
  - Operator diversity (`BashOperator`, `@task.bash`, `@task`, and `PythonOperator` side by side)
- `dags/consume_astronaut_asset.py` (`example_asset_consumer`): a second Dag scheduled on the
  `current_astronauts` Asset that `example_complex` produces, demonstrating Asset-based
  (data-aware) scheduling between two Dags.
- `dags/mission_log_sql_templates.py` (`example_sql_templates`): demonstrates
  `template_searchpath`. Each task's `sql=` argument is a filename resolved against
  `include/sql/`, and the `.sql` files there are still rendered through Jinja. Runs against
  SQLite, so it needs no external database. See Setup for the connection this Dag needs.

Project contents
================

- `dags`: the Dag files described earlier.
- `Dockerfile`: pins the Astro Runtime image used for local development.
- `include/sql`: the `.sql` files that `example_sql_templates` resolves through
  `template_searchpath`.
- `packages.txt`: OS-level packages to install. Empty by default.
- `requirements.txt`: Python packages to install beyond what Astro Runtime provides by default.
  Includes `apache-airflow-providers-sqlite`, needed by `example_sql_templates`.
- `plugins`: custom or community Airflow plugins. Empty by default.
- `airflow_settings.yaml`: local-only file for Airflow connections, variables, and pools. Not
  checked in to version control. See Setup for how to create it from `airflow_settings.example.yaml`.
- `airflow_settings.example.yaml`: checked-in template with the pool and connection this project
  needs. Copy it to `airflow_settings.yaml` after cloning.
- `tests/dags/test_dag_example.py`: parametrized tests that every Dag in this project must pass
  (no import errors, tags present, `retries` set).

Setup
=====

`airflow_settings.yaml` isn't checked in to version control (it can hold connection secrets), so
after cloning, copy the checked-in template and let Astro CLI apply it:

```bash
cp airflow_settings.example.yaml airflow_settings.yaml
```

This creates the `launch_pad` pool (caps how many `example_complex` launch tasks run at once) and
the `sqlite_mission_log` connection (used by `example_sql_templates`, pointing at a database file
under `include/sqlite/` created automatically on first use) on every `astro dev start`.

Start Airflow locally by running `astro dev start`. This spins up five Docker containers
(Postgres, scheduler, Dag processor, API server, and triggerer) and opens the Airflow UI at
`http://localhost:8080/`. Sign in with the default local credentials: username `admin`,
password `admin`.

Without `airflow_settings.yaml`, create the pool and connection directly instead:

```bash
astro dev run pools set launch_pad 2 "Caps concurrent launch tasks for the example_complex demo DAG"
astro dev run connections add sqlite_mission_log --conn-type sqlite --conn-host /usr/local/airflow/include/sqlite/mission_log.db
```

Run the tests with:

```bash
astro dev pytest
```

Deploy to Astronomer
=====================

For instructions on pushing this project to a Deployment on Astronomer, see the
[Astronomer documentation](https://www.astronomer.io/docs/astro/deploy-code/).
