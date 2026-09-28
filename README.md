Overview
========

This project is a learning reference for Apache Airflow 3, built with the Astro CLI. It
demonstrates core Airflow concepts through a single space-mission-themed example Dag, rather
than through many small, disconnected snippets.

Dag contents
============

- `dags/complex.py` (`example_complex`): the main teaching example. A tour of TaskGroups,
  parallel tasks, `chain()`, fan-in/fan-out with the `>>` operator, `depends_on_past`, retries,
  `execution_timeout`, the TaskFlow API (`@task`), dynamic task mapping (`.expand()`/`.partial()`),
  Asset outlets, Params, branching (`@task.branch`), `short_circuit`, sensors (`@task.sensor`),
  trigger rules, `on_failure_callback`, pools, and operator diversity (`BashOperator`,
  `@task.bash`, `@task`, `PythonOperator` side by side). Read the module docstring at the top of
  the file for a full list with explanations.
- `dags/consume_astronaut_asset.py` (`example_asset_consumer`): a second Dag scheduled on the
  `current_astronauts` Asset that `example_complex` produces, demonstrating Asset-based
  (data-aware) scheduling between two Dags.

Project contents
================

- `dags`: the Dag files described earlier.
- `Dockerfile`: pins the Astro Runtime image version used for local development.
- `include`: additional files to include in the project. Empty by default.
- `packages.txt`: OS-level packages to install. Empty by default.
- `requirements.txt`: Python packages to install beyond what Astro Runtime provides by default.
- `plugins`: custom or community Airflow plugins. Empty by default.
- `airflow_settings.yaml`: local-only file for Airflow connections, variables, and pools. Not
  checked in to version control. See Setup for the pool this project needs.
- `tests/dags/test_dag_example.py`: parametrized tests that every Dag in this project must pass
  (no import errors, tags present, `retries` set).

Setup
=====

Start Airflow locally by running `astro dev start`. This spins up five Docker containers
(Postgres, scheduler, Dag processor, API server, and triggerer) and opens the Airflow UI at
`http://localhost:8080/`. Sign in with the default local credentials: username `admin`,
password `admin`.

`example_complex` requires a `launch_pad` pool with two slots, used to cap how many launch tasks
run at once. `airflow_settings.yaml` isn't checked in to version control (it can hold connection
secrets), so after a fresh clone, create the pool with:

```bash
astro dev run pools set launch_pad 2 "Caps concurrent launch tasks for the example_complex demo DAG"
```

Alternatively, add the pool to your own local `airflow_settings.yaml` so it's created
automatically on every `astro dev start`:

```yaml
airflow:
  pools:
    - pool_name: launch_pad
      pool_slot: 2
      pool_description: Caps concurrent launch tasks for the example_complex demo DAG
```

Run the tests with:

```bash
astro dev pytest
```

Deploy to Astronomer
=====================

For instructions on pushing this project to a Deployment on Astronomer, see the
[Astronomer documentation](https://www.astronomer.io/docs/astro/deploy-code/).
