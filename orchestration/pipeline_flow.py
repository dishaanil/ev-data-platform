from prefect import flow, task
import subprocess
import os


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_DIR = os.path.join(PROJECT_ROOT, "ev_dbt")


@task
def ingest_acn_sessions():
    result = subprocess.run(
        ["python", "ingestion/load_acn_sessions.py"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("ACN ingestion failed")


@task
def run_dbt_models():
    result = subprocess.run(
        ["dbt", "run"],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("dbt run failed")


@task
def run_dbt_tests():
    result = subprocess.run(
        ["dbt", "test"],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("dbt test failed")


@flow(name="ev-data-platform-flow")
def ev_data_platform_flow():
    ingest_acn_sessions()
    run_dbt_models()
    run_dbt_tests()


if __name__ == "__main__":
    ev_data_platform_flow()