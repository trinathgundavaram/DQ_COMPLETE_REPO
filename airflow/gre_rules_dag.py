import os
import sys
from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param


_RULES_ENGINE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rules_engine")
if _RULES_ENGINE_DIR not in sys.path:
    sys.path.insert(0, _RULES_ENGINE_DIR)

from gre_rules_task import run_gre_rules, DEFAULT_VARIABLE_KEY

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


_DAG_PARAMS = {
    "variable_key": Param(
        DEFAULT_VARIABLE_KEY, type="string",
        description="Airflow Variable (JSON) holding this run's connection "
                    "choice, environment, and scope. Override at trigger time "
                    "to point this DAG at a different config without editing it.",
    ),
}


def _run_gre_rules_callable(**context):
    params = context["params"]
    dag_run = context.get("dag_run")
    dag_run_conf = dict(dag_run.conf or {}) if dag_run else {}


    variable_key = dag_run_conf.pop("variable_key", None) or params["variable_key"]
    return run_gre_rules(
        variable_key=variable_key,
        overrides=dag_run_conf,
    )


with DAG(
    dag_id="gre_rules_engine",
    description="Runs the GRE rules engine (rules_engine/) using the connection, "
                "environment, and scope configured in the Airflow Variable named "
                "by the `variable_key` param (default: gre_rules_config).",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params=_DAG_PARAMS,
    tags=["gre", "data-quality", "rules-engine"],
) as gre_rules_engine_dag:

    run_gre_rules_task = PythonOperator(
        task_id="run_gre_rules",
        python_callable=_run_gre_rules_callable,
    )
