"""
gre_rules_dag.py
-----------------
Example Airflow DAG that calls the GRE rules engine through
gre_rules_task.run_gre_rules() -- the only bridge point between Airflow
and the rules_engine/ framework. This is the ONE file in this
integration that lives outside rules_engine/ (Airflow needs an actual
DAG file to discover); gre_rules_task.py, run_rules.py, db/, and the
framework itself all live one level down, inside rules_engine/ -- see
this file's sys.path bootstrap below for how the import resolves.

This DAG hardcodes NOTHING about the run itself -- not which connection
to use, not the environment, not the scope (project/process/rule_group/
rule_variant). All of that lives in ONE Airflow Variable (JSON), so you
can point this same DAG at a different scope, a different environment,
or switch from Teradata to Postgres, just by editing the Variable --
never this file. See rules_engine/gre_rules_task.py's module docstring
and `_load_runtime_variable()`'s docstring for the full Variable shape.

This package supports Teradata + Postgres source connectivity only (see
rules_engine/db/connection_factory.py), and every run uses EXACTLY ONE
of them -- never both -- selected by the Variable's "connection_type" key.

Setup required in Airflow before this DAG can run
----------------------------------------------------
1. Connection: an Airflow Connection holding credentials for WHICHEVER
   ONE source you're using for a given run.

   Teradata:
     Connection Id:   gre_teradata          (default -- or your own,
                                              named via "connection_id"
                                              in the Variable below)
     Connection Type: Teradata (or "Generic" if the Teradata provider
                       isn't installed -- host/login/password are all
                       this module reads)
     Host / Login / Password:  <as usual>
     Extra (optional):  {"logmech": "LDAP"}   -- defaults to LDAP if omitted

   Postgres/Aurora:
     Connection Id:   gre_postgres          (default -- or your own,
                                              named via "connection_id"
                                              in the Variable below)
     Connection Type: Postgres
     Host / Schema / Login / Password / Port: <as usual>
     Extra (optional):  {"sslmode": "prefer"}

   Only ONE of these is read per run -- whichever "connection_type" the
   Variable (or a trigger-time override) names.

2. Variable: an Airflow Variable (JSON) holding everything else --
   which connection to use, environment/metadata db/log level, and the
   run's scope at whatever level you need.
     Variable Key: gre_rules_config          (or your own, see the
                                               `variable_key` DAG param
                                               below)
     Variable Value, e.g. (Teradata, process-level scope):
       {
         "connection_type": "teradata",
         "environment": "PROD",
         "meta_db": "GRE_META_PROD",
         "log_level": "INFO",
         "process_name": "UNIVERSE_VALIDATION",
         "run_params": {"year": "2026", "month": "9"}
       }

     ...or (Postgres, one specific rule_group):
       {
         "connection_type": "postgres",
         "connection_id": "gre_postgres_prod",
         "environment": "PROD",
         "meta_db": "GRE_META_PROD",
         "rule_group": "ODAG3"
       }

   Leave "project_name"/"process_name"/"rule_group"/"rule_variant" out
   (or null) at whatever level you don't want to scope by -- see
   rules_engine/runner.py::run_by_scope()'s docstring for exactly how
   project/process/rule_group/rule_variant combine.

No .env file is used or needed -- see gre_rules_task.py's
module docstring for exactly how the Connection + Variable above become
the env vars the rules engine expects.

Calling this at any level / with a one-off override
--------------------------------------------------------
Trigger the DAG with a run configuration (Airflow UI "Trigger DAG w/
config", the CLI's -c, or the REST API) to run a one-off scope, or a
different Variable entirely, WITHOUT editing anything stored:

    {"process_name": "UNIVERSE_VALIDATION", "run_key": "BACKFILL_2026_08"}

    {"variable_key": "gre_rules_config_qa"}

    {"connection_type": "postgres", "rule_group": "ODAG3"}

Every key in that JSON is merged over the configured Variable's config
(`overrides=` below) -- any key you don't pass keeps the Variable's own
value. `variable_key` in the run config even lets one trigger swap which
Variable is read entirely, still without touching this file.

Two task styles are shown below (pick one, or delete the one you don't
use) -- a classic PythonOperator and a TaskFlow @task. Both call the
exact same run_gre_rules() function, and both resolve `variable_key`
from a DAG param (defaulting to gre_rules_task's own
default) rather than a hardcoded constant, so it too can be overridden
per trigger via the Airflow UI/API without editing this file.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

# gre_rules_task.py (and everything else this integration needs -- db/,
# run_rules.py) lives one level down, inside rules_engine/, so that
# folder is the ONLY thing left outside it. Add it to sys.path before
# importing, rather than relying on the Airflow worker's DAG-processing
# setup to have done so already.
_RULES_ENGINE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rules_engine")
if _RULES_ENGINE_DIR not in sys.path:
    sys.path.insert(0, _RULES_ENGINE_DIR)

from gre_rules_task import run_gre_rules, DEFAULT_VARIABLE_KEY

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# The only thing a trigger needs to touch to point this DAG at a
# different config entirely -- everything else (connection, scope,
# environment, ...) lives inside whichever Variable this names.
_DAG_PARAMS = {
    "variable_key": Param(
        DEFAULT_VARIABLE_KEY, type="string",
        description="Airflow Variable (JSON) holding this run's connection "
                    "choice, environment, and scope. Override at trigger time "
                    "to point this DAG at a different config without editing it.",
    ),
}


# ---------------------------------------------------------------------------
# Option A -- classic PythonOperator
# ---------------------------------------------------------------------------

def _run_gre_rules_callable(**context):
    """
    python_callable for the PythonOperator below. Resolves `variable_key`
    from the DAG's own `params` (itself overridable at trigger time), and
    merges `dag_run.conf` over the Variable's config via `overrides=` --
    so a manual/triggered run can override ANY input (connection_type,
    scope, run_key, ...) without editing the Variable or this DAG.
    """
    params = context["params"]
    dag_run = context.get("dag_run")
    dag_run_conf = dict(dag_run.conf or {}) if dag_run else {}
    # variable_key is also settable via dag_run.conf -- pull it out so it
    # isn't also forwarded into `overrides` (run_gre_rules() takes it as
    # its own argument, not a run_rules() scope key).
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
) as dag:

    run_gre_rules_task = PythonOperator(
        task_id="run_gre_rules",
        python_callable=_run_gre_rules_callable,
    )


# ---------------------------------------------------------------------------
# Option B -- TaskFlow API (@dag / @task) equivalent of the same DAG under
# a different dag_id, so both can be present without colliding. Delete
# this whole block if you only want Option A (or vice versa).
# ---------------------------------------------------------------------------

@dag(
    dag_id="gre_rules_engine_taskflow",
    description="TaskFlow-style equivalent of gre_rules_engine.",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params=_DAG_PARAMS,
    tags=["gre", "data-quality", "rules-engine"],
)
def gre_rules_engine_taskflow():

    @task
    def run_gre_rules_taskflow(params=None, dag_run=None):
        dag_run_conf = dict(dag_run.conf or {}) if dag_run else {}
        variable_key = dag_run_conf.pop("variable_key", None) or params["variable_key"]
        return run_gre_rules(
            variable_key=variable_key,
            overrides=dag_run_conf,
        )

    run_gre_rules_taskflow()


gre_rules_engine_taskflow()
