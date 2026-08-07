"""
dashboard/streamlit_app.py
-----------------------------
Live, filterable/sortable DQ dashboard.

Reads DIRECTLY from the engine's existing result tables — no new tables,
no duplicated storage. This is a GENERIC dashboard: it discovers
project/process names from dq_rules at load time rather than hardcoding
any one project, and it has no notion of a fixed set of audience names —
whatever severities and stratified-sample config a project defines simply
show up. A different project onboarded onto the engine gets the same
dashboard for free.

Findings are shown joined against the additive dq_exception_dispositions
table at READ TIME ONLY — dq_exceptions itself is never mutated.

Run with:
    streamlit run dashboard/streamlit_app.py
Requires the same DQ_META_CONNECTION / DQ_<NAME>_* env vars as the engine.
"""

import os
import sys
import threading
from datetime import date, timedelta

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.env_config import get_meta_db
from rules_engine.executor import execute_query
from db.connection_factory import ConnectionFactory

st.set_page_config(page_title="DQ Engine Dashboard", layout="wide")


# Fix: @st.cache_resource caches at the PROCESS level, not per-session —
# every concurrent Streamlit user/tab on this server shares the SAME
# ConnectionFactory/connection object returned here. Most DB-API drivers
# (teradatasql, psycopg2, pyodbc, databricks-sql-connector) don't guarantee
# a single Connection is safe for concurrent cursor use from multiple
# threads, and Streamlit runs concurrent sessions on separate threads — so
# two users querying at the same moment could interleave/corrupt each
# other's cursor state. Rather than give every session its own connection
# (multiplies open connections against the source DB, which may have tight
# connection limits), we keep the single shared/pooled connection and
# serialize access to it with a lock — safe, and appropriate for a
# lightweight analyst dashboard where a query is a quick metadata read.
_query_lock = threading.Lock()


@st.cache_resource
def _get_metadata_conn():
    cf = ConnectionFactory()
    cf.load()
    td = cf.get(os.getenv("DQ_META_CONNECTION", "teradata"))
    return cf, td


def _q(td, meta_db, sql, params=None):
    with _query_lock:
        return pd.DataFrame(execute_query(td, sql, params))


def main():
    cf, td = _get_metadata_conn()
    meta_db = get_meta_db()

    st.title("Data Quality Engine — Live Dashboard")

    # ── Project / process selection — discovered from dq_rules (joined
    #    through dq_scope, since dq_rules is scope_id-keyed, not raw
    #    project_name/process_name), nothing hardcoded to any one project. ──
    projects_df = _q(td, meta_db, f"""
        SELECT DISTINCT s.project_name
        FROM {meta_db}.dq_rules r JOIN {meta_db}.dq_scope s ON s.scope_id = r.scope_id
        ORDER BY 1
    """)
    if projects_df.empty:
        st.warning("No projects found in dq_rules yet.")
        return
    project = st.sidebar.selectbox("Project", projects_df["project_name"].tolist())

    processes_df = _q(td, meta_db, f"""
        SELECT DISTINCT s.process_name
        FROM {meta_db}.dq_rules r JOIN {meta_db}.dq_scope s ON s.scope_id = r.scope_id
        WHERE s.project_name = ?
        ORDER BY 1
    """, [project])
    process = st.sidebar.selectbox("Process", ["(all)"] + processes_df["process_name"].tolist())

    view = st.sidebar.radio("View", ["Findings", "Stratified sample", "Engine health"])

    date_range = st.sidebar.selectbox("Window", ["Daily (last 1 day)", "Weekly (last 7 days)", "Monthly (last 31 days)"])
    days = {"Daily (last 1 day)": 1, "Weekly (last 7 days)": 7, "Monthly (last 31 days)": 31}[date_range]
    since = (date.today() - timedelta(days=days)).isoformat()

    # Every query below joins to dq_scope via whatever FK chain gets it
    # there (dq_rules.scope_id directly; dq_run_control.scope_id via
    # run_id; dq_sampling_config.scope_id via config_id) and filters on
    # the same "sc" alias, so this clause is reusable everywhere. Values
    # are bound as params (not interpolated) even though selectbox/
    # multiselect inputs are DB-sourced — defense in depth, same standard
    # applied to CLI-sourced values in rules_engine/.
    process_clause = "AND sc.process_name = ?" if process != "(all)" else ""
    process_params = [process] if process != "(all)" else []

    if view == "Findings":
        st.subheader(f"Data Quality Findings ({date_range})")

        severities_df = _q(td, meta_db, f"""
            SELECT DISTINCT r.severity
            FROM {meta_db}.dq_rules r JOIN {meta_db}.dq_scope sc ON sc.scope_id = r.scope_id
            WHERE sc.project_name = ? AND r.severity IS NOT NULL
            ORDER BY 1
        """, [project])
        all_severities = severities_df["severity"].tolist()
        sev_filter = st.multiselect("Severity", all_severities, default=all_severities)
        if sev_filter:
            sev_sql = "(" + ", ".join(["?"] * len(sev_filter)) + ")"
            sev_params = list(sev_filter)
        else:
            sev_sql = "('')"
            sev_params = []

        # Findings are scoped through dq_rules (which carries scope_id) —
        # dq_exceptions itself only has run_id/rule_id, not project/process.
        findings = _q(td, meta_db, f"""
            SELECT
                e.exception_id, e.run_id, e.rule_code, r.rule_name, r.severity, r.check_type,
                e.table_name, e.primary_key_str, e.created_at,
                d.disposition_type, d.disposition_reason, d.disposed_by, d.disposed_at
            FROM {meta_db}.dq_exceptions e
            JOIN {meta_db}.dq_rules r ON r.rule_id = e.rule_id
            JOIN {meta_db}.dq_scope sc ON sc.scope_id = r.scope_id
            LEFT JOIN (
                SELECT * FROM {meta_db}.dq_exception_dispositions WHERE effective_flag = 1
            ) d ON d.exception_id = e.exception_id
            WHERE sc.project_name = ? {process_clause}
              AND r.severity IN {sev_sql}
              AND e.created_at >= TIMESTAMP ?
            ORDER BY e.created_at DESC
        """, [project] + process_params + sev_params + [f"{since} 00:00:00"])

        if findings.empty:
            st.info("No findings in this window.")
        else:
            st.dataframe(findings, use_container_width=True, hide_index=True)
            st.download_button("Download CSV", findings.to_csv(index=False), file_name="dq_findings.csv")

    elif view == "Stratified sample":
        st.subheader("Stratified Sample")
        # Scoped through dq_sampling_config (config_id -> scope_id) —
        # dq_sample_selections itself only carries config_id.
        samples = _q(td, meta_db, f"""
            SELECT ss.sample_run_id, ss.sample_cycle, ss.case_key, ss.determination_type,
                   ss.functional_area, ss.priority_rank, ss.selected_flag
            FROM {meta_db}.dq_sample_selections ss
            JOIN {meta_db}.dq_sampling_config cfg ON cfg.config_id = ss.config_id
            JOIN {meta_db}.dq_scope sc ON sc.scope_id = cfg.scope_id
            WHERE sc.project_name = ? {process_clause} AND ss.selected_flag = 1
            ORDER BY ss.sample_cycle DESC, ss.priority_rank ASC
        """, [project] + process_params)
        if samples.empty:
            st.info("No stratified sample generated yet for this project/process.")
        else:
            latest_cycle = samples["sample_cycle"].max()
            st.caption(f"Latest sample cycle: {latest_cycle}")
            st.dataframe(samples[samples["sample_cycle"] == latest_cycle],
                        use_container_width=True, hide_index=True)

            mix = samples[samples["sample_cycle"] == latest_cycle]["determination_type"].value_counts(normalize=True)
            st.bar_chart(mix)

    else:  # Engine health
        st.subheader("Engine Health — rule/connection failures (never a data finding)")
        # Scoped through dq_run_control (run_id -> scope_id) —
        # dq_rule_execution itself only carries run_id.
        engine_health = _q(td, meta_db, f"""
            SELECT re.run_id, re.rule_code, re.status, re.execution_time, re.run_timestamp
            FROM {meta_db}.dq_rule_execution re
            JOIN {meta_db}.dq_run_control rc ON rc.run_id = re.run_id
            JOIN {meta_db}.dq_scope sc ON sc.scope_id = rc.scope_id
            WHERE sc.project_name = ? {process_clause}
              AND re.status IN ('ERROR', 'SKIP')
              AND re.run_timestamp >= TIMESTAMP ?
            ORDER BY re.run_timestamp DESC
        """, [project] + process_params + [f"{since} 00:00:00"])
        if engine_health.empty:
            st.success("No engine errors in this window.")
        else:
            st.dataframe(engine_health, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
