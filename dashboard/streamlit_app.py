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

Findings are shown joined against the additive dq_case_dispositions table
at READ TIME ONLY — dq_exceptions itself is never mutated.

Run with:
    streamlit run dashboard/streamlit_app.py
Requires the same DQ_META_CONNECTION / DQ_<NAME>_* env vars as the engine.
"""

import os
import sys
from datetime import date, timedelta

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.env_config import get_meta_db
from core.executor import execute_query
from db.connection_factory import ConnectionFactory

st.set_page_config(page_title="DQ Engine Dashboard", layout="wide")


@st.cache_resource
def _get_metadata_conn():
    cf = ConnectionFactory()
    cf.load()
    td = cf.get(os.getenv("DQ_META_CONNECTION", "teradata"))
    return cf, td


def _q(td, meta_db, sql, params=None):
    return pd.DataFrame(execute_query(td, sql, params))


def main():
    cf, td = _get_metadata_conn()
    meta_db = get_meta_db()

    st.title("Data Quality Engine — Live Dashboard")

    # ── Project / process selection — discovered from dq_rules, nothing
    #    hardcoded to any one project. ──────────────────────────────────────
    projects_df = _q(td, meta_db, f"SELECT DISTINCT project_name FROM {meta_db}.dq_rules ORDER BY 1")
    if projects_df.empty:
        st.warning("No projects found in dq_rules yet.")
        return
    project = st.sidebar.selectbox("Project", projects_df["project_name"].tolist())

    processes_df = _q(td, meta_db,
        f"SELECT DISTINCT process_name FROM {meta_db}.dq_rules WHERE project_name = '{project}' ORDER BY 1")
    process = st.sidebar.selectbox("Process", ["(all)"] + processes_df["process_name"].tolist())

    view = st.sidebar.radio("View", ["Findings", "Stratified sample", "Engine health"])

    date_range = st.sidebar.selectbox("Window", ["Daily (last 1 day)", "Weekly (last 7 days)", "Monthly (last 31 days)"])
    days = {"Daily (last 1 day)": 1, "Weekly (last 7 days)": 7, "Monthly (last 31 days)": 31}[date_range]
    since = (date.today() - timedelta(days=days)).isoformat()

    process_clause = f"AND e.process_name = '{process}'" if process != "(all)" else ""

    if view == "Findings":
        st.subheader(f"Data Quality Findings ({date_range})")

        severities_df = _q(td, meta_db,
            f"SELECT DISTINCT severity FROM {meta_db}.dq_rules WHERE project_name = '{project}' AND severity IS NOT NULL ORDER BY 1")
        all_severities = severities_df["severity"].tolist()
        sev_filter = st.multiselect("Severity", all_severities, default=all_severities)
        sev_sql = "(" + ", ".join(f"'{s}'" for s in sev_filter) + ")" if sev_filter else "('')"

        findings = _q(td, meta_db, f"""
            SELECT
                e.exception_id, e.run_id, e.rule_code, r.rule_name, r.severity, r.check_type,
                e.table_name, e.primary_key_str, e.created_at,
                d.disposition_type, d.disposition_reason, d.disposed_by, d.disposed_at
            FROM {meta_db}.dq_exceptions e
            JOIN {meta_db}.dq_rules r ON r.rule_id = e.rule_id
            LEFT JOIN (
                SELECT * FROM {meta_db}.dq_case_dispositions WHERE effective_flag = 1
            ) d ON d.exception_id = e.exception_id
            WHERE e.project_name = '{project}' {process_clause}
              AND r.severity IN {sev_sql}
              AND e.created_at >= TIMESTAMP '{since} 00:00:00'
            ORDER BY e.created_at DESC
        """)

        if findings.empty:
            st.info("No findings in this window.")
        else:
            st.dataframe(findings, use_container_width=True, hide_index=True)
            st.download_button("Download CSV", findings.to_csv(index=False), file_name="dq_findings.csv")

    elif view == "Stratified sample":
        st.subheader("Stratified Sample")
        samples = _q(td, meta_db, f"""
            SELECT sample_run_id, sample_cycle, case_key, determination_type,
                   functional_area, priority_rank, selected_flag
            FROM {meta_db}.dq_sample_selections
            WHERE project_name = '{project}' {process_clause} AND selected_flag = 1
            ORDER BY sample_cycle DESC, priority_rank ASC
        """)
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
        engine_health = _q(td, meta_db, f"""
            SELECT run_id, rule_code, status, execution_time, run_timestamp
            FROM {meta_db}.dq_rule_execution
            WHERE project_name = '{project}' {process_clause}
              AND status IN ('ERROR', 'SKIP')
              AND run_timestamp >= TIMESTAMP '{since} 00:00:00'
            ORDER BY run_timestamp DESC
        """)
        if engine_health.empty:
            st.success("No engine errors in this window.")
        else:
            st.dataframe(engine_health, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
