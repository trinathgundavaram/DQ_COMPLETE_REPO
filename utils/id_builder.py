from datetime import datetime


def generate_run_id(
    project: str,
    process: str,
    run_type: str,
    run_mode: str,
    batch_id: str = None,
    start_date=None,
    end_date=None,
) -> str:
    """
    Build a deterministic, human-readable run_id.

    Format:
      BATCH : {PROJECT}_{PROCESS}_{RUN_TYPE}_BATCH_{batch_id}_{YYYYMMDD_HHMM}
      DATE  : {PROJECT}_{PROCESS}_{RUN_TYPE}_DATE_{start_date}_{end_date}_{YYYYMMDD_HHMM}
      FULL  : {PROJECT}_{PROCESS}_{RUN_TYPE}_FULL_{YYYYMMDD_HHMM}
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")   # seconds prevent same-minute collision

    if run_mode == "BATCH" and batch_id:
        dataset = batch_id
    elif run_mode == "DATE" and start_date and end_date:
        dataset = f"{start_date}_{end_date}"
    else:
        dataset = "FULL"

    return f"{project}_{process}_{run_type}_{run_mode}_{dataset}_{ts}"
