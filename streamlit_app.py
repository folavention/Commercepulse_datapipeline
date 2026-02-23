import os
import signal
import subprocess
import sys
from pathlib import Path

import pandas as pd
import streamlit as st


ROOT = Path(__file__).resolve().parent
GENERATOR_SCRIPT = ROOT / "data" / "commercepulse_data_pack" / "src" / "live_event_generator.py"
PIPELINE_LOOP_SCRIPT = ROOT / "SRC" / "live_pipeline_loop.py"
WAREHOUSE_FACTS_DIR = ROOT / "data" / "warehouse" / "facts"
LOG_DIR = ROOT / "data" / "live_events" / "logs"


def _is_running(proc):
    return proc is not None and proc.poll() is None


def _start_process(cmd, log_path):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = open(log_path, "a", encoding="utf-8")
    creationflags = 0
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    process = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        creationflags=creationflags,
    )
    return process, log_file


def _stop_process(proc, log_file=None):
    if not _is_running(proc):
        return
    if os.name == "nt":
        proc.terminate()
    else:
        proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    if log_file is not None and not log_file.closed:
        log_file.close()


def _load_csv(path):
    if not path.exists():
        return pd.DataFrame(), f"Missing file: {path}"
    try:
        return pd.read_csv(path), None
    except Exception as exc:
        return pd.DataFrame(), f"Failed to read {path}: {exc}"


def _tail_log(path, max_lines=25):
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    return "\n".join(lines[-max_lines:])


def _render_metrics():
    order_daily, order_daily_err = _load_csv(WAREHOUSE_FACTS_DIR / "facts_order_daily.csv")
    payment, payment_err = _load_csv(WAREHOUSE_FACTS_DIR / "facts_payment.csv")
    refunds, refunds_err = _load_csv(WAREHOUSE_FACTS_DIR / "facts_refunds.csv")
    shipment, shipment_err = _load_csv(WAREHOUSE_FACTS_DIR / "facts_shipment.csv")

    errors = [err for err in [order_daily_err, payment_err, refunds_err, shipment_err] if err]
    if errors:
        for err in errors:
            st.warning(err)
        st.caption(f"Expected facts folder: {WAREHOUSE_FACTS_DIR}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Orders", int(order_daily["order_id"].nunique()) if "order_id" in order_daily else 0)
    c2.metric("Payments", int(len(payment)))
    c3.metric("Refunds", int(len(refunds)))
    c4.metric("Shipments", int(len(shipment)))
    st.caption(
        f"Local rows -> order_daily={len(order_daily)} payment={len(payment)} "
        f"refunds={len(refunds)} shipment={len(shipment)}"
    )

    if not order_daily.empty and {"event_date", "event_count"}.issubset(order_daily.columns):
        chart_df = (
            order_daily.groupby("event_date", as_index=False)["event_count"]
            .sum()
            .sort_values("event_date")
        )
        st.subheader("Daily Event Volume")
        st.line_chart(chart_df.set_index("event_date"))
    else:
        st.info("No warehouse facts available yet. Start the pipeline and refresh.")


def main():
    st.set_page_config(page_title="CommercePulse Live Control", layout="wide")
    st.title("CommercePulse Live Control Panel")

    if "generator_proc" not in st.session_state:
        st.session_state.generator_proc = None
    if "generator_log_handle" not in st.session_state:
        st.session_state.generator_log_handle = None
    if "pipeline_proc" not in st.session_state:
        st.session_state.pipeline_proc = None
    if "pipeline_log_handle" not in st.session_state:
        st.session_state.pipeline_log_handle = None

    with st.sidebar:
        st.header("Generator Settings")
        out_dir = st.text_input("Output Directory", value="data/live_events")
        interval_ms = st.number_input("Event Interval (ms)", min_value=0, value=300, step=50)
        max_events_input = st.text_input("Max Events (optional)", value="")

        st.header("Pipeline Settings")
        loop_sleep = st.number_input("Loop Sleep (seconds)", min_value=0.0, value=2.0, step=0.5)
        load_bigquery = st.checkbox("Load to BigQuery each cycle", value=False)
        bigquery_mode = st.selectbox("BigQuery Mode", options=["append", "replace"], index=0)

        st.header("Refresh")
        st.caption("Use Streamlit's R key or this button to refresh dashboard data.")
        st.button("Refresh Now")

    max_events = max_events_input.strip()
    max_events_arg = []
    if max_events:
        try:
            int(max_events)
            max_events_arg = ["--max-events", max_events]
        except ValueError:
            st.sidebar.warning("Max Events must be an integer.")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Generator")
        gen_running = _is_running(st.session_state.generator_proc)
        st.write(f"Status: {'Running' if gen_running else 'Stopped'}")

        if st.button("Start Generator", disabled=gen_running):
            cmd = [
                sys.executable,
                str(GENERATOR_SCRIPT),
                "--out",
                out_dir,
                "--continuous",
                "--event-interval-ms",
                str(int(interval_ms)),
            ] + max_events_arg
            proc, handle = _start_process(cmd, LOG_DIR / "generator.log")
            st.session_state.generator_proc = proc
            st.session_state.generator_log_handle = handle
            st.success("Generator started.")

        if st.button("Stop Generator", disabled=not gen_running):
            _stop_process(st.session_state.generator_proc, st.session_state.generator_log_handle)
            st.session_state.generator_proc = None
            st.session_state.generator_log_handle = None
            st.success("Generator stopped.")

        st.code(_tail_log(LOG_DIR / "generator.log"), language="text")

    with col2:
        st.subheader("Ingest + Transform Loop")
        pipe_running = _is_running(st.session_state.pipeline_proc)
        st.write(f"Status: {'Running' if pipe_running else 'Stopped'}")

        if st.button("Start Pipeline Loop", disabled=pipe_running):
            cmd = [
                sys.executable,
                str(PIPELINE_LOOP_SCRIPT),
                "--sleep-seconds",
                str(float(loop_sleep)),
            ]
            if load_bigquery:
                cmd.extend(["--load-bigquery", "--bigquery-mode", bigquery_mode])
            proc, handle = _start_process(cmd, LOG_DIR / "pipeline.log")
            st.session_state.pipeline_proc = proc
            st.session_state.pipeline_log_handle = handle
            st.success("Pipeline loop started.")

        if st.button("Stop Pipeline Loop", disabled=not pipe_running):
            _stop_process(st.session_state.pipeline_proc, st.session_state.pipeline_log_handle)
            st.session_state.pipeline_proc = None
            st.session_state.pipeline_log_handle = None
            st.success("Pipeline loop stopped.")

        st.code(_tail_log(LOG_DIR / "pipeline.log"), language="text")

    st.divider()
    st.header("Warehouse Snapshot")
    _render_metrics()


if __name__ == "__main__":
    main()
