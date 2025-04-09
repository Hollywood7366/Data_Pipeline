import os
import smtplib
import traceback
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from airflow.utils.log.logging_mixin import LoggingMixin
from dotenv import load_dotenv

from utils.util import base_path

load_dotenv(dotenv_path=f"{base_path()}/airflow.env")


def get_task_logs(task_instance, try_number=None):
    try:
        # logger = LoggingMixin().log

        if not try_number:
            try_number = task_instance.try_number

        log_try_number = try_number - 1 if try_number > 1 else 1

        dag_id = task_instance.dag_id
        task_id = task_instance.task_id
        run_id = task_instance.run_id

        # log_base = os.path.expanduser(os.getenv('AIRFLOW__LOGGING__BASE_LOG_FOLDER', '~/logs'))
        log_file = os.path.join(
            "/opt/airflow/logs",
            f"dag_id={dag_id}",
            f"run_id={run_id}",
            f"task_id={task_id}",
            f"attempt={log_try_number}.log",
        )

        if os.path.exists(log_file):
            with open(log_file, "r") as f:
                return f.read()
        else:
            return f"[!] Log file not found at: {log_file}"

    except Exception as e:
        return f"Error retrieving logs: {str(e)}"


def get_dag_run_summary(context):
    dag_run = context.get("dag_run")
    summary = {}

    if dag_run:
        task_instances = dag_run.get_task_instances()

        for ti in task_instances:
            start_date = (
                ti.start_date.strftime("%Y-%m-%d %H:%M:%S")
                if ti.start_date
                else "Not started"
            )
            end_date = (
                ti.end_date.strftime("%Y-%m-%d %H:%M:%S")
                if ti.end_date
                else "Not finished"
            )
            duration = str(ti.duration) if ti.duration else "N/A"

            summary[ti.task_id] = {
                "status": ti.state,
                "start_time": start_date,
                "end_time": end_date,
                "duration": duration,
                "try_number": ti.try_number,
            }

    return summary


def send_task_email(context):
    sender = os.getenv("AIRFLOW__SMTP__SMTP_USER")
    password = os.getenv("AIRFLOW__SMTP__SMTP_PASSWORD")
    smtp_host = os.getenv("AIRFLOW__SMTP__SMTP_HOST")
    smtp_port = int(os.getenv("AIRFLOW__SMTP__SMTP_PORT", 587))

    recipient = os.getenv(
        "AIRFLOW__SMTP__SMTP_MAIL_TO", "sarimsikander24@gmail.com"
    )

    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    execution_date = context["execution_date"]
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    task_logs = get_task_logs(task_instance)
    dag_summary = get_dag_run_summary(context)

    info_warnings = []
    if task_logs:
        info_warnings = [
            line
            for line in task_logs.splitlines()
            if "INFO" in line or "WARNING" in line
        ]

    summary_html = (
        "<h3>DAG Run Summary</h3><table border='1' cellpadding='5'>"
    )
    summary_html += "<tr><th>Task</th><th>Status</th><th>Start Time</th><th>End Time</th><th>Duration</th></tr>"

    for task, details in dag_summary.items():
        row_style = ""
        if task == task_id:
            row_style = "background-color: #ffeb99;"
        elif details["status"] == "failed":
            row_style = "background-color: #ffcccc;"

        summary_html += f"<tr style='{row_style}'>"
        summary_html += f"<td>{task}</td>"
        summary_html += f"<td>{details['status']}</td>"
        summary_html += f"<td>{details['start_time']}</td>"
        summary_html += f"<td>{details['end_time']}</td>"
        summary_html += f"<td>{details['duration']}</td>"
        summary_html += "</tr>"

    summary_html += "</table>"

    if context.get("exception"):
        subject = f"Airflow Alert: DAG {dag_id} - Task {task_id} FAILED"
        status = "FAILED"
        error_message = str(context.get("exception", "Unknown error"))
        error_traceback = (
            "".join(
                traceback.format_tb(context.get("exception").__traceback__)
            )
            if context.get("exception")
            else "No traceback available"
        )

        error_html = f"""
        <h3>Error Details</h3>
        <div style="background-color: #ffcccc; padding: 10px; border-radius: 5px;">
            <strong>Error Message:</strong> {error_message}
        </div>
        <h4>Full Traceback:</h4>
        <pre style="background-color: #f5f5f5; padding: 10px; border-radius: 5px; overflow-x: auto; font-size: 12px;">
{error_traceback}
        </pre>
        """
    else:
        subject = (
            f"Airflow Success: DAG {dag_id} - Task {task_id} SUCCEEDED"
        )
        status = "SUCCEEDED"
        error_html = ""

    formatted_logs = f"""
    <h3>Task Logs</h3>
    <pre style="background-color: #f5f5f5; padding: 10px; border-radius: 5px; overflow-x: auto; max-height: 500px; font-size: 12px;">
{task_logs or "No logs available"}
    </pre>
    """

    if info_warnings:
        formatted_info = "<br>".join(info_warnings)
        info_html = f"""
        <h3>Info and Warning Messages</h3>
        <pre style="background-color: #f5f5f5; padding: 10px; border-radius: 5px; overflow-x: auto; max-height: 500px; font-size: 12px;">
{formatted_info}
        </pre>
        """
    else:
        info_html = "<h3>No Info or Warning Messages</h3>"

    html = f"""
    <h2>Airflow Task Status Update</h2>
    <p><strong>DAG</strong>: {dag_id}</p>
    <p><strong>Task</strong>: {task_id}</p>
    <p><strong>Execution Date</strong>: {execution_date}</p>
    <p><strong>Notification Time</strong>: {current_time}</p>
    <p><strong>Status</strong>: <span style="color: {'red' if status == 'FAILED' else 'green'};">{status}</span></p>

    {summary_html}
    {error_html}
    {formatted_logs}
    {info_html}

    <p style="font-size: 12px; color: #666;">
        <i>This is an automated message from your Airflow system.</i>
    </p>
    """

    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(html, "html"))

    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(sender, password)
        server.send_message(msg)
        server.quit()
        print(
            f"Email notification sent for {dag_id}.{task_id} - Status: {status}"
        )
    except Exception as e:
        print(f"Failed to send email: {str(e)}")


def send_dag_success_email(context):
    context["exception"] = None
    send_task_email(context)


def send_dag_failure_email(context):
    send_task_email(context)
