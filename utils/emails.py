import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

from utils.util import base_path

load_dotenv(dotenv_path=f"{base_path()}/airflow.env")


def send_task_email(context):
    sender = os.getenv("AIRFLOW__SMTP__SMTP_USER")
    password = os.getenv("AIRFLOW__SMTP__SMTP_PASSWORD")
    smtp_host = os.getenv("AIRFLOW__SMTP__SMTP_HOST")
    smtp_port = int(os.getenv("AIRFLOW__SMTP__SMTP_PORT", 587))

    recipient = os.getenv("AIRFLOW__SMTP__SMTP_MAIL_TO", "sarimsikander24@gmail.com")

    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    execution_date = context["execution_date"]
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if context.get("exception"):
        subject = f"Airflow Alert: DAG {dag_id} - Task {task_id} FAILED"
        status = "FAILED"
        error_message = str(context.get("exception", "Unknown error"))
    else:
        subject = f"Airflow Success: DAG {dag_id} - Task {task_id} SUCCEEDED"
        status = "SUCCEEDED"
        error_message = "N/A"

    html = f"""
    <h2>Airflow Task Status Update</h2>
    <p><strong>DAG</strong>: {dag_id}</p>
    <p><strong>Task</strong>: {task_id}</p>
    <p><strong>Execution Date</strong>: {execution_date}</p>
    <p><strong>Notification Time</strong>: {current_time}</p>
    <p><strong>Status</strong>: {status}</p>
    <p><strong>Error</strong>: {error_message}</p>
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
        print(f"Email notification sent for {dag_id}.{task_id} - Status: {status}")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")


def send_dag_success_email(context):
    context["exception"] = None
    send_task_email(context)


def send_dag_failure_email(context):
    send_task_email(context)
