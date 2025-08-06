from typing import Dict, Any, Optional
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.models import TaskInstance, DagRun, DAG
from urllib.parse import quote
import traceback
from airflow.models import Variable


def slack_failure_callback(context: Dict[str, Any]) -> None:
    """
    Callback function to send Slack notifications on DAG failures.

    Args:
        context: The Airflow context dictionary containing task instance,
                DAG, and runtime information
    """
    task_instance: TaskInstance = context["task_instance"]
    dag: DAG = context["dag"]
    dag_id: str = dag.dag_id
    task_id: str = task_instance.task_id
    dag_run: DagRun = context["dag_run"]
    execution_date: str = str(dag_run.execution_date)
    webserver_url: str = Variable.get(
        "airflow_url", default_var="http://localhost:7277"
    )
    log_url: str = f"{webserver_url}/log?dag_id={dag_id}&task_id={task_id}&execution_date={quote(execution_date)}"

    container_output: str = "No container logs available"
    try:
        error: Optional[Any] = context.get("exception")
        if error and hasattr(error, "logs"):
            container_output = "".join(error.logs)
    except Exception as e:
        container_output = f"Error retrieving container logs: {str(e)}"

    error_traceback: Optional[Any] = context.get("exception") or context.get("error")
    if error_traceback and container_output != "No container logs available":
        tb: str = str(error_traceback)
    else:
        tb: str = str(traceback.format_exc())

    # Reason: Rendering in Slack looks poorly.
    # Solution: Cutting message to be shorter, max 500 Symbols.
    if len(tb) > 500:
        tb = tb[-500:]
    if len(container_output) > 500:
        container_output = container_output[-500:]

    message: str = f"""
    *DAG Failed*: `{dag_id}`
    *Run ID*: {dag_run.run_id}
    *Task*: {task_id}
    *Execution Time*: {execution_date}
    *Traceback*: ```{tb}```
    *Container Output*: ```{container_output}```
    *<{log_url}|View Logs>*
    """

    slack_channel = Variable.get("slack_channel", default_var="#analytics_alerts")

    slack_operator: SlackAPIPostOperator = SlackAPIPostOperator(
        task_id=f"slack_notification_{task_id}",
        slack_conn_id="",
        text=message,
        channel=slack_channel,
    )
    slack_operator.execute(context=context)