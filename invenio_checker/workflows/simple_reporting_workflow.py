"""Showcase a sample workflow definition."""

from invenio.workflows.definitions import WorkflowBase
from ..tasks.simple_reporting_workflow_tasks import report_error


class sample_workflow(WorkflowBase):

    """This is a Simpe reporting workflow."""

    workflow = [report_error]
    title = "Simpe reporting workflow"
