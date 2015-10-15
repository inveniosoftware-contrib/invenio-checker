"""Showcase a sample workflow definition."""

from invenio_workflows.definitions import WorkflowBase
from ..tasks.simple_reporting_workflow_tasks import report_error


class simple_reporting_workflow(WorkflowBase):

    """This is a Simpe reporting workflow."""

    workflow = [report_error]
    title = "Simpe reporting workflow"


__all__ = ('simple_reporting_workflow',)
