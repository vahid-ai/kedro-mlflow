"""Project pipelines."""

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

# Import the specific pipeline creation function
from kedro_cyber_defense.pipelines.data_processing.pipeline import create_pipeline as dp_pipeline

def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    # pipelines = find_pipelines()
    pipelines = {}
    pipelines["dp"] = dp_pipeline()
    pipelines["__default__"] = pipelines["dp"]
    return pipelines
