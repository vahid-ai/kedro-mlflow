"""
Pipeline definition for data processing.
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import load_raw_data, clean_data, transform_with_ray

def create_pipeline(**kwargs) -> Pipeline:
    """Creates the data processing pipeline.

    Args:
        kwargs: Ignore any potential unused parameters.

    Returns:
        The data processing pipeline.
    """
    return pipeline(
        [
            node(
                func=load_raw_data,
                inputs="raw_cic_ids_data", # Matches catalog entry
                outputs="intermediate_raw_table", # Temporary output name
                name="load_raw_data_node",
            ),
            node(
                func=clean_data,
                inputs="intermediate_raw_table", # Input is the output of previous node
                outputs="cleaned_data_table",   # Output of the cleaning step
                name="clean_data_node",
            ),
            node(
                func=transform_with_ray,
                inputs="cleaned_data_table",
                outputs="ray_dataset", # Output Ray Dataset
                name="transform_with_ray_node",
            ),
        ],
        namespace="data_processing",
        inputs="raw_cic_ids_data",
        outputs="ray_dataset" # Final output is now the Ray Dataset
    ) 