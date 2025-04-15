"""
Nodes for the data processing pipeline.
"""

import ibis
import logging
import re
import pyarrow as pa
import ray
# from typing import Callable, Any, Dict # No longer needed

logger = logging.getLogger(__name__)

# Helper function for renaming
def to_snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    # Replace spaces and slashes with underscores
    s3 = re.sub(r'[ /]', '_', s2)
    # Remove any characters that are not alphanumeric or underscore
    s4 = re.sub(r'[^a-z0-9_]', '', s3)
    # Collapse multiple underscores
    s5 = re.sub(r'_+', '_', s4)
    return s5

def load_raw_data(raw_data_table: ibis.Table) -> ibis.Table:
    """Loads the raw data using the custom Ibis TableDataset and logs basic info.

    Args:
        raw_data_table: An Ibis Table expression provided by the custom dataset.

    Returns:
        The same Ibis Table expression.
    """
    # partitioned_input = loader_func() # Removed manual loading logic
    # filepaths = list(partitioned_input.keys())

    # if not filepaths:
    #     raise ValueError("No input files found matching the S3 pattern.")

    # logger.info(f"Found {len(filepaths)} files to load from S3.")
    # logger.info(f"First few files: {filepaths[:3]}")

    try:
        # Connection and loading is handled by the dataset now
        # con = ibis.duckdb.connect()
        # con.con.execute("INSTALL httpfs; LOAD httpfs;")
        # raw_data_table = con.read_csv(...)

        logger.info(f"Raw data schema: {raw_data_table.schema()}")
        # count = raw_data_table.count().execute()
        # logger.info(f"Raw data row count: {count}")
        logger.info("Successfully loaded raw data via custom Ibis TableDataset.")

    except Exception as e:
        logger.error(f"Error processing loaded raw data table: {e}")
        raise

    return raw_data_table 

def clean_data(raw_table: ibis.Table) -> ibis.Table:
    """Cleans the raw data table.
    - Renames columns to snake_case.
    - Drops the 'filename' column.
    - Replaces infinite values in flow_byts_s and flow_pkts_s with NA.
    - Casts label column to string.
    Args:
        raw_table: The raw Ibis table expression.

    Returns:
        The cleaned Ibis table expression.
    """
    logger.info(f"Original columns: {raw_table.columns}")

    # Rename columns to snake_case
    rename_mapping = {to_snake_case(col): col for col in raw_table.columns}
    rename_mapping = {new: old for new, old in rename_mapping.items() if new != old}

    if rename_mapping:
        logger.info(f"Renaming mapping: {rename_mapping}")
        cleaned_table = raw_table.rename(**rename_mapping)
    else:
        cleaned_table = raw_table
    logger.info(f"Renamed columns: {cleaned_table.columns}")

    # Drop the filename column if it exists
    filename_col_name = to_snake_case('filename')
    if filename_col_name in cleaned_table.columns:
        cleaned_table = cleaned_table.drop(filename_col_name)
        logger.info("Dropped 'filename' column.")

    # --- Step 1: Cast potentially string columns to float --- 
    cols_to_cast_float = ['flow_byts_s', 'flow_pkts_s']
    cast_mutations = {}
    for col in cols_to_cast_float:
        if col in cleaned_table.columns:
            cast_mutations[col] = cleaned_table[col].cast('float64')
            logger.info(f"Adding cast to float64 for column '{col}'.")
        else:
             logger.warning(f"Column '{col}' not found for float casting.")

    if cast_mutations:
        cleaned_table = cleaned_table.mutate(**cast_mutations)
        logger.info("Applied float casting mutations.")
        logger.info(f"Schema after casting: {cleaned_table.schema()}")

    # --- Step 2: Replace infinite/NaN values with None (NULL) --- 
    cols_to_check_inf = ['flow_byts_s', 'flow_pkts_s']
    non_finite_mutations = {}
    inf = ibis.literal(float('inf'))
    neg_inf = ibis.literal(float('-inf'))

    for col in cols_to_check_inf:
        if col in cleaned_table.columns:
            # Check if the column is now numeric before calling isnan()
            if isinstance(cleaned_table[col], (ibis.expr.types.NumericColumn, ibis.expr.types.FloatingColumn)):
                is_non_finite_cond = (
                    cleaned_table[col].isnan() |
                    (cleaned_table[col] == inf) |
                    (cleaned_table[col] == neg_inf)
                )
                non_finite_mutations[col] = (
                    ibis.case()
                    .when(is_non_finite_cond, None) # Use None for NULL
                    .else_(cleaned_table[col])
                    .end()
                    .cast("float64") # Ensure type consistency
                )
                logger.info(f"Added mutation rule for non-finite values in '{col}'.")
            else:
                logger.warning(f"Column '{col}' is not numeric after casting, skipping non-finite check.")
        else:
            logger.warning(f"Column '{col}' not found for non-finite check.")

    # --- Step 3: Cast Label column --- 
    label_mutation = {}
    if 'label' in cleaned_table.columns:
        label_mutation['label'] = cleaned_table['label'].cast('string')
        logger.info("Added mutation rule for casting 'label' column to string.")
    else:
         logger.warning("Column 'label' not found for casting.")

    # --- Step 4: Apply non-finite and label mutations --- 
    final_mutations = {**non_finite_mutations, **label_mutation}
    if final_mutations:
        cleaned_table = cleaned_table.mutate(**final_mutations)
        logger.info("Applied final mutations for non-finite replacement and label casting.")

    logger.info(f"Final cleaned data schema: {cleaned_table.schema()}")
    return cleaned_table 

def transform_with_ray(cleaned_table: ibis.Table) -> ray.data.Dataset:
    """Converts the cleaned Ibis table to a Ray Dataset and performs a count.

    Args:
        cleaned_table: The cleaned Ibis table expression.

    Returns:
        A Ray Dataset containing the data.
    """
    logger.info("Starting Ray transformation node.")
    ray_ds = None
    try:
        # Initialize Ray locally
        # ignore_reinit_error=True is useful for iterative development in notebooks/testing
        # In production, consider more robust Ray cluster management
        if not ray.is_initialized():
             ray.init(ignore_reinit_error=True)
             logger.info("Initialized Ray locally.")
        else:
             logger.info("Ray already initialized.")

        # Execute Ibis query to get PyArrow table
        logger.info("Executing Ibis query to fetch data into Arrow format...")
        arrow_table = cleaned_table.to_pyarrow()
        logger.info(f"Fetched data into Arrow table with schema: {arrow_table.schema}")
        logger.info(f"Arrow table size: {arrow_table.nbytes / (1024*1024):.2f} MB")

        # Create Ray Dataset from Arrow table
        logger.info("Creating Ray Dataset from Arrow table...")
        # Consider specifying parallelism strategy if needed for large data
        ray_ds = ray.data.from_arrow(arrow_table)
        logger.info("Successfully created Ray Dataset.")

        # Perform a simple Ray operation (e.g., count)
        count = ray_ds.count()
        logger.info(f"Ray Dataset contains {count} rows.")

        # Perform further Ray/Dask transformations here in the future
        # e.g., ray_ds = ray_ds.map_batches(...) 
        #       dask_df = ray_ds.to_dask()
        #       processed_dask_df = dask_df.persist() # Example Dask computation

    except Exception as e:
        logger.error(f"Error during Ray transformation: {e}")
        raise
    # finally:
        # Shutdown Ray - careful if other nodes/processes need it
        # Consider managing Ray lifecycle via hooks or dedicated setup/teardown nodes
        # if ray.is_initialized():
        #    logger.info("Shutting down Ray...")
        #    ray.shutdown()

    if ray_ds is None:
        raise RuntimeError("Ray dataset creation failed.")

    logger.info("Finished Ray transformation node.")
    return ray_ds 