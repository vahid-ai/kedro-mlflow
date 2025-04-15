"""Custom Ibis Table Dataset for Kedro."""

import ibis
import ibis.expr.types as ir
from kedro.io.core import AbstractDataset, DatasetError
from typing import Any, Dict


class TableDataset(AbstractDataset[None, ir.Table]):
    """Loads data from an Ibis table expression based on backend connection
    and specified loading function (e.g., read_csv).
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        filepath: str,
        connection: Dict[str, Any],
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
    ):
        """Creates a new instance of TableDataset.

        Args:
            filepath: The path to the data file(s), potentially with wildcards.
            connection: Configuration for connecting to an Ibis backend.
                        Must include a 'backend' key (e.g., 'duckdb').
            load_args: Additional arguments passed to the Ibis backend's
                       loading function (e.g., read_csv arguments).
            save_args: Additional arguments passed to the Ibis backend's
                       saving function (e.g., create_table arguments).
                       Not used in this simplified load-only version.
        """
        self._filepath = filepath
        self._connection_config = connection.copy() # Keep a copy
        self._load_args = load_args.copy() if load_args else {}
        self._save_args = save_args.copy() if save_args else {}
        self._table = None # Cache for the loaded table expression

    def _load(self) -> ir.Table:
        """Loads the data as an Ibis table expression."""
        if self._table is not None:
            return self._table

        backend_name = self._connection_config.pop("backend", None)
        if not backend_name:
            raise DatasetError("'connection.backend' must be specified.")

        try:
            # Get the backend connection function (e.g., ibis.duckdb.connect)
            backend_connect_func = getattr(ibis, backend_name).connect
        except AttributeError as exc:
            raise DatasetError(f"Invalid Ibis backend specified: {backend_name}") from exc

        try:
            # Connect to the backend
            con = backend_connect_func(**self._connection_config)

            # Handle DuckDB specific S3 setup
            if backend_name == "duckdb":
                try:
                    con.con.execute("INSTALL httpfs; LOAD httpfs;")
                    # Add any other necessary DuckDB S3 config here if needed
                    # e.g., con.con.execute("SET s3_use_ssl=true;")
                except Exception as duckdb_exc:
                    # Log warning but proceed, httpfs might be pre-installed/loaded
                    self._logger.warning(
                        f"Could not install/load DuckDB httpfs extension: {duckdb_exc}. "
                        f"S3 access might fail if not configured globally."
                    )

            # Get the loading function (defaults to read_csv if not specified)
            # Note: Original code used file_format, this uses load_args['func'] for flexibility
            load_func_name = self._load_args.pop("func", "read_csv")
            load_func = getattr(con, load_func_name)

            # Load the table
            self._logger.info(
                f"Loading data from {self._filepath} using {backend_name}.{load_func_name}"
            )
            # Add union_by_name=True for schema flexibility
            load_options = self._load_args.copy()
            load_options['union_by_name'] = True 
            table_expr = load_func(self._filepath, **load_options)
            self._table = table_expr
            return table_expr

        except Exception as exc:
            raise DatasetError(f"Failed to load Ibis table: {exc}") from exc

    def _save(self, data: Any) -> None:
        """Saving is not implemented in this simplified version."""
        raise NotImplementedError("Saving is not implemented for this custom dataset.")

    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes the attributes of the dataset."""
        return dict(
            filepath=self._filepath,
            connection=self._connection_config,
            load_args=self._load_args,
            save_args=self._save_args,
        ) 