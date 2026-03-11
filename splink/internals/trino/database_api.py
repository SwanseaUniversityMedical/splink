from __future__ import annotations

import logging
import math
from typing import Union, List, Any

import trino
import pandas as pd
from sqlalchemy import CursorResult, text
from sqlalchemy.engine import Engine

from splink.internals.database_api import AcceptableInputTableType, DatabaseAPI
from splink.internals.dialects import (
    TrinoDialect,
)
import datetime as dt
import math
from decimal import Decimal
from typing import Any
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_string_dtype,
)

from .dataframe import TrinoDataFrame
from .trino_helpers.trino_helpers import (
    create_temporary_trino_connection,
    validate_trino_connection,
)
from ..misc import ensure_is_list

logger = logging.getLogger(__name__)

import re

def _rewrite_float_casts_for_trino(sql: str) -> str:
    sql = re.sub(r"\bAS\s+FLOAT8\b", "AS DOUBLE", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bAS\s+FLOAT\b", "AS DOUBLE", sql, flags=re.IGNORECASE)

    # gamma_* ||  -> cast(gamma_* as varchar) ||
    sql = re.sub(
        r'(\bgamma_[A-Za-z0-9_]+\b)\s*\|\|',
        r'CAST(\1 AS VARCHAR) ||',
        sql,
        flags=re.IGNORECASE,
    )

    # || gamma_*  -> || cast(gamma_* as varchar)
    sql = re.sub(
        r'\|\|\s*(\bgamma_[A-Za-z0-9_]+\b)',
        r'|| CAST(\1 AS VARCHAR)',
        sql,
        flags=re.IGNORECASE,
    )

    # quoted unique id columns in concatenations
    sql = re.sub(
        r'(\b"unique_id_[lr]"\b)\s*\|\|',
        r'CAST(\1 AS VARCHAR) ||',
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r'\|\|\s*(\b"unique_id_[lr]"\b)',
        r'|| CAST(\1 AS VARCHAR)',
        sql,
        flags=re.IGNORECASE,
    )

    # unquoted unique id columns if they ever appear
    sql = re.sub(
        r'(\bunique_id_[lr]\b)\s*\|\|',
        r'CAST(\1 AS VARCHAR) ||',
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r'\|\|\s*(\bunique_id_[lr]\b)',
        r'|| CAST(\1 AS VARCHAR)',
        sql,
        flags=re.IGNORECASE,
    )

    sql = re.sub(
        r'\bCAST\s*\(\s*CAST\(([^)]+?) AS VARCHAR\)\s+AS VARCHAR\s*\)',
        r'CAST(\1 AS VARCHAR)',
        sql,
        flags=re.IGNORECASE,
    )

    return sql

class TrinoAPI(DatabaseAPI[CursorResult[Any]]):
    sql_dialect = TrinoDialect()

    def __init__(
        self,
        engine: Engine,
        catalog: str = "iceberg",
        schema: str = "splink",
        schema_location: str = "s3a://splink/"
    ):
        super().__init__()
        if not isinstance(engine, Engine):
            raise ValueError(
                "You must supply a sqlalchemy engine to create a TrinoAPI."
            )

        self._engine = engine
        self._catalog = catalog
        self._db_schema = schema
        self._schema_location = schema_location

        self._create_splink_schema()

    @property
    def fully_qualified_schema(self) -> str:
        return ".".join([
            self._quote_identifier(self._catalog),
            self._quote_identifier(self._db_schema),
        ])

    def _execute_sql_against_backend(
            self,
            final_sql: str,
            templated_name: str = None,
            physical_name: str = None,
    ) -> CursorResult[Any]:
        final_sql = _rewrite_float_casts_for_trino(final_sql)
        with self._engine.begin() as con:
            return con.execute(text(final_sql))

    def _create_splink_schema(self) -> None:
        if self._schema_location:
            sql = f"""
            CREATE SCHEMA IF NOT EXISTS {self.fully_qualified_schema}
            WITH (location = '{self._schema_location}')
            """
        else:
            sql = f"CREATE SCHEMA IF NOT EXISTS {self.fully_qualified_schema}"

        self._execute_sql_against_backend(sql)

    def delete_table_from_database(self, name: str) -> None:
        fq_name = f"{self.fully_qualified_schema}.{self._quote_identifier(name)}"
        try:
            self._execute_sql_against_backend(f"DROP VIEW IF EXISTS {fq_name}")
        # TODO: what exception would trino throw if you ran a DROP TABLE against a view?
        except Exception:
            pass
        self._execute_sql_against_backend(f"DROP TABLE IF EXISTS {fq_name}")

    def table_to_splink_dataframe(
            self,
            templated_name: str,
            physical_name: str,
    ) -> TrinoDataFrame:
        return TrinoDataFrame(templated_name, physical_name, self)

    def table_exists_in_database(self, table_name: str) -> bool:
        sql = f"""
        SELECT 1
        FROM {self._catalog}.information_schema.tables
        WHERE table_schema = '{self._db_schema}'
        AND table_name = '{table_name}'
        """
        rec = self._execute_sql_against_backend(sql).fetchall()
        return len(rec) > 0

    def _table_registration(
            self,
            input: AcceptableInputTableType,
            table_name: str,
    ) -> None:
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        try:
            import pyarrow as pa
            if isinstance(input, pa.Table):
                input = input.to_pandas()
        except ImportError:
            pass

        if not isinstance(input, pd.DataFrame):
            raise TypeError(
                "Trino table registration currently supports pandas DataFrame, "
                "dict, list[dict], and pyarrow.Table."
            )

        self.delete_table_from_database(table_name)

        fq_name = f"{self.fully_qualified_schema}.{self._quote_identifier(table_name)}"

        if input.empty:
            columns_sql = ", ".join(
                f"{self._quote_identifier(col)} {self._pandas_dtype_to_trino(dtype)}"
                for col, dtype in input.dtypes.items()
            )
            sql = f"CREATE TABLE {fq_name} ({columns_sql})"
            self._execute_sql_against_backend(sql)
            return

        values_sql = []
        for row in input.itertuples(index=False, name=None):
            row_sql = ", ".join(self._python_value_to_trino_literal(v) for v in row)
            values_sql.append(f"({row_sql})")

        aliases = ", ".join(self._quote_identifier(c) for c in input.columns)

        sql = f"""
        CREATE TABLE {fq_name} AS 
        SELECT *
        FROM (
            VALUES {", ".join(values_sql)}
        ) AS t({aliases})
        """
        self._execute_sql_against_backend(sql)

    @property
    def accepted_df_dtypes(self):
        accepted = [pd.DataFrame]
        try:
            import pyarrow as pa
            accepted.append(pa.Table)
        except ImportError:
            pass
        return accepted

    def _python_value_to_trino_literal(self, value: Any) -> str:
        if value is None:
            return "NULL"

        # pandas / numpy missing values
        try:
            if pd.isna(value):
                return "NULL"
        except TypeError:
            # Some values (e.g. lists) don't behave nicely with pd.isna
            pass

        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"

        if isinstance(value, int) and not isinstance(value, bool):
            return str(value)

        if isinstance(value, float):
            if math.isnan(value):
                return "NULL"
            if math.isinf(value):
                raise ValueError("Cannot serialise +/-inf to Trino literal")
            return repr(value)

        if isinstance(value, Decimal):
            return f"DECIMAL '{str(value)}'"

        if isinstance(value, dt.datetime):
            if value.tzinfo is not None and value.utcoffset() is not None:
                # Trino supports TIMESTAMP WITH TIME ZONE literals
                s = value.isoformat(sep=" ", timespec="microseconds")
                return f"TIMESTAMP '{s}'"
            else:
                s = value.isoformat(sep=" ", timespec="microseconds")
                return f"TIMESTAMP '{s}'"

        if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
            return f"DATE '{value.isoformat()}'"

        if isinstance(value, dt.time):
            if value.tzinfo is not None and value.utcoffset() is not None:
                s = value.isoformat(timespec='microseconds')
                return f"TIME '{s}'"
            else:
                s = value.isoformat(timespec='microseconds')
                return f"TIME '{s}'"

        # basic array support
        if isinstance(value, (list, tuple)):
            inner = ", ".join(self._python_value_to_trino_literal(v) for v in value)
            return f"ARRAY[{inner}]"

        # basic dict -> JSON literal as VARCHAR cast to JSON
        if isinstance(value, dict):
            import json
            s = json.dumps(value, ensure_ascii=False).replace("'", "''")
            return f"CAST('{s}' AS JSON)"

        s = str(value).replace("'", "''")
        return f"'{s}'"

    def _quote_identifier(self, name: str) -> str:
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    def _pandas_dtype_to_trino(self, dtype) -> str:
        if is_integer_dtype(dtype):
            return "BIGINT"
        if is_float_dtype(dtype):
            return "DOUBLE"
        if is_bool_dtype(dtype):
            return "BOOLEAN"
        if is_datetime64_any_dtype(dtype):
            return "TIMESTAMP"
        if is_string_dtype(dtype):
            return "VARCHAR"
        return "VARCHAR"


