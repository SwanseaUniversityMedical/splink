from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from splink.internals.input_column import InputColumn
from splink.internals.splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .database_api import TrinoAPI


class TrinoDataFrame(SplinkDataFrame):
    db_api: TrinoAPI

    def __init__(self, df_name, physical_name, db_api):
        super().__init__(df_name, physical_name, db_api)
        self._catalog = db_api._catalog
        self._db_schema = db_api._db_schema
        self.physical_name = f"{self.physical_name}"

    @property
    def columns(self) -> list[InputColumn]:
        sql = f"""
        SELECT column_name
        FROM {self._catalog}.information_schema.columns
        WHERE table_schema = '{self._db_schema}'
          AND table_name = '{self.physical_name}'
        ORDER BY ordinal_position
        """
        res = self.db_api._execute_sql_against_backend(sql).mappings().all()
        cols = [r["column_name"] for r in res]

        return [InputColumn(c, sqlglot_dialect_str="trino") for c in cols]

    def validate(self):
        if not isinstance(self.physical_name, str):
            raise ValueError(
                f"{self.templated_name} is not a string dataframe.\n"
                "Trino Linker requires input data"
                " to be a string containing the name of the"
                " Trino table."
            )

        sql = f"""
        SELECT table_name
        FROM {self._catalog}.information_schema.tables
        WHERE table_schema = '{self._db_schema}'
          AND table_name = '{self.physical_name}'
        """

        res = self.db_api._execute_sql_against_backend(sql).mappings().all()
        if len(res) == 0:
            raise ValueError(
                f"{self.physical_name} does not exist in the Trino catalog/schema provided.\n"
                "Trino Linker requires input data"
                " to be a string containing the name of a"
                " table that exists in the provided catalog and schema."
            )

    def _drop_table_from_database(self, force_non_splink_table=False):
        self._check_drop_table_created_by_splink(force_non_splink_table)
        self.db_api.delete_table_from_database(self.physical_name)

    def as_record_dict(self, limit=None):
        fq_name = (
            f"{self.db_api.fully_qualified_schema}."
            f'{self.db_api._quote_identifier(self.physical_name)}'
        )
        sql = f"""
        SELECT *
        FROM {fq_name}
        """
        if limit:
            sql += f" LIMIT {limit}"
        res = self.db_api._execute_sql_against_backend(sql).mappings().all()
        return [dict(r) for r in res]