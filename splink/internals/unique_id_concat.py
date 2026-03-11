from __future__ import annotations

from splink.internals.input_column import InputColumn

CONCAT_SEPARATOR = "-__-"


def _composite_unique_id_from_nodes_sql(
    unique_id_cols: list[InputColumn], table_prefix: str | None = None
) -> str:
    """
    Returns:
        str: e.g. 'cast(l."source_dataset" as varchar) || -__- || cast(l."unique_id" as varchar)'
    """
    if table_prefix:
        table_prefix = f"{table_prefix}."
    else:
        table_prefix = ""

    cols = [f"cast({table_prefix}{c.name} as varchar)" for c in unique_id_cols]

    return f" || '{CONCAT_SEPARATOR}' || ".join(cols)


def _composite_unique_id_from_edges_sql(unique_id_cols, l_or_r, table_prefix=None):
    """
    Returns:
        str: e.g. 'cast("source_dataset_l" as varchar) || -__- || cast("unique_id_l" as varchar)'
    """

    if table_prefix:
        table_prefix = f"{table_prefix}."
    else:
        table_prefix = ""

    if l_or_r == "l":
        cols = [f"cast({table_prefix}{c.name_l} as varchar)" for c in unique_id_cols]
    if l_or_r == "r":
        cols = [f"cast({table_prefix}{c.name_r} as varchar)" for c in unique_id_cols]
    if l_or_r is None:
        cols = [f"cast({table_prefix}{c.name} as varchar)" for c in unique_id_cols]

    return f" || '{CONCAT_SEPARATOR}' || ".join(cols)
