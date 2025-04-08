from typing import List, Optional
import pandas as pd

from src.file_utils import load_json
from src.helpers.gsw_string_values_handler import number_to_gsw_str


def group_episode_df_to_intervals(
        df: pd.DataFrame,
        interval_size: int,
        col_name_of_interval_time: str,
        gsw_string_types_columns: List[str],
        agg_function_by_column_json_path: str
    ):

    agg_function_by_column = load_json(agg_function_by_column_json_path)

    df[col_name_of_interval_time] = df['Hours'].transform(lambda x: int((x // interval_size) * interval_size))
    # GSW to numeric
    for col in gsw_string_types_columns:
        df[col] = df[col].apply(lambda s: int(s[0]) if s is not None else s)
    agg_func_mapping = {col: agg_function_by_column.get(col, agg_function_by_column["default"]) for col in df.columns}
    df = df.groupby([col_name_of_interval_time]).agg(agg_func_mapping).reset_index(drop=True)
    # Back to GSW str value
    for col in gsw_string_types_columns:
        df[col] = df[col].apply(lambda n: number_to_gsw_str(col, n))

    return df


