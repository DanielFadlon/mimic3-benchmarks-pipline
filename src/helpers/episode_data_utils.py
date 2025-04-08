from typing import List
import numpy as np
import pandas as pd
from src.helpers.gsw_string_values_handler import unique_handle_for_non_numeric_gsw


def episode_numpy_to_dataframe(episode_data: np.array, gsw_string_types_columns: List[str]):
    columns = [h.replace(' ', '_') for h in episode_data['header']]
    numeric_columns = [col for col in columns if col not in gsw_string_types_columns]
    non_numeric_cols = gsw_string_types_columns

    df = pd.DataFrame(episode_data['X'], columns=columns)
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce', axis=0)

    # ASUMMING that the NON NUMERIC COLUMNS are Glascow_comma_scale
    df[non_numeric_cols] = df[non_numeric_cols].replace('', None)
    df = unique_handle_for_non_numeric_gsw(df)

    df['episode'] = episode_data['name'][:-4] # Remove '.csv' suffix
    return df
