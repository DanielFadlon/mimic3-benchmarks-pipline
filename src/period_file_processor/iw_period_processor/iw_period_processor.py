from typing import Optional, Callable
import pandas as pd
from src.helpers.episode_data_utils import episode_numpy_to_dataframe
from src.period_file_processor.period_interval_utils import group_episode_df_to_intervals
from src.file_utils import load_json
from src.period_file_processor.period_file_processor import PeriodFileProcessor

HOURS_TEMP_COLUMN_NAME = 'hours_tmp'

class IWPeriodProcessor(PeriodFileProcessor):
    def __init__(self,
                 task: str,
                 set_type: str,
                 history_volume: int,
                 interval_size: Optional[str] = None,
                 agg_function_by_column_json_path: Optional[str] = "configs/event_agg_function_by_column.json",
                 feature_name_mapping_path: Optional[str] = "configs/feature_name_mapping.json"):

        super().__init__(task, set_type, interval_size)
        self.history_volume = history_volume
        self.interval_size = interval_size
        self.agg_function_by_column_json_path = agg_function_by_column_json_path
        self.feature_name_mapping = load_json(feature_name_mapping_path)

    def process_instance(self, instance):
        super().process_instance(instance)
        df = episode_numpy_to_dataframe(instance, self.string_types_columns)

        df[HOURS_TEMP_COLUMN_NAME] = df['Hours'].astype(int)
        if self.interval_size:
                df = group_episode_df_to_intervals(
                    df,
                    self.interval_size,
                    col_name_of_interval_time=HOURS_TEMP_COLUMN_NAME,
                    gsw_string_types_columns=self.string_types_columns,
                    agg_function_by_column_json_path=self.agg_function_by_column_json_path
                )
        df = df.sort_values(by=['Hours'])
        history_text = ""
        for feat in df.columns:
            if feat in [HOURS_TEMP_COLUMN_NAME, 'Hours', 'episode']:
                continue

            history_data = self._get_history_for_feature(df, feat)[-self.history_volume:]
            history_text += self.feature_name_mapping.get(feat, feat)
            history_text += ': '
            for hist_obj in history_data:
                val = hist_obj['value']
                time = hist_obj['time']
                history_text += f'{val} (t={time}), '
            history_text = history_text[:-2] + '.\n' if len(history_data) > 0 else history_text + "None.\n"


        current_time_passed_in_hours = df[HOURS_TEMP_COLUMN_NAME].iloc[-1]
        prefix = f"The patient's hospital journey began at time t=0 hours and has now reached t={current_time_passed_in_hours} hours."
        text = f'{prefix}\n\n{history_text}\n'

        res_instance = pd.Series({'episode': df['episode'].iloc[0], 'text': text, 'Hours': df['Hours'].iloc[-1]})

        # Extract to function
        if self.task in ["in-hospital-mortality", "decompensation"]:
            res_instance['label'] = instance['y']
        elif self.task in ['phenotyping', 'multitask']:
            pass # TODO

        return res_instance

    @staticmethod
    def _get_history_for_feature(sorted_df: pd.DataFrame, feature_col_name: str):
        previous_value = -1
        history = []

        for i, instance_i in sorted_df.iterrows():
            # Floor to interval hours if needed
            time_i = int(instance_i[HOURS_TEMP_COLUMN_NAME])
            # First instance - init the history list
            if previous_value == -1:
                history.append({"time": time_i, "value": instance_i[feature_col_name]})
            # If the value has changed, add the new value to the history list
            elif (not pd.isna(instance_i[feature_col_name])) and instance_i[feature_col_name] != previous_value:
                history.append({"time": time_i, "value": instance_i[feature_col_name]})

            previous_value = instance_i[feature_col_name]

        return history
