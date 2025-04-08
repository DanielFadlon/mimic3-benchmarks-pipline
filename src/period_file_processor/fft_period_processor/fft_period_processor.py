from typing import Optional
from src.period_file_processor.period_interval_utils import group_episode_df_to_intervals
from src.helpers.episode_data_utils import episode_numpy_to_dataframe
from src.period_file_processor.period_file_processor import PeriodFileProcessor


class FFTPeriodProcessor(PeriodFileProcessor):
    def __init__(self,
                 task: str,
                 set_type: str,
                 interval_size: Optional[str] = None,
                 agg_function_by_column_json_path: Optional[str] = "configs/event_agg_function_by_column.json"
                ):
        super().__init__(task, set_type, interval_size)
        self.interval_size = interval_size
        self.agg_function_by_column_json_path = agg_function_by_column_json_path

    def process_instance(self, instance):
        super().process_instance(instance)
        df = episode_numpy_to_dataframe(instance, self.string_types_columns)
        if self.interval_size:
            df = group_episode_df_to_intervals(
                df,
                self.interval_size,
                col_name_of_interval_time="tmp_hours",
                gsw_string_types_columns=self.string_types_columns,
                agg_function_by_column_json_path=self.agg_function_by_column_json_path
            )
            df = df.drop(columns=['tmp_hours'])


        if self.task in ["in-hospital-mortality", "decompensation"]:
            df['label'] = instance['y']
        elif self.task in ['phenotyping', 'multitask']:
            pass # TODO

        return df.ffill().iloc[-1]
