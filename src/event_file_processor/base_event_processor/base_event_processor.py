
from typing import Optional
from src.gsw_string_values_handler import number_to_gsw_str
from src.event_file_processor.event_file_processor import EventFileProcessor
from src.file_utils import load_json

class BaseEventProcessor(EventFileProcessor):
    def __init__(self,
                 task: str,
                 set_type: str,
                 interval_size: int,
                 agg_function_by_column_json_path: Optional[str] = "configs/event_agg_function_by_column.json",
                ):
        super().__init__(task, set_type)
        self.interval_size = interval_size
        self.agg_function_by_column = load_json(agg_function_by_column_json_path)



    def process_instance(self, instance):
        super().process_instance(instance)
        df = super().episode_numpy_to_dataframe(instance)
        if self.interval_size:
            df['Hours'] = (df['Hours'] // self.interval_size) * self.interval_size
            # GSW to numeric
            for col in self.string_types_columns:
                df[col] = df[col].apply(lambda s: int(s[0]) if s is not None else s)

            agg_func_mapping = {col: self.agg_function_by_column.get(col, self.agg_function_by_column["default"]) for col in df.columns}
            df = df.groupby(['Hours']).agg(agg_func_mapping).reset_index(drop=True)
            # Back to GSW str value
            for col in self.string_types_columns:
                df[col] = df[col].apply(lambda n: number_to_gsw_str(col, n, with_num_in_str=True))

        df = df.sort_values(by=['Hours'])

        # Handle label: TODO extract to function
        match self.task:
            case 'in-hospital-mortality':
                df['label'] = instance['y']
            case 'multitask':
                # Mask = 1 & label = 1
                ihm_label = instance['ihm'][1] and  instance['ihm'][2]
                # Decompensation During the case
                decomp_label = any(instance['decomp'][1])
                #Length of stay >= 14 days
                los_label = instance['los'][1][0] > 330
                labels_res = {
                    'ihm': ihm_label,
                    'decomp': decomp_label,
                    'los': los_label,
                }
                phenos = load_json("configs/phenotyping.json")
                res_phenos = {phenos[i]: pheno_i_label for i, pheno_i_label in enumerate(instance['pheno'])}
                labels_res.update(res_phenos)

                for name, label in labels_res.items():
                    df[f'label_{name}'] = label
            case _:
                raise Exception(f"Task {self.task} is not supported")

        return df

