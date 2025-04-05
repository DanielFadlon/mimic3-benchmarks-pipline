# Upadte path to root for script
import os
import re
import sys
import os.path as o
from typing import List, Optional
from enum import Enum
import concurrent.futures
import pandas as pd
import numpy as np
sys.path.append(o.abspath(o.join(o.dirname(sys.modules[__name__].__file__), "../..")))

from src.get_reader import get_reader
from src.period_file_processor.string_values_handler import GSW_STR_COLUMNS_NAMES, unique_handle_for_non_numeric_gsw

# Ignore all warnings
import warnings
warnings.filterwarnings("ignore")

class PeriodFileProcessor:
    def __init__(self,
                 task: str,
                 set_type: str
            ):
        self.reader = get_reader(task, set_type)
        self.task = task
        self.set_type = set_type
        self.listfile = pd.read_csv(f'data/{task}/{set_type}_listfile.csv')
        self.string_types_columns = GSW_STR_COLUMNS_NAMES

    def process_instance(self, instance):
        pass


    def worker(self, start, end):
        # Each thread works on a slice of the range
        batch_res = []
        for i in range(start, end):
            instance_i = self.reader.read_example(i)
            red_object = self.process_instance(instance_i)
            batch_res.append(red_object)
        return batch_res


    def process(self, output_dir: Optional[str] = None):
        res = []
        num_instances = self.listfile.shape[0]

        num_threads = 32  # Number of threads to use
        range_per_thread = num_instances // num_threads  # Each thread works on an equal range

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            print('START')

            # Submit tasks to the executor
            for i in range(num_threads):
                start = i * range_per_thread
                end = (i + 1) * range_per_thread if i != num_threads - 1 else num_instances  # Handle last thread's range
                print(f'dispatch - {start} - {end}')
                futures.append(executor.submit(self.worker, start, end))

            # Wait for all futures to complete and gather results
            for future in concurrent.futures.as_completed(futures):
                print('aggregate results')
                res.extend(future.result())  # Add results to the final list

        res_df = pd.DataFrame(res)

        # Handle time
        res_df = res_df.rename(columns={'Hours': 'TimeFromHospFeat'})
        res_df['TimeFromHospFeat'] = res_df['TimeFromHospFeat'].astype(int)
        res_df['TimeFromHosp'] = pd.to_timedelta(res_df['TimeFromHospFeat'], unit='h')

        res_df = res_df.set_index(['episode', 'TimeFromHosp'], drop=True)
        res_df['set_type'] = self.set_type

        output_dir = output_dir if output_dir else f"data/{self.task}/processed_by_period"
        res_df.to_parquet(os.path.join(output_dir, f'{self.set_type}.parquet'))
        print("FINISH - ", self.set_type)


    @staticmethod
    def to_intervals(df, interval_size=6):
        print("Before intervals - size: ", df.shape[0])
        df['interval'] = df.groupby('stay')['period_length'].transform(lambda x: (x // interval_size).astype(int))
        res = df.groupby(['stay', 'interval']).agg({
            'y_true': lambda x: int((x == 1).any()),  # 1 if any label is 1
            'period_length': 'max'  # max p in the interval
        }).reset_index()
        print("After intervals - size: ", res.shape[0])
        return res


    def episode_numpy_to_dataframe(self, episode_data: np.array):
        columns = [h.replace(' ', '_') for h in episode_data['header']]
        numeric_columns = [col for col in columns if col not in self.string_types_columns]
        non_numeric_cols = self.string_types_columns

        df = pd.DataFrame(episode_data['X'], columns=columns)
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce', axis=0)

        # ASUMMING that the NON NUMERIC COLUMNS are Glascow_comma_scale
        df[non_numeric_cols] = df[non_numeric_cols].replace('', None)
        df = unique_handle_for_non_numeric_gsw(df)

        df['episode'] = episode_data['name'][:-4] # Remove '.csv' suffix
        return df
