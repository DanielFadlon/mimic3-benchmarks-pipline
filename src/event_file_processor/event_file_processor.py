import os
from typing import Optional
import concurrent.futures
import numpy as np
import pandas as pd

from src.helpers.gsw_string_values_handler import GSW_STR_COLUMNS_NAMES
from src.helpers.get_reader import get_reader

class EventFileProcessor:
    def __init__(self,
                 task: str,
                 set_type: str,
                ):
        self.reader = get_reader(task, set_type)
        self.task = task
        self.set_type = set_type
        self.listfile = pd.read_csv(f'data/{task}/{set_type}_listfile.csv')
        self.string_types_columns = GSW_STR_COLUMNS_NAMES


    def process_instance(self, instance) -> pd.DataFrame:
        pass


    def worker(self, start, end):
        # Each thread works on a slice of the range
        batch_res = []
        for i in range(start, end):
            instance_i = self.reader.read_example(i)
            red_object = self.process_instance(instance_i)
            batch_res.append(red_object)
        return pd.concat(batch_res, ignore_index=True)


    def process(self, output_dir: str):
        res_dfs_list = []
        num_cases = self.listfile.shape[0]

        num_threads = 32  # Number of threads to use
        range_per_thread = num_cases // num_threads  # Each thread works on an equal range


        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            print('START')

            for i in range(num_threads):
                start = i * range_per_thread
                end = (i + 1) * range_per_thread if i != num_threads - 1 else num_cases  # Handle last thread's range
                print(f'dispatch - {start} - {end}')
                futures.append(executor.submit(self.worker, start, end))

            # Wait for all futures to complete and gather results
            for future in concurrent.futures.as_completed(futures):
                print('aggregate results')
                res_dfs_list.append(future.result())


        res_df = pd.concat(res_dfs_list)
        # Handle time
        res_df = res_df.rename(columns={'Hours': 'TimeFromHospFeat'})
        res_df['TimeFromHospFeat'] = res_df['TimeFromHospFeat'].astype(int)
        res_df['TimeFromHosp'] = pd.to_timedelta(res_df['TimeFromHospFeat'], unit='h')

        res_df = res_df.set_index(['episode', 'TimeFromHosp'], drop=True)
        res_df['set_type'] = self.set_type

        res_df.to_parquet(os.path.join(output_dir, f'{self.set_type}.parquet'))
        print("FINISH - ", self.set_type)

