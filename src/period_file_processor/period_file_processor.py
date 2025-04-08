# Upadte path to root for script
import os
from typing import Optional
import concurrent.futures
import pandas as pd

from src.helpers.get_reader import get_reader
from src.helpers.gsw_string_values_handler import GSW_STR_COLUMNS_NAMES

# Ignore all warnings
import warnings
warnings.filterwarnings("ignore")

class PeriodFileProcessor:
    def __init__(self,
                 task: str,
                 set_type: str,
                 interval_size: Optional[int] = None
            ):
        self.reader = get_reader(task, set_type)
        self.task = task
        self.set_type = set_type
        self.listfile = pd.read_csv(f'data/{task}/{set_type}_listfile.csv')
        self.string_types_columns = GSW_STR_COLUMNS_NAMES
        self.interval_size = interval_size

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

        if self.interval_size:
            res_df['TimeFromHospFeat'] = res_df['TimeFromHospFeat'].apply(lambda x: (x // self.interval_size) * self.interval_size)
        res_df['TimeFromHospFeat']
        res_df = res_df.set_index(['episode', 'TimeFromHosp'], drop=True)
        res_df['set_type'] = self.set_type

        output_dir = output_dir if output_dir else f"data/{self.task}/processed_by_period"
        res_df.to_parquet(os.path.join(output_dir, f'{self.set_type}.parquet'))
        print("FINISH - ", self.set_type)
