import argparse
import sys
import os.path
from typing import List, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(sys.modules[__name__].__file__), "..")))
from src.event_file_processor.base_event_processor.base_event_processor import BaseEventProcessor

def get_sets(set_type: str) -> List[str]:
    return ['train', 'val', 'test'] if set_type == 'all' else [set_type]

def execute_base(task: str, set_type: str, output_path: str, interval_size: int):
    for set_type in get_sets(set_type):
        base_pro = BaseEventProcessor(
            task=task,
            set_type=set_type,
            interval_size=interval_size
        )

        base_pro.process(output_path)


def main():
    """
     - python script_name.py fft decompensation data/processed_data/decomp/fft/6h_interval all --interval_size 6
    """
    use_hardcoded = True

    if use_hardcoded:
        task = "multitask"
        output_path = "tmp"
        set_type = "val"
        interval_size = 1
    else:
        parser = argparse.ArgumentParser(description="Run FFT or IW period processors.")
        parser.add_argument("task", help="Task name (e.g., 'decompensation').")
        parser.add_argument("output_path", help="Output path for processed data.")
        parser.add_argument("set_type", choices=["train", "val", "test", "all"], help="Data split to process.")
        parser.add_argument("--interval_size", type=int, help="Interval size (required for fft).")

        args = parser.parse_args()
        task = args.task
        output_path = args.output_path
        set_type = args.set_type
        interval_size = args.interval_size

    execute_base(task, set_type, output_path, interval_size)

main()
