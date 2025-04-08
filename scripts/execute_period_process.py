import argparse
import sys
import os.path
from typing import List, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(sys.modules[__name__].__file__), "..")))
from src.period_file_processor.iw_period_processor.iw_period_processor import IWPeriodProcessor
from src.period_file_processor.fft_period_processor.fft_period_processor import FFTPeriodProcessor

def get_sets(set_type: str) -> List[str]:
    return ['train', 'val', 'test'] if set_type == 'all' else [set_type]


def execute_fft(task: str, set_type: str, output_path: str, interval_size: int):
    for set_type in get_sets(set_type):
        fft_pro = FFTPeriodProcessor(
            task=task,
            set_type=set_type,
            interval_size=interval_size
        )

        fft_pro.process(output_path)


def execute_iw(task: str, set_type: str, output_path: str, history_volume: Optional[int] = 3):
    for set_type in get_sets(set_type):
        fft_pro = IWPeriodProcessor(
            task=task,
            set_type=set_type,
            history_volume=history_volume,
        )

        fft_pro.process(output_path)



def main():
    """
    Examples:
    FFT:
     - python script_name.py fft decompensation data/processed_data/decomp/fft/6h_interval all --interval_size 6

    IW:
     - python script_name.py iw decompensation data/processed_data/decomp/iw/history_3 all
     - python script_name.py iw decompensation data/processed_data/decomp/iw/history_5 all --history_volume 5
    """
    use_hardcoded = False

    if use_hardcoded:
        method = "fft"
        task = "decompensation"
        output_path = "data/processed_data/decomp/fft/6h_interval"
        set_type = "all"
        interval_size = 6
        history_volume = 3  # only used for IW
    else:
        parser = argparse.ArgumentParser(description="Run FFT or IW period processors.")
        parser.add_argument("method", choices=["fft", "iw"], help="Processing method to use.")
        parser.add_argument("task", help="Task name (e.g., 'decompensation').")
        parser.add_argument("output_path", help="Output path for processed data.")
        parser.add_argument("set_type", choices=["train", "val", "test", "all"], help="Data split to process.")
        parser.add_argument("--interval_size", type=int, help="Interval size (required for fft).")
        parser.add_argument("--history_volume", type=int, default=3, help="History volume (for iw).")

        args = parser.parse_args()
        method = args.method
        task = args.task
        output_path = args.output_path
        set_type = args.set_type
        interval_size = args.interval_size
        history_volume = args.history_volume


    if method == "fft":
        execute_fft(task, set_type, output_path, interval_size)
    elif method == "iw":
        execute_iw(task, set_type, output_path, history_volume)

main()
