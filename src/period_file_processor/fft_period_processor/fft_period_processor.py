from src.period_file_processor.period_file_processor import PeriodFileProcessor


class FFTPeriodProcessor(PeriodFileProcessor):
    def __init__(self, task, set_type):
        super().__init__(task, set_type)

    def process_instance(self, instance):
        super().process_instance(instance)
        df = super().episode_numpy_to_dataframe(instance)

        # Extract to function
        if self.task in ["in-hospital-mortality", "decompensation"]:
            df['label'] = instance['y']
        elif self.task in ['phenotyping', 'multitask']:
            pass # TODO

        return df.ffill().iloc[-1]
