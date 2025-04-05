# Period File Processor

The **PeriodFileProcessor** is a flexible, multi-threaded framework for processing healthcare episodes stored in files. It is designed to extract structured, time-based data slices ("periods") from a large set of patient files, producing a consolidated dataset per set type (e.g., train, test, val).

Specifically, for **Decompensation** task.
This class serves as a base class. To use it, subclass it and implement the `process_instance()` method.

---

## 🔍 Overview

The processor reads a list of patient episodes along with their associated period lengths from a list file. For each one, it extracts structured features from defined time windows, producing data suitable for downstream modeling or analysis.

---

## 🧱 Base Class

### `PeriodFileProcessor`

#### Constructor
```python
PeriodFileProcessor(task: str, set_type: str)
```

- `task`: name of the task folder under `data/`
- `set_type`: one of `train`, `test`, or `val`

#### Methods to Implement

```python
def process_instance(self, instance):
    ...
```

> You must override this in your subclass.
> It receives one episode instance and should return a dictionary or pandas series, one per period.

---

## ⚙️ Built-in Methods

### `process(output_dir: Optional[str] = None)`
- Uses 32 threads to process all episode files
- Returns a DataFrame indexed by `['episode', 'TimeFromHosp']`
- Saves output as a `.parquet` file

### `episode_numpy_to_dataframe(episode_data: np.array)`
- Parses raw data into a cleaned `DataFrame`

---

## 🚀 How to Extend

To create your own processor:

```python
from src.period_file_processor.base import PeriodFileProcessor

class MyCustomPeriodProcessor(PeriodFileProcessor):
    def process_instance(self, instance):
        # Your logic here to process the file
        return [
            {
                "Hours": 12,
                "feature_1": value_1,
                ...
                "y_true": label,
            },
            ...
        ]
```

Then run:

```python
processor = MyCustomPeriodProcessor(task="hip_fracture", set_type="train")
processor.process()
```

---

## 📤 Output Format

The final output is saved to:

```
data/<task>/processed_by_period/<set_type>.parquet
```

Indexed by:

- `episode`
- `TimeFromHosp` (as `Timedelta`)

Includes metadata columns like:

- `TimeFromHospFeat` (in hours)
- `set_type` (train/test/val)

---

## 📌 Dependencies

- `pandas`
- `numpy`
- `concurrent.futures`
- Custom modules: `get_reader`, `string_values_handler`

---
