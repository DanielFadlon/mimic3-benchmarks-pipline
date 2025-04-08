from src.readers import DecompensationReader, InHospitalMortalityReader, LengthOfStayReader, MultitaskReader, PhenotypingReader



def get_reader(task, set_type):
    dataset_dir = f"data/{task}/{set_type}" if set_type != 'val' else  f"data/{task}/train" # val-set is a slice from the train-set
    listfile = f"data/{task}/{set_type}_listfile.csv"

    if task == "decompensation":
        return DecompensationReader(dataset_dir=dataset_dir, listfile=listfile)

    if task == "in-hospital-mortality":
        return InHospitalMortalityReader(dataset_dir=dataset_dir, listfile=listfile)

    if task == "length-of-stay":
        return LengthOfStayReader(dataset_dir=dataset_dir, listfile=listfile)

    if task == "multitask":
        return MultitaskReader(dataset_dir=dataset_dir, listfile=listfile)

    if task == "phenotyping":
        return PhenotypingReader(dataset_dir=dataset_dir, listfile=listfile)
