import os
from datasets import Dataset
import pandas as pd

subfolders = ["flan_fsnoopt_data", "flan_fsopt_data", "flan_zsnoopt_data", "flan_zsopt_data"]
chose_paraquets_count = [18,42,8,9]
dataset_index = 1

dataset_name = subfolders[dataset_index]
selected_count = chose_paraquets_count[dataset_index]

filtered_datasets_folder = "filtered_datasets"
filtered_folder_path = os.path.join(filtered_datasets_folder, dataset_name)

subset_repo_id = "0xAIT/flan-subset"

for i in range(selected_count):
    file = f"{i}.parquet"
    print(file)
    file_path = os.path.join(filtered_folder_path, file)
    df = pd.read_parquet(file_path)
    dataset = Dataset.from_pandas(df)
    print(f"Pushing {file} to hub")
    dataset.push_to_hub(subset_repo_id, config_name=dataset_name, split=dataset_name)


