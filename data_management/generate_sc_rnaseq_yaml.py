import os
import yaml
import sys

yamlData = {
    "package_type": "Single-cell RNA-Seq",
    "tis": "Michigan/Broad/Princeton",
    "data_generators": "Rajasree Menon",
    "dataset_description": ""
}
experiments = []

if len(sys.argv) == 1:
    print("Error. Please specify directory: python3 generate_sc_rnaseq_yaml.py /path/to/bulk/upload")
    exit(1)
    
dir = sys.argv[1]
for root, dirs, files in os.walk(dir):
    if root == dir:
        continue
    sample_id = os.path.split(root)[1]
    experiment = {
        "internal_experiment_id": sample_id,
        "files": []
    }
    for file in files:
        experiment['files'].append({
            'redcap_id': sample_id,
            'spectrack_sample_id': sample_id,
            'relative_file_path_and_name': sample_id + '/' + file,
            'file_metadata': ""
        })
    experiments.append({
        "experiment": experiment
    })
yamlData["experiments"] = experiments
with open(os.path.join(dir, 'bulk-manifest.yaml'), 'w') as file:
    yaml.dump(yamlData, file)
