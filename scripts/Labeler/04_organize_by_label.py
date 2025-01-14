import os
import argparse
from collections import defaultdict
from pmc_oa.label_utils.label_propagation import load_label_dict
from pmc_oa.utils.json_utils import save_json

def create_subset_json(
        root: str, 
        out: str,
        subsets:list[str]=["other", "commercial", "noncommercial"]):
    
    # Loop through each subset and process labels
    for subset in subsets:
        label_dict: dict[str, int] = load_label_dict(os.path.join(root, subset))

        # Initialize a defaultdict with list as the default factory
        result_dict = defaultdict(list)

        # Populate the defaultdict
        for key, value in label_dict.items():
            result_dict[value].append(key)

        # Convert back to a regular dictionary and sort
        sorted_result_dict = dict(sorted(result_dict.items()))

        # Save the JSON file
        save_json(sorted_result_dict, os.path.join(out, f"{subset}.json"))

if __name__ == "__main__":
    # Argument parser setup
    parser = argparse.ArgumentParser(description="Process subsets by taxonomy.")
    parser.add_argument(
        "--root",
        type=str,
        default="/pasteur/u/ale9806/Repositories/pmc-oa/DinoFeatures/",
        help="Root directory containing subset folders."
    )
    parser.add_argument(
        "--out",
        type=str,
        default="/pasteur/u/ale9806/Repositories/pmc-oa/subsets_by_taxonomy",
        help="Output directory for the JSON files."
    )
    args = parser.parse_args()

    # Run the function with command-line arguments
    create_subset_json(args.root, args.out)