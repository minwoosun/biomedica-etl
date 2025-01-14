import os
import csv
import math
import pandas as pd


def generate_json_path_csv(directory_path: str, output_csv: str) -> None:
    """Generate a CSV file containing all JSON file paths from the given directory."""

    if os.path.exists(output_csv):
        print(f"{output_csv} already exists. Skipping generation.")
        return  # Skip if the CSV file already exists

    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["json_path"])  # Header
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith(".json"):
                    writer.writerow([os.path.join(root, file)])


def split_csv(input_csv: str, n_parts: int) -> None:
    """
    Splits a CSV file into n parts and saves each part in the same directory as the input file.

    Parameters:
    - input_csv (str): Path to the input CSV file.
    - n_parts (int): Number of parts to split the file into.
    """
    # Get the directory and file name of the input file
    input_dir = os.path.dirname(input_csv)
    input_name = os.path.basename(input_csv).rsplit('.', 1)[0]  # Remove file extension

    # Load the CSV file
    df = pd.read_csv(input_csv)
    total_rows = len(df)

    # Calculate the number of rows per file
    rows_per_part = math.ceil(total_rows / n_parts)

    # Split the DataFrame into parts
    for i in range(n_parts):
        start_idx = i * rows_per_part
        end_idx = min((i + 1) * rows_per_part, total_rows)  # Avoid index overflow
        part_df = df.iloc[start_idx:end_idx]

        # Construct the output file path
        output_file = os.path.join(input_dir, f"{input_name}_part_{i + 1}.csv")
        part_df.to_csv(output_file, index=False)

    print(f"Split {input_csv} into {n_parts} parts saved in {input_dir}.")


def main():

    # create entrez dir
    os.makedirs("./data/entrez/", exist_ok=True)
    os.makedirs("./data/entrez/logs", exist_ok=True)
    os.makedirs("./data/json_entrez/", exist_ok=True)

    # >>> convert harded coded path into relative later <<<
    original_json_commercial = "./data/json/"
    original_json_noncommercial = "./data/json/"
    original_json_other = "./data/json/"
    
    output_csv_path_commercial = "./data/entrez/commercial_json_paths.csv"
    output_csv_path_noncommercial = "./data/entrez/noncommercial_json_paths.csv"
    output_csv_path_other = "./data/entrez/other_json_paths.csv"
    
    generate_json_path_csv(directory_path=original_json_commercial,
                          output_csv=output_csv_path_commercial)
    
    generate_json_path_csv(directory_path=original_json_noncommercial,
                          output_csv=output_csv_path_noncommercial)
    
    generate_json_path_csv(directory_path=original_json_other,
                          output_csv=output_csv_path_other)


if __name__ == "__main__":

    main()


    # # split for parllel 
    # split_csv(output_csv_path_commercial, 3)   
    # split_csv(output_csv_path_noncommercial, 2)  

# python scripts/05a_file_path_batching.py