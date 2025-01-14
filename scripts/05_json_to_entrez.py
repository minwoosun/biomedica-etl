import os
import json
import csv
import argparse
import xmltodict
import time
import argparse
from Bio import Entrez
from typing import List, Dict, Any, Optional, Union, Set


def load_json(file_path: str) -> Optional[List[Dict[str, Any]]]:
    """
    Load a JSON file that contains a list of dictionaries.
    
    :param file_path: Path to the JSON file
    :return: List of dictionaries
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, list):
       #          print(f"Loaded {len(data)} dictionaries.")
                return data
            else:
                raise ValueError("The JSON file does not contain a list of dictionaries.")
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except ValueError as ve:
        print(f"Value Error: {ve}")
    return []


def fetch_pubmed_info(pmid: str, email: str, api: str) -> dict:

    """
    Fetch PubMed information for a given PubMed ID (PMID) and return it as a dictionary.

    This function uses the Entrez efetch API to retrieve PubMed data in XML format, 
    which is then converted into a Python dictionary using the `xmltodict` library.

    Parameters
    ----------
    pmid : str
        The PubMed ID (PMID) for which information is to be fetched.

    Returns
    -------
    dict
        A dictionary representation of the PubMed record fetched using the given PMID.
    
    Examples
    --------
    >>> pubmed_info = fetch_pubmed_info("12345678")
    >>> print(pubmed_info)
    {'PubmedArticleSet': {'PubmedArticle': {...}}}

    Notes
    -----
    - You need to set the `Entrez.email` to a valid email address for NCBI API access.
    - Make sure to have `xmltodict` and `Bio.Entrez` properly installed and configured.
    
    """
    # Set your email
    Entrez.email = email
    Entrez.api_key = api
    
    # Fetch data using the PubMed ID (pmid)
    handle = Entrez.efetch(db="pubmed", id=pmid, rettype="xml", retmode="text")
    record = handle.read()
    handle.close()
    
    # Convert XML to dictionary
    pubmed_dict = xmltodict.parse(record)

    # 1 second delay
    time.sleep(1)
    
    return pubmed_dict


def extract_mesh_terms(dictionary: dict) -> list[str]:
    """
    Fetches PubMed information for a given PubMed ID (PMID) and returns it as a dictionary.

    Parameters:
    - pmid (str): PubMed ID (PMID) of the article to fetch.
    - email (str): Email address for PubMed Entrez API access.
    - api (str): API key for PubMed Entrez API access.

    Returns:
    - dict: A dictionary containing the PubMed data in XML format, parsed into a Python dictionary.
    """
    mesh_dictionary = dictionary.get('MedlineCitation', {}).get('MeshHeadingList', {})
     
    if mesh_dictionary is None:
        return []  # Return an empty list if mesh_dictionary is None

    text_values = []
    
    # Iterate through each MeshHeading entry
    for mesh_heading in mesh_dictionary.get('MeshHeading',{}):
        # Extract #text from DescriptorName
        try: 
            if '#text' in mesh_heading.get('DescriptorName', {}):
                text_values.append(mesh_heading['DescriptorName']['#text'])
        except:
            continue
        
    return text_values


def extract_reference_list(pubmed_dict, error_list):
    """
    Safely extracts the ReferenceList from pubmed_dict and logs errors if any occur.
    Args:
        pubmed_dict (dict): The dictionary containing PubMed data.
        error_log_path (str): Path to the error log file.
    Returns:
        list: Reference list if successful, otherwise an empty list.
    """
    try:
        ref_list = pubmed_dict.get('PubmedData', {}).get('ReferenceList', {}).get('Reference', [])
        return ref_list
    except Exception as e:
        # Log the error to the error log file
        error_message = f"Error: {str(e)}\nPubMed Dictionary: {pubmed_dict}\n\n"
        error_list.append(error_message)
        return error_list  # Return an empty list if an error occurs


def extract_pmids_pmcids(data: Optional[List[Dict[str, Any]]]) -> Dict[str, List[str]]:
    """
    Extract 'pmid' and 'pmcid' values from a list of dictionaries.

    :param data: A list of dictionaries (can be None)
    :return: A dictionary with keys 'pmid' and 'pmcid', each containing a list of values.
    """
    result = {"pmid": [], "pmcid": []}

    if data is None:
        print("No data provided.")
        return result

    for entry in data:
        if 'pmid' in entry:
            result['pmid'].append(entry['pmid'])
        if 'accession_id' in entry:
            result['pmcid'].append(entry['accession_id'])

   #  print(f"Extracted {len(result['pmid'])} PMIDs and {len(result['pmcid'])} PMCIDs.")
    return result


def search_nested_dict(d: dict, key: str) -> Optional[dict]:
    """
    Recursively searches for a key in a nested dictionary.

    Parameters:
    - d (dict): The dictionary to search.
    - key (str): The key to search for.

    Returns:
    - dict | None: The value associated with the key if found, otherwise None.
    """
    # Check if the key exists at the current level of the dictionary
    if key in d:
        return d[key]
    
    # If the key is not at the current level, loop through the values
    # and check if any value is another dictionary. If so, recurse into it.
    for k, v in d.items():
        if isinstance(v, dict):
            result = search_nested_dict(v, key)
            if result is not None:
                return result
    # Return None if the key is not found
    return None


def ensure_abstract(abstract: Union[str, dict, None]) -> str:
    """
    Ensures that the abstract is returned as a string. If the abstract is None, 
    it returns an empty string. If the abstract is a dictionary with a '#text' field, 
    it returns that value. If it is another dictionary, it combines its values using 
    the `concat_abstract` function. If it's a string, it returns the string as is.

    Args:
    abstract (Union[str, dict, None]): The abstract to be processed, which can be 
    a string, dictionary, or None.

    Returns:
    str: The processed abstract as a single string.
    """
    if abstract is None:
        return ""
        
    elif isinstance(abstract, str):
        return abstract
    
    elif isinstance(abstract, dict):
        if '#text' in abstract:
            return abstract['#text']
        else:
            return concat_abstract(abstract)

    else:
        return str(abstract)


def concat_abstract(abstract: dict) -> str:
    """
    Combines the values of a dictionary into a single string. It handles lists 
    and nested dictionaries by recursively concatenating their elements or values. 
    If a value is a list, its elements are joined into a string. If a value is a 
    nested dictionary, it is processed with `ensure_abstract`.

    Args:
    abstract (dict): The abstract dictionary to be concatenated.

    Returns:
    str: The combined values of the dictionary as a single string.
    """
    combined_values = []
    for key, value in abstract.items():
        if isinstance(value, list):
            # If the value is a list, join its elements into a string
            combined_values.append(' '.join(map(str, value)))
        elif isinstance(value, dict):
            # Recursively handle nested dictionaries
            combined_values.append(ensure_abstract(value))
        else:
            # If the value is not a list or dict, append it as a string
            combined_values.append(str(value))
    return ' '.join(combined_values)


def json_to_entrez(json_path: str, out_path: str, email: str, api: str) -> None:
    """
    Update a JSON file with data retrieved from the Entrez API.

    This function reads a JSON file containing article metadata, extracts PMIDs,
    retrieves additional information from the Entrez API, and updates the original
    JSON file with the retrieved data. If API calls fail, the function retries with
    a specified delay.

    Parameters:
    -----------
    json_path : str
        Path to the JSON file containing article metadata.
    out_path : str
        Path to the JSON file with Entrez data that will get outputted
    email : str
        Email address required for Entrez API requests (for usage tracking).
    api : str
        API key for Entrez, used to increase the request rate limit.

    Raises:
    -------
    Exception
        If all attempts to retrieve data from the Entrez API fail.
    """

    max_attempts=10
    delay=1

    # load json and get ids
    dictionaries = load_json(json_path)
    ids = extract_pmids_pmcids(dictionaries)
    pmids = ids['pmid']
    pmids_notNone = [pmid for pmid in pmids if pmid is not None] 

    if not pmids_notNone:
        return

    # make entrez efetch api call (200 articles in one batch)
    for attempt in range(max_attempts):
        print(attempt)
        try:
            # Attempt to fetch PubMed info
            data_entrez = fetch_pubmed_info(pmid=pmids, email=email, api=api)
            break
        except Exception as e:
            if attempt < max_attempts - 1:
                time.sleep(delay)  # Wait before retrying
            else:
                data_entrez = None  # Return None if all attempts fail

    articles = data_entrez['PubmedArticleSet']['PubmedArticle']

    if isinstance(articles, dict):
        articles = [articles]

    # for loop to extract data
    for article in articles:
        try:
            mesh = extract_mesh_terms(article)
        except:
            mesh = None

    # dictionary with pmid key and entrez data as value
    articles_dict = {}
    for i, pmid in enumerate(pmids_notNone):
        articles_dict[pmid] = articles[i]
    
    for dictionary in dictionaries:
        
        pmid = dictionary['pmid']
        
        if pmid is not None:
            article = articles_dict[pmid]
    
            # extract info
            abstract = search_nested_dict(article, 'AbstractText')
            abstract = ensure_abstract(abstract)
            mesh = extract_mesh_terms(article)

            error_list = []
            reference_list = extract_reference_list(article, error_list)
            reference_count = len(reference_list) if reference_list else 0
    
            # replace fields in dictionary
            dictionary['abstract'] = abstract
            dictionary['mesh'] = mesh
            dictionary['reference_ids'] = reference_list
            dictionary['reference_count'] = reference_count
        else:
            continue

    # Save the updated list back to the original file
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(dictionaries, f, ensure_ascii=False, indent=4)

    # return dictionaries, articles_dict
    

def load_csv_file(csv_path: str) -> Set[str]:
    """Load a CSV file containing JSON paths into a set."""
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        return {row[0] for row in reader}


def load_log_file(log_file_path: str) -> Set[str]:
    """Load the log file containing completed file paths, creating it if it doesn't exist."""
    if not os.path.exists(log_file_path):
        print(f"Log file not found. Creating new log file at {log_file_path}.")
        # Create an empty log file
        with open(log_file_path, 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False, indent=4)
        return set()  # Return an empty set for completed files

    # Load existing log file
    with open(log_file_path, 'r', encoding='utf-8') as f:
        return set(json.load(f))


def save_log_file(log_file_path: str, completed_files: Set[str]) -> None:
    """Save the set of completed file paths to the log file."""
    with open(log_file_path, 'w', encoding='utf-8') as f:
        json.dump(list(completed_files), f, ensure_ascii=False, indent=4)


def json_to_entrez_from_csv(
    csv_path: str, 
    out_dir_path: str,
    completed_log_file_path: str, 
    error_log_file_path: str,
    email: str, 
    api: str
) -> None:
    """Process JSON files listed in a CSV, tracking progress in a log file."""
    all_files = load_csv_file(csv_path)
    completed_files = load_log_file(completed_log_file_path)
    error_files = load_log_file(error_log_file_path)

    os.makedirs(out_dir_path, exist_ok=True)

    # Determine which files are remaining to process
    remaining_files = all_files - completed_files
    total_files = len(remaining_files)

    for idx, json_path in enumerate(remaining_files, start=1):
        try:
            # set json output path
            filename = os.path.basename(json_path)
            out_json_path = out_dir_path + filename
            
             # Process the JSON file         
            json_to_entrez(json_path, out_json_path, email, api)
            print(f"[{idx}/{total_files}] Processed {json_path} ({(idx/total_files)*100:.2f}%)", flush=True)

            # Add to log and save
            completed_files.add(json_path)
            save_log_file(completed_log_file_path, completed_files)

        except Exception as e:
            # Log the error
            error_files.add(json_path)
            save_log_file(error_log_file_path, error_files)
            print(f"[{idx}/{total_files}] Error processing {json_path}: {e} ({(idx/total_files)*100:.2f}%)", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Process JSON files listed in a CSV, tracking progress in log files.")
    parser.add_argument(
        "--csv-path", 
        type=str, 
        required=True, 
        help="Path to the CSV file containing the list of JSON file paths."
    )
    parser.add_argument(
        "--out-dir-path", 
        type=str, 
        required=True, 
        help="Path to the output directory where processed files will be saved."
    )
    parser.add_argument(
        "--completed-log-file-path", 
        type=str, 
        required=True, 
        help="Path to the log file that tracks completed files."
    )
    parser.add_argument(
        "--error-log-file-path", 
        type=str, 
        required=True, 
        help="Path to the log file that tracks error files."
    )
    parser.add_argument(
        "--email", 
        type=str, 
        required=True, 
        help="Email address required for PubMed API requests."
    )
    parser.add_argument(
        "--api", 
        type=str, 
        required=True, 
        help="API key for PubMed Entrez API requests."
    )

    args = parser.parse_args()

    # Call the function with the parsed arguments
    json_to_entrez_from_csv(
        csv_path=args.csv_path,
        out_dir_path=args.out_dir_path,
        completed_log_file_path=args.completed_log_file_path,
        error_log_file_path=args.error_log_file_path,
        email=args.email,
        api=args.api,
    )


if __name__ == "__main__":
    
    main()
