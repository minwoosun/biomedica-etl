
import os
import re
import io
import csv
import json
import hashlib
import xmltodict
import pandas as pd
import xml.etree.ElementTree as ET

from pathlib import Path
from lxml import etree
from datetime import datetime
from PIL import Image
from typing import List, Dict, Union, Optional


def build_json(data_dir: str, file_list_path: str, batch_size: int, license: str, log_files: dict[str, str] = None, log=True) -> None:
    """
    Builds JSON files containing metadata, figure captions, and image paths for articles 
    in the provided file list. This function downloads articles, extracts metadata and 
    figures, and saves them in a JSON format. It also logs completed and error files.

    Parameters:
    - data_dir (str): Path to the main data directory.
    - file_list_path (str): Path to the CSV file containing the list of articles to process.
    - ftp (ftplib.FTP): An active FTP connection to download article media.
    - log_files (dict[str, str]): Dictionary containing paths for log files (error and complete).
    - batch_size (int): Number of articles to process before creating a new JSON file.
    - email (str): Email address for PubMed Entrez API access.
    - api (str): API key for PubMed Entrez API access.
    - image_string (bool): If True, encode images as base64 strings and include them in the JSON output. Defaults to False.

    Returns:
    - None
    """

    # Set paths
    filelist_name = os.path.splitext(os.path.basename(file_list_path))[0]
    filelist_name = filelist_name.replace("_fixed", "")
    out_dir = Path(data_dir) / license / f"media_files/" 
    json_dir = Path(data_dir) / f'json/' / license 
    json_dir.mkdir(parents=True, exist_ok=True)

    # Initialize JSON file if it doesn't exist
    json_file_path = json_dir / f'{filelist_name}_0.json'
    if not Path(json_file_path).exists():
        print(f"Initializing new JSON file: {json_file_path}")
        with open(json_file_path, 'w') as f:
            json.dump([], f)

    # column names for log csv files
    error_columns = ['File', 'Citation', 'Accession_ID', 'Date', 'License', 'Traceback']
    complete_columns = ['Accession_ID', 'json_path']

    for article_row in path_generator(file_list_path):
        
        file_path = "/pub/pmc/" + article_row["File"]

        ###########################
        #    Extract meta data    #
        ###########################

        try:
            article = build_nxml_metadata(file_path=file_path, 
                                          article_row=article_row,
                                          data_dir=Path(data_dir), 
                                          out_dir=out_dir)
            
        except Exception as e:
            if log:
                article_row['Traceback'] = e
                create_log_file(log_files['error'], error_columns)
                append_row_to_csv(log_files['error'], article_row)
            
            # dict of None
            article = default_article_dict()

        #####################
        #   write to json   #
        #####################

        # Open the JSON file in read+write mode
        with open(json_file_path, 'r+') as f:
            # Load current content from JSON file
            try:
                f.seek(0)  # Reset file pointer to the start
                current_data = json.load(f) if Path(json_file_path).stat().st_size != 0 else []

            except json.JSONDecodeError as e:
                if log:
                    article_row['Traceback'] = e
                    create_log_file(log_files['error'], error_columns)
                    append_row_to_csv(log_files['error'], article_row)
                    # print(f"Error reading JSON: {e}")
                current_data = []

            # If batch size is reached, start a new file
            if len(current_data) == (batch_size-1):
                # batch_iter +=1
                batch_sub_index = get_next_index(json_dir=json_dir, filelist_name=filelist_name)
                json_file_path = json_dir / f'{filelist_name}_{batch_sub_index}.json'
                print(f"Starting new batch: {json_file_path}")
                with open(json_file_path, 'w') as new_file:
                    json.dump([], new_file)  # Initialize the new file

            # Append the processed article to the list
            current_data.append(article)

            # Move file pointer back to the start before writing the updated data
            f.seek(0)
            json.dump(current_data, f, indent=4)

            # Truncate to ensure no leftover data
            f.truncate()
        
        if log:
            log_complete = {'Acession_ID':article_row['Accession_ID'],
                            'json_path':json_file_path}
            create_log_file(log_files['complete'], complete_columns)
            append_row_to_csv(log_files['complete'], log_complete)


def get_next_index(json_dir: str, filelist_name: str) -> int:
    """
    Get the next version index for a given file list name within a directory.

    This function scans the specified directory for JSON files matching the 
    pattern '<filelist_name>_<batch_number>_<version>.json'. It extracts the 
    version number and returns the next available version index to prevent overwriting 
    existing files.

    Args:
        json_dir (str): The directory containing the JSON files.
        filelist_name (str): The base name of the file list.

    Returns:
        int: The next version index (i.e., max version found + 1). If no matching 
             files are found, returns 0.
    """
    # Pattern to match files of the form 'filelist_name_0_0.json'
    pattern = re.compile(rf'{re.escape(filelist_name)}_(\d+)\.json')
    print(pattern) 
    # Track the maximum version index found
    max_version = -1

    # Iterate over all files in the directory
    for filename in os.listdir(json_dir):
        match = pattern.match(filename)
        if match:
            # Extract the version index from the matched filename
            version = int(match.group(1))
            max_version = max(max_version, version)

    # Return the next version index (max version + 1)
    return max_version + 1


def path_generator(csv_file_path: str):
    """
    Generates dictionaries from each row of the CSV file containing article metadata.

    Parameters:
    - csv_file_path (str): Path to the CSV file containing the article data.

    Yields:
    - dict[str, str]: A dictionary for each row containing 'File', 'Citation', 'Accession_ID', 'Date', and 'License' keys.
    """
    with open(csv_file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield {
                'File': row['File'],
                'Citation': row['Citation'], 
                'Accession_ID': row['Accession_ID'],
                'Date': row['Date'],
                'License': row['License'],
                'PMID': row['PMID']
            }


def create_log_file_path(directory: str, batch_index: int, log_type: str, log_dir: str) -> str:
    """
    Generates the file path for log files (error or complete) for a specific batch.

    Parameters:
    - directory (str): Path to the batch directory.
    - batch_index (int): Index of the current batch.
    - log_type (str): Type of log file ('error' or 'complete').
    - log_dir (str): Directory where the log files will be saved.

    Returns:
    - str: The generated file path for the log file.
    """
    # Extract the last directory name
    last_directory = os.path.basename(os.path.normpath(directory))
    file_name = f'{last_directory}_{log_type}_batch_{batch_index}.csv'
    file_path = log_dir + file_name
    return file_path


def create_log_file(file_path: str, columns: list[str]) -> None:
    """
    Creates a CSV log file with specified columns.

    Parameters:
    - file_path (str): Path to the CSV file to be created.
    - columns (list[str]): List of column names for the CSV file.

    Returns:
    - None
    """
    # Check if the file already exists
    if os.path.exists(file_path):
        return
    else: 
        df = pd.DataFrame(columns=columns)
        df.to_csv(file_path, index=False)


def append_row_to_csv(file_path: str, row: dict[str, str]) -> None:
    """
    Appends a single row of data to an existing CSV file.

    Parameters:
    - file_path (str): Path to the existing CSV file.
    - row (dict[str, str]): A dictionary representing the row of data to append.

    Returns:
    - None
    """
    # Create a DataFrame with the row and append it to the CSV
    df = pd.DataFrame([row])
    df.to_csv(file_path, mode='a', header=False, index=False)

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#    Functions for extracting metadata from various sources   >
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

def build_nxml_metadata(file_path: str, article_row: dict[str, str], data_dir: Path, out_dir: Path) -> dict:
    """
    Extracts metadata and figure captions from an article, builds a metadata dictionary, 
    and returns it. The function downloads the article, extracts the image and figure data, 
    and stores it in a structured format.

    Parameters:
    - file_path (str): Path to the article file to be processed.
    - article_row (dict[str, str]): Dictionary containing article information such as 'Citation', 'License', 'Accession_ID', etc.
    - out_dir (Path): Directory where the article's media files will be downloaded and processed.

    Returns:
    - dict: A dictionary containing the article metadata, figures, captions, and context.
    """
    # build paths
    pmcid = Path(file_path).name.replace('.tar.gz', '')
    path_to_media = out_dir / pmcid
    path_to_nxml = get_nxml_path(path_to_media)
    path_to_images = get_image_paths(path_to_media)
    
    try:
        image_ids = get_image_identifier(path_to_images)
    except:
        image_ids = None

    #######################
    #      file list      #
    #######################

    try: 
        filelist_dict = extract_info_from_filelist_row(article_row)
    except:
        filelist_dict = {
        "accession_id": pmcid,
        "citation": None,
        "license": None,
        "date": None,
        "journal": None,
        "pmid": None,
        }

    ##################
    #   figure_set   #
    ##################

    try:
        figure_set = []
        
        if image_ids:
            # build figure set
            for index, id in enumerate(image_ids):
                if path_to_images:
                    try:
                        caption = extract_caption_from_nxml(path_to_nxml, id)
                    except:
                        caption = None
                    image_path = path_to_images[index] if path_to_images is not None else None
                else:
                    caption = None
                    image_path = None

                try:
                    hash = hash_image(image_path)
                except:
                    hash = None

                # build dictionary
                image_dict = {
                        'image_id':id if id else None,
                        'image_file_name': os.path.basename(image_path) if image_path else None,
                        'image_path': image_path if image_path else None,      
                        'caption': caption if caption else None,
                        'hash': hash
                }   
            #   image_dict['image_path'] = image_path if image_path else None        
                figure_set.append(image_dict)
    
    except:
        figure_set = None
    
    ##################
    #      nxml      #
    ##################
    
    # Parse the NXML file
    try:
        tree = etree.parse(path_to_nxml)
        root = tree.getroot()
        nxml = ''.join(root.itertext()) if root is not None else None
    except: 
        nxml = None

    # Extract meta data from nxml
    try:
        # if nxml file exists, should be able to generate
        nxml_metadata = extract_metadata_from_nxml(path_to_nxml)
    except:
        nxml_metadata = {
        'title': None,
        'keyword': None,
        'pmid': None,
        'article_categories': None,
        'abstract': None,
        'mesh': None,
        # 'reference_ids': None,
        # 'reference_count': None,
    }
        
    # if pmid exists from filelist don't worry
    if filelist_dict.get('pmid') is not None:
        nxml_metadata.pop('pmid', None)

    # Extract context
    try: 
        context_dict = extract_context(path_to_nxml)
    except: 
        context_dict = None

    ####################################
    #      build output dictionary     #
    ####################################

    article:dict = {}

    # Include data from file list
    article.update(filelist_dict)
    
    # Include data from nxml
    article.update(nxml_metadata)

    # Include figure set
    article['figure_set'] = figure_set

    # Include context
    article['context'] = context_dict

    # Include nxml (at the end for ease of reading)
    article['nxml'] = nxml

    return article


def log_pmcid(log_file_path: Path, pmcid: str) -> None:
    """
    Log a PMCID to the specified CSV log file. If the file doesn't exist, create it 
    and write the header. If it exists, append the new PMCID as a row.

    Args:
        log_file_path (Path): Path object representing the CSV log file.
        pmcid (str): The PMCID to log.
    """
    # Ensure the parent directory exists
    log_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Check if the log file already exists
    file_exists = log_file_path.is_file()

    # Open the file in append mode ('a'), creating it if necessary
    with log_file_path.open(mode='a', newline='') as log_file:
        writer = csv.writer(log_file)

        # If the file doesn't exist, write the header first
        if not file_exists:
            writer.writerow(['PMCID'])  # Write header

        # Write the new PMCID as a row
        writer.writerow([pmcid])


def extract_metadata_from_nxml(nxml_file_path: str) -> dict:
    """
    Extracts metadata from an NXML file, including title, keywords, funding information, 
    and PubMed data. This function also retrieves Mesh terms using the PubMed API.

    Parameters:
    - nxml_file_path (str): Path to the NXML file to extract metadata from.
    - email (str): Email address for PubMed Entrez API access.
    - api (str): API key for PubMed Entrez API access.

    Returns:
    - dict: A dictionary containing metadata such as title, abstract, keywords, mesh terms, and funding information.
    """   
    with open(nxml_file_path, 'r', encoding='utf-8') as file:
        xml_content = xmltodict.parse(file.read())
    
    try:
        title = search_nested_dict(xml_content, "title-group")['article-title'] or None
        keyword = search_nested_dict(xml_content, "kwd") or None
        pub_ids = search_nested_dict(xml_content, "article-id")
        pmid = next((item['#text'] for item in pub_ids if item.get('@pub-id-type') == 'pmid'), None)
        article_categories = search_nested_dict(xml_content, "article-categories") or None
    except:
        title = None
        keyword = None
        pub_ids = None
        pmid = None
        article_categories = None

    # clean up
    try:
        title = ensure_title(title)
    except:
        title = None

    # later fill None in with entrez api
    output_dict = {
        'title': title,
        'keyword': keyword,
        'pmid': pmid,
        'article_categories': article_categories,
        'abstract': None,
        'mesh': None,
        # 'reference_ids': None,
        # 'reference_count': None,
    }

    return output_dict


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


def ensure_unix(date_string: str) -> Optional[str]:
    """
    Convert a date string in the format 'YYYY Mon DD' to Unix time string.

    Args:
        date_string (str): The date string to convert.

    Returns:
        Optional[str]: The Unix time as a string, or None if the conversion fails.
    """

    if date_string is None:
        return None  # Handle None input at the beginning
            
    try:
        # Parse the date string in the format 'YYYY Mon DD'
        unix_time = int(datetime.strptime(date_string, '%Y %m %d').timestamp())
        return str(unix_time)
    except ValueError:
        # Return None if the date_string format is invalid
        return date_string


def ensure_title(title: Union[str, dict, None]) -> str:
    """
    Ensures that the title is returned as a string. If the title is None, 
    it returns an empty string. If the title is a dictionary, it combines 
    the values into a single string. If the title is a string, it returns the string as is.

    Args:
    title (Union[str, dict, None]): The title to be processed, which can be a string, 
    dictionary, or None.

    Returns:
    str: The processed title as a single string.
    """
    if title is None:
        return None

    elif isinstance(title, str):
        return title

    elif isinstance(title, dict):
        # Combine all values in the dictionary into a single string
        combined_values = []
        for key, value in title.items():
            if isinstance(value, list):
                # If the value is a list, join its elements into a string
                combined_values.append(' '.join(map(str, value)))
            else:
                # If the value is not a list, just append it as a string
                combined_values.append(str(value))
        return ' '.join(combined_values)
    
    else:
        return str(title)


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

    
def extract_caption_from_nxml(file_path: str, image_identifier: str) -> str:
    """
    Extracts the caption associated with a specific image in an NXML file.

    Parameters:
    - file_path (str): The path to the NXML file.
    - image_identifier (str): The identifier or file name of the image.

    Returns:
    - str: The caption text associated with the image or an empty string if no caption is found.
    """
    # Load the XML file
    tree = etree.parse(file_path)

    # Define the namespaces used in the XML file (adjust as necessary based on the XML file)
    namespaces = {
        'xlink': 'http://www.w3.org/1999/xlink'
    }

    # Find the graphic element by file name or some identifier
    graphic = tree.xpath(f"//graphic[@xlink:href='{image_identifier}']", namespaces=namespaces)

    # Get the parent figure element of this graphic
    figure = graphic[0].getparent() if graphic else None

    # Extract the caption text if a figure element was found
    if figure is not None:
        caption = figure.find('.//caption', namespaces=namespaces)
        caption_text = etree.tostring(caption, method='text', encoding='unicode').strip() if caption is not None else 'No caption found'
    else:
        # this error can happen when image exists but no caption (e.g. image of an equation)
        caption_text = ''

    return caption_text
    

def get_image_identifier(list_of_path_to_image: list[str]) -> list[str]:
    """
    Extracts image identifiers (file names without extensions) from a list of image file paths.

    Parameters:
    - list_of_path_to_image (list[str]): A list of image file paths.

    Returns:
    - list[str]: A list of image identifiers (file names without extensions).
    """
    try:
        if not list_of_path_to_image:
            return None

        # Extract file names without extension
        image_identifiers = [os.path.splitext(os.path.basename(path))[0] for path in list_of_path_to_image]
        return image_identifiers

    except Exception as e:
        return None


def get_image_paths(path_to_pmcid: str) -> Optional[List[str]]:
    """
    Retrieves all image file paths with a .jpg extension in a given directory.

    Parameters:
    - path_to_pmcid (str): Path to the directory containing the images.

    Returns:
    - list[str] | None: A list of image file paths or None if no images are found.
    """
    image_file_paths = []

    try:
        # Iterate over all entries in the directory
        for entry in os.listdir(path_to_pmcid):
            # Construct full path
            full_path = os.path.join(path_to_pmcid, entry)
            
            # Check if the entry is a file and ends with .jpg
            if os.path.isfile(full_path) and entry.endswith('.jpg'):
                image_file_paths.append(full_path)
        
        if not image_file_paths:
            return None
        
        return image_file_paths
    
    except Exception as e:
        return None
    
    
def get_nxml_path(path_to_pmcid: str) -> Optional[str]:
    """
    Retrieves the path to an .nxml file in the given directory.

    Parameters:
    - path_to_pmcid (str): Path to the directory containing the NXML files.

    Returns:
    - str | None: The path to the first .nxml file found or None if no .nxml files are present.
    """
    nxml_file_paths = []

    for entry in os.listdir(path_to_pmcid):
        full_path = os.path.join(path_to_pmcid, entry)
        
        # Check if the entry is a file and ends with .nxml
        if os.path.isfile(full_path) and entry.endswith('.nxml'):
            nxml_file_paths.append(full_path)
    
    # Check the number of .nxml files found
    if len(nxml_file_paths) == 0:
        return None
    elif len(nxml_file_paths) > 1:
        return nxml_file_paths[0] 
    else:
        return nxml_file_paths[0]


def extract_context(nxml_path: str, remove_redundant: bool=True) -> dict:
    """
    Extracts context surrounding figure references from an NXML file and maps it to the corresponding image IDs.
    """
    tree = ET.parse(nxml_path)
    root = tree.getroot()

    paragraphs = []
    id_map = map_rid_to_image(nxml_path)  # Map of rids to image IDs

    # Find all <xref> elements where ref-type is 'fig'
    for xref in root.findall(".//xref[@ref-type='fig']"):
        rid = normalize_id(xref.attrib.get('rid', None))  # Normalize the rid

        if rid and rid in id_map:
            parent_paragraph = find_parent(root, xref)
            if parent_paragraph is not None:
                cleaned_paragraph = clean_paragraph_except_xref(parent_paragraph, id_map)
                paragraphs.append({id_map[rid]: cleaned_paragraph})

    combined_dict = combine_paragraphs(paragraphs)

    # Remove duplicate paragraphs for each figure ID
    if remove_redundant:
        for key in combined_dict:
            combined_dict[key] = remove_duplicates(combined_dict[key])
        
    return combined_dict


def normalize_id(rid: Optional[str]) -> Optional[str]:
    """
    Normalizes the ID to ensure consistency between <xref> and <fig> elements.
    """
    if rid:
        return rid.strip().replace("-", ".")  # Normalize by replacing hyphens with dots
    return None


def find_parent(root, child):
    """
    Finds the parent paragraph or section of the given element.
    """
    for parent in root.iter():
        if child in list(parent):
            if parent.tag == 'p':  # Assuming <p> tags are used for paragraphs
                return parent
    return None


def clean_paragraph_except_xref(paragraph, rid_to_image_map):
    """
    Cleans the paragraph by preserving only <xref ref-type='fig'> tags.
    """
    cleaned_parts = []

    def process_element(elem):
        if elem.tag == 'xref' and elem.attrib.get('ref-type') == 'fig':
            rid = normalize_id(elem.attrib.get('rid'))
            if rid in rid_to_image_map:
                elem.set('rid', rid_to_image_map[rid])
                cleaned_parts.append(ET.tostring(elem, encoding='unicode', method='xml'))
        else:
            if elem.text:
                cleaned_parts.append(elem.text)
        
        for child in elem:
            process_element(child)

        if elem.tail:
            cleaned_parts.append(elem.tail)

    process_element(paragraph)
    return ''.join(cleaned_parts)


def combine_paragraphs(paragraphs):
    """
    Combines paragraphs into a dictionary.
    """
    combined_dict = {}
    for entry in paragraphs:
        for image_id, paragraph in entry.items():
            if image_id not in combined_dict:
                combined_dict[image_id] = []
            combined_dict[image_id].append(paragraph)
    return combined_dict


def map_rid_to_image(nxml_path: str) -> dict[str, str]:
    """
    Maps figure IDs (rids) to corresponding image file names from an NXML file.
    """
    tree = ET.parse(nxml_path)
    root = tree.getroot()

    rid_to_image = {}
    for fig in root.findall(".//fig"):
        rid = normalize_id(fig.attrib.get('id'))  # Normalize the fig ID
        graphic = fig.find(".//graphic")

        if rid and graphic is not None:
            image_path = graphic.attrib.get('{http://www.w3.org/1999/xlink}href', None)
            if image_path:
                image_file_with_ext = os.path.basename(image_path)
                rid_to_image[rid] = image_file_with_ext

    return rid_to_image


def remove_duplicates(paragraphs: List[str]) -> List[str]:
    """
    Removes duplicate paragraphs from a list while preserving order.
    
    Parameters:
    - paragraphs (List[str]): A list of paragraphs.
    
    Returns:
    - List[str]: A list of unique paragraphs.
    """
    seen = set()  # Track seen paragraphs
    unique_paragraphs = []

    for paragraph in paragraphs:
        if paragraph not in seen:
            unique_paragraphs.append(paragraph)
            seen.add(paragraph)

    return unique_paragraphs

def extract_journal(citation: str) -> Optional[str]:
    """
    Extracts the journal name from the citation string, accounting for parentheses and other cases.

    Args:
        citation (str): The citation string from which the journal needs to be extracted.

    Returns:
        Optional[str]: The extracted journal name if found, otherwise None.
    """
    # Updated regular expression to handle journal names with parentheses and spaces
    match = re.match(r"([A-Za-z\s().,-]+?)[.;]", citation)
    if match:
        return match.group(1).strip()
    return None

def extract_info_from_filelist_row(row: Dict[str, str]) -> Dict[str, Optional[str]]:
    """
    Extracts information from a dictionary row generated by path_generator.

    Args:
        row (Dict[str, str]): A dictionary containing article information with keys:
                              'File', 'Citation', 'Accession_ID', 'Date', and 'License'.

    Returns:
        Dict[str, Optional[str]]: A dictionary containing the extracted information
                                  (accession_id, citation, license, date, journal).
    """
    
    # If the row doesn't have the required fields, return None values
    if not row:
        return {
            'accession_id': None,
            'citation': None,
            'license': None,
            'date': None,
            'journal': None,
            'pmid':None,
        }
    
    # Extract the journal from the Citation (assuming extract_journal is defined elsewhere)
    try:
        journal = extract_journal(row['Citation'])
    except:
        journal = None

    # Pull date from citation
    try:
        date = extract_date_from_citation(row['Citation'])
    except:
        date = None

    try:
        date_unix = ensure_unix(date)
    except:
        date_unix = None
    
    # get data, if empty string assign None
    pmid = row.get('PMID')
    if pmid == "":
        pmid = None

    accession_id = row.get('Accession_ID')
    if accession_id == "":
        accession_id = None

    citation = row.get('Citation')
    if citation == "":
        citation = None   
    
    license = row.get('License')
    if license == "":
        license = None 

    # Create a dictionary for the article
    # .get returns None by default
    article: Dict[str, Optional[str]] = {
        'accession_id': accession_id,
        'citation': citation,
        'license': license,
        'date': date_unix,
        'journal': journal,
        'pmid': pmid
    }
    
    return article


def hash_image(image_path: str, max_size=(1000, 1000)) -> str:
    """
    Generate a SHA-256 hash of an image, resizing it to prevent memory issues.

    Parameters:
    ----------
    image_path : str
        The file path to the image to be hashed.
    max_size : tuple (int, int)
        Maximum size (width, height) to which the image will be resized.

    Returns:
    -------
    str
        The hexadecimal SHA-256 hash of the resized image.
    """
    # Open the image
    with Image.open(image_path) as image:
        # Resize the image in place while preserving aspect ratio
        image.thumbnail(max_size)

        # Convert CMYK to RGB if needed
        if image.mode == 'CMYK':
            image = image.convert('RGB')

        # Convert the image to a byte array
        return _hash_image_bytes(image)


def _hash_image_bytes(image: Image.Image) -> str:
    """
    Convert the image to PNG and generate a SHA-256 hash from its bytes.

    Parameters:
    ----------
    image : PIL.Image.Image
        The PIL Image object to be hashed.

    Returns:
    -------
    str
        The hexadecimal SHA-256 hash of the image.
    """
    with io.BytesIO() as byte_array:
        image.save(byte_array, format='PNG')
        image_bytes = byte_array.getvalue()

    # Generate SHA-256 hash
    hash_object = hashlib.sha256(image_bytes)
    return hash_object.hexdigest()


def extract_date_from_citation(citation: str, default_month: int = 1, default_day: int = 1) -> Optional[str]:
    # Step 1: Split the citation to find the title and the part after it
    parts = citation.split('. ', 1)  # Split at the first occurrence of '. '
    if len(parts) < 2:
        return None  # If the split fails, there's no date

    # Extract the part after the title, where the date should appear
    after_title = parts[1]

    # Step 2: Regex to match year, month/season, and day (if they appear)
    date_pattern = re.search(r'(\b\d{4}\b)(?:\s+([A-Za-z]{3,6}))?(?:\s+(\d{1,2}))?', after_title)

    if not date_pattern:
        print(f"Error logged for citation: {citation}")
        return None  # only 2 None in 4M

    # Step 3: Extract year, month/season, and day from the match
    year = int(date_pattern.group(1))
    month_or_season = date_pattern.group(2)
    day_str = date_pattern.group(3)

    # Step 4: Handle month, season, or default month
    if month_or_season in ['Spring', 'Summer', 'Autumn', 'Fall', 'Winter']:
        month = parse_season_to_month(month_or_season)
    else:
        month = parse_month_str(month_or_season) if month_or_season else default_month

    # Step 5: Handle the day
    day = int(day_str) if day_str else default_day

    # Step 6: Return formatted date as "YYYY-MM-DD"
    return f"{year:04d} {month:02d} {day:02d}"


def parse_month_str(month_str: Optional[str]) -> int:
    """Convert a 3-letter month abbreviation to a month number (1-12)."""
    months = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }
    if not month_str:
        raise ValueError("Month string is missing.")
    return months[month_str.capitalize()]


def parse_season_to_month(season: str) -> int:
    """Convert a season to a corresponding starting month."""
    seasons = {
        'Spring': 3,  # March
        'Summer': 6,  # June
        'Autumn': 9,  # September
        'Fall': 9,    # Same as Autumn
        'Winter': 12  # December
    }
    return seasons.get(season.capitalize(), 1)  # Default to January if not recognized


def default_article_dict() -> dict:
    """
    Returns a default dictionary structure for the article with all expected keys 
    and None as default values.
    """
    default_dict = {
        "accession_id": None,
        "citation": None,
        "license": None,
        "date": None,
        "journal": None,
        "title": None,
        "abstract": None,
        "keyword": None,
        "pmid": None,
        "article_categories": None,
        "mesh": None,
        # "reference_ids": None,
        # "reference_count": None,
        "figure_set": None,
        "context": None,
        "nxml": None,
    }
    
    return default_dict