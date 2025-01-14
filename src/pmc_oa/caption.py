import os
import re
from lxml import etree
import xml.etree.ElementTree as ET
from typing import List, Dict, Union, Optional


#--- Extract caption
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
            return ""

        # Extract file names without extension
        image_identifiers = [os.path.splitext(os.path.basename(path))[0] for path in list_of_path_to_image]
        return image_identifiers

    except Exception as e:
        return ""

def get_image_paths(path_to_pmcid: str) -> Union[list[str], None]:
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

def get_nxml_path(path_to_pmcid: str) -> Union[str, None]:
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

# def extract_context(nxml_path: str) -> dict[str, list[str]]:
#     """
#     Extracts context surrounding figure references from an NXML file and maps it to the corresponding image IDs.

#     Parameters:
#     - nxml_path (str): Path to the NXML file.

#     Returns:
#     - dict[str, list[str]]: A dictionary where keys are image IDs and values are lists of surrounding context paragraphs.
#     """
#     tree = ET.parse(nxml_path)
#     root = tree.getroot()

#     paragraphs = []

#     # rid : image_id
#     id_map = map_rid_to_image(nxml_path)
    
#     # Find all <xref> elements where ref-type is 'fig'
#     for xref in root.findall(".//xref[@ref-type='fig']"):
#         # Get the rid attribute (figure ID)
#         rid = xref.attrib.get('rid', None)  # Handle missing 'rid' gracefully
        
#         if rid:  # Proceed only if 'rid' is not None
#             # Find the parent paragraph (by finding the parent node manually)
#             parent_paragraph = find_parent(root, xref)
            
#             if parent_paragraph is not None:
#                 # Get the full paragraph text
#                 paragraph_text = ''.join(parent_paragraph.itertext())
#                 paragraphs.append({id_map[rid]: paragraph_text})
#             else:
#                 # Append an empty string if the paragraph is missing
#                 paragraphs.append({id_map[rid]: ""})
    
#     # Initialize an empty dictionary to combine text values as a list of unique paragraphs
#     combined_dict = {}
    
#     # Iterate through the list of dictionaries
#     for entry in paragraphs:
#         for key, value in entry.items():
#             if key in combined_dict:
#                 # Check if the current value is not already in the list (and it's not empty)
#                 if value and value not in combined_dict[key]:
#                     # If not, append the new value to the list
#                     combined_dict[key].append(value)
#             else:
#                 # If the key does not exist, create a new list with the value (or empty list if value is "")
#                 combined_dict[key] = [value] if value else []

#     return combined_dict

# # Example of a helper function to find the parent element (assuming you have something similar)
# def find_parent(root: ET.Element, element: ET.Element) -> Union[ET.Element, None]:
#     """
#     Finds the parent element of a given XML element within a tree.

#     Parameters:
#     - root (ET.Element): The root element of the XML tree.
#     - element (ET.Element): The element whose parent is to be found.

#     Returns:
#     - ET.Element | None: The parent element of the given element, or None if no parent is found.
#     """
#     # This function will find the parent node of the given element
#     for parent in root.iter():
#         if element in parent:
#             return parent
#     return None

# def map_rid_to_image(nxml_path: str) -> dict[str, str]:
#     """
#     Maps figure IDs to corresponding image file names (without extensions) from an NXML file.

#     Parameters:
#     - nxml_path (str): Path to the NXML file.

#     Returns:
#     - dict[str, str]: A dictionary where keys are figure IDs (rids) and values are image file names.
#     """
#     tree = ET.parse(nxml_path)
#     root = tree.getroot()

#     rid_to_image = {}

#     # Find all <fig> elements (assuming figures are stored in <fig> tags)
#     for fig in root.findall(".//fig"):
#         # Get the rid from the fig id attribute (e.g., id="fig1")
#         rid = fig.attrib.get('id', None)
        
#         # Find the graphic element inside the fig element
#         graphic = fig.find(".//graphic")
        
#         if rid and graphic is not None:
#             # Extract the image file name from the xlink:href attribute
#             image_path = graphic.attrib.get('{http://www.w3.org/1999/xlink}href', None)
#             if image_path:
#                 # Remove directory information and file extension
#                 image_file = os.path.splitext(os.path.basename(image_path))[0]
#                 # Map rid to the image file name (without extension)
#                 rid_to_image[rid] = image_file

#     return rid_to_image

def extract_context(nxml_path: str) -> dict:
    """
    Extracts context surrounding figure references from an NXML file and maps it to the corresponding image IDs.
    
    Parameters:
    - nxml_path (str): Path to the NXML file.
    
    Returns:
    - dict[str, list[str]]: A dictionary where keys are image IDs and values are lists of surrounding context paragraphs.
    """
    tree = ET.parse(nxml_path)
    root = tree.getroot()

    paragraphs = []
    id_map = map_rid_to_image(nxml_path)  # Map of rids to image IDs

    # Find all <xref> elements where ref-type is 'fig'
    for xref in root.findall(".//xref[@ref-type='fig']"):
        rid = xref.attrib.get('rid', None)

        if rid and rid in id_map:  # Proceed only if 'rid' is valid and in the id_map
            parent_paragraph = find_parent(root, xref)
            
            if parent_paragraph is not None:
                # Clean the paragraph, substituting rid with image ID and keeping only figure <xref> tags
                cleaned_paragraph = clean_paragraph_except_xref(parent_paragraph, id_map)
                
                # Append the cleaned paragraph to the list, keyed by the image ID
                paragraphs.append({id_map[rid]: cleaned_paragraph})
    
    # Combine the results into a dictionary mapping image ID to paragraphs
    combined_dict = combine_paragraphs(paragraphs)
    
    return combined_dict


def find_parent(root, child):
    """
    Finds the parent paragraph or section of the given element.
    
    Parameters:
    - root: The root of the XML tree.
    - child: The child element for which to find the parent.
    
    Returns:
    - The parent paragraph or section element, if found.
    """
    # Walk up the tree to find the first paragraph or section element
    for parent in root.iter():
        if child in list(parent):
            if parent.tag == 'p':  # Assuming <p> tags are used for paragraphs
                return parent
    return None

def clean_paragraph_except_xref(paragraph, rid_to_image_map):
    """
    Cleans the paragraph by preserving only <xref ref-type="fig"> tags, removing all other tags, and 
    replacing the rid attribute with the corresponding image ID.
    
    Parameters:
    - paragraph: The paragraph element.
    - rid_to_image_map: A dictionary mapping rids to image IDs.
    
    Returns:
    - Cleaned paragraph text with only figure <xref> tags preserved and all other tags removed,
      and with the rid attribute replaced by the corresponding image ID.
    """
    # Initialize a list to hold the cleaned parts of the paragraph
    cleaned_parts = []

    # Recursively go through the element and its children
    def process_element(elem):
        if elem.tag == 'xref' and elem.attrib.get('ref-type') == 'fig':
            # Get the current rid
            rid = elem.attrib.get('rid')
            if rid in rid_to_image_map:
                # Replace the rid with the corresponding image ID
                elem.set('rid', rid_to_image_map[rid])
            
            # Keep figure-related <xref> tags as they are, with the substituted rid
            cleaned_parts.append(ET.tostring(elem, encoding='unicode', method='xml'))
        else:
            # Append the text content of non-<xref> elements (or their tails)
            if elem.text:
                cleaned_parts.append(elem.text)
        
        # Recursively process all child elements
        for child in elem:
            process_element(child)
        
        # Append the tail of the element, if any (text after the tag closes)
        if elem.tail:
            cleaned_parts.append(elem.tail)
    
    # Start processing from the root paragraph
    process_element(paragraph)

    # Join all the cleaned parts and return the result as a string
    return ''.join(cleaned_parts)

def combine_paragraphs(paragraphs):
    """
    Combines paragraphs into a dictionary, where the key is the image ID and the value is the list of paragraphs.
    
    Parameters:
    - paragraphs (list): List of dictionaries mapping image IDs to paragraphs.
    
    Returns:
    - dict: A combined dictionary with image IDs as keys and lists of paragraphs as values.
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
    Maps figure IDs (rids) to corresponding image file names (with or without extensions) from an NXML file.

    Parameters:
    - nxml_path (str): Path to the NXML file.

    Returns:
    - dict[str, str]: A dictionary where keys are figure IDs (rids) and values are image file names.
    """
    tree = ET.parse(nxml_path)
    root = tree.getroot()

    rid_to_image = {}

    # Find all <fig> elements
    for fig in root.findall(".//fig"):
        # Get the rid from the fig id attribute (e.g., id="F1")
        rid = fig.attrib.get('id', None)
        
        # Find the graphic element inside the fig element
        graphic = fig.find(".//graphic")
        
        if rid and graphic is not None:
            # Extract the image file name from the xlink:href attribute
            image_path = graphic.attrib.get('{http://www.w3.org/1999/xlink}href', None)
            if image_path:
                # Extract image file name without the extension (for the first scenario)
                image_file_no_ext = os.path.splitext(os.path.basename(image_path))[0]
                # Extract image file name with extension (for the second scenario)
                image_file_with_ext = os.path.basename(image_path)

                # If the rid is like 'F1', map it to the corresponding image file (with extension)
                rid_to_image[rid] = image_file_with_ext  # Use the full image file name (with extension)

    return rid_to_image