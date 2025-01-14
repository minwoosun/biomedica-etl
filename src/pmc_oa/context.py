from lxml import etree
from typing import List, Dict, Union


def extract_context(nxml_path: str, temp_id_map: Dict[str, str]) -> Dict[str, List[str]]:
    """
    Extracts context surrounding figure references from an NXML file and maps it to the corresponding image IDs.
    
    Parameters:
    - nxml_path (str): Path to the NXML file.
    - temp_id_map (dict): Temporary mapping from `rid` to image IDs.
    
    Returns:
    - dict[str, list[str]]: A dictionary where keys are image IDs and values are lists of surrounding context paragraphs.
    """
    tree = etree.parse(nxml_path)
    root = tree.getroot()

    paragraphs = []
    id_map = temp_id_map  # rid to image_id map

    # Find all <xref> elements where ref-type is 'fig'
    for xref in root.findall(".//xref[@ref-type='fig']"):
        rid = xref.attrib.get('rid', None)

        if rid and rid in id_map:  # Proceed only if 'rid' is valid and in the id_map
            parent_paragraph = find_parent(root, xref)
            
            if parent_paragraph is not None:
                # Clean the paragraph: keep only <xref ref-type="fig"> tags and remove all others
                cleaned_paragraph = clean_paragraph_except_xref(parent_paragraph)
                
                # Append the cleaned paragraph to the list
                paragraphs.append({id_map[rid]: cleaned_paragraph})
    
    # Combine the results into a dictionary mapping image ID to paragraphs
    combined_dict = combine_paragraphs(paragraphs)
    
    return combined_dict


def find_parent(root: etree._Element, element: etree._Element) -> Union[etree._Element, None]:
    """
    Finds the parent element of a given XML element within a tree.
    
    Parameters:
    - root (etree._Element): The root element of the XML tree.
    - element (etree._Element): The element whose parent is to be found.
    
    Returns:
    - etree._Element | None: The parent element of the given element, or None if no parent is found.
    """
    for parent in root.iter():
        if element in parent:
            return parent
    return None

def clean_paragraph_except_xref(paragraph: etree._Element) -> str:
    """
    Converts a paragraph element to a string while keeping only <xref> tags with ref-type="fig".
    Removes unnecessary namespace attributes and ensures the text around the <xref> tags is preserved.
    
    Parameters:
    - paragraph (etree._Element): The paragraph element to clean.

    Returns:
    - str: The cleaned paragraph with only figure <xref> tags intact and no namespace attributes.
    """
    cleaned_parts = []

    # Recursively process elements in the paragraph to retain text and figure references
    for element in paragraph.iter():
        if element.tag == 'xref' and element.attrib.get('ref-type') == 'fig':
            # Remove namespaces in attributes
            element.attrib.pop('xmlns:xlink', None)
            element.attrib.pop('xmlns:mml', None)

            # Keep <xref ref-type="fig"> with its attributes
            xref_str = etree.tostring(element, encoding='unicode', method='xml')
            cleaned_parts.append(xref_str)
        elif element.tag != 'xref' and element.text:
            # Add the text inside other elements but ignore the tags themselves
            cleaned_parts.append(element.text)

        if element.tail:
            # Add tail text that comes after the current element
            cleaned_parts.append(element.tail)

    # Join the cleaned parts into a final string
    cleaned_paragraph = ''.join(cleaned_parts)
    
    # Wrap the cleaned text in a <p> tag and return it
    return f'<p>{cleaned_paragraph.strip()}</p>'


def combine_paragraphs(paragraphs: List[Dict[str, str]]) -> Dict[str, List[str]]:
    """
    Combines paragraphs into a dictionary where the key is the image ID and the value is a list of paragraphs.
    
    Parameters:
    - paragraphs (List[Dict[str, str]]): List of paragraph dictionaries where keys are image IDs and values are paragraphs.

    Returns:
    - Dict[str, List[str]]: Combined dictionary of paragraphs by image ID.
    """
    combined_dict = {}

    for entry in paragraphs:
        for key, value in entry.items():
            if key in combined_dict:
                # Append only unique paragraphs
                if value not in combined_dict[key]:
                    combined_dict[key].append(value)
            else:
                combined_dict[key] = [value]

    return combined_dict