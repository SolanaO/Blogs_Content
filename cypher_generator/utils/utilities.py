"""Collection of basic Python helper functions"""

import json
from typing import Any, List, Dict
import pickle
from itertools import combinations
import random
from collections import defaultdict

def write_json(an_object: List[Any], file_path: str ) -> None:
    """Writes a Python object to a json file."""
    with open(file_path, "w") as fp:
        json.dump(an_object, fp)

def read_json(file_path: str) -> Any:
    """Reads a json file to the Python object it contains."""
    with open(file_path, 'rb') as fp:
        data=json.load(fp)
        return data

def write_pkl(an_object: Any, file_path: str) -> None:
    """Writes a Python object to a pickle file."""
    with open(file_path, 'wb') as f:
        pickle.dump(an_object, f)

def read_pkl(an_object: Any, file_path: str) -> Any:
    """Reads a pickle file."""
    with open(file_path, 'rb') as f:
        an_object = pickle.load(f)
        return an_object
    
def extract_subdict(my_dict: Dict, 
                    keys_to_extract: List[str]
                    )-> Dict:
    """Extracts a subdictionary of a dictionary."""
    return {key: my_dict[key] for key in keys_to_extract if key in my_dict}

def filter_empty_sublists(lst: List
                          )->List:
    """Filters empty sublists from a list of lists."""
    return [sublist for sublist in lst if any(sublist)]

def filter_empty_dict_values(lst: List[Dict]
                             )-> List[Dict]:
    """Filter out dictionaries with empty list values."""
    return [d for d in lst if any(d.values())]

def flatten_list(nested_list: List[List]) -> List[str]:
    """Flattens a nested Python list."""
    flat_list = []
    for sublist in nested_list:
        flat_list.extend(sublist)
    return flat_list

def create_pairs(elements):
    pairs = []
    for i in elements:
        for j in elements:
            if j!=i:
                pairs.append((i, j))
    return pairs

def get_random_elements(input_list, n):
    """Extract n random elements from a list."""
    # Ensure n is not greater than the number of elements in the set
    n = min(n, len(input_list))

    # Randomly sample n elements from the set
    return random.sample(input_list, n)


#### THESE NEED REVIEW ####

def get_distinct_random_pairs(nlist: List[Any],
                      used_pairs: List=[]):
    """Extracts distinct random pairs from a list of sublists."""
    grouped_elements = {}

    for sublist in nlist:
        if sublist[0] not in grouped_elements:
            grouped_elements[sublist[0]] = []
        grouped_elements[sublist[0]].append(sublist)

    pairs = []
    for key, values in grouped_elements.items():
        if len(values)>1:
            pairs.extend(combinations(values,2))
        
    # Remove pairs that have already been used
    pairs = [pair for pair in pairs if pair not in used_pairs]

    if len(pairs) > 4:
        chosen_pair = random.choice(pairs)
        used_pairs.append(chosen_pair)
        result = [chosen_pair[0][0], chosen_pair[0][1], 
                  chosen_pair[0][2], chosen_pair[1][1],
                  chosen_pair[1][2]]
        return result
    else:
        return None
    
def get_distinct_random_pairs_optimized(nlist: List[Any], 
                      used_pairs=None):
    """Extract distinct random pairs from a list of sublists."""
    
    if used_pairs is None:
        used_pairs=set()
        
    grouped_elements = defaultdict(list)

    for sublist in nlist:
        grouped_elements[sublist[0]].append(sublist)

    valid_pairs = set()
    for key, values in grouped_elements.values():
        if len(values) > 1:
            for pair in combinations(values, 2):
                if pair not in used_pairs:
                    valid_pairs.add(pair)

    if len(valid_pairs) > 4:
        chosen_pair = random.choice(tuple(valid_pairs))
        used_pairs.add(chosen_pair)
        result = [chosen_pair[0][0], chosen_pair[0][1], 
                  chosen_pair[0][2], chosen_pair[1][1],
                  chosen_pair[1][2]]
        return result
    else:
        return None

