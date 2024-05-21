"""Functions to extract information from structured_schema"""

from typing import Any, List, Union
import re
from neo4j import time
from Levenshtein import distance

# Import local modules
from utils.utilities import *

def retrieve_datatypes(jschema: Dict
                       ) -> Any:
    """Retrieves the set of datatypes present in the graph."""
    all_types = []
    all_nodes = get_nodes_list(jschema)
    for label in all_nodes:
        node_info = jschema['node_props'][label]
        node_types = [el['datatype'] for el in node_info]
        all_types = all_types + node_types
    return set(all_types)


#### NODES ####

def get_nodes_list(jschema: Dict
                   ) -> List[str]:
    """Returns the list of node labels in the graph."""
    return list(jschema['node_props'].keys())

def get_node_properties(jschema: Dict,
                        label: str,
                        datatypes: bool=False,
                        datatype: str=""
                        ) -> Any:
    """Function to extract a list of properties for a given node.
    Options to return the datatypes or a properties of specific datatype only."""
   
    node_info = jschema['node_props'][label]
    if datatypes:
        if len(datatype) > 1:
            props = [el['property'] for el in node_info if el['datatype'] == datatype]
        else:
            props = node_info
    else:
        props = [el['property'] for el in node_info]
    return props

def get_nodes_properties_of_datatype(jschema: Dict,
                                     nodes: List[str], 
                                     datatype: str=""
                                     ) -> List[Dict]:
    """Function to extract the properties of given datatype, for a list of nodes."""
    outputs = []
    for node in nodes:
        output = get_node_properties(jschema, node, datatypes=True, datatype=datatype)
        outputs.append({node: output})
        
    # Filter out the nodes that do not have any property of specified type
    return filter_empty_dict_values(outputs)

#### RELATIONSHIPS ####

def extract_relationships_list(jschema: Dict,
                               formatted: bool=False
                               )-> Any:
    """Extracts the list of relationships in one of the following formats:
    formatted=True - each relationship is a string: (:start)-[:type]->(:end)
    formatted=False - each relationship is a dictionary with keys: start, type, end.
    """
    if formatted:
        rels_list=[]
        for el in jschema['relationships']:
            formatted_rels = f"(:{el['start']})-[:{el['type']}]->(:{el['end']})" 
            rels_list.append(formatted_rels)
    else:
        rels_list = jschema['relationships']
    return rels_list  

def get_relationships_with_datatype(jschema: Dict,
                                    datatype: str,
                                    )->List[str]:
        """Returns a list of relationship types that have attributes with specified datatype."""
        sampler = get_relationships_properties_of_datatype(jschema, datatype)
        rels_string = [list(e.keys())[0] for e in sampler]
        return rels_string

def get_relationships_properties_of_datatype(jschema: Dict,
                                             datatype: str
                                             ) -> List[Any]:
    """Extracts relationships properties of specified datatype."""
    outputs = []
    for rel in list(jschema['rel_props'].keys()):
        props = jschema['rel_props'][rel]
        selected_props = [el['property'] for el in props if el['datatype'] == datatype]
        if len(selected_props) > 0:
            outputs.append({rel:selected_props})
    return outputs

#### SERIALIZE TEMPORAL DATA FOR SAVING ####

def neo4j_date_to_string(v: str
                         )-> str:
    """Convert neo4j.time.Date to ISO formatted string."""
    """Sample neo4j.time.Date(2023, 10, 25)'"""
    return f"{v.year}-{v.month:02d}-{v.day:02d}"

def neo4j_datetime_to_string(v: str
                             )-> str:
    """Convert neo4j.time.DateTime to ISO formatted string."""
    """Sample neo4j.time.DateTime(2023, 11, 10, 12, 23, 32, 0, tzinfo=<UTC>)"""
    return f"{v.year}-{v.month:02d}-{v.day:02d} T {v.hour:02d}:{v.minute:02d}:{v.second:02d} {v.tzinfo}"

def transform_temporals_in_dict(d: Dict
                                )-> Dict:
    """Transform neo4j.time objects in a dictionary to ISO formatted strings."""
    for key, value in d.items():
        if isinstance(value, time.Date):
            d[key] = neo4j_date_to_string(value)
        elif isinstance(value, time.DateTime):
            d[key] = neo4j_datetime_to_string(value)
    return d

def serialize_nodes_data(entries: List[Dict], 
                        )->List[Dict]:
    """Function to parse the Neo4j.time entries from extracted instances
    for a list of nodes."""
    
    for sublist in entries:
        for rec in sublist:
            rec['Instance']['properties'] = transform_temporals_in_dict( rec['Instance']['properties'])
    return entries

def serialize_relationships_data(entries: List[Dict], 
                                 )->List[Dict]:
    """Function to parse the Neo4j.time entries from extracted instances
    for a list of relationships."""

    for sublist in entries:
        for rec in sublist:
            t = list(rec.keys())
            rec[t[0]] = transform_temporals_in_dict(rec[t[0]])
            rec[t[1]] = transform_temporals_in_dict(rec[t[1]])
            rec[t[2]] = transform_temporals_in_dict(rec[t[2]])
    return entries

#### PARSED INSTANCES ###

def parse_node_instances_datatype(jschema: List[Dict],
                                  INSTANCES_NODES: List[Dict],
                                  nodes: List[str], 
                                  datatype: str,
                                  )->List[Any]:
    """Parse instances of nodes and properties with specified data type.
    Format [[label, property, value], ...]"""

    # Parse date and date_time in extracted instances
    sampler = serialize_nodes_data(INSTANCES_NODES)

    # Get the nodes and the properties of specifid datatype
    np_datatype = get_nodes_properties_of_datatype(jschema,nodes, datatype) 

    full_result = []

    for el in np_datatype:
        label = list(el.keys())[0]
        props_label = el[label] 

        result_label = []
        sampler_label = [e for e in sampler if e[0]['Instance']['Label']==label]

        for instance in sampler_label[0]:
            parsed_dict = extract_subdict(instance['Instance']['properties'], props_label) 
            parsed_instance = [[label, key, value] for key, value in parsed_dict.items() if key and value]
            if parsed_instance:
                full_result.append(parsed_instance)

    return flatten_list(full_result)

def filter_relationships_instances(jschema: Dict,
                                   INSTANCES_RELS: List[Dict],
                                   datatype_start: str,
                                   datatype_end: str
                                   )-> List[Dict]:
    """Parses a list of relationships. It extracts those properties for both source and target nodes that are of specified data types.
        
    NOTE: This will give errors if the instance does not have the required combination of source-target data types.
    """

    result = []

    for coll in INSTANCES_RELS:
        for instance in coll:
            triple = list(instance.keys())
            
            label_start = triple[0][:-6]
            selected_props_start = get_node_properties(jschema, label_start, True, datatype_start)
            selected_start = extract_subdict(instance[triple[0]], selected_props_start)
            
            label_end = triple[2][:-4]
            selected_props_end =  get_node_properties(jschema, label_end, True, datatype_end)
            selected_end = extract_subdict(instance[triple[2]], selected_props_end)
            
            rel = triple[1]

            if selected_start and selected_end:
                result.append([label_start, selected_start, rel, label_end, selected_end])
    return result

#### EXTRACT LOCAL GRAPH INFO ####

def get_graph_neighborhood(jschema: Dict,
                           entity: str, 
                           lev_dist: int,
                           ) -> Union[List[Any], List[Any], List[Any]]:
    """
    Extracts all node labels, their properties and corresponding relationships information 
    that are at a certain Levenshtein distance from a given string or contain the given string. 
    To speed up the process the schema file is used.
    """
    
    node_properties = jschema['node_props'] 
    nodes = list(node_properties.keys())
    relationships = jschema['relationships']
    relationships_properties = jschema['rel_props'] 

    # Get a list of similar node labels
    local_nodes =  [e for e in nodes if (distance(entity, e) <= lev_dist)]
    
    # Get all the nodes, and their properties, that have similar labels to entity
    local_nodes_properties = extract_subdict(node_properties, local_nodes)
    
    # Get all relationships that involve at least one of the local_nodes
    local_relationship_full = [e for e in relationships if e['start'] in local_nodes or e['end'] in local_nodes]
    local_relationships = [e for e in local_relationship_full if not isinstance(e.get('type'), dict)]

    # Extract the local_relationship types
    local_relationships_types = [e['type'] for e in local_relationships]
        
    # Extract the local relationships properties
    local_relationships_properties = extract_subdict(relationships_properties, local_relationships_types)

    return local_nodes_properties, local_relationships_properties, local_relationships

def get_subgraph_schema(jschema: Dict,
                        entities: List[str], 
                        lev_dist: int,
                        formatted: bool=True,
                        ) -> Union[List[Any], List[Any], List[Any]]:
    """
    Extracts all node labels, their properties and corresponding relationships information for all the entities given in a list. 
    """
    subgraph_nodes = [] 
    subgraph_relationships = []
    subgraph_relationships_properties = []

    for entity in entities:
        nodes, relationships_properties, relationships = get_graph_neighborhood(jschema, entity, lev_dist)
        subgraph_nodes.append(nodes)
        subgraph_relationships_properties.append(relationships_properties)
        subgraph_relationships.append(relationships)

    structured_subschema = {
        "node_props": subgraph_nodes,
        "rel_props": subgraph_relationships_properties,
        "relationships": subgraph_relationships,
        }

    # Format local node properties
    formatted_local_nodes_props = []
    for entry in subgraph_nodes:
        node = list(entry.keys())[0]
        props_str = ", ".join(
            [f"{el['property']}: {el['datatype']}"  for el in entry[node]]
            )
        formatted_local_nodes_props.append(f"{node} {{{props_str}}}")

    # Format local relationship properties
    formatted_local_rel_props = []
    for sublist in subgraph_relationships_properties:
        for rel in list(sublist.keys()):
            props_str = ", ".join(
                [f"{el['property']}: {el['datatype']}" for el in sublist[rel]]
                )
            formatted_local_rel_props.append(f"{rel} {{{props_str}}}")

    # Format local relationships
    formatted_local_rels = []
    for sublist in subgraph_relationships:
        formatted_rel = [
            f"(:{el['start']})-[:{el['type']}]->(:{el['end']})" for el in sublist
        ]
        formatted_local_rels.extend(formatted_rel)
    
    subschema = "\n".join(
        [
            "Node properties are the following:",
            ",".join(formatted_local_nodes_props),
            "Relationship properties are the following:",
            ",".join(formatted_local_rel_props),
            "The relationships are the following:",
            ",".join(formatted_local_rels),
        ]
    )

    if formatted:
        return subschema
    else:
        return structured_subschema


   

    




  

    
 
        
  

   

    

    



        
