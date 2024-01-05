# Neccessary Imports

import json
import csv
import zipfile
import os
from typing import List, Dict
import pandas as pd
import string

class ArXivDataProcessor:
    """
    Class for processing large arxiv zip file, 
    featuring unzipping and parsing functionalities as follows:
    - read the large JSON file and select the articles with a specified topic
    - parse the id column from topic/id_number to id_number
    - evaluate the token length of the abstract
    - combine the title and the abstract into a single corpus
    - save only selected articles
    """

    def __init__(self, 
                 data_path: str):
        
        self.data_path = data_path

    def unzip_file(self
                   )-> None:
        """
        Unzips the arxiv file (1.2GB) into a json file (3.7GB). 
        """

        zip_path = self.data_path+"archive.zip"
        with zipfile.ZipFile(zip_path, "r") as fzip:
            # Extracts the data to a JSON file
            fzip.extractall(self.data_path)

    def select_topic(self, 
                     topic: str
                     )-> List[Dict]:

        """
        Function to extract articles with provided 'topic' and to parse
        the 'id' column.
        """

        # Initialize an empty list to store the extracted entries.
        filtered_records = []

        # Variables for the data filename and path.
        arxiv_file = "arxiv-metadata-oai-snapshot.json"
        arxiv_file_path = self.data_path + arxiv_file

        with open(arxiv_file_path, "r") as f:
            for line in f:
                try:
                    # Load the JSON object from the line
                    data = json.loads(line)

                    # Split the 'id' field and check the topic 
                    id_split = data["id"].split("/")
                    if id_split[0] == topic:
                        data["id"] = id_split[1]
                        # Keep this record for the chosen topic
                        filtered_records.append(data)

                except json.JSONDecodeError:
                    # Print an error message if this line isn't valid JSON
                    print(f"Couldn't parse: {line}")

        print(f"There are {len(filtered_records)} with {topic} topic.")
        
        return filtered_records


    def count_tokens(self, 
                     paragraph: str
                     ) -> int:
        """
        Count the number of tokens in a paragraph.
        """

        # Remove punctuation from the paragraph
        trans = str.maketrans("", "", string.punctuation)
        clean_paragraph = paragraph.translate(trans)

        # Tokenize the cleaned paragraph into words by splitting on spaces
        tokens = clean_paragraph.split()

        # Return the number of tokens
        return len(tokens)

    def select_articles(self,
                        entries: List[Dict],
                        cols=None, # Chose columns to keep
                        min_length=3,
                        max_length=1000,
                        keep_abs_length = False,
                        build_corpus=True
                        )-> pd.DataFrame:
        """
        Selects articles with the chosen topic.

        entries - the articles data for the topic of choice
        cols - columns to keep, if None all the columns are kept
        min_length - min tokens in abstract, default is 3
        max_length - max tokens in the abstract, default is 1000
        keep_abs_length - retain the abstract length column, default is False 
        build_corpus - create a column with title+abstract, default is True
        """

        # Write data as a pandas dataframe
        df = pd.DataFrame(entries)
        # Create a column that records the abstract token length
        df["abs_length"] = [self.count_tokens(x) for x in df["abstract"]]
        # Choose the articles based on abstract length
        df_selected = df[(df["abs_length"] >= min_length) & (df["abs_length"] <= max_length)]
        # Retain if retaining the abs_length
        if keep_abs_length:
            cols += ["abs_length"] 
        else:
            cols = cols
        # Create a corpus column
        df_selected = df_selected.copy()
        if build_corpus:
            df_selected["corpus"] = df["title"] + ";" + df["abstract"]
            cols+=["corpus"]
        # Retain only the selected columns
        if cols:
            df_selected = df_selected[cols]

        print(f"There are {df_selected.shape[0]} articles selected.")
        return df_selected

    def save_selected_data(self, 
                           selected_data: pd.DataFrame,
                           topic: str
                           ) -> None:
        """
        Saves the selected data to a csv file.
        """
        # The output filename
        output_file = f"selected_{topic}.csv"
        # Save the selected data
        selected_data.to_csv(self.data_path+output_file, index=False)