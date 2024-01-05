"""Graph database connector and query parsers"""

from typing import Any, Dict, List
import neo4j
from neo4j.exceptions import CypherSyntaxError
import pandas as pd


class Neo4jGraph:
    """Neo4j wrapper for graph operations."""

    def __init__(
        self, 
        url: str, 
        username: str, 
        password: str, 
        database: str = "neo4j",
        ) -> None:
        """Create a new Neo4j graph wrapper instance."""
        self._driver = neo4j.GraphDatabase.driver(url,
                                                   auth=(username, password))
        self._database = database

        # Verify the connection
        try:
            self._driver.verify_connectivity()
        except neo4j.exceptions.ServiceUnavailable:
            raise ValueError(
                "Could not connect to Neo4j database. "
                "Please ensure that the url is correct."
            )
        except neo4j.exceptions.AuthError:
            raise ValueError(
                "Could not connect to Neo4j database. "
                "Please ensure that the username and password are correct."
            )
        
    def close(self):
        """Closes the Neo4j connection."""
        if self.driver is not None:
            self.driver.close()
    
    def query(self, 
              cypher_query: str, 
              params: dict = {}
              ) -> List[Dict[str, Any]]:
        """Query Neo4j database."""

        with self._driver.session(database=self._database) as session:
            try:
                data = session.run(cypher_query, params)
                return [r.data() for r in data]
            except CypherSyntaxError as e:
                raise ValueError(
                    "Cypher Statement is not valid\n" f"{e}") 
            
    def load_data(self,
                   cypher_query: str,
                   df: pd.DataFrame,
                   ):
        """Load data to Neo4j from a Pandas dataframe."""

        self.query(cypher_query,
                   params={'rows': df.to_dict('records')})
    
        
        
    
        
    
