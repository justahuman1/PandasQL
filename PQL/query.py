# Initialize Query Class
import pandas as pd

class queryRunner:
    def __init__(self):
        pass

    def query(self, query: str) -> pd.DataFrame:
        """
            Execute a query with pandas and get a dataframe
            Returns {db}
        """
        def _passer(**kwargs):
            return pd.read_sql_query(kwargs['query'], self.engine)
        return self._connectionController(_passer, query=query)

    def raw_query(self, query: str) -> list:
        """
            Execute a raw query with sqlite
            and get raw sqlite returns
            Returns {list}
        """
        def _passer(**kwargs):
            return self.engine.execute(kwargs['query']).fetchall()
        return self._connectionController(_passer, query=query)

