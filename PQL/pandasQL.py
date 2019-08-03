from PQL import conversionMethods, queryRunner, metaInfo
import os
import types
import sqlite3 as sq
from sqlite3 import Error
from sqlalchemy import create_engine


class PandasQL(conversionMethods, metaInfo, queryRunner):
    """
        A class designed to make writing sql against pandas
        easier by integrating sqlite. Run sql in memory or to
        a physical database with less than 3 lines of code!
    """
    def __init__(
        self, db_name: str=None, custom_path: str=None, in_memory: bool=False
    ) -> None:
        """
            db_name:
                The name of the database we will be utilizing
                --> Include '.db' extension
                --> Creates db if not in current directory or custom_path
            custom_path:
                If the path you are utilizing is not the folder of the file
                utilizing this class, please include a custom location or your
                db will be made in the abspath of this __file__.
        """
        self.db = None
        self.cpath = custom_path or os.path.abspath('')
        self._in_mem = in_memory
        self.db_loc = self._create_connect_database(db_name)
        conversionMethods.__init__(self)
        metaInfo.__init__(self)
        queryRunner.__init__(self)

    def _connectionController(
        self, command: types.BuiltinFunctionType,
        **kwargs: dict
    ) -> "Command Response":
        if self._in_mem:
            return command(**kwargs)
        else:
            self.engine = sq.connect(self.db_loc)
            res = command(**kwargs)
            self.engine.close()
            return res

    def _create_connect_database(self, db_name: str) -> str:
        """
            create a database connection to a SQLite database
            db_name = Name of the database ==> Ex: database.db
                --> INCLUDE ".db" extension
        """
        if self._in_mem:
            temp_name = 'file::memory:?cache=shared'
            self.db = sq.connect(temp_name, uri=True)
            self.engine = self.db
            print("Memory DB Initiated")
            return temp_name

        if not db_name[-3:] == '.db':
            print("Please include the '.db' extensions", end="\n")
            raise ValueError

        db_path = os.path.join(self.cpath, db_name)

        try:
            self.db = sq.connect(db_path)
            print(f"{sq.version} -- File DB:\t{db_path}")
        except Error as e:
            print(e)
        finally:
            self.db.close()

        return db_path
