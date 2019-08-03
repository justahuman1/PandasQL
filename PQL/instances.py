# Initialize Meta Methods


class metaInfo:
    def __init__(self):
        pass

    def fix_cpath(self, custom_path: str, db_name: str) -> None:
        """
            custom_path: os.path.join result of the custom location
            db_name: The name of the database we will be utilizing
                - Include '.db' extension
                - Creates db if not in current directory or custom_path
        """
        self.cpath = custom_path
        print(f"Reinitialized at {self.cpath}")
        self.db_loc = self._create_connect_database(db_name)

    def meta_table_metadata(self, table_name: str) -> list:
        """
            Returns a list of information about the table_name
            Returns {list}
        """
        def _passer(**kwargs):
            return self.engine.execute(
                f"PRAGMA table_info({kwargs['table_name']});").fetchall()
        return self._connectionController(_passer, table_name=table_name)

    def meta_db_tables(self) -> list:
        """
            Returns a list of currently existing tables
            Returns {list}
        """
        def _passer(**kwargs):
            data = self.engine.execute("""
                SELECT * FROM sqlite_master WHERE type='table';
            """).fetchall()
            table_names = [i[1] for i in data]
            return table_names
        return self._connectionController(_passer)

