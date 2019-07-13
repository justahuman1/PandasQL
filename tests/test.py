from PQL import PandasQL
import pandas as pd


class FileDbTest:
    def __init__(self):
        self.file_db = self.createDb()

    def createDb(self):
        return PandasQL(db_name='test.db')

    def xl_to_db(self):
        self.file_db.xlsx_to_db_table(
            '../data/xl_sample.xlsx',
            tab_num=1,
            custom_table_name='test_table_xl',
            if_exists='replace'
        )

    def csv_to_db(self):
        self.file_db.csv_to_db_table(
            '../data/csv_sample.csv',
            custom_table_name='test_table_csv',
            if_exists='replace'
        )

    def query_df(self, q):
        assert isinstance(self.file_db.query(q), pd.DataFrame)


class MemoryDbTest:
    def __init__(self):
        self.mem_db = self.createDb()

    def createDb(self):
        return PandasQL(in_memory=True)

    def xl_to_db(self):
        self.mem_db.xlsx_to_db_table(
            '../data/xl_sample.xlsx',
            tab_num=1,
            custom_table_name='test_table_xl',
            if_exists='replace'
        )

    def csv_to_db(self):
        self.mem_db.csv_to_db_table(
            '../data/csv_sample.csv',
            custom_table_name='test_table_csv',
            if_exists='replace'
        )

    def query_df(self, q):
        assert isinstance(self.mem_db.query(q), pd.DataFrame)


class Tester:
    def case_filedbtest_xl(self):
        test = FileDbTest()
        test.xl_to_db()
        test.query_df("SELECT * FROM test_table_xl limit 5")

    def case_memorydbtest(self):
        mem_test = MemoryDbTest()
        mem_test.xl_to_db()
        mem_test.query_df("SELECT * FROM test_table_xl limit 5")


def TEST_MEMORY_DB():
    Tester().case_memorydbtest()
    return "Memory DB"


def TEST_FILE_DB():
    Tester().case_filedbtest_xl()
    return "File DB"


if __name__ == "__main__":
    test_type = TEST_FILE_DB()
    print(f'{test_type} test Successfully Completed')


# ########### SINGLE UNIT TESTS ##################

# from pandasQL import PandasQL
# from inspect import signature

# print((PandasQL.query.__annotations__))
