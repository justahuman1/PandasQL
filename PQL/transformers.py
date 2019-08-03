# Initialize Conversion methods

import pandas as pd


class conversionMethods:
    def __init__(self):
        pass

    def xlsx_to_db_table(
        self, excel_file_location: str,
        tab_num: int=0, custom_table_name: str=None,
        skip_rows: int=0, if_exists: str="fail"
    ) -> pd.DataFrame:
        """
            Assumes that the csv is formatted:
                i.e: headers are col_names and data is normalized
            Returns {df}
            ==================
                excel_file_location: the relative path of the excel file

                tab_num: The excel sheet to turn into a table (start index: 0)

                custom_table_name: the name of the table in the db file
                    - The tab name is used as the default table name
                    - Unless custom_table_name is present

                if_exists: If this table already exists
                    - "fail", "replace", or "append"
        """

        def _passer(**kwargs):
            xl = pd.ExcelFile(kwargs['excel_file_location'])
            custom_table_name = (
                kwargs['custom_table_name'] or
                xl.sheet_names[kwargs['tab_num']]
            )
            page_df = xl.parse(
                sheet_name=kwargs['tab_num'], skiprows=kwargs['skip_rows']
            )
            custom_table_name = custom_table_name.strip().replace(
                ' ', '_').replace('-', '').replace('__', '_').lower()
            page_df.columns = [
                str(i).strip().replace(' ', '_').replace('-', '').replace(
                    '__', '_').lower()
                for i in page_df.columns
            ]
            page_df.to_sql(
                custom_table_name,
                con=self.engine, if_exists=kwargs['if_exists']
            )
        return self._connectionController(
            _passer, custom_table_name=custom_table_name,
            excel_file_location=excel_file_location, tab_num=tab_num,
            skip_rows=skip_rows, if_exists=if_exists)

    def df_to_db_table(
        self, df: pd.DataFrame, custom_table_name: str, if_exists: str="fail"
    ) -> None:
        """
            Convert a df into a db_table
            ============
            df: The dataframe to convert to a table in the db

            custom_table_name: A name for the table in the db

            if_exists: If this table already exists
                - "fail", "replace", or "append"
        """
        def _passer(**kwargs):
            df.to_sql(
                kwargs['custom_table_name'], con=self.engine,
                if_exists=kwargs['if_exists'])

        self._connectionController(
            _passer, custom_table_name=custom_table_name, if_exists=if_exists)

    def csv_to_db_table(
        self,
        csv_location: str,
        custom_table_name: str=None,
        if_exists: str='fail'
    ) -> None:
        """
            Assumes that the csv is formatted:
                i.e: headers are col_names and data is normalized

            csv_location: the relative path of the file from this file

            custom_table_name: the name of the table in the db file
                - The csv file_name is used as the default table name
                - Unless custom_table_name is present

            if_exists: If this table already exists, catch options:
                - "fail", "replace", or "append"
        """
        def _passer(**kwargs):
            csv = pd.read_csv(kwargs['csv_location'])
            custom_table_name = (
                kwargs['custom_table_name'] or
                os.path.basename(kwargs['csv_location'])
            )
            custom_table_name = custom_table_name.strip().replace(
                ' ', '_').replace('-', '').lower()
            csv.columns = [
                str(i).strip().replace(' ', '_').replace('-', '').lower()
                for i in csv.columns
            ]
            csv.to_sql(
                custom_table_name,
                con=self.engine, if_exists=kwargs['if_exists']
            )
        self._connectionController(
            _passer, csv_location=csv_location,
            custom_table_name=custom_table_name,
            if_exists=if_exists)

