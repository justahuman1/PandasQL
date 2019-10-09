# Pandas Query Language - PQL

Seamless integration between sqlite3 and pandas; create and query databases from excel, csv files in less than 3 lines of code!

SparkSQL allows you to integrate direct sql into your dataframes without any additional low-level overhead (such as closing connections and auto indexing). I wanted it to be the same way for pandas, hence PQL. All the sqlalchemy methods and connection managers are abstracted and we can simply query the database, in-memory, or to file.

I made this as I was dealing with TB files at my place of work; using this helped me create high-level generator functions to easily handle the data via sqlite (afterall, sqlite storage limit is 140 terrabytes). I was easily able to store around 11 TB's of data into sqlite and chunk it for pandas analysis.

This is not a replacement for SparkSQL and provides no framework for distributed computing. That can be additionaly utilized via dask (pip install dask). It is simply an abstraction to provide the same workflow as the Spark environment.



> In-memory example:
```
from PQL import PandasQL

pql = PandasQL(in_memory=True)

# csv to db
pql.csv_to_db_table(
    './data/csv_sample.csv',
    tab_num=0
    custom_table_name='test_table_csv',
    if_exists='replace'
)

sample_df = pql.query("SELECT * FROM test_table_csv limit 5")

new_df = sample_df.sample(5)

# df to table
pql.df_to_db_table(
    df=new_df,
    custom_table_name="new_df_to_db_table",
    if_exists="fail"
)

new_sample_df = pql.query("SELECT * FROM new_df_to_db_table limit 5")


```

## Upcoming Features
- Table creation abstractions
- Data Insertion abstractions
- Generator Functions for big data chunks
- Dask Integration
