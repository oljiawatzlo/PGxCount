import os
import pandas as pd
import json
import sqlite3

class ETL:
    def __init__(self, source_path, output_path) -> None:
        self.source_path = source_path
        self.output_path = output_path
    
    def function(self) -> None:
        list_dir = os.listdir(self.source_path)
        for dir in list_dir:
            path = f'{self.source_path}/{dir}/CPIC/ReportCPIC/{dir}_pgx_cpic_summary.json'
            with open(path, "r") as json_file:
                # Load the JSON data into a Python dictionary
                data = json.load(json_file)
                # 'data' will now contain the JSON data as a Python dictionary
                print(data)
            
    def save_database(self, your_database_name, df) -> None:
        
        database_name = your_database_name
        db_name = f'{output_path}/{database_name}.db'
        conn = sqlite3.connect(db_name)
        
        # Replace 'table_name' with the desired table name
        table_name = 'your_table_name'

        # Use the 'to_sql' method of the DataFrame to save it to the database
        df.to_sql(table_name, conn, if_exists='append', index=False)
        conn.close()
            
            
if __name__ == "__main__":
    source_path = '/Users/bordinphat/Documents/testcall'
    output_path = '.out'
    obj = ETL(source_path=source_path, output_path=output_path)
    obj.function()