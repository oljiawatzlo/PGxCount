import os
import pandas as pd

class ETL:
    def __init__(self, source_path, output_path) -> None:
        self.source_path = source_path
        self.output_path = output_path
    
    def Functon(self) -> None:
        list = os.listdir(self.source_path)
        for i in list:
            print(i)
            print(type(i))
            print('*'*20)
            
if __name__ == "__main__":
    source_path = '/Users/bordinphat/Documents/testcall'
    output_path = '.out/test.csv'
    obj = ETL(source_path=source_path, output_path=output_path)