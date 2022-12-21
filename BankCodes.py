import os
import pandas as pd

def main():
    newPath = 'tablas'
    df = prepareData()
    exist = os.path.exists(newPath)
    if not exist:
        os.makedirs(newPath)


    df.to_parquet(f"tablas/bank_codes.parquet")


def prepareData():
    df = pd.read_csv(f'201907-280819/Instrucciones/CODIFIS.txt', 
    sep='\t', 
    skiprows=8, 
    skipfooter=31, 
    encoding = "ISO-8859-1", 
    on_bad_lines='skip',
    header=None,
    dtype=str)
    df.columns = ['bank_code', 'bank_name']
    return df

main()




