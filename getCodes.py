import pandas as pd
from os import listdir

def readData():
    ls_instrucciones = listdir('202201/Instrucciones')
    files = [file for file in ls_instrucciones if file.startswith('Modelo-MB1')][0]

    parquetName = files.split(sep='.')[0]
    skiprow = 4
    skipfoot = 10
    path = f"202201/Instrucciones/{files}"
    df = pd.read_csv(path, encoding = "ISO-8859-1", on_bad_lines='skip', sep='\t', skipfooter=skipfoot, skiprows=skiprow)#, header= None)
    print(df)
    df.to_parquet(f"tablas/{parquetName}.parquet")


readData()