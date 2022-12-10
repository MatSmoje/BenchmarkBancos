import pandas as pd
from os import listdir

def readData():
    ls_instrucciones = listdir('201907-280819/Instrucciones')
    files = [file for file in ls_instrucciones if file.startswith('Modelo')]

    for file in files: 
        parquetName = file.split(sep='.')[0]
        if 'MB1' in file: 
            skiprow = 4
            skipfoot = 10
        elif 'MC1' in file: 
            skiprow = 4
            skipfoot = 17
        elif 'MC2' in file: 
            skiprow = 4
            skipfoot = 11
        elif 'MR1' in file: 
            skiprow = 3
            skipfoot = 10

        #if 'MC1' in file:
        print(f"Procesando {file}")
        path = f"201907-280819/Instrucciones/{file}"
        df = pd.read_csv(path, encoding = "ISO-8859-1", on_bad_lines='skip', sep='\t', skipfooter=skipfoot, skiprows=skiprow, header= None)
        df.columns = ['Codigo','Descripcion']
        df['Codigo'] = df['Codigo'].astype('str').apply(lambda x: x.lstrip())
        df['Descripcion'] = df['Descripcion'].astype('str').apply(lambda x: x.lstrip().strip())

        for idx, row in df.iterrows():
            if len(row['Codigo']) > 7: 
                val = row[0].split(sep=' ')
                df.at[idx,'Codigo'] = val[0]
                df.at[idx,'Descripcion'] = ' '.join(val[1:])


            #df[1] = df[1].apply(lambda x: x.strip())
            
        print(df)

            #print('----')
        df.to_parquet(f"tablas/{parquetName}.parquet")


readData()