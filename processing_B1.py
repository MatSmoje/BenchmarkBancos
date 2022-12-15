import os
import pandas as pd

#B1 = un estado de situación financiera consolidado.
#Para los archivos del tipo B1, el orden de las columnas es la siguiente:
#1. Código contable. Es un campo de 7 digitos que identifica el concepto contable que se describe en el archivo "Modelo-MB1.txt".
#2. Monto Moneda Chilena No Reajustable (Valor en millones de pesos chilenos)
#3. Monto Moneda reajustable por factores de IPC (Valor en millones de pesos chilenos)
#4. Monto Moneda reajustable por Tipo de Cambio (Valor en millones de pesos chilenos)
#5. Monto en Moneda Extranjera de acuerdo al tipo de cambio de representación contable usado por el banco (Valor en millones de pesos chilenos)


def main():
    tableName = 'estado_situacion_financiera'
    newPath = 'tablas'

    exist = os.path.exists(newPath)
    if not exist:
        os.makedirs(newPath)

    files = getFiles()
    df_Final = pd.DataFrame()
    for file in files: 
        bank_code = file.split('.')[0][-3:]
        df = prepareData(file)
        df['bank_code'] = bank_code
        df_Final = pd.concat([df_Final,df])
    df_Final = df_Final.reset_index(drop = True)
    print(df_Final)
    df_Final.to_parquet(f"tablas/B1.parquet")

def getFiles():
    path = '201907-280819'
    ls_instrucciones = os.listdir(path)
    files = [f"{path}/{file}" for file in ls_instrucciones if file.startswith('b')]
    return files

def getTableName(file):
    with open(file) as f:
        first_line = f.readline().strip('\n')
    return first_line.split('\t')[1].lower().replace('-','_').replace(' ','_')

def prepareData(file):
    print(file)
    df = pd.read_csv(f'{file}', sep='\t', skiprows=1, encoding = "ISO-8859-1", on_bad_lines='skip')
    df.columns = ['codigo', 'monto_no_reajustable', 'monto_reajustable_IPC', 'monto_reajustable_tipo_cambio', 'monto_moneda_extranjera']

    for col in df.columns:
        if col == 'codigo':
            pass
        else: 
            df[col] = df[col].apply(lambda x: x.replace(',','.')).astype('float')
    
    return df

main()




