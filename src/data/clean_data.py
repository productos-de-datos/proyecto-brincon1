"""
Documentación

Se buscan todos los archivos csv y se retorna los nombres en una lista. 
Se concatenan todos los archivos por el indice de la columna y se obtiene un único archivo.
Se convierte la primera columna a formato fecha.
Se rellena los espacios vacios por la media de cada filal
Y por último se asigna un nombre a las columnas y se guarda el archivo archivo csv.

"""

def clean_data():
    """Realice la limpieza y transformación de los archivos CSV.

    Usando los archivos data_lake/raw/*.csv, cree el archivo data_lake/cleansed/precios-horarios.csv.
    Las columnas de este archivo son:

    * fecha: fecha en formato YYYY-MM-DD
    * hora: hora en formato HH
    * precio: precio de la electricidad en la bolsa nacional

    Este archivo contiene toda la información del 1997 a 2021.


    """
    import pandas as pd
    import glob
    import os

    # merging the files
    files_joined = os.path.join('data_lake/raw/', "*.csv")

    # Return a list of all joined files
    list_files = glob.glob(files_joined)

    # Merge files by joining all files
    df = pd.concat(map(pd.read_csv, list_files), ignore_index=True)

    df['0'] = pd.to_datetime(df['0'], format="%Y/%m/%d")
    
    df = df.T.fillna(df.mean(axis=1,  numeric_only=True)).T
 
    df.rename(columns={'0':'fecha', '1':'00', '2':'01', '3':'02', '4':'03', '5':'04', '6':'05', '7':'06', 
                        '8':'07', '9':'08', '10':'09', '11':'10', '12':'11', '13':'12', '14':'13', '15':'14', '16':'15',
                        '17':'16', '18':'17', '19':'18', '20':'19', '21':'20', '22':'21', '23':'22', '24':'23'},
               inplace=True)

    df.to_csv('data_lake/cleansed/precios-horarios.csv', encoding='utf-8', index=False)


    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()

clean_data()