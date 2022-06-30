"""
Documentación

Se buscan todos los archivos csv y se retorna los nombres en una lista. 
Se concatenan todos los archivos por el indice de la columna y se obtiene un único archivo.
Se convierte la primera columna a formato fecha.
Se rellena los espacios vacios por la media de cada fila. Al usar axis=0, podemos completar los 
valores que faltan en cada columna con los promedios de las filas.
Se eliminan los duplicados de acuerdo a la columna fecha.
Con melt se transforman las filas a columnas y se duplica la fecha para cada registro.
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

    df['fecha'] = pd.to_datetime(df['fecha'], format="%Y/%m/%d")

    df = df.where(df.notna(), df.mean(axis=1, numeric_only=True), axis=0)
 
    df = df.drop_duplicates(['fecha'], keep='first')

    df = pd.melt(df, id_vars="fecha")
    df= df.sort_values(by = ['fecha', 'variable'])
    df.rename(columns={'variable': 'hora', 
                           'value': 'precio'}, inplace=True)

    df.to_csv('data_lake/cleansed/precios-horarios.csv', encoding='utf-8', index=True)


    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()

clean_data()