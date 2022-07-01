# coding=utf-8

"""
Módulo de limpieza de datos.
-------------------------------------------------------------------------------

En este módulo se unen todos los archivos csv que esten en la ruta indicada, se 
crea una lista de ellos y se unen en un archivo.
Luego, se convierte el campo fecha a formato fecha, se rellenan los espacios vacíos 
con el promedio de las filas y se elimnan duplicados.
Además, con melt se transforman las filas en columnas para obtener las tres columnas 
requeridas y se guarda el archivo ordenado.

    Realice la limpieza y transformación de los archivos CSV.

    Usando los archivos data_lake/raw/*.csv, cree el archivo data_lake/cleansed/precios-horarios.csv.
    Las columnas de este archivo son:

    * fecha: fecha en formato YYYY-MM-DD
    * hora: hora en formato HH
    * precio: precio de la electricidad en la bolsa nacional

    Este archivo contiene toda la información del 1997 a 2021.
    
Test_
>>> os.path.isfile("data_lake/cleansed/precios-horarios.csv") 
True
>>> pd.read_csv('data_lake/cleansed/precios-horarios.csv').head()
        fecha  hora    precio
0  1995-07-20     0  1.409435
1  1995-07-20     1  1.073000
2  1995-07-20     2  1.073000
3  1995-07-20     3  1.073000
4  1995-07-20     4  1.073000

"""

import pandas as pd
import glob
import os

def clean_data():
    
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

    df.to_csv('data_lake/cleansed/precios-horarios.csv', encoding='utf-8', index=False)


    #raise NotImplementedError("Implementar esta función")

def test_04():
    assert pd.clean_data().columns.tolist() == [
        "fecha", "hora", "precio"]


if __name__ == "__main__":

    import doctest

    doctest.testmod()

    clean_data()