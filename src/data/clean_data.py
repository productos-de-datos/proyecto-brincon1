
"""
Módulo de limpieza de datos.
-------------------------------------------------------------------------------

En este módulo se unen todos los archivos csv que esten en la ruta indicada, se
crea una lista de ellos y se unen en un archivo.
Luego, se convierte el campo fecha a formato fecha, se rellenan los espacios vacíos
con el promedio de las filas y se elimnan duplicados.
Además, con melt se transforman las filas en columnas para obtener las tres columnas
requeridas y se guarda el archivo ordenado.

Requerimiento:
Usando los archivos data_lake/raw/*.csv, cree el archivo data_lake/cleansed/precios-horarios.csv.
Las columnas de este archivo son:

    * fecha: fecha en formato YYYY-MM-DD
    * hora: hora en formato HH
    * precio: precio de la electricidad en la bolsa nacional

    Este archivo contiene toda la información del 1997 a 2021.

"""

import os
import glob
import pandas as pd

def clean_data():
    """Realiza la limpieza y transformación de los archivos CSV."""
    files_joined = os.path.join('data_lake/raw/', "*.csv")
    list_files = glob.glob(files_joined)
    merge_files = pd.concat(map(pd.read_csv, list_files), ignore_index=True)
    merge_files['fecha'] = pd.to_datetime(merge_files['fecha'], format="%Y/%m/%d")

    delete_na = merge_files.where(merge_files.notna(), merge_files.mean(axis=1, numeric_only=True),
                                    axis=0)
    delete_duplicate = delete_na.drop_duplicates(['fecha'], keep='first')

    data_transform = pd.melt(delete_duplicate, id_vars="fecha")
    data = data_transform.sort_values(by = ['fecha', 'variable'])
    data.rename(columns={'variable': 'hora',
                           'value': 'precio'}, inplace=True)

    data.to_csv('data_lake/cleansed/precios-horarios.csv', encoding='utf-8', index=False)

    #raise NotImplementedError("Implementar esta función")
def test_04():
    """Evalua el nombre y numero de columnas de un archivo"""
    assert pd.read_csv('data_lake/cleansed/precios-horarios.csv').columns.tolist() == ["fecha",
                                                                            "hora", "precio"]
    assert pd.read_csv('data_lake/cleansed/precios-horarios.csv').shape[1] == 3

if __name__ == "__main__":

    import doctest
    doctest.testmod()
    clean_data()
