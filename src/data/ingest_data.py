"""
Módulo de ingestión de datos.
-------------------------------------------------------------------------------

El nombre de los archivos a descargar son fechas consecutivas así que se hace un
rango para obtener la url de cada archivo y un condicional para los archivos de los
años 2016 y 2017 que terminan en xls.
Se descargan con la libreria requests y se guardan en la ruta requerida.

Requerimiento:
Del repositorio jdvelasq/datalabs/precio_bolsa_nacional/xls/ descarga los
archivos de precios de bolsa nacional en formato xls a la capa landing. La
descarga debe realizarse usando únicamente funciones de Python.

"""
import os
import requests as req

def ingest_data():
    """Ingeste los datos externos a la capa landing del data lake."""
    for num in range(1995, 2022):
        if num in range(2016, 2018):
            url = f'https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{num}.xls?raw=true'
            file = req.get(url, stream = True)
            with open(f'data_lake/landing/{num}.xls', 'wb') as files:
                for chunk in file:
                    if chunk:
                        files.write(chunk)
        else:
            url = f'https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{num}.xlsx?raw=true'
            file = req.get(url, allow_redirects=True)
            with open(f'data_lake/landing/{num}.xlsx', 'wb') as files:
                for chunk in file:
                    if chunk:
                        files.write(chunk)

    #raise NotImplementedError("Implementar esta función")
def test_02():
    """Evalua la creación de algunos archivos"""
    assert os.path.isfile('data_lake/landing/1995.xlsx') is True
    assert os.path.isfile('data_lake/landing/2016.xls') is True

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    ingest_data()
