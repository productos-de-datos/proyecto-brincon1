"""
Módulo de ingestión de datos.
-------------------------------------------------------------------------------

"""


def ingest_data():
    """Ingeste los datos externos a la capa landing del data lake.

    Del repositorio jdvelasq/datalabs/precio_bolsa_nacional/xls/ descarge los
    archivos de precios de bolsa nacional en formato xls a la capa landing. La
    descarga debe realizarse usando únicamente funciones de Python.

    """

    #import wget
    import os

    # se especifica el lugar donde se guardarán los archivos descargados
    #rutaDescarga = os.chdir('data_lake/landing/')

    # Dado que los archivos que se quieren descargar estan en la misma ruta y son numeros consecutivos se hace un ciclo for
    # para acceder a obtener la url de cada archivo. Además, como tenemos dos archivos xls (2016 y 2017) se hace un condicional 
    # para obtener la url de dichos archivos. Como ya tenemos las url, con wget descargamos todos los archivos.

    # for num in range(1995, 2022):
    #     if num in range(2016, 2018):
    #         urlArchivosXls = 'https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{}.xls?raw=true'.format(num)
    #         wget.download(urlArchivosXls)
    #     else:
    #         urlArchivosXlsx = 'https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{}.xlsx?raw=true'.format(num)
    #         wget.download(urlArchivosXlsx )

    for num in range(1995, 2022):
        if num in range(2016, 2018):
            #urlArchivosXls = https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{}.xls?raw=true.format(num)
            curl https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{}.xls?raw=true.format(num) -o data_lake/landing/{}.xls.format(num)
        else:
            #urlArchivosXlsx = https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{}.xlsx?raw=true.format(num)
            curl https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{}.xlsx?raw=true.format(num) -o data_lake/landing/{}.xls.format(num)
    
    #curl https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/1995.xlsx?raw=true -o data_lake/landing/1995.xlsx
    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()

ingest_data()