
"""
Módulo pipeline.
-------------------------------------------------------------------------------
En este módulo se llaman todas las funciones creadas y se van ejecutando una 
después de la otra. Para hacer esto se hace uso de las dependencias y se
guardan los archivos en la ruta especificada.

Construya un pipeline de Luigi que:

* Importe los datos xls
* Transforme los datos xls a csv
* Cree la tabla unica de precios horarios.
* Calcule los precios promedios diarios
* Calcule los precios promedios mensuales

En luigi llame las funciones que ya creo.

"""
import luigi
from luigi import Task, LocalTarget

class DownloadFiles(Task):

    def output(self):
        return LocalTarget('data_lake/landing/archivo.txt')

    def run(self):
        from ingest_data import ingest_data
        with self.output().open("w") as outfile:
            ingest_data()

class TransformFiles(Task):

    def requires(self):
        return DownloadFiles()
    
    def output(self):
        return LocalTarget('data_lake/raw/archivo.txt')

    def run(self):
        from transform_data import transform_data
        with self.output().open("w") as outfile:
            transform_data()

class TablaUnicaPrecios(Task):
    
    def requires(self):
        return TransformFiles()
    
    def output(self):
        return LocalTarget('data_lake/cleansed/archivo.txt')

    def run(self):
        from clean_data import clean_data
        with self.output().open("w") as outfile:
            clean_data()

class PrecioPromedioDiario(Task):
    
    def requires(self):
        return TablaUnicaPrecios()
    
    def output(self):
        return LocalTarget('data_lake/business/archivo.txt')

    def run(self):
        from compute_daily_prices import compute_daily_prices
        with self.output().open("w") as outfile:
            compute_daily_prices()

class PrecioPromedioMensual(Task):
    
    def requires(self):
        return PrecioPromedioDiario()
    
    def output(self):
        return LocalTarget('data_lake/business/archivo.txt')

    def run(self):
        from compute_monthly_prices import compute_monthly_prices
        with self.output().open("w") as outfile:
            compute_monthly_prices()


if __name__ == "__main__":
    luigi.run(["PrecioPromedioMensual", "--local-scheduler"])

    import doctest

    doctest.testmod()