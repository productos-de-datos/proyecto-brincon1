
"""
Construya un pipeline de Luigi que:

* Importe los datos xls
* Transforme los datos xls a csv
* Cree la tabla unica de precios horarios.
* Calcule los precios promedios diarios
* Calcule los precios promedios mensuales

En luigi llame las funciones que ya creo.

"""
from ingest_data import ingest_data
from transform_data import transform_data
from clean_data import clean_data
from compute_daily_prices import compute_daily_prices
from compute_monthly_prices import compute_monthly_prices
import luigi
from luigi import Task, LocalTarget

if __name__ == "__main__":

    class DownloadFiles(Task):
      
        def output(self):
            return luigi.LocalTarget('data_lake/landing/1995.txt')

        def run(self):
            with self.output().open("w") as outfile:
                ingest_data()

    class TransformFiles(Task):

        def requires(self):
            return DownloadFiles()
   
        def output(self):
            return luigi.LocalTarget('data_lake/raw/precios.txt')

        def run(self):
            with self.output().open("w") as outfile:
                transform_data()

    class TablaUnicaPrecios(Task):
        def requires(self):
            return TransformFiles()

        def output(self):
            return luigi.LocalTarget('data_lake/cleansed/1995.txt')

        def run(self):
            with self.output().open("w") as outfile:
                clean_data()

    class PrecioPromedioDiario(Task):
        def requires(self):
            return TablaUnicaPrecios()

        def output(self):
            return luigi.LocalTarget('data_lake/business/1995.txt')

        def run(self):
            with self.output().open("w") as outfile:
                compute_daily_prices()

    class PrecioPromedioMensual(Task):
        # def requires(self):
        #      return TablaUnicaPrecios()

        def output(self):
            return luigi.LocalTarget('data_lake/business/1995.txt')

        def run(self):
            with self.output().open("w") as outfile:
                compute_monthly_prices()
 
    #raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    
luigi.run(["PrecioPromedioDiario", "--local-scheduler"])