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
            return luigi.LocalTarget('data_lake/raw/1995.txt')

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
        #     return TablaUnicaPrecios()

        def output(self):
            return luigi.LocalTarget('data_lake/business/1995.txt')

        def run(self):
            with self.output().open("w") as outfile:
                compute_monthly_prices()
                

    
        
                # else:
                #     url = 'https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{}.xlsx?raw=true'.format(num)
                #     file = req.get(url, allow_redirects=True)
                #     with self.output().open('data_lake/landing/{}.xlsx'.format(num), 'wb') as outfile:
                #         outfile.write(file.content)
                #         self.save_path.replace("*", f"{num}")

    # https://gist.github.com/tomsing1/4c433655b3ac1aedb372ddfb1c7954db
    # https://stackoverflow.com/questions/52694065/atomically-read-from-excel-for-luigi-workflow

    # from PIL import Image
    # import requests
    # import luigi

    # class DownloadImages(luigi.Task):
    #     save_path = f"img/*.jpg"

    #     def output(self):
    #         return luigi.LocalTarget(self.save_path)

    #     def run(self):
    #         img_ids = [1,2,3]
    #         self.imgs = []
    #         for img_id in img_ids:
    #             img = Image.open(requests.get("https://i.kym-cdn.com/entries/icons/original/000/000/007/bd6.jpg", stream=True).raw)
    #             img.save(self.save_path.replace("*", f"img_{img_id}"))



    #raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":
    import doctest

    doctest.testmod()
    
luigi.run(["PrecioPromedioDiario", "--local-scheduler"])