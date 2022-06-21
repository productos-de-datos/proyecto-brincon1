def transform_data():
    """Transforme los archivos xls a csv.

    Transforme los archivos data_lake/landing/*.xls a data_lake/raw/*.csv. Hay
    un archivo CSV por cada archivo XLS en la capa landing. Cada archivo CSV
    tiene como columnas la fecha en formato YYYY-MM-DD y las horas H00, ...,
    H23.

    """

    # Se hace un ciclo for porque tenemos dos archivos xls así que en el condicional primero se transformas dichos archivos a xlsx
    # y despues se transforman a csv utilizando la funcion openpyxl, la cual abre cada archivo, lo recorre por filas y luego recorre 
    # cada data de cada fila y lo convierte a un valor. Despues ese valor se escribe en cada fila de cada archivo csv y se guardan.

    # for num in range(1995, 2022):
    #     if num in range(2016, 2018):
    #         pyexcel.save_as(file_name='data_lake/landing/{}.xls'.format(num), dest_file_name='data_lake/landing/{}.xlsx'.format(num))
    #         ob = csv.writer(open("data_lake/raw/{}.csv".format(num),'w', newline = ""))
    #         data = openpyxl.load_workbook('data_lake/landing/{}.xlsx'.format(num)).active
    #         for r in data.rows:
    #             row = [a.value for a in r]
    #             ob.writerow(row)
 
    #     else:
    #         ob = csv.writer(open("data_lake/raw/{}.csv".format(num),'w', newline = ""))
    #         data = openpyxl.load_workbook('data_lake/landing/{}.xlsx'.format(num)).active
    #         for r in data.rows:
    #             row = [a.value for a in r]
    #             ob.writerow(row)

    import pandas as pd

    for num in range(1995, 2022):
        if num in range(2016, 2018):
            data_xls = pd.read_excel('data_lake/landing/{}.xls'.format(num), index_col=None, header=None)
            data_xls.to_csv('data_lake/raw/{}.csv'.format(num), encoding='utf-8', index=False, header=False)
        else:
            data_xls = pd.read_excel('data_lake/landing/{}.xlsx'.format(num), index_col=None, header=None)
            data_xls.to_csv('data_lake/raw/{}.csv'.format(num), encoding='utf-8', index=False, header=False)

    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()

transform_data()