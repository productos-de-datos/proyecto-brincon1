def transform_data():
    """Transforme los archivos xls a csv.

    Transforme los archivos data_lake/landing/*.xls a data_lake/raw/*.csv. Hay
    un archivo CSV por cada archivo XLS en la capa landing. Cada archivo CSV
    tiene como columnas la fecha en formato YYYY-MM-DD y las horas H00, ...,
    H23.

    """

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
