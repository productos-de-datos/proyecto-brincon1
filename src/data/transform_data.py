"""
Documentación

Se hacen dos condicionales, el primero para leer los archivos terminados en xls y el segundo para leer los archivos xlsx.
Se eliminan las filas que no tengan al menos 10 valores distintos de N/A. Esto se hace porque hay archivos que tienen filas
vacias en el encabezado y al final de la página.
Además, se inicia desde la fila 1 para no considerar el nombre de las columnas y que por defecto se asigne una lista 
como nombre. También se seleccionan solo las columnas de interes, de la 0 a la 24 y se asigna la columna 0 con formato YYYY-MM-DD.
y despues hacer el reemplazo de los nombres más fácil.

"""

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
            df = data_xls.dropna(axis=0, thresh=10)
            df = df.iloc[1:]
            df = df[df.columns[0:25]] 
            df[0] = pd.to_datetime(df[0], format="%Y/%m/%d")
            df.to_csv('data_lake/raw/{}.csv'.format(num), encoding='utf-8', index=False, header=True)
        else:
            data_xls = pd.read_excel('data_lake/landing/{}.xlsx'.format(num), index_col=None, header=None)
            df = data_xls.dropna(axis=0, thresh=10)
            df = df.iloc[1:]
            df = df[df.columns[0:25]] 
            df[0] = pd.to_datetime(df[0], format="%Y/%m/%d")
            df.to_csv('data_lake/raw/{}.csv'.format(num), encoding='utf-8', index=False, header=True)


    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()

transform_data()
