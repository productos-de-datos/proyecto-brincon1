
def clean_data():
    """Realice la limpieza y transformación de los archivos CSV.

    Usando los archivos data_lake/raw/*.csv, cree el archivo data_lake/cleansed/precios-horarios.csv.
    Las columnas de este archivo son:

    * fecha: fecha en formato YYYY-MM-DD
    * hora: hora en formato HH
    * precio: precio de la electricidad en la bolsa nacional

    Este archivo contiene toda la información del 1997 a 2021.


    """
    import pandas as pd
    import glob
    import os

    for num in range(1995, 2022):
        if num in range (1995, 2000):
            filename = 'data_lake/raw/{}.csv'.format(num)
            df = pd.read_csv(filename)
            df = df.iloc[2:]
            df.to_csv('data_lake/raw/{}.csv'.format(num), encoding='utf-8', index=False, header=0)
        elif num in range (2000, 2018):
            filename = 'data_lake/raw/{}.csv'.format(num)
            df = pd.read_csv(filename)
            df = df.iloc[1:]
            df.to_csv('data_lake/raw/{}.csv'.format(num), encoding='utf-8', index=False, header=0)

    # merging the files
    files_joined = os.path.join('data_lake/raw/', "*.csv")

    # Return a list of all joined files
    list_files = glob.glob(files_joined)
    print(list_files)

    print("** Merging multiple csv files into a single pandas dataframe **")
    # Merge files by joining all files
    dataframe = pd.concat(map(pd.read_csv, list_files), ignore_index=True, axis=0)
    dataframe.to_csv('data_lake/cleansed/precios-horarios.csv', encoding='utf-8', index=False, header=0)

    # raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()

clean_data()