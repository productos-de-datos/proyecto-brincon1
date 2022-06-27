"""
Documentación:

"""

from matplotlib.pyplot import axis


def make_features():
    """Prepara datos para pronóstico.

    Cree el archivo data_lake/business/features/precios-diarios.csv. Este
    archivo contiene la información para pronosticar los precios diarios de la
    electricidad con base en los precios de los días pasados. Las columnas
    correspoden a las variables explicativas del modelo, y debe incluir,
    adicionalmente, la fecha del precio que se desea pronosticar y el precio
    que se desea pronosticar (variable dependiente).

    En la carpeta notebooks/ cree los notebooks de jupyter necesarios para
    analizar y determinar las variables explicativas del modelo.

    """
    import pandas as pd
    import numpy as np

  
    df = pd.read_csv('data_lake/cleansed/precios-horarios.csv')

    print(df)


    # X, y = df.data, df.target
    # print(X.shape, y.shape)


    #select_columns.to_csv('data_lake/business/features/precios-diarios.csv', encoding='utf-8', index=False)

    
    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()

make_features()