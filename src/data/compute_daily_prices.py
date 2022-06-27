"""
Documentación

Se agrega una nueva columna llamada promedio diario, la cual suma el precio de cada hora y lo divide en 24.
Después se selecciona solo la primera y la última columna y se guardan en un nuevo archivo.
"""

def compute_daily_prices():
    """Compute los precios promedios diarios.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio diario (sobre las 24 horas del dia) para cada uno de los dias. Las
    columnas del archivo data_lake/business/precios-diarios.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio diario de la electricidad en la bolsa nacional



    """
    import pandas as pd

    df = pd.read_csv('data_lake/cleansed/precios-horarios.csv')
    
    df['avg_daily_price'] = df.mean(axis=1, numeric_only=True)

    seleccion_columnas = df.iloc[:, [0,25]]

    seleccion_columnas.to_csv('data_lake/business/precios-diarios.csv', encoding='utf-8', index=False)

    # raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()

compute_daily_prices()