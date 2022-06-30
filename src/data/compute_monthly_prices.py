"""
Documentación:

Se establece la fecha como indice al momento de leer el archivo CSV.
Se calcula el promedio diario y con este se calcula el promedio mensual.
Se eliminan los indices y con estos como columnas se seleccionan, se unen
y se crea una nueva columna estableciendo el día 10 para todos los meses.
Por ùltimo se selecciona la columna que contiene la fecha unida y el precio promedio mensual.

"""


def compute_monthly_prices():
    """Compute los precios promedios mensuales.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio mensual. Las
    columnas del archivo data_lake/business/precios-mensuales.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio mensual de la electricidad en la bolsa nacional

    """
    
    import pandas as pd
  
    df = pd.read_csv('data_lake/cleansed/precios-horarios.csv')
    df['fecha'] = pd.to_datetime(df['fecha'], format="%Y/%m/%d")
    df = df.set_index('fecha')

    df = df.resample('M').mean()
    df = df.reset_index()
    df = df.iloc[:, [0, 2]]

    df.to_csv('data_lake/business/precios-mensuales.csv', encoding='utf-8', index=False)
  
    #raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":
    import doctest

    doctest.testmod()


compute_monthly_prices()