"""
Módulo de computación de precios diarios.
-------------------------------------------------------------------------------

En este módulo se asigna como indice la columna fecha en formato fecha y después
se calcula el precio promedio por día. Selecciona el precio promedio diario y
se guarda con indice.

Requerimiento:
Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
promedio diario (sobre las 24 horas del dia) para cada uno de los dias. Las
columnas del archivo data_lake/business/precios-diarios.csv son:

    * fecha: fecha en formato YYYY-MM-DD
    * precio: precio promedio diario de la electricidad en la bolsa nacional

"""
import pandas as pd

def compute_daily_prices():
    """Compute los precios promedios diarios."""
    read_file = pd.read_csv('data_lake/cleansed/precios-horarios.csv')
    read_file['fecha'] = pd.to_datetime(read_file['fecha'], format="%Y/%m/%d")
    file_index = read_file.set_index('fecha')

    average_daily = file_index.resample('D').mean()
    data = average_daily.iloc[:, [1]]

    data.to_csv('data_lake/business/precios-diarios.csv', encoding='utf-8', index=True)

    # raise NotImplementedError("Implementar esta función")
def test_05():
    """Evalua el nombre y numero de columnas de un archivo"""
    assert pd.read_csv('data_lake/business/precios-diarios.csv').columns.tolist() == ["fecha",
                                                                            "precio"]
    assert pd.read_csv('data_lake/business/precios-diarios.csv').shape[1] == 2

def test_06():
    """Evalua los datos del precio promedio diaria"""
    assert pd.read_csv('data_lake/business/precios-diarios.csv').precio.head().tolist() == [
        1.4094347826086957, 4.924333333333333, 1.2695, 0.9530833333333332, 4.305916666666667
    ]

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    compute_daily_prices()
