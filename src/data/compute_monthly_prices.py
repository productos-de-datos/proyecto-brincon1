"""
Módulo de computación de precios mensuales.
-------------------------------------------------------------------------------

En este módulo se asigna como indice la columna fecha en formato fecha y después
se calcula el precio promedio por mes. Se selecciona el precio promedio mensual 
y se guarda con indice.

Requerimiento:
Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el precio
promedio mensual. Las
columnas del archivo data_lake/business/precios-mensuales.csv son:

    * fecha: fecha en formato YYYY-MM-DD
    * precio: precio promedio mensual de la electricidad en la bolsa nacional

"""
import pandas as pd

def compute_monthly_prices():
    """Compute los precios promedios mensuales."""

    read_file = pd.read_csv('data_lake/cleansed/precios-horarios.csv')
    read_file['fecha'] = pd.to_datetime(read_file['fecha'], format="%Y/%m/%d")
    file_index = read_file.set_index('fecha')

    average_monthly = file_index.resample('M').mean()
    data = average_monthly.iloc[:, [1]]
    data.to_csv('data_lake/business/precios-mensuales.csv', encoding='utf-8', index=True)

    #raise NotImplementedError("Implementar esta función")
def test_07():
    """Evalua el nombre y numero de columnas de un archivo"""
    assert pd.read_csv('data_lake/business/precios-mensuales.csv').columns.tolist() == ["fecha",
                                                                            "precio"]
    assert pd.read_csv('data_lake/business/precios-mensuales.csv').shape[1] == 2

def test_08():
    """Evalua los datos del precio promedio diaria"""
    assert pd.read_csv('data_lake/business/precios-mensuales.csv').precio.head().tolist() == [
        1.5401994263285024, 7.086462365591398, 10.955819444444444, 10.445442204301076, 
        27.534781944444447]

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    compute_monthly_prices()
