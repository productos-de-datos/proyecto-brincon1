"""
Módulo de creación del gráfico de precios mensuales.
-------------------------------------------------------------------------------

Esta función lo que hace es crear un gráfico de lineas a partir del archivo de
precios promedios mensuales.

Requerimiento:
Usando el archivo data_lake/business/precios-mensuales.csv, crea un grafico de
lines que representa los precios promedios mensuales.

El archivo se debe salvar en formato PNG en data_lake/business/reports/figures/monthly_prices.png.

"""
import os
import pandas as pd
import matplotlib.pyplot as plt

def make_monthly_prices_plot():
    """Crea un grafico de lines que representa los precios promedios mensuales."""
    read_file = pd.read_csv('data_lake/business/precios-mensuales.csv', index_col=0,
                            parse_dates=True)
    plt.figure(figsize=(14, 5))
    plt.plot(read_file)
    plt.xlabel('Fecha')
    plt.ylabel('Precio promedio mensual')
    plt.savefig('data_lake/business/reports/figures/monthly_prices.png')

    #raise NotImplementedError("Implementar esta función")

def test_10():
    """Evalua la creación del gráfico"""
    assert os.path.isfile('data_lake/business/reports/figures/monthly_prices.png') is True

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    make_monthly_prices_plot()
