"""
Documentación:

Esta función lo que hace es crear un gráfico de lineas a partir del archivo de precios promedios mensuales.
"""

def make_daily_monthly_plot():
    """Crea un grafico de lines que representa los precios promedios mensuales.

    Usando el archivo data_lake/business/precios-mensuales.csv, crea un grafico de
    lines que representa los precios promedios mensuales.

    El archivo se debe salvar en formato PNG en data_lake/business/reports/figures/monthly_prices.png.

    """
    import pandas as pd
    import matplotlib.pyplot as plt

    df = pd.read_csv('data_lake/business/precios-mensuales.csv', index_col=0, parse_dates=True)
 
    df.plot(figsize=(12, 6))
    plt.savefig('data_lake/business/reports/figures/monthly_prices.png')


    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()

make_daily_monthly_plot()