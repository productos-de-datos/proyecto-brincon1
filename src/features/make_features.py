"""
Módulo de preparación de datos para pronóstico.
-------------------------------------------------------------------------------

Debido a que tenemos una serie de tiempo con una única variable dependiente no
es necesario hacer cálculos y solo se guarda el archivo en la nueva ruta.

Requerimiento:
Cree el archivo data_lake/business/features/precios-diarios.csv. Este
archivo contiene la información para pronosticar los precios diarios de la
electricidad con base en los precios de los días pasados. Las columnas
correspoden a las variables explicativas del modelo, y debe incluir,
adicionalmente, la fecha del precio que se desea pronosticar y el precio
que se desea pronosticar (variable dependiente).

En la carpeta notebooks/ cree los notebooks de jupyter necesarios para
analizar y determinar las variables explicativas del modelo.

"""

import os
import pandas as pd

def make_features():
    """Prepara datos para pronóstico."""

    data = pd.read_csv('data_lake/business/precios-diarios.csv', index_col=0)
    data.to_csv('data_lake/business/features/precios-diarios.csv', encoding='utf-8', index=True)

    #raise NotImplementedError("Implementar esta función")

def test_11():
    """Evalua la creación del archivo"""
    assert os.path.isfile('data_lake/business/features/precios-diarios.csv') is True

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    make_features()
