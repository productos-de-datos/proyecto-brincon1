"""
Módulo de entrenamiento del modelo.
-------------------------------------------------------------------------------
En este módulo se selecciona la columna precios que es la que se desea pronósticar,
se remueve la tendencia, el componente ciclico, se escalan los datos, se construye
la matriz de regresores y luego se entrena el modelo y se guarda.
Todo se siguió de:
https://jdvelasq.github.io/courses/notebooks/sklearn_supervised_10_neural_networks/1-03_pronostico_series_de_tiempo.html

Requerimiento:

Con las features entrene el modelo de pronóstico de precios diarios y
salvelo en models/precios-diarios.pkl

"""

import os
import pickle as pkl
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.neural_network import MLPRegressor

def load_data():
    """Entrena el modelo de pronóstico de precios diarios."""
    read_file = pd.read_csv('data_lake/business/features/precios-diarios.csv')
    data = read_file["precio"]
    return data

def tendencia_removida():
    """Remueve la tendencia"""
    data = load_data()
    data_d1 = [data[t] - data[t - 1] for t in range(1, len(data))]
    return data_d1

def comp_ciclica_removida():
    """Remueve el componente ciclico"""
    data_d1 = tendencia_removida()
    data_d1d12 = [data_d1[t] - data_d1[t - 12] for t in range(12, len(data_d1))]
    return data_d1d12

def valores_escalados():
    """Escala los valores"""
    data_d1d12 = comp_ciclica_removida()
    scaler = MinMaxScaler()
    data_d1d12_scaled = scaler.fit_transform(np.array(data_d1d12).reshape(-1, 1))
    data_d1d12_scaled = [u[0] for u in data_d1d12_scaled] # el largo de los datos escalados es 9404
    return data_d1d12_scaled

def matriz_regresores():
    """Construye la matrix de regresores"""
    data_d1d12_scaled = valores_escalados()
    P = 13
    X = []
    for t in range(P - 1, len(data_d1d12_scaled) - 1):
        X.append([data_d1d12_scaled[t - n] for n in range(P)])
    d = data_d1d12_scaled[P:] # el largo de X y d es 9391
    return X

def save_model(model):
    """Guarda el modelo"""
    with open("src/models/precios-diarios.pkl", "wb") as file:
        pkl.dump(model, file, pkl.HIGHEST_PROTOCOL)

def train_daily_model():
    """Entrena el modelo de pronóstico de precios diarios."""
    X = matriz_regresores()
    data_d1d12_scaled = valores_escalados()
    H = 4  # Se escoge arbitrariamente
    np.random.seed(123456)
    mlp = MLPRegressor(
        hidden_layer_sizes=(H,),
        activation="logistic",
        learning_rate="adaptive",
        momentum=0.0,
        learning_rate_init=0.002,
        max_iter=100000,
    )
    model = mlp.fit(X[0:8441], data_d1d12_scaled[0:8441])# 9391 - 950 = 8441. 9391 es el largo de X
                                                    # y 950 es aproximadamente el 10% de los datos
    save_model(model)

    #raise NotImplementedError("Implementar esta función")

def test_12():
    """Evalua la creación del gráfico"""
    assert os.path.isfile("src/models/precios-diarios.pkl") is True

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    train_daily_model()
