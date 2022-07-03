"""
Módulo de pronostico con el modelo entrenado.
-------------------------------------------------------------------------------

En este módulo se carga el modelo, se hace el prónostico y después se desescala
los datos para llevar los valores a la escala de los datos originales.
El pronóstico del precio se une junto a la columna fecha y precio real y se
guardan en un archivo.

Requerimiento:
Cree el archivo data_lake/business/forecasts/precios-diarios.csv. Este
    archivo contiene tres columnas:

    * La fecha.

    * El precio promedio real de la electricidad.

    * El pronóstico del precio promedio real.

"""

import os
import pickle as pkl
import pandas as pd
import numpy as np
from train_daily_model import matriz_regresores
from train_daily_model import valores_escalados
from train_daily_model import tendencia_removida
from train_daily_model import load_data
from train_daily_model import comp_ciclica_removida
from sklearn.preprocessing import MinMaxScaler

def load_model():
    """Carga el modelo"""
    with open("src/models/precios-diarios.pkl", "rb") as file:
        model = pkl.load(file)
    return model

def make_forecasts():
    """Construye los pronosticos con el modelo entrenado final."""
    model = load_model()
    X = matriz_regresores()
    # pronostico
    y_d1d12_scaled_m2 = model.predict(X)
    return y_d1d12_scaled_m2

def desescalar_datos():
    """Desescala los datos pronosticados"""
    data_d1d12_scaled = valores_escalados()
    data_d1 = tendencia_removida()
    data_d1d12 = comp_ciclica_removida()
    data = load_data()
    P = 13
    scaler = MinMaxScaler()
    y_d1d12_scaled_m2 = make_forecasts()

    obj = scaler.fit(np.array(data_d1d12).reshape(-1, 1))
    y_d1d12_scaled_m2 = data_d1d12_scaled[0:P] + y_d1d12_scaled_m2.tolist()
    y_d1d12_m2 = obj.inverse_transform([[u] for u in y_d1d12_scaled_m2])
    y_d1d12_m2 = [u[0] for u in y_d1d12_m2.tolist()]
    y_d1_m2 = [y_d1d12_m2[t] + data_d1[t] for t in range(len(y_d1d12_m2))]
    y_d1_m2 = data_d1[0:12] + y_d1_m2
    y_m2 = [y_d1_m2[t] + data[t] for t in range(len(y_d1_m2))]
    y_m2 = [data[0]] + y_m2

    return y_m2

def save_pronostico():
    """Guarda los datos pronosticados y los reales."""
    y_m2 = desescalar_datos()
    df_2 = pd.DataFrame(y_m2)
    df_1 = pd.read_csv('data_lake/business/features/precios-diarios.csv')
    data = pd.concat([df_1, df_2], axis=1)
    data.columns = ["fecha", "precio_real", "pronostico_precio"]
    data.to_csv('data_lake/business/forecasts/precios-diarios.csv',
                encoding='utf-8', index=False, header=True)

    #raise NotImplementedError("Implementar esta función")

def test_13():
    """Evalua la creación del archivo"""
    assert os.path.isfile('data_lake/business/forecasts/precios-diarios.csv') is True

def test_14():
    """Evalua el nombre y numero de columnas"""
    assert pd.read_csv('data_lake/business/forecasts/precios-diarios.csv').columns.tolist() == [
                        "fecha", "precio_real", "pronostico_precio"]
    assert pd.read_csv('data_lake/business/forecasts/precios-diarios.csv').shape[1] == 3

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    save_pronostico()
