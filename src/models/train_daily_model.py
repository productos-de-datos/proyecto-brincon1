"""
Documentación:

"""
def train_daily_model():
    """Entrena el modelo de pronóstico de precios diarios.

    Con las features entrene el modelo de pronóstico de precios diarios y
    salvelo en models/precios-diarios.pkl


    """
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from sklearn.model_selection import GridSearchCV
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import RandomForestRegressor

    import pickle
    import os

    def load_data():

        df = pd.read_csv('data_lake/business/features/precios-diarios.csv')
        df['fecha'] = pd.to_datetime(df['fecha'], format="%Y/%m/%d")
        df = df.set_index('fecha')
        df = df.asfreq("D")
        df = df.sort_index()
        y = df["avg_daily_price"]
        x = df.copy()
        x.drop(["avg_daily_price"], axis=1, inplace=True)
        return x, y

    ## forma 2
        # df = pd.read_csv('data_lake/business/features/precios-diarios.csv')
        # df['fecha'] = pd.to_datetime(df['fecha'], format="%Y/%m/%d")
        # df = df.set_index('fecha').asfreq("D")
        # y = df["avg_daily_price"]
        # x = df.copy()
        # x.drop(["avg_daily_price"], axis=1, inplace=True)
    
    def make_train_test_split(x, y):

        (x_train, x_test, y_train, y_test) = train_test_split(
            x,
            y,
            test_size=0.25,
            random_state=123456,
        )
        return x_train, x_test, y_train, y_test

    # def train_model():

    #     x, y = load_data()
    #     x_train, x_test, y_train, y_test = make_train_test_split(x, y)

    #     # Crea el preprocesador
    #     scaler = StandardScaler()

    #     # Entrena el preprocesador. Note que se calcula
    #     # unicamente para el conjunto de entrenamiento
    #     scaler.fit(x_train)

    #     # Escala los conjuntos de entrenamiento y prueba
    #     x_train = scaler.transform(x_train)
    #     x_test  = scaler.transform(x_test)


    #     forecaster = ForecasterAutoreg(
    #                 regressor = RandomForestRegressor(random_state=123),
    #                 lags = 6
    #             )

    #     #forecaster.fit(y=datos_train['y'])
    #     return forecaster

    def save_model(model):

        import pickle

        with open("src/models/precios-diarios.pickle", "wb") as file:
            pickle.dump(model, file)

        
    # def load_model():

    #     import pickle

    #     with open("src/models/precios-diarios.pickle", "rb") as file:
    #         model = pickle.load(file)

    #     return model

    def train_model():

        x, y = load_data()
        x_train, x_test, y_train, y_test = make_train_test_split(x, y)

        # Crea el preprocesador
        scaler = StandardScaler()

        # Entrena el preprocesador. Note que se calcula
        # unicamente para el conjunto de entrenamiento
        scaler.fit(x_train)

        # Escala los conjuntos de entrenamiento y prueba
        x_train = scaler.transform(x_train)
        x_test  = scaler.transform(x_test)

        model = ForecasterAutoreg(
                regressor = RandomForestRegressor(random_state=123),
                lags = 6
             )

        model.fit(y_train)

        save_model(model)
    
    train_model()

    #     estimator = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=12345)
    #     estimator.fit(x_train, y_train)
    #     mse, mae, r2 = eval_metrics(y_test, y_pred=estimator.predict(x_test))
    #     if verbose > 0:
    #         report(estimator, mse, mae, r2)

    #     best_estimator = load_best_estimator()
    #     if best_estimator is None or estimator.score(x_test, y_test) > best_estimator.score(
    #         x_test, y_test
    #     ):
    #         best_estimator = estimator

    #     save_best_estimator(best_estimator)
    
    # train_estimator(0.1, 0.05)


    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()

train_daily_model()