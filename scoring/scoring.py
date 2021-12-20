from typing import Dict, List
import os
import sys
import boto3
import numpy as np
import pandas as pd
import pyspark
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,avg,max,count
import matplotlib.pyplot as plt
from fbprophet import Prophet

def modelo_prophet_cotizantes(fecha_actualizacion):
    """ Función para correr el modelo Prophet de cotizantes
    :param fecha_actualizacion: Fecha del momento en que se actualiza el modelo 
    """

    path_in = (
        "s3://"
        + os.environ["BUCKET_MODLES_OUTPUT"]
        + "/"
        + os.environ["PIPELINE_DATA"]
        + end_date
        + "/" + os.environ["PROCESSED_COTIZANTES"] + fecha_actualizacion + ".csv"
    )

    df=pd.read_csv(path_in, header=0,sep=';', encoding='utf-8')
    # df=pd.read_csv("s3://data-porv-dev-sandbox/caso-uso/data-trends/data/mercado-laboral/cotizantes" +fecha_actualizacion+".csv", 
    #                 header=0,sep=';', encoding='utf-8')
    df.rename(columns={'Fecha':'ds','Cotizantes':'y'}, inplace = True)
    m = Prophet(changepoint_prior_scale = 0.1,
    seasonality_prior_scale = 1.0,
    seasonality_mode = 'additive')
    m.fit(df)
    future = m.make_future_dataframe(periods=36, freq='M')
    fut = future
    forecast = m.predict(fut)
    py_cot=forecast[['ds', 'yhat']].iloc[-36:]
    py_cot.rename(columns={'ds':'Fecha','yhat':'cotizantes'}, inplace = True)
    return(py_cot[['cotizantes']])

def modelo_prophet_recaudo(fecha_actualizacion):
    """ Función para correr el modelo Prophet de cotizantes
    :param fecha_actualizacion: Fecha del momento en que se actualiza el modelo 
    """

    path_in = (
        "s3://"
        + os.environ["BUCKET_MODLES_OUTPUT"]
        + "/"
        + os.environ["PIPELINE_DATA_3"]
        + end_date
        + "/" + os.environ["PROCESSED_RECAUDO"] + fecha_actualizacion + ".csv"
    )

    df=pd.read_csv(path_in, header=0,sep=';', encoding='utf-8')

    # df=pd.read_csv("s3://data-porv-dev-sandbox/caso-uso/caso3/Recaudo_M"+fecha_actualizacion+".csv", header=0,sep=';', encoding='utf-8')
    df.rename(columns={'Fecha':'ds','Recaudo Bruto Industria':'y'}, inplace = True)
    df = df.dropna()
    m = Prophet(changepoint_prior_scale = 0.5,
    seasonality_prior_scale = 0.01,
    seasonality_mode = 'additive')
    m.fit(df)
    future = m.make_future_dataframe(periods=36, freq='M')
    fut = future
    forecast = m.predict(fut)
    py_cot=forecast[['ds', 'yhat']].iloc[-35:]
    py_cot.rename(columns={'ds':'Fecha','yhat':'recaudo'}, inplace = True)
    df.rename(columns={'ds':'Fecha','y':'recaudo'}, inplace = True)
    ultimo_valor=df.iloc[len(df)-1:]
    py_cot= pd.concat([ultimo_valor, py_cot])
    return(py_cot[['recaudo']])

def modelo_prophet_TD(fecha_actualizacion):
    """ Función para correr el modelo Prophet de tasa de desempleo
    :param fecha_actualizacion: Fecha del momento en que se actualiza el modelo 
    """
    # df = td_dane(fecha_actualizacion) # guardar antes en un master datatable - FE - "s3://data-porv-dev-sandbox/caso-uso/data-trends/modelo/tasa-desempleo/"
    # df2 = df_def(df) # guardar antes en un master datatable - FE - "s3://data-porv-dev-sandbox/caso-uso/data-trends/modelo/tasa-desempleo/feature_eng"
    
    path_in = (
            "s3://"
            + os.environ["BUCKET_MODLES_OUTPUT"]
            + "/"
            + os.environ["PIPELINE_DATA"]
            + end_date
            + "/" + os.environ["PROCESSED_TASA_DESEMPLEO_MDT"] + fecha_actualizacion + ".csv"
        )

    df=pd.read_csv(path_in, header=0,sep=';', encoding='utf-8')

    # ruta = "s3://data-porv-dev-sandbox/caso-uso/data-trends/data/mercado-laboral/MDT_"
    # df2 = pd.read_csv(ruta + fecha_actualizacion + ".csv", header=0, sep=';', encoding='utf-8')

    pandemia = pd.DataFrame({
      'holiday': 'pandemia',
      'ds': pd.to_datetime(['2020-03-31', '2020-04-30', '2020-05-31',
                            '2020-06-30', '2020-07-31', '2020-08-31', 
                            '2020-09-30', '2020-10-31', '2020-11-30', 
                            '2020-12-31', '2021-01-31', '2021-02-28',
                            '2021-03-31', '2021-04-30', '2021-05-31']),
      'lower_window': 0,
      'upper_window': 1,
    })

    holidays = pandemia
    
    m = Prophet(changepoint_prior_scale = 0.5,
    seasonality_prior_scale = 0.1,
    seasonality_mode = 'additive',
    holidays= holidays)
    m.add_regressor('recaudo')
    m.add_regressor('cotizantes')
    m.fit(df2)
    future = m.make_future_dataframe(periods=36, freq='M')
    fut = future.iloc[-36:]
    fut['recaudo'] = modelo_prophet_recaudo(fecha_actualizacion).values
    fut['cotizantes'] = modelo_prophet_cotizantes(fecha_actualizacion).values
    forecast = m.predict(fut)
    fig = m.plot(forecast)
    return (forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(36))

def scoring(input_paths: Dict[str, List[str]], start_period: str, end_period: str, spark):
    model = modelo_prophet_TD("202106")
    print("#$###########################################################")
    print(model)
