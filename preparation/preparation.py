

# -*- coding: utf-8 -*-
# Example
# Cupo Propensity Model

# IMPORTS

# Standard libraries
import os
import sys
import boto3
import numpy as np
import pandas as pd
import pyspark
import seaborn as sns
from typing import Dict, List
from pyspark.sql import SparkSession
import src.commons.commons_helper as helper
from pyspark.sql.functions import col,sum,avg,max,count

import src.commons.commons_helper as helper
from src.preparation.prep_salarios import salarios


def conteo_cotizantes(fecha_inicial, fecha_final, spark, input_paths, output_path):
    """ Función para extraer el conteo de cotizantes a PO de la tabla aportes
    :param fecha_inicial: Formato: AAAAMM y entre comillas. Tener en cuenta que la historia confiable es de los últimos 4 años
    :param fecha_final: Formato: AAAAMM y entre comillas.
    :return: Archivo con datos de cotizantes
    """
    
    df = helper.read_parquet(input_paths["cuentas"], spark)
    pd_df = df.groupBy("periodo_pago").agg(count("cuenta").alias("Cotizantes")).toPandas()
    pd_df["periodo_pago"] = pd_df.periodo_pago.astype(int).astype(str)
    pd_df["periodo_anio"] = pd_df.periodo_pago.str[:4]
    pd_df["periodo_mes"] = pd_df.periodo_pago.str[4:]
    pd_df.sort_values(["periodo_anio","periodo_mes"], ascending=[True, True] ,inplace=True)
    pd_df = pd_df[["periodo_pago", "Cotizantes"]]
    pd_df["periodo_pago"] = pd_df.periodo_pago.astype(int)
    pd_df = pd_df[(pd_df['periodo_pago'] >= int("201801")) & (pd_df['periodo_pago'] <= int(fecha_final)-2)]
    pd_df['Fecha'] = pd.date_range(start='31/01/2018', periods=len(pd_df), freq='M') 
    hist = pd.read_csv(input_paths["historia"][0], header=0, sep=';', encoding='utf-8')
    hist = hist.iloc[12:]
    hist['Fecha'] = pd.date_range(start='31/01/2001', periods=len(hist), freq='M') 
    hist=hist[['Fecha','NUM_COTIZANTES']]
    hist.rename(columns={'NUM_COTIZANTES':'Cotizantes'}, inplace = True)
    cot_def=pd.concat([hist, pd_df])
    cot_def=cot_def[['Fecha','Cotizantes']]
    
    path_salida = output_path + os.environ["PROCESSED_COTIZANTES"] + fecha_final

    helper.export_csv(cot_def,path_salida)
    # cot_def.to_csv(path_salida + str(int(fecha_final))+ ".csv",
    #                     sep = ';',
    #                     decimal='.',
    #                     index = False)
    return cot_def

def master_data(fecha_actualizacion, cot_def, spark, input_paths, output_path):
    """ Función para crear el df definitivo que se procesará en el modelo
    :param fecha_actualización: Fecha de corte de los datos. Formato: AAAAMM y entre comillas
    :return: Archivo con datos necesarios para el modelo
    """
    #Tasa de desempleo
    #path_salida = "s3://data-porv-dev-sandbox/caso-uso/data-trends/data/mercado-laboral/MDT_"#######
    path_salida = output_path + os.environ["PROCESSED_TASA_DESEMPLEO_MDT"] + fecha_actualizacion

    # df_dane = spark.read.option("delimiter", "|").option("header", "true").csv(path_desempleo + file_dane + fecha_actualizacion + ".csv").toPandas()
    df_dane = pd.read_csv(input_paths["desempleo"][0], header=0, sep='|', encoding='utf-8')
    df_dane['ds'] = pd.date_range(start='31/01/2001', periods=len(df_dane), freq='M')
    df_dane.rename(columns={'TD':'y'}, inplace = True)
    df = df_dane[['ds', 'y']]
    
    #Recaudo
    
    df_recaudo=pd.read_csv(input_paths["recaudo"][0], header=0, sep=';', encoding='utf-8')
    df_recaudo['Fecha'] = pd.date_range(start='31/01/2001', periods=len(df_recaudo), freq='M') 
    df_recaudo.rename(columns={'Fecha':'Fecha','Recaudo Bruto Industria':'Recaudo'}, inplace = True)
    df['recaudo']=df_recaudo['Recaudo'].iloc[:-1].values
    
    #Cotizantes
    df['cotizantes']=cot_def['Cotizantes'].values

    helper.export_csv(df,path_salida)
    # df.to_csv(path_salida + str(int(fecha_actualizacion))+ ".csv",
    #                     sep = ';',
    #                     decimal='.',
    #                     index = False)
    
    return(df)

# def salario() -> pd.Dataframe:
#     df = 
#     return df

def process(input_paths: Dict[str, List[str]], start_date, end_date, spark) -> pd.DataFrame:
    
    output_path = (
        "s3://"
        + os.environ["BUCKET_MODLES_OUTPUT"]
        + "/"
        + os.environ["PIPELINE_DATA"]
        + end_date
        + "/"
    )
    

    cot_def = conteo_cotizantes(start_date, end_date, spark, input_paths, output_path)
    df_mdt = master_data(end_date, cot_def, spark, input_paths, output_path)
    #df_mdt = master_data(end_date, cot_def, spark, input_paths)
    df_salarios = salarios(start_date, end_date, spark, input_paths)
    print(df_mdt.head(20))
    print(df_salarios.head(20))

    return df_mdt


# def check_no_duplicate_idx(df_list: List[pd.DataFrame]) -> None:
#     """
#     This functions checks that all dfs in list have unique indices.
#     """

#     for df in df_list:
#         assert (
#             df.index.duplicated().sum() == 0
#         ), "One of the dfs has repeated indices. Cannot concatenate."


# def create_mdt(df_list: List[pd.DataFrame]) -> pd.DataFrame:
#     """
#     This function receives a list of dataframes and concatenates them
#     horizontally, then removes the entries which index is in the 2nd argiment.
#     """

#     return df_list
