

# -*- coding: utf-8 -*-
# Example
# Cupo Propensity Model

# preparation.py
# MVP 1.0

# This file contains the function 'process' which reads all data sources and
# creates and exports the pre master data table. This file is part of the
# productive pipeline.

# %%

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
from pyspark.sql.functions import col,sum,avg,max,count

def conteo_cotizantes(fecha_inicial, fecha_final, spark):
    """ Función para extraer el conteo de cotizantes a PO de la tabla aportes
    :param fecha_inicial: Formato: AAAAMM y entre comillas. Tener en cuenta que la historia confiable es de los últimos 4 años
    :param fecha_final: Formato: AAAAMM y entre comillas.
    :return: Archivo con datos de cotizantes
    """
    path_entrada = "s3://data-porv-dev-sandbox-de/estandarizado/productos/po/"
    file_aportes = "productos_po-cta-cuenta-aporte_ods_H"
    historia = "s3://data-porv-dev-sandbox/caso-uso/caso3/Informe_SAR_1051108_09042021_1013.csv"#####
    path_salida = "s3://data-porv-dev-sandbox/caso-uso/data-trends/data/mercado-laboral/"+"cotizantes"#######
    
    df = spark.read.parquet(path_entrada + file_aportes + fecha_inicial + "_" + fecha_final)
    pd_df = df.groupBy("periodo_pago").agg(count("cuenta").alias("Cotizantes")).toPandas()
    pd_df["periodo_pago"] = pd_df.periodo_pago.astype(int).astype(str)
    pd_df["periodo_anio"] = pd_df.periodo_pago.str[:4]
    pd_df["periodo_mes"] = pd_df.periodo_pago.str[4:]
    pd_df.sort_values(["periodo_anio","periodo_mes"], ascending=[True, True] ,inplace=True)
    pd_df = pd_df[["periodo_pago", "Cotizantes"]]
    pd_df["periodo_pago"] = pd_df.periodo_pago.astype(int)
    pd_df = pd_df[(pd_df['periodo_pago'] >= int("201801")) & (pd_df['periodo_pago'] <= int(fecha_final)-2)]
    pd_df['Fecha'] = pd.date_range(start='31/01/2018', periods=len(pd_df), freq='M') 
    hist = pd.read_csv(historia, header=0, sep=';', encoding='utf-8')
    hist = hist.iloc[12:]
    hist['Fecha'] = pd.date_range(start='31/01/2001', periods=len(hist), freq='M') 
    hist=hist[['Fecha','NUM_COTIZANTES']]
    hist.rename(columns={'NUM_COTIZANTES':'Cotizantes'}, inplace = True)
    cot_def=pd.concat([hist, pd_df])
    cot_def=cot_def[['Fecha','Cotizantes']]
    cot_def.to_csv(path_salida + str(int(fecha_final))+ ".csv",
                        sep = ';',
                        decimal='.',
                        index = False)
    return cot_def

def master_data(fecha_actualizacion, cot_def):
    """ Función para crear el df definitivo que se procesará en el modelo
    :param fecha_actualización: Fecha de corte de los datos. Formato: AAAAMM y entre comillas
    :return: Archivo con datos necesarios para el modelo
    """
    #Tasa de desempleo
    
    path_desempleo = 's3://data-porv-dev-sandbox/fuentes_publicas/dane/desempleo/'####
    file_dane = 'dane_desempleo-0_tnal-mensual_M'####
    df_dane = pd.read_csv(path_desempleo + file_dane + fecha_actualizacion + ".csv", header=0, sep='|', encoding='utf-8')
    df_dane['ds'] = pd.date_range(start='31/01/2001', periods=len(dane), freq='M')
    df_dane.rename(columns={'TD':'y'}, inplace = True)
    df = df_dane[['ds', 'y']]
    
    #Recaudo
    
    df_recaudo=pd.read_csv("s3://data-porv-dev-sandbox/caso-uso/caso3/Recaudo_M"+
                           fecha_actualizacion+".csv", header=0, sep=';', encoding='utf-8')
    df_recaudo['Fecha'] = pd.date_range(start='31/01/2001', periods=len(df_recaudo), freq='M') 
    df_recaudo.rename(columns={'Fecha':'Fecha','Recaudo Bruto Industria':'Recaudo'}, inplace = True)
    df['recaudo']=df_recaudo['Recaudo'].iloc[:-1].values
    
    
    #Cotizantes
    df['cotizantes']=cot_def['Cotizantes'].values
    
    return(df)

# def salario() -> pd.Dataframe:
#     df = 
#     return df

# def process(input_paths: Dict[str, List[str]], start_date, end_date, spark) -> pd.DataFrame:
def process(spark) -> pd.DataFrame:
    
    cot_def = conteo_cotizantes("201711", "202111", spark)
    df_mdt = master_data("202109", cot_def)

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
