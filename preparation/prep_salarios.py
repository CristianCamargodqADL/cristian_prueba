
import os
import sys
import boto3
import numpy as np
import pandas as pd
import pyspark
import s3fs
from pyspark.sql import SparkSession
from pyspark.sql.functions import add_months, col, date_format, sum, avg, max, count, expr, lit, concat, last_day, to_date, when
from pyspark.sql.types import StringType,IntegerType
import src.commons.commons_helper as helper
from datetime import datetime

def last_day_of_month(any_day):
        next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
        return next_month - datetime.timedelta(days=next_month.day)

def salarios(fecha_inicial, fecha_final, spark, input_paths):
    """ Función para extraer el salario base promedio de la tabla aportes
    :param fecha_inicial: Formato: AAAAMM y entre comillas. Tener en cuenta que la historia confiable es de los últimos 4 años
    :param fecha_final: Formato: AAAAMM y entre comillas.
    :return: Archivo con la tasa de desempleo
    """
    print(input_paths["cuentas"][0])
    df = spark.read.parquet(input_paths["cuentas"][0])
    sp_df = df.groupBy("periodo_pago").agg(expr('percentile(salario_base, array(0.50))')[0].alias("Salario_Mediano"),
                                           expr('percentile(salario_base, array(0.05))')[0].alias("Salario_p5"),
                                           expr('percentile(salario_base, array(0.25))')[0].alias("Salario_p25"),
                                           expr('percentile(salario_base, array(0.75))')[0].alias("Salario_p75"),
                                           expr('percentile(salario_base, array(0.95))')[0].alias("Salario_p95"))

    #                                        .toPandas()
    # pd_df["periodo_pago"] = pd_df.periodo_pago.astype(int).astype(str)
    # pd_df["Anio"] = pd_df.periodo_pago.str[:4]
    # pd_df["Mes"] = pd_df.periodo_pago.str[4:]
    # pd_df.sort_values(["Anio","Mes"], ascending=[True, True] ,inplace=True)
    # pd_df = pd_df[["periodo_pago","Anio","Salario_Mediano","Salario_p5","Salario_p25", "Salario_p75","Salario_p95"]]
    # pd_df["periodo_pago"] = pd_df.periodo_pago.astype(int)
    # pd_df = pd_df[(pd_df['periodo_pago'] >= int(fecha_inicial)) & (pd_df['periodo_pago'] <= int(fecha_final)-2)]
    
    # anio=fecha_inicial[:4]
    # mes=fecha_inicial[-2:]
       
    # fecha_i=last_day_of_month(datetime.date(int(anio),int(mes), 1))
    # pd_df['Fecha'] = pd.date_range(fecha_i, periods=len(pd_df), freq='M')
    
    sp_df = sp_df.withColumn("periodo_pago",(col("periodo_pago").cast(IntegerType()))).withColumn("periodo_pago",(col("periodo_pago").cast(StringType())))
    sp_df = sp_df.withColumn("Anio",col("periodo_pago").substr(lit(0), lit(4))).withColumn("Mes",sp_df.periodo_pago.substr(lit(5), lit(6)))
    sp_df = sp_df.select("periodo_pago","Anio","Mes","Salario_Mediano","Salario_p5","Salario_p25", "Salario_p75","Salario_p95")

    sp_df = sp_df.withColumn("periodo_pago",(col("periodo_pago").cast(IntegerType())))
    sp_df = sp_df.filter(col("periodo_pago").between(int(fecha_inicial),int(fecha_final)-2))
    sp_df = sp_df.withColumn("first_day",concat(col('Anio'), lit('-'), col('Mes'), lit('-'), lit('01')))
    #pd_df = sp_df.withColumn("Fecha",last_day(to_date(col("first_day")))).toPandas()
    pd_df = sp_df.withColumn("Fecha",last_day(to_date(col("first_day"))))

    #print(pd_df.printSchema())

    #print(pd_df.show(5))


    ###################################################################################################

    # df_minimo=pd.read_csv(input_paths["salario"][0], header=0, sep=';', encoding='utf-8')
    # df_minimo["Anio"]=df_minimo["Anio"].astype(str)

    # df_def=pd_df.merge(df_minimo, on='Anio', how='left')
    
    # print(df_def)

    # df_def["var_mensual"] = ""

    # for i in range(len(df_def['Salario_Mediano'])):
    #     if i > 0:
    #         df_def['var_mensual'][i] = ((df_def['Salario_Mediano'][i])/(df_def['Salario_Mediano'][i-1]))-1
    #     else:
    #         df_def['var_mensual'][i] = 0

    # df_def["Ratio_Minimos"] = ""

    # for i in range(len(df_def['Salario_Mediano'])):
    #     if i > 0:
    #         df_def['Ratio_Minimos'][i] = ((df_def['Salario_Mediano'][i])/(df_def['Minimo_Mensual'][i]))
    #     else:
    #         df_def['Ratio_Minimos'][i] = 0

    # df_def=df_def[["Fecha", "Salario_Mediano", "var_mensual","Salario_p5", "Salario_p25", "Salario_p75", "Salario_p95", "Minimo_Mensual", "Ratio_Minimos"]]
           
    # df_def.to_csv("s3://data-porv-dev-sandbox/caso-uso/data-trends/data/mercado-laboral/"+"salario"+str(int(fecha_final))+".csv",
    #                     sep = ';',
    #                     decimal='.',
    
    ###################################################################################################

    df_minimo = spark.read.option("delimiter", ";").option("header", "true").option("encoding", "utf-8").csv(input_paths["salario"][0])

    df_minimo = df_minimo.withColumn("Anio",(col("Anio").cast(StringType())))
    
    df_def = pd_df.join(df_minimo,pd_df["Anio"] == df_minimo["Anio"], "left")

    df_def_back = df_def.withColumn("periodo_pago_cast",(col("periodo_pago").cast(StringType())))
    df_def_back = df_def_back.withColumn("periodo_new", to_date("periodo_pago_cast", "yyyyMM"))
    df_def_back = df_def_back.withColumn("periodo_new_2", date_format(add_months("periodo_new", 1), "yyyyMM"))
    df_def_back = df_def_back.withColumn("periodo_new_2", (col("periodo_new_2").cast(StringType())))
    df_def_back = df_def_back.select(col("periodo_new_2").alias("periodo_new_2"), col("Salario_Mediano").alias("Salario_Mediano_New")) 

    df_def = df_def.join(df_def_back,df_def["periodo_pago"] == df_def_back["periodo_new_2"], "left")
    
    df_def = df_def.withColumn("var_mensual", (df_def["Salario_Mediano"] / df_def["Salario_Mediano_New"]) - 1)

    df_def = df_def.withColumn("Ratio_Minimos", (df_def["Salario_Mediano"] / df_def["Minimo_Mensual"]))

    df_def = df_def.withColumn("Ratio_Minimos", when(df_def.Salario_Mediano_New.isNull(), 0) .otherwise(df_def.Ratio_Minimos))
    
    df_def_fin = df_def.select("Fecha", "Salario_Mediano", "var_mensual","Salario_p5", "Salario_p25", "Salario_p75", "Salario_p95", "Minimo_Mensual", "Ratio_Minimos")

    df_def_fin = df_def_fin.na.fill(0)
    df_def_fin = df_def_fin.toPandas()

    

    output_path = (
        "s3://"
        + os.environ["BUCKET_MODLES_OUTPUT"]
        + "/"
        + os.environ["PIPELINE_DATA"]
        + fecha_final
        + "/"
    )

    path_salida = output_path + os.environ["PROCESSED_SALARIOS"] + str(int(fecha_final))

    #now = datetime.now()

    #df_def_fin.write.csv("s3://data-porv-dev-sandbox/caso-uso/data-trends/data/mercado-laboral/"+"salario_prueba.csv")
    #helper.export_csv(df_def_fin,"s3://data-porv-dev-sandbox/caso-uso/data-trends/data/mercado-laboral/"+"salario_"+str(int(fecha_final)))
    helper.export_csv(df_def_fin, path_salida)

    #now2 = datetime.now()

    # current_time = now2.strftime("%H:%M:%S")
    # print("Finish Time =", current_time)
    # print("Difference Time = ", now2 - now)
    
    return df_def_fin