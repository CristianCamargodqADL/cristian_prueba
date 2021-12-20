import os
import configparser
import datetime as datetime

from typing import Dict
from dateutil.relativedelta import relativedelta
from src.commons.commons_helper import LOGGER


def get_paths(
    start_period: str,
    end_period: str,
    job_name: str,
) -> Dict:

    """
    Get the input paths required for the given process
    :param start_period: Start period required for data that is not historical
    :param end_period: End period for both historical and monthly data. Corresponds to the score date
    :param job_name: Used to identify which data paths should be returned
    """

    input_paths = {}
    if job_name == "prep":
        input_paths = get_prep_paths(
            start_period, end_period
        )
    elif job_name == "scoring":
        input_paths = get_scoring_paths(
            end_period, bucket, data_path, job_name
        )

    return input_paths

def read_config(config_name: str):
    """
    Parse the config.ini file to find the data source name and relative delta
    :return:
    """
    config = configparser.ConfigParser()
    config.sections()
    config.read(os.environ["CONFIG_PATH"])

    period_source_dict = {}
    for key, val in config.items(config_name):
        period_source_dict[key] = val

    return period_source_dict

def read_config() -> Dict:
    """
    Parse the config.ini file to find the data source name and relative delta
    :return:
    """
    period_source_dict = {
        "cuentas": "0M",
        "historia": "0M",
        "desempleo": "0M",
        "recaudo": "0M",
        "salario": "0M"
    }
    return period_source_dict

def search_files_from_dynamo_parquet(data_source_name: str, start_period: str, end_period: str) -> str:
    paths = {
        "cuentas":"s3://data-porv-dev-sandbox-de/estandarizado/productos/po/productos_po-cta-cuenta-aporte_ods_H{0}_{1}",
        "historia":"s3://data-porv-dev-sandbox/caso-uso/caso3/Informe_SAR_1051108_09042021_1013.csv",
        "desempleo":"s3://data-porv-dev-sandbox/fuentes_publicas/dane/desempleo/dane_desempleo-0_tnal-mensual_M{}.csv",
        "recaudo":"s3://data-porv-dev-sandbox/caso-uso/caso3/Recaudo_M{}.csv",
        "salario":"s3://data-porv-dev-sandbox/caso-uso/caso3/Salario_minimo_{}.csv"
    }
    if data_source_name == "cuentas":
        return paths.get(data_source_name).format(start_period, end_period)
    elif data_source_name == "salario":
        return paths.get(data_source_name).format(end_period[:4])
    else:
        return paths.get(data_source_name).format(end_period)

def get_bucket():
    return os.environ["BUCKET_INPUT"]

def get_prep_paths(start_period, end_period):
    """
    Get the input paths required for preparation process
    :param start_period: Start period required for data that is not historical
    :param end_period: End period for both historical and monthly data. Corresponds to the score date
    """

    period_source_dict = read_config()

    dict_paths = {}
    missing_file = []
    for source_name, deltas in period_source_dict.items():
        for relative_delta in deltas.split(" "):

            bucket = get_bucket()

            path = search_files_from_dynamo_parquet(source_name, start_period, end_period)

            dict_paths[source_name] = [path]

    #         checkfile = helper.get_s3_files("s3://" + bucket + path + "/_SUCCESS", "1")
    #         if checkfile:

    #             storage = AWSDynamodb(os.environ["TABLE_NAME_STD"])
    #             key_value = {"bucket_name": bucket, "path": path}
    #             path = storage.get(key_value)
    #             try:
    #                 if "" == path:
    #                     return {}
    #                 if source_name in dict_paths:
    #                     files = dict_paths[source_name]
    #                     files.append("s3://{}{}".format(bucket, path["Item"]["path"]))
    #                     dict_paths[source_name] = files
    #                 else:
    #                     dict_paths[source_name] = [
    #                         "s3://{}{}".format(bucket, path["Item"]["path"])
    #                     ]
    #             except Exception as error:
    #                 LOGGER.error(error)
    #                 sys.exit(
    #                     f"No se encontro la insercion de la fuente estandarizada para el archivo {checkfile[0]}."
    #                 )
    #         else:
    #             missing_file.append(path.split("/")[-1])

    # if len(missing_file) > 0:
    #     LOGGER.info(f"missing_file pipepile: {missing_file}")
    #     notify_ses(missing_file, period, "bocc")
    #     raise ValueError("Missing File...")

    return dict_paths

def get_scoring_paths(
    end_period: str,
    bucket: str,
    pipeline_data: str,
) -> Dict:

#     """
#     It returns the paths required for the scoring process. Checks if the paths exist in s3
#     In case of missing files a sns notification will be sent and False will be returned.
#     :param end_period: Score date
#     :param bucket: Bucket where input data that is located
#     :param pipeline_data: Path where the pipeline data is being stores (MDT location)
#     """

#     config_mdt_files = "mdt"
#     dict_paths_scoring = read_config(config_mdt_files)

#     for key_file, file_path in list(dict_paths_scoring.items()):

#         input_data = (
#             pipeline_data
#             + str(end_period.strftime("%Y%m"))
#             + "/"
#             + os.environ["PREPARATION"]
#             + "/"
#         )

#         dict_paths_scoring.update(
#             {key_file: "s3://" + bucket + "/" + input_data + file_path}
#         )

#     return dict_paths_scoring

    input_paths = {}
    return input_paths