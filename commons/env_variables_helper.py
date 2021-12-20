# -*- coding: utf-8 -*-
import os
import configparser

TEST_JOB_ID = "00000000-0000-0000-0000-000000000000"

# Set value for the host of localstatck
LOCALSTACK_HOST = "http://localhost:4572"
LOCALSTACK_HOST_SNS = "http://localhost:4575"
LOCALSTACK_HOST_DYNAMO = "http://localhost:4569"

if (
    "IN_A_TEST_DOCKER_CONTAINER" in os.environ
    and os.environ["IN_A_TEST_DOCKER_CONTAINER"] == "True"
):
    LOCALSTACK_HOST = "http://localstack:4572"
    LOCALSTACK_HOST_SNS = "http://localstack:4575"
    LOCALSTACK_HOST_DYNAMO = "http://localstack:4569"
    os.environ["AWS_ACCESS_KEY_ID"] = "foo"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"


def set_env_variables(aws_access_key_id, aws_secret_access_key, env):
    """
    Set necessary environment variables
    :param aws_access_key_id: AWS access key id
    :param aws_secret_access_key: AWS secret key
    :param env: 'pro' for production environment, 'dev' for development environment
    :return: True if succeeds to load environment variables, otherwise False.
    """
    config = configparser.ConfigParser()
    config.sections()
    config.read("config.ini")

    os.environ["env"] = env

    # use case
    os.environ["USE_CASE"] = config["general"]["use_case"]

    os.environ["TABLE_NAME_STD"] = config[env]["table_dynamo_std"]
    os.environ["TABLE_NAME_INTEGRATION"] = config[env]["table_dynamo_integration_"]
    os.environ["TABLE_NAME_PROCESS"] = config[env]["table_dynamo_process_"]
    # os.environ["STD_DATA_DYNAMO"] = config[env]["std_data_dynamo"]

    os.environ["BUCKET_INPUT"] = config[env]["bucket_input"]
    os.environ["BUCKET_MODLES_OUTPUT"] = config[env]["bucket_models_output"]
    os.environ["PIPELINE_DATA"] = config[env]["pipeline_data"]

    #output_paths
    os.environ["PROCESSED_SALARIOS"] = config["output_paths"]["processed_salarios"]
    os.environ["PROCESSED_COTIZANTES"] = config["output_paths"]["processed_cotizantes"]
    os.environ["PROCESSED_RECAUDO"] = config["output_paths"]["processed_recaudo"]
    os.environ["PROCESSED_TASA_DESEMPLEO_COTIZANTES"] = config["output_paths"]["processed_tasa_desempleo_cotizantes"]
    os.environ["PROCESSED_TASA_DESEMPLEO_MDT"] = config["output_paths"]["processed_tasa_desempleo_MDT"]
    os.environ["PROCESSED_SCORING_TASA_DESEMPLEO"] = config["output_paths"]["processed_scoring_tasa_desempleo"]
    os.environ["EXTENSION"] = config["output_paths"]["extension"]

    #stages
    os.environ["PREPARATION"] = list(config["pipeline_info"]["pipeline_stages"].split(" "))[0]
    os.environ["SCORING"] = list(config["pipeline_info"]["pipeline_stages"].split(" "))[1]


    # else:
    #     # Buckets
    #     os.environ["BUCKET_INPUT"] = config[env]["bucket_pipeline"]
    #     os.environ["BUCKET_RESOURCES"] = config[env]["bucket_resources"]
    #     os.environ["BUCKET_MODLES_OUTPUT"] = config[env]["bucket_output"]

    # stages
    # os.environ["PREPARATION"] = list(
    #     config["pipeline_info"]["pipeline_stages"].split(" ")
    # )[0]
    # os.environ["FE"] = list(config["pipeline_info"]["pipeline_stages"].split(" "))[1]
    # os.environ["SCORING"] = list(config["pipeline_info"]["pipeline_stages"].split(" "))[
    #     2
    # ]
    # os.environ["BACKTESTING"] = list(
    #     config["pipeline_info"]["pipeline_stages"].split(" ")
    # )[3]
    # os.environ["LEARNING"] = list(
    #     config["pipeline_info"]["pipeline_stages"].split(" ")
    # )[5]

    # os.environ["SCORE_DATA_COLPENSIONS"] = config["scoring_models"][
    #     "score_data_colpensions"
    # ]
    # os.environ["SCORE_DATA_AFP"] = config["scoring_models"]["score_data_afp"]

    # Paths
    # os.environ["PROCESSED_CLIENTES"] = config["output_paths"]["processed_clientes"]
    # os.environ["PROCESSED_PO"] = config["output_paths"]["processed_po"]
    # os.environ["PROCESSED_INTERACCIONES"] = config["output_paths"][
    #     "processed_interacciones"
    # ]
    # os.environ["PROCESSED_SYQ"] = config["output_paths"]["processed_syq"]
    # os.environ["PROCESSED_VOLUNTARIAS"] = config["output_paths"][
    #     "processed_voluntarias"
    # ]
    # os.environ["PROCESSED_BONOS_PENSIONALES"] = config["output_paths"][
    #     "processed_bonos_pensionales"
    # ]
    # os.environ["PROCESSED_BONOS_SOLICITUD"] = config["output_paths"][
    #     "processed_bonos_solicitud"
    # ]
    # os.environ["FUENTES_UNIDAS"] = config["output_paths"]["fuentes_unidas"]
    # os.environ["MDT_PATH"] = config["mdt"]["mdt_path"]

    # Models info
    # os.environ["AFP_FEATURES"] = config["scoring"]["model_afp_features"]
    # os.environ["XGB_AFP"] = config["scoring"]["xgb_model_afp"]
    # os.environ["XGB_COLPENSIONES"] = config["scoring"]["xgb_model_colpensiones"]
    # os.environ["COLPENSIONES_FEATURES"] = config["scoring"][
    #     "model_colpensiones_features"
    # ]
    # os.environ["VALUE_PER_CLIENTE"] = config["scoring"]["value_per_cliente"]
    # os.environ["SCORING_AFP"] = config["scoring"]["scoring_afp"]
    # os.environ["SCORING_COLPENSIONES"] = config["scoring"]["scoring_colpensiones"]
    # os.environ["THRESHOLS"] = config["scoring"]["threshols"]

    # Score models
    # os.environ["SCORE_PATH_COL"] = config["scoring_models"]["score_data_colpensions"]
    # os.environ["SCORE_PATH_AFP"] = config["scoring_models"]["score_data_afp"]

    # Output path
    # os.environ["SCORE_PATH"] = config[env]["score_resultados_data"]
    # os.environ["STD_DATA"] = config[env]["std_data"]
    # os.environ["PIPELINE_DATA"] = config[env]["pipeline_data"]
    # os.environ["RESOURCES_DATA"] = config[env]["resources_data"]

    # Sns
    # os.environ["SNS_TOPIC_QUALITY"] = config[env]["sns_topic_quality"]

    # Dynamo


    # Parameter store
    # os.environ["SMMLV"] = config[env]["pstore_smmlv"]
    # os.environ["SMMLV"] = config["others"]["smmlv"]

    # CloudWatch Log Group
    # os.environ["CLOUDWATCH_LOG_GROUP"] = config[env]["cloudwatch_log_group"]

    # if "AWS_BATCH_JOB_ID" not in os.environ:
    #     os.environ["AWS_BATCH_JOB_ID"] = TEST_JOB_ID
    # if aws_access_key_id != "":
    #     os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
    # if aws_secret_access_key != "":
    #     os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key


# #Otro
# os.environ["STD_DATA"] = config["otro"]["std_data"]
# os.environ["STD_DATA_DYNAMO"] = config["otro"]["std_data_dynamo"]
# os.environ["PIPELINE_DATA"] = config["otro"]["pipeline_data"]
# os.environ["SCORE_RESULTADOS_DATA"] = config["otro"]["score_resultados_data"]
# os.environ["RESOURCES_DATA"] = config["otro"]["resources_data"]
# os.environ["PSTORE_SMMLV"] = config["otro"]["pstore_smmlv"]
# os.environ["SNS_TOPIC_QUALITY"] = config["otro"]["sns_topic_quality"]
# os.environ["CLOUDWATCH_LOG_GROUP"] = config["otro"]["cloudwatch_log_group"]

# #sources_periods_prep
# os.environ["CLIENTES"] = config["sources_periods_prep"]["clientes"]
# os.environ["PO"] = config["sources_periods_prep"]["po"]
# os.environ["INTERACCIONES"] = config["sources_periods_prep"]["interacciones"]
# os.environ["SYQ"] = config["sources_periods_prep"]["syq"]
# os.environ["VOLUNTARIAS"] = config["sources_periods_prep"]["voluntarias"]
# os.environ["BONOS-PENSIONALES"] = config["sources_periods_prep"]["bonos-pensionales"]

# #special_cases
# os.environ["INTERACCIONES"] = config["special_cases"]["interacciones"]
# os.environ["PO_CTA_CUENTA_APORTE"] = config["special_cases"]["po_cta_cuenta_aporte"]

# #others
# os.environ["SMMLV"] = config["others"]["smmlv"]