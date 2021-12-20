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
    # elif job_name == "fe":
    #    input_paths = get_fe_paths(
    #        start_period, end_period
    #    )
    elif job_name == "scoring":
        input_paths = get_scoring_paths(
            start_period, end_period
        )
    # elif job_name == "backtesting":
    #    input_paths = get_backtesting_paths(
    #        start_period,
    #        end_period
    #    )

    return input_paths

def get_prep_paths(start_period, end_period):
    """
    Get the input paths required for preparation process
    :param start_period: Start period required for data that is not historical
    :param end_period: End period for both historical and monthly data. Corresponds to the score date
    """
    input_paths = {}

    return input_paths

def get_scoring_paths(start_period, end_period):
    """
    Get the input paths required for preparation process
    :param start_period: Start period required for data that is not historical
    :param end_period: End period for both historical and monthly data. Corresponds to the score date
    """
    input_paths = {}
    
    return input_paths