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

import pandas as pd
from typing import Dict, List


def process(input_paths: Dict[str, List[str]]) -> pd.DataFrame:

    # Check that all dfs have unique indices

    check_no_duplicate_idx([])

    # Create pre-master data table

    pre_mdt = create_mdt(["a", "b", "c"])

    # Return pre-mdt as df

    return pre_mdt


def check_no_duplicate_idx(df_list: List[pd.DataFrame]) -> None:
    """
    This functions checks that all dfs in list have unique indices.
    """

    for df in df_list:
        assert (
            df.index.duplicated().sum() == 0
        ), "One of the dfs has repeated indices. Cannot concatenate."


def create_mdt(df_list: List[pd.DataFrame]) -> pd.DataFrame:
    """
    This function receives a list of dataframes and concatenates them
    horizontally, then removes the entries which index is in the 2nd argiment.
    """

    return df_list
