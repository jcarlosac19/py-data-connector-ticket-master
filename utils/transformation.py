from typing import Any
import pandas as pd
import numpy as np
import json

def filter_keys_inplace(data: dict[str, Any], keys_to_keep: list[str]) -> None:
    """
    Remove all keys not in keys_to_keep from the dictionary in-place.

    :param data: Original dictionary to modify.
    :param keys_to_keep: Keys to keep.
    """
    for key in list(data.keys()):
        if key not in keys_to_keep:
            del data[key]

def replace_dots_in_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replaces dots in the column names of a pandas DataFrame with underscores.

    :param df: DataFrame whose column names need to be modified
    :return: DataFrame with modified column names
    """
    df.columns = df.columns.str.replace(".", "_", regex=False)
    return df

def lowercase_keys(obj: np.dtypes.ObjectDType) -> Any:
    """
    Recursively converts all dictionary keys to lowercase, including nested arrays.

    :param obj: Dictionary or list to convert
    :return: Dictionary or list with lowercase keys
    """
    if isinstance(obj, dict):
        return {k.lower(): lowercase_keys(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [lowercase_keys(v) if isinstance(v, (dict, list)) else v for v in obj]
    else:
        return obj

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies transformations to clean column names and standardize JSON fields.

    :param df: Input DataFrame
    :return: Transformed DataFrame
    """
    df = replace_dots_in_column_names(df)

    if "place_postalCode" in df.columns:
        df = df.drop(
            "place_postalCode", axis=1
        ) 

    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = df[col].apply(
                    lambda x: json.loads(x) if isinstance(x, str) else x
                )
                df[col] = df[col].apply(
                    lowercase_keys
                ) 
            except (json.JSONDecodeError, TypeError):
                continue 
    return df