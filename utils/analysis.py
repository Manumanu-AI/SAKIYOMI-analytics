# utils/analysis.py

import pandas as pd
from typing import List, Optional

def filter_users_by_plan(df: pd.DataFrame, plans: Optional[List[str]] = None) -> pd.DataFrame:
    """
    DataFrameを課金プランでフィルタリングします。

    Args:
        df (pd.DataFrame): 元のDataFrame。
        plans (List[str], optional): フィルタリングするプランのリスト。デフォルトはNone。

    Returns:
        pd.DataFrame: フィルタリングされたDataFrame。
    """
    if not plans:
        return df
    return df[df['Plan'].isin(plans)]

def filter_users_by_billing_status(df: pd.DataFrame, statuses: Optional[List[str]] = None) -> pd.DataFrame:
    """
    DataFrameを課金ステータスでフィルタリングします。

    Args:
        df (pd.DataFrame): 元のDataFrame。
        statuses (List[str], optional): フィルタリングするステータスのリスト。デフォルトはNone。

    Returns:
        pd.DataFrame: フィルタリングされたDataFrame。
    """
    if not statuses:
        return df
    return df[df['Billing Status'].isin(statuses)]

def filter_billing(df: pd.DataFrame, plans: Optional[List[str]] = None, statuses: Optional[List[str]] = None) -> pd.DataFrame:
    """
    DataFrameを課金プランと課金ステータスでフィルタリングします。

    Args:
        df (pd.DataFrame): 元のDataFrame。
        plans (List[str], optional): フィルタリングするプランのリスト。デフォルトはNone。
        statuses (List[str], optional): フィルタリングするステータスのリスト。デフォルトはNone。

    Returns:
        pd.DataFrame: フィルタリングされたDataFrame。
    """
    if plans:
        df = filter_users_by_plan(df, plans)
    if statuses:
        df = filter_users_by_billing_status(df, statuses)
    return df
