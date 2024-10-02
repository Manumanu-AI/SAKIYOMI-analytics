# utils/analysis.py

import pandas as pd
from typing import List

def filter_users_by_plan(df: pd.DataFrame, plans: List[str]) -> pd.DataFrame:
    """
    DataFrameを課金プランでフィルタリングします。

    Args:
        df (pd.DataFrame): 元のDataFrame。
        plans (List[str]): フィルタリングするプランのリスト。

    Returns:
        pd.DataFrame: フィルタリングされたDataFrame。
    """
    if not plans:
        return df
    return df[df['Plan'].isin(plans)]
