# payment_run_analysis_page.py

import streamlit as st
from datetime import datetime, timedelta, timezone
import pandas as pd
from config.firebase import db
from application.billing_service import BillingService
from utils.analysis import filter_billing  # フィルタリング関数のインポート

# 定数
DEFAULT_N_DAYS = 7  # デフォルトのn日間

# 日付フォーマットをdatetime型に変換
def convert_str_to_datetime(date_str):
    try:
        # ドキュメントIDが 'YYYY-MM-DD' 形式の場合
        return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError:
        # フォーマットが異なる場合はNoneを返す
        return None

# Firestoreから指定期間中のラン数を取得し合計する
def fetch_run_counts(user_id, start_date, end_date):
    """
    指定ユーザーのパフォーマンスデータを取得し、指定期間中のラン数を合計します。
    """
    run_types = ['feed_run', 'reel_run', 'feed_theme_run', 'reel_theme_run', 'data_analysis_run']
    run_counts = {run_type: 0 for run_type in run_types}

    performance_ref = db.collection('users').document(user_id).collection('performance')
    performance_docs = performance_ref.stream()

    for doc in performance_docs:
        doc_id = doc.id  # ドキュメントIDを取得
        performance_data = doc.to_dict()
        date_obj = convert_str_to_datetime(doc_id)
        if date_obj and start_date <= date_obj <= end_date:
            for run_type in run_types:
                run_counts[run_type] += performance_data.get(run_type, 0)

    return run_counts

# 全てのユーザーのbillingデータとラン数を取得し、指定期間中のラン数を合計する
def get_payment_run_data(n_days):
    """
    全てのユーザーからbillingのpayment_dateを取得し、その後n日間のラン数を合計します。
    """
    users_ref = db.collection('users')
    users_docs = users_ref.stream()

    billing_service = BillingService()
    all_data = []

    for user_doc in users_docs:
        user_id = user_doc.id
        user_data = user_doc.to_dict()
        display_name = user_data.get('display_name', 'Unknown User')

        # billing情報を取得
        billing_response = billing_service.list_billing(user_id)
        if billing_response['status'] == 'success':
            billing_list = billing_response['billing_list']
            if billing_list:
                # payment_dateが存在しない場合も考慮してデフォルト値を設定
                latest_billing = sorted(
                    billing_list,
                    key=lambda x: x.get('payment_date') if x.get('payment_date') else datetime.min.replace(tzinfo=timezone.utc),
                    reverse=True
                )[0]
                payment_date = latest_billing.get('payment_date')
                plan = latest_billing.get('plan', 'None')
                status = latest_billing.get('status', 'None')

                if payment_date:
                    # payment_dateがtimezone-awareでない場合はUTCに設定
                    if payment_date.tzinfo is None:
                        payment_date = payment_date.replace(tzinfo=timezone.utc)

                    # payment_dateからn日間の範囲を設定
                    start_period = payment_date
                    end_period = payment_date + timedelta(days=n_days)

                    # ラン数を取得
                    run_counts = fetch_run_counts(user_id, start_period, end_period)

                    # ラン数の合計
                    run_count_total = sum(run_counts.values())

                    # データを追加
                    all_data.append({
                        'UID': user_id,
                        'Display Name': display_name,
                        'Payment Date': payment_date.strftime('%Y-%m-%d'),
                        'Plan': plan,
                        'Billing Status': status,
                        'Feed Run': run_counts.get('feed_run', 0),
                        'Reel Run': run_counts.get('reel_run', 0),
                        'Feed Theme Run': run_counts.get('feed_theme_run', 0),
                        'Reel Theme Run': run_counts.get('reel_theme_run', 0),
                        'Data Analysis Run': run_counts.get('data_analysis_run', 0),
                        'Run Count Total': run_count_total
                    })
            else:
                # billing_listが空の場合
                continue
        else:
            # billing情報の取得に失敗した場合はスキップ
            st.error(f"Failed to retrieve billing for User ID: {user_id}, Error: {billing_response.get('message')}")
            continue

    return all_data

# 表示用のDataFrameを作成
def prepare_payment_run_dataframe(data):
    return pd.DataFrame(data)

# Streamlit UI
st.set_page_config(page_title="Payment Run Analysis Dashboard", layout="wide")
st.title("Payment Run Analysis Dashboard")

# サイドバーでn日間の選択
with st.sidebar:
    st.title("フィルター")
    n_days = st.number_input("Payment Dateからの期間 (日数)", min_value=1, max_value=365, value=DEFAULT_N_DAYS)
    submit_button = st.button("データを取得")

# データの取得をボタン押下時のみ行い、セッションステートに保存
if submit_button:
    with st.spinner('データを取得中...'):
        payment_run_data = get_payment_run_data(n_days)

        if not payment_run_data:
            st.warning("指定された条件に該当するデータが見つかりませんでした。")
            st.session_state['payment_run_df'] = pd.DataFrame()  # 空のDataFrameを保存
        else:
            # DataFrameを準備
            payment_run_df = prepare_payment_run_dataframe(payment_run_data)
            st.session_state['payment_run_df'] = payment_run_df
            st.success("データの取得が完了しました。")

# データが取得されている場合のみ表示
if 'payment_run_df' in st.session_state and not st.session_state['payment_run_df'].empty:

    # 課金プランによる絞り込み
    plan_options = ['feed', 'reel', 'both', 'internal', 'None']
    selected_plans = st.multiselect("課金プランで絞り込む", options=plan_options, default=plan_options)

    # 課金ステータスによる絞り込み
    billing_status_options = ['active', 'cancelled', 'pending', 'None']
    selected_billing_statuses = st.multiselect("課金ステータスで絞り込む", options=billing_status_options, default=billing_status_options)

    # フィルタリングを適用
    filtered_df = st.session_state['payment_run_df']
    filtered_df = filter_billing(filtered_df, plans=selected_plans, statuses=selected_billing_statuses)

    if filtered_df.empty:
        st.warning("指定されたフィルタに該当するデータがありません。")
    else:
        # DataFrameを表示
        st.dataframe(filtered_df)

        # CSVダウンロード機能
        def convert_df(df):
            return df.to_csv(index=False).encode('utf-8')

        csv = convert_df(filtered_df)
        st.download_button(
            label="データをCSVとしてダウンロード",
            data=csv,
            file_name='payment_run_data.csv',
            mime='text/csv',
        )
