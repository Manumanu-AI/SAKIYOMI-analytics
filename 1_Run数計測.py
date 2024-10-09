# analysis_page.py

import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
from config.firebase import db
from application.billing_service import BillingService
from utils.analysis import filter_billing  # 絞り込み関数のインポート

# 日付範囲をdatetime型に変換
def convert_str_to_datetime(date_str):
    # ドキュメントIDが 'YYYY-MM-DD' 形式の場合
    return datetime.strptime(date_str, '%Y-%m-%d')

# Firestoreから全てのパフォーマンスデータを取得し、該当しないデータに0を設定する
def fetch_all_performance(user_id, display_name, start_date, end_date):
    # 日付範囲内でのデフォルト値を設定
    data = {
        'UID': user_id,
        'Display Name': display_name,
        'Feed Run': {date_str.strftime('%Y-%m-%d'): 0 for date_str in pd.date_range(start_date, end_date)},
        'Reel Run': {date_str.strftime('%Y-%m-%d'): 0 for date_str in pd.date_range(start_date, end_date)},
        'Feed Theme Run': {date_str.strftime('%Y-%m-%d'): 0 for date_str in pd.date_range(start_date, end_date)},
        'Reel Theme Run': {date_str.strftime('%Y-%m-%d'): 0 for date_str in pd.date_range(start_date, end_date)},
        'Data Analysis Run': {date_str.strftime('%Y-%m-%d'): 0 for date_str in pd.date_range(start_date, end_date)}
    }
    performance_ref = db.collection('users').document(user_id).collection('performance')

    # 全ての performance ドキュメントを取得
    performance_docs = performance_ref.stream()

    for doc in performance_docs:
        doc_id = doc.id
        performance_data = doc.to_dict()
        try:
            date_obj = convert_str_to_datetime(doc_id)  # ドキュメントIDを日付に変換
            date_str = date_obj.strftime('%Y-%m-%d')

            # 各種ラン数を更新
            data['Feed Run'][date_str] = performance_data.get('feed_run', 0)
            data['Reel Run'][date_str] = performance_data.get('reel_run', 0)
            data['Feed Theme Run'][date_str] = performance_data.get('feed_theme_run', 0)
            data['Reel Theme Run'][date_str] = performance_data.get('reel_theme_run', 0)
            data['Data Analysis Run'][date_str] = performance_data.get('data_analysis_run', 0)

        except ValueError:
            # 日付に変換できないドキュメントIDはスキップ
            pass

    return data

# 全てのユーザーのパフォーマンスデータとbillingを取得し、日付ごとのデータをまとめる
def get_all_users_run_data(start_date, end_date):
    users_ref = db.collection('users')
    users_docs = users_ref.stream()

    billing_service = BillingService()
    all_data = []
    for user_doc in users_docs:
        user_id = user_doc.id
        display_name = user_doc.to_dict().get('display_name', 'Unknown User')

        # すべてのパフォーマンスデータを取得
        user_data = fetch_all_performance(user_id, display_name, start_date, end_date)

        # billing情報を取得
        billing_response = billing_service.list_billing(user_id)
        if billing_response['status'] == 'success':
            billing_list = billing_response['billing_list']
            # 最新のbillingを取得（例としてpayment_dateでソート）
            if billing_list:
                # payment_dateが存在しない場合も考慮してデフォルト値を設定
                latest_billing = sorted(
                    billing_list,
                    key=lambda x: x.get('payment_date', datetime.min),
                    reverse=True
                )[0]
                plan = latest_billing.get('plan', 'None')
                status = latest_billing.get('status', 'None')
            else:
                plan = 'None'
                status = 'None'
        else:
            plan = 'None'
            status = 'None'

        user_data['Plan'] = plan  # 'Plan' カラムとして追加
        user_data['Billing Status'] = status  # 'Billing Status' カラムとして追加
        all_data.append(user_data)

    return all_data

# 表示用のDataFrameを作成
def prepare_dataframe_for_display(run_data, date_range):
    rows = []

    for user_data in run_data:
        # 各Run Typeごとに1行にまとめる
        for run_type in ['Feed Run', 'Reel Run', 'Feed Theme Run', 'Reel Theme Run', 'Data Analysis Run']:
            row = {
                'UID': user_data['UID'],
                'Display Name': user_data['Display Name'],
                'Run Type': run_type,
                'Plan': user_data.get('Plan', 'None'),
                'Billing Status': user_data.get('Billing Status', 'None')
            }

            # 各日付のデータを取得して、日付ごとのデータをカラムに追加
            for date_str in date_range:
                row[date_str] = user_data[run_type].get(date_str, 0)

            # 指定期間中のrun数の合計を計算
            run_total = sum(user_data[run_type].get(date_str, 0) for date_str in date_range)
            row['Run Count Total'] = run_total

            rows.append(row)

    return pd.DataFrame(rows)

# Streamlit UI
st.set_page_config(page_title="Run Activity Dashboard", layout="wide")
st.title("Run Activity Dashboard")
st.markdown("### UID, Display Name, Run Type, Plan, Billing Status, 日付ごとのラン数, Run Count Total を表示")

# サイドバーで日付選択
with st.sidebar:
    st.title("フィルター")
    start_date = st.date_input("開始日", value=datetime.now().date() - timedelta(days=7))
    end_date = st.date_input("終了日", value=datetime.now().date())

    # 日付範囲をリスト化
    date_range = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()

    # Submitボタン
    submit_button = st.button("データを取得")

# データの取得をボタン押下時のみ行い、セッションステートに保存
if submit_button:
    with st.spinner('データを取得中...'):
        run_data = get_all_users_run_data(start_date, end_date)

        if not run_data:
            st.warning("指定された日付範囲内にデータが見つかりませんでした。")
            st.session_state['run_data_df'] = pd.DataFrame()  # 空のDataFrameを保存
        else:
            # DataFrameを準備
            run_data_df = prepare_dataframe_for_display(run_data, date_range)
            st.session_state['run_data_df'] = run_data_df
            st.success("データの取得が完了しました。")

# データが取得されている場合のみフィルタリングオプションを表示
if 'run_data_df' in st.session_state and not st.session_state['run_data_df'].empty:
    st.subheader("絞り込みオプション")

    # ページ内に絞り込みオプションを配置
    plan_options = ['feed', 'reel', 'both', 'internal', 'None']
    selected_plans = st.multiselect("課金プランで絞り込む", options=plan_options, default=plan_options)

    billing_status_options = ['active', 'cancelled', 'pending', 'None']
    selected_billing_statuses = st.multiselect("課金ステータスで絞り込む", options=billing_status_options, default=billing_status_options)

    # フィルタリングを適用
    filtered_df = filter_billing(
        df=st.session_state['run_data_df'],
        plans=selected_plans if selected_plans else None,
        statuses=selected_billing_statuses if selected_billing_statuses else None
    )

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
            file_name='run_data.csv',
            mime='text/csv',
        )
