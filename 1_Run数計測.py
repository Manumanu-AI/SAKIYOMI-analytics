import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
from config.firebase import db
from application.billing_service import BillingService
from utils.analysis import filter_billing

def convert_str_to_datetime(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d')

def fetch_all_performance_and_user_index(user_id, user_data, start_date, end_date):
    # Add created_at to the data dictionary
    data = {
        'UID': user_id,
        'Display Name': user_data.get('display_name', 'Unknown User'),
        'Created At': user_data.get('created_at', 'Unknown'),  # Add created_at field
        'Feed Run': {date_str.strftime('%Y-%m-%d'): 0 for date_str in pd.date_range(start_date, end_date)},
        'Reel Run': {date_str.strftime('%Y-%m-%d'): 0 for date_str in pd.date_range(start_date, end_date)},
        'Feed Theme Run': {date_str.strftime('%Y-%m-%d'): 0 for date_str in pd.date_range(start_date, end_date)},
        'Reel Theme Run': {date_str.strftime('%Y-%m-%d'): 0 for date_str in pd.date_range(start_date, end_date)},
        'Data Analysis Run': {date_str.strftime('%Y-%m-%d'): 0 for date_str in pd.date_range(start_date, end_date)}
    }
    
    performance_ref = db.collection('users').document(user_id).collection('performance')
    performance_docs = performance_ref.stream()

    for doc in performance_docs:
        doc_id = doc.id
        performance_data = doc.to_dict()
        try:
            date_obj = convert_str_to_datetime(doc_id)
            date_str = date_obj.strftime('%Y-%m-%d')

            data['Feed Run'][date_str] = performance_data.get('feed_run', 0)
            data['Reel Run'][date_str] = performance_data.get('reel_run', 0)
            data['Feed Theme Run'][date_str] = performance_data.get('feed_theme_run', 0)
            data['Reel Theme Run'][date_str] = performance_data.get('reel_theme_run', 0)
            data['Data Analysis Run'][date_str] = performance_data.get('data_analysis_run', 0)

        except ValueError:
            pass

    user_index_ref = db.collection('users').document(user_id).collection('user_index')
    user_index_docs = user_index_ref.stream()

    for doc in user_index_docs:
        user_index_data = doc.to_dict()
        data[f"{doc.id.capitalize()} Langsmith Project Name"] = user_index_data.get('langsmith_project_name', 'None')

    return data

def get_all_users_run_data(start_date, end_date):
    users_ref = db.collection('users')
    users_docs = users_ref.stream()

    billing_service = BillingService()
    all_data = []
    
    for user_doc in users_docs:
        user_id = user_doc.id
        user_data = user_doc.to_dict()  # Get all user data including created_at

        # Get performance and user index data
        user_data_with_performance = fetch_all_performance_and_user_index(user_id, user_data, start_date, end_date)

        # Get billing information
        billing_response = billing_service.list_billing(user_id)
        if billing_response['status'] == 'success':
            billing_list = billing_response['billing_list']
            if billing_list:
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

        user_data_with_performance['Plan'] = plan
        user_data_with_performance['Billing Status'] = status
        all_data.append(user_data_with_performance)

    return all_data

def prepare_dataframe_for_display(run_data, date_range):
    rows = []

    for user_data in run_data:
        for run_type in ['Feed Run', 'Reel Run', 'Feed Theme Run', 'Reel Theme Run', 'Data Analysis Run']:
            row = {
                'UID': user_data['UID'],
                'Display Name': user_data['Display Name'],
                'Created At': user_data['Created At'],  # Add created_at to the row
                'Run Type': run_type,
                'Plan': user_data.get('Plan', 'None'),
                'Billing Status': user_data.get('Billing Status', 'None'),
                'Feed Langsmith Project Name': user_data.get('Feed Langsmith Project Name', 'None'),
                'Reel Langsmith Project Name': user_data.get('Reel Langsmith Project Name', 'None')
            }

            for date_str in date_range:
                row[date_str] = user_data[run_type].get(date_str, 0)

            run_total = sum(user_data[run_type].get(date_str, 0) for date_str in date_range)
            row['Run Count Total'] = run_total

            rows.append(row)

    return pd.DataFrame(rows)

# Streamlit UI code remains the same
st.set_page_config(page_title="Run Activity Dashboard", layout="wide")
st.title("Run Activity Dashboard")
st.markdown("### UID, Display Name, Created At, Run Type, Plan, Billing Status, 日付ごとのラン数, Run Count Total を表示")

with st.sidebar:
    st.title("フィルター")
    start_date = st.date_input("開始日", value=datetime.now().date() - timedelta(days=7))
    end_date = st.date_input("終了日", value=datetime.now().date())

    date_range = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()
    submit_button = st.button("データを取得")

if submit_button:
    with st.spinner('データを取得中...'):
        run_data = get_all_users_run_data(start_date, end_date)

        if not run_data:
            st.warning("指定された日付範囲内にデータが見つかりませんでした。")
            st.session_state['run_data_df'] = pd.DataFrame()
        else:
            run_data_df = prepare_dataframe_for_display(run_data, date_range)
            st.session_state['run_data_df'] = run_data_df
            st.success("データの取得が完了しました。")

if 'run_data_df' in st.session_state and not st.session_state['run_data_df'].empty:
    st.subheader("絞り込みオプション")

    plan_options = ['feed', 'reel', 'both', 'internal', 'None']
    selected_plans = st.multiselect("課金プランで絞り込む", options=plan_options, default=plan_options)

    billing_status_options = ['active', 'cancelled', 'pending', 'None']
    selected_billing_statuses = st.multiselect("課金ステータスで絞り込む", options=billing_status_options, default=billing_status_options)

    filtered_df = filter_billing(
        df=st.session_state['run_data_df'],
        plans=selected_plans if selected_plans else None,
        statuses=selected_billing_statuses if selected_billing_statuses else None
    )

    if filtered_df.empty:
        st.warning("指定されたフィルタに該当するデータがありません。")
    else:
        st.dataframe(filtered_df)

        def convert_df(df):
            return df.to_csv(index=False).encode('utf-8')

        csv = convert_df(filtered_df)
        st.download_button(
            label="データをCSVとしてダウンロード",
            data=csv,
            file_name='run_data.csv',
            mime='text/csv',
        )
