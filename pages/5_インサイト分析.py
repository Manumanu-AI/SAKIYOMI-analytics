# pages/5_インサイト分析.py

import streamlit as st
import pandas as pd
from application.insight_service import InsightService
from domain.insight import Insight
import traceback
from datetime import datetime

@st.experimental_dialog("投稿データを追加", width="large")
def add_insight_dialog():
    with st.form("new_insight_form"):
        post_url = st.text_input("Post URL")
        plot = st.text_area("Plot")
        save_count = st.number_input("Save Count", min_value=0, step=1)
        like_count = st.number_input("Like Count", min_value=0, step=1)
        reach_count = st.number_input("Reach Count", min_value=0, step=1)
        new_reach_count = st.number_input("New Reach Count", min_value=0, step=1)
        followers_reach_count = st.number_input("Followers Reach Count", min_value=0, step=1)
        posted_at = st.date_input("Posted At")

        submitted = st.form_submit_button("保存")
        if submitted:
            service = InsightService()
            user_id = st.session_state.get('user_info', {}).get('localId')
            new_insight = Insight(
                user_id=user_id,
                post_url=post_url,
                plot=plot,
                save_count=save_count,
                like_count=like_count,
                reach_count=reach_count,
                new_reach_count=new_reach_count,
                followers_reach_count=followers_reach_count,
                posted_at=posted_at,
                created_at=datetime.now()
            )
            result = service.create_new_insight(new_insight)
            if result["status"] == "success":
                st.success("新しい投稿データが追加されました")
                st.rerun()
            else:
                st.error("投稿データの追加に失敗しました")

@st.experimental_dialog("投稿データを編集", width="large")
def edit_insight_dialog(insights_df):
    post_id = st.selectbox("編集する投稿を選択", options=insights_df['post_id'].tolist())
    insight_to_edit = insights_df[insights_df['post_id'] == post_id].iloc[0]

    with st.form("edit_insight_form"):
        post_url = st.text_input("Post URL", value=insight_to_edit['post_url'])
        plot = st.text_area("Plot", value=insight_to_edit['plot'])
        save_count = st.number_input("Save Count", value=insight_to_edit['save_count'], min_value=0, step=1)
        like_count = st.number_input("Like Count", value=insight_to_edit['like_count'], min_value=0, step=1)
        reach_count = st.number_input("Reach Count", value=insight_to_edit['reach_count'], min_value=0, step=1)
        new_reach_count = st.number_input("New Reach Count", value=insight_to_edit['new_reach_count'], min_value=0, step=1)
        followers_reach_count = st.number_input("Followers Reach Count", value=insight_to_edit['followers_reach_count'], min_value=0, step=1)
        posted_at = st.date_input("Posted At", value=insight_to_edit['posted_at'])

        submitted = st.form_submit_button("更新")
        if submitted:
            service = InsightService()
            user_id = st.session_state.get('user_info', {}).get('localId')
            updated_insight = Insight(
                post_id=post_id,
                user_id=user_id,
                post_url=post_url,
                plot=plot,
                save_count=save_count,
                like_count=like_count,
                reach_count=reach_count,
                new_reach_count=new_reach_count,
                followers_reach_count=followers_reach_count,
                posted_at=posted_at,
                created_at=insight_to_edit['created_at']
            )
            result = service.update_insight(updated_insight)
            if result["status"] == "success":
                st.success(f"Post {post_id} updated successfully")
                st.rerun()
            else:
                st.error(f"Failed to update post {post_id}")

def main():
    st.title("インサイトデータ表示")

    if 'logged_in' not in st.session_state or not st.session_state['logged_in']:
        st.warning("ログインしていません。先にログインしてください。")
        return

    user_id = st.session_state.get('user_info', {}).get('localId')
    if not user_id:
        st.error("ユーザー情報が見つかりません。再度ログインしてください。")
        return

    service = InsightService()

    st.sidebar.write("デバッグ情報:")
    st.sidebar.write(f"ログイン状態: {st.session_state.get('logged_in', False)}")
    st.sidebar.write(f"ユーザーID: {user_id}")

    try:
        insights = service.get_insights_by_user(user_id)
        st.sidebar.write(f"取得したインサイト数: {len(insights)}")
        
        if insights:
            insights_df = pd.DataFrame([insight.dict() for insight in insights])
            st.sidebar.write("データフレーム作成成功")
            st.sidebar.write(f"データフレームの行数: {len(insights_df)}")

            if not insights_df.empty:
                insights_df['posted_at'] = pd.to_datetime(insights_df['posted_at'])

                # カラムの順序を指定
                column_order = ['post_id', 'post_url', 'plot', 'save_count', 'like_count', 'reach_count', 'new_reach_count', 'followers_reach_count', 'posted_at']
                insights_df = insights_df[column_order]

                st.dataframe(
                    insights_df,
                    column_config={
                        "post_id": st.column_config.TextColumn("Post ID"),
                        "post_url": st.column_config.TextColumn("Post URL"),
                        "plot": st.column_config.TextColumn("Plot"),
                        "save_count": st.column_config.NumberColumn("Save Count"),
                        "like_count": st.column_config.NumberColumn("Like Count"),
                        "reach_count": st.column_config.NumberColumn("Reach Count"),
                        "new_reach_count": st.column_config.NumberColumn("New Reach Count"),
                        "followers_reach_count": st.column_config.NumberColumn("Followers Reach Count"),
                        "posted_at": st.column_config.DatetimeColumn("Posted At", format="YYYY-MM-DD HH:mm:ss"),
                    },
                    hide_index=True,
                )

                col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
                
                with col1:
                    if st.button("投稿データを追加"):
                        add_insight_dialog()

                with col2:
                    if st.button("投稿データを編集"):
                        edit_insight_dialog(insights_df)

                with col3:
                    post_id_to_delete = st.selectbox("削除する投稿を選択", options=insights_df['post_id'].tolist())

                with col4:
                    if st.button("削除"):
                        result = service.delete_insight(user_id, post_id_to_delete)
                        if result["status"] == "success":
                            st.success(f"Post {post_id_to_delete} deleted successfully")
                            st.rerun()
                        else:
                            st.error(f"Failed to delete post {post_id_to_delete}")

            else:
                st.info("インサイトデータがありません。データフレームが空です。")
        else:
            st.info("インサイトデータがありません。get_insights_by_userが空のリストを返しました。")

    except Exception as e:
        st.error(f"エラーが発生しました: {str(e)}")
        st.sidebar.write("エラーの詳細:")
        st.sidebar.code(traceback.format_exc())

if __name__ == "__main__":
    main()
