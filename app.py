import streamlit as st
import pandas as pd
import boto3
import io
import datetime
from botocore.config import Config

st.set_page_config(page_title="갤러리 대시보드", layout="wide")

st_header_col, st_space, st_date_col, st_time_col = st.columns([5, 1, 2, 3])

with st_header_col:
    st.title("📊 갤러리 활동 대시보드")

@st.cache_data(ttl=300)
def load_data_from_r2():
    try:
        aws_access_key_id = st.secrets["CF_ACCESS_KEY_ID"]
        aws_secret_access_key = st.secrets["CF_SECRET_ACCESS_KEY"]
        account_id = st.secrets["CF_ACCOUNT_ID"]
        bucket_name = st.secrets["CF_BUCKET_NAME"]
    except KeyError:
        st.error("Secrets 설정 오류")
        return pd.DataFrame()

    s3 = boto3.client(
        's3',
        endpoint_url=f'https://{account_id}.r2.cloudflarestorage.com',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        config=Config(signature_version='s3v4')
    )

    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
    except Exception as e:
        st.error(f"R2 접속 실패: {e}")
        return pd.DataFrame()

    all_dfs = []
    
    if 'Contents' in response:
        files = [f for f in response['Contents'] if f['Key'].endswith('.xlsx')]
        if not files:
            return pd.DataFrame()
            
        for file in files:
            file_key = file['Key']
            try:
                obj = s3.get_object(Bucket=bucket_name, Key=file_key)
                data = obj['Body'].read()
                df = pd.read_excel(io.BytesIO(data))
                all_dfs.append(df)
            except:
                continue
    
    if not all_dfs:
        return pd.DataFrame()

    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df['수집시간'] = pd.to_datetime(final_df['수집시간'])
    return final_df

df = load_data_from_r2()

if not df.empty:
    min_date = df['수집시간'].dt.date.min()
    max_date = df['수집시간'].dt.date.max()

    with st_date_col:
        selected_date = st.date_input(
            "📅 날짜 선택",
            value=max_date, min_value=min_date, max_value=max_date
        )

    with st_time_col:
        start_hour, end_hour = st.slider(
            "⏰ 시간대 선택",
            0, 24, (0, 24), step=1, format="%d시"
        )

    # 데이터 필터링
    day_filtered_df = df[df['수집시간'].dt.date == selected_date]
    
    if end_hour == 24:
        filtered_df = day_filtered_df[day_filtered_df['수집시간'].dt.hour >= start_hour]
    else:
        filtered_df = day_filtered_df[
            (day_filtered_df['수집시간'].dt.hour >= start_hour) & 
            (day_filtered_df['수집시간'].dt.hour < end_hour)
        ]

    st.markdown("---")

    if filtered_df.empty:
        st.warning(f"⚠️ {selected_date} 데이터가 없습니다.")
    else:
        # [수정됨] KPI 계산 (중복 제거된 순수 유저 수)
        total_posts = filtered_df['작성글수'].sum()
        total_comments = filtered_df['작성댓글수'].sum()
        
        # 여기서 nunique()를 쓰면 해당 기간 내 중복 활동자는 1명으로 계산됨
        active_users = filtered_df['ID(IP)'].nunique()

        col1, col2, col3 = st.columns(3)
        col1.metric("📝 총 게시글", f"{total_posts:,}개")
        col2.metric("💬 총 댓글", f"{total_comments:,}개")
        col3.metric("👥 순수 활동 유저", f"{active_users:,}명") # 라벨 변경

        tab1, tab2, tab3 = st.tabs(["📈 시간대별 추이", "🏆 유저 랭킹", "🍰 유저 타입 비율"])

        with tab1:
            st.subheader(f"{selected_date} 시간대별 활동 지표")
            
            # [핵심 수정] 시간대별 집계 방식 변경
            # ID(IP) 컬럼에 nunique 함수를 적용하여 중복 제거된 유저 수를 구함
            time_agg = filtered_df.groupby('수집시간').agg({
                '작성글수': 'sum',
                '작성댓글수': 'sum',
                'ID(IP)': 'nunique'
            }).rename(columns={'ID(IP)': '활동유저수'})
            
            # 그래프 그리기
            st.line_chart(time_agg)
            
            # (옵션) 데이터프레임으로도 보여주기 (확인용)
            with st.expander("상세 데이터 보기"):
                st.dataframe(time_agg)

        with tab2:
            st.subheader("🔥 활동왕 랭킹 (Top 20)")
            # 랭킹은 단순히 합산하면 되므로 기존 유지 (많이 활동한 사람이니까 중복 합산이 맞음)
            user_df = filtered_df.groupby(['닉네임', 'ID(IP)', '유저타입'])[['총활동수', '작성글수', '작성댓글수']].sum().reset_index()
            top_users = user_df.sort_values(by='총활동수', ascending=False).head(20)
            
            st.dataframe(
                top_users,
                column_config={
                    "총활동수": st.column_config.ProgressColumn(format="%d", min_value=0, max_value=int(top_users['총활동수'].max()) if not top_users.empty else 100),
                },
                hide_index=True, use_container_width=True
            )

        with tab3:
            st.subheader("📊 고닉 vs 유동 비율 (순수 유저 기준)")
            # 유저 타입 비율도 '활동 횟수' 기준이 아니라 '사람 머릿수' 기준으로 보고 싶다면 아래처럼 수정
            # 중복 제거 후 유저 타입 세기
            unique_users = filtered_df.drop_duplicates(subset=['ID(IP)'])
            type_counts = unique_users['유저타입'].value_counts()
            
            # 만약 활동량(글+댓글) 기준 비율을 보고 싶다면 아래 주석 해제 후 위 2줄 주석 처리
            # type_counts = filtered_df['유저타입'].value_counts()
            
            st.bar_chart(type_counts)

else:
    st.info("데이터 로딩 중...")
