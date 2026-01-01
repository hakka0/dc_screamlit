import streamlit as st
import pandas as pd
import boto3
import io
import math
from botocore.config import Config

# --- [설정] 페이지 기본 설정 ---
st.set_page_config(page_title="갤러리 대시보드", layout="wide")

# 레이아웃: 헤더와 필터 영역 분리
st_header_col, st_space, st_date_col, st_time_col = st.columns([5, 1, 2, 3])

with st_header_col:
    st.title("📊 갤러리 활동 대시보드")

# --- [함수] Cloudflare R2에서 데이터 가져오기 ---
@st.cache_data(ttl=300)
def load_data_from_r2():
    try:
        aws_access_key_id = st.secrets["CF_ACCESS_KEY_ID"]
        aws_secret_access_key = st.secrets["CF_SECRET_ACCESS_KEY"]
        account_id = st.secrets["CF_ACCOUNT_ID"]
        bucket_name = st.secrets["CF_BUCKET_NAME"]
    except KeyError:
        st.error("Secrets 설정 오류: Streamlit 관리자 페이지에서 키를 확인해주세요.")
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

# --- [메인] 데이터 처리 ---
df = load_data_from_r2()

if not df.empty:
    min_date = df['수집시간'].dt.date.min()
    max_date = df['수집시간'].dt.date.max()

    # --- 우측 상단 필터 UI ---
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

    # --- 데이터 필터링 로직 ---
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
        st.warning(f"⚠️ {selected_date} 해당 시간대에 데이터가 없습니다.")
    else:
        # --- KPI 지표 ---
        total_posts = filtered_df['작성글수'].sum()
        total_comments = filtered_df['작성댓글수'].sum()
        active_users = filtered_df['ID(IP)'].nunique()

        col1, col2, col3 = st.columns(3)
        col1.metric("📝 총 게시글", f"{total_posts:,}개")
        col2.metric("💬 총 댓글", f"{total_comments:,}개")
        col3.metric("👥 순수 활동 유저", f"{active_users:,}명")

        # --- 탭 구성 ---
        tab1, tab2, tab3 = st.tabs(["📈 시간대별 추이", "🏆 유저 랭킹", "👥 전체 유저 검색"])

        # [Tab 1] 시간대별 추이
        with tab1:
            st.subheader(f"{selected_date} 시간대별 활동 지표")
            time_agg = filtered_df.groupby('수집시간').agg({
                '작성글수': 'sum',
                '작성댓글수': 'sum',
                'ID(IP)': 'nunique'
            }).rename(columns={'ID(IP)': '활동유저수'})
            st.line_chart(time_agg)

        # [Tab 2] 활동왕 랭킹 (Top 20)
        with tab2:
            st.subheader("🔥 활동왕 랭킹 (Top 20)")
            ranking_df = filtered_df.groupby(['닉네임', 'ID(IP)', '유저타입'])[['총활동수', '작성글수', '작성댓글수']].sum().reset_index()
            top_users = ranking_df.sort_values(by='총활동수', ascending=False).head(20)
            
            st.dataframe(
                top_users,
                column_config={
                    "총활동수": st.column_config.ProgressColumn(format="%d", min_value=0, max_value=int(top_users['총활동수'].max()) if not top_users.empty else 100),
                },
                hide_index=True, use_container_width=True
            )

        # [Tab 3] 전체 유저 일람 (검색 & 페이지네이션)
        with tab3:
            st.subheader("🔍 유저 검색 및 전체 목록")

            # 1. 유저별 데이터 집계
            user_list_df = filtered_df.groupby(['닉네임', 'ID(IP)', '유저타입']).agg({
                '작성글수': 'sum',
                '작성댓글수': 'sum',
                '총활동수': 'sum'
            }).reset_index()
            
            # 닉네임 기준 오름차순(가나다순) 정렬
            user_list_df = user_list_df.sort_values(by='닉네임', ascending=True)

            # 2. 검색 기능
            search_options = [f"{row['닉네임']} ({row['ID(IP)']})" for index, row in user_list_df.iterrows()]
            
            search_query = st.selectbox(
                "👤 유저 검색 (닉네임이나 ID를 입력하면 자동완성 됩니다)",
                options=[""] + search_options,
                index=0
            )

            # 검색 필터링
            target_df = user_list_df
            if search_query != "":
                target_nick = search_query.split(" (")[0]
                target_id = search_query.split(" (")[-1].replace(")", "")
                target_df = user_list_df[
                    (user_list_df['닉네임'] == target_nick) & 
                    (user_list_df['ID(IP)'] == target_id)
                ]

            # 3. 커스텀 페이지네이션 (버튼 밀착 배치)
            if target_df.empty:
                st.info("검색 결과가 없습니다.")
            else:
                items_per_page = 15
                total_items = len(target_df)
                total_pages = math.ceil(total_items / items_per_page)

                # Session State 관리
                if 'user_page' not in st.session_state:
                    st.session_state.user_page = 1
                if st.session_state.user_page > total_pages:
                    st.session_state.user_page = 1

                # [수정됨] 레이아웃 비율 조정 (8.5 : 0.75 : 0.75)
                # 텍스트(8.5)가 공간을 대부분 차지하고, 버튼(0.75)들을 오른쪽 끝으로 밀어냅니다.
                # 버튼들의 컬럼 크기가 작아서 서로 가까이 붙게 됩니다.
                if total_pages > 1:
                    col_info, col_prev, col_next = st.columns([8.5, 0.75, 0.75])

                    # 왼쪽: 페이지 정보 (수직 중앙 정렬 느낌을 위해 line-height 추가)
                    with col_info:
                        st.markdown(f"<div style='padding-top: 5px;'><b>{st.session_state.user_page}</b> / {total_pages} 페이지 (총 {total_items}명)</div>", unsafe_allow_html=True)
                    
                    # 오른쪽 끝: 이전 버튼
                    with col_prev:
                        if st.button("◀ 이전", use_container_width=True):
                            if st.session_state.user_page > 1:
                                st.session_state.user_page -= 1
                                st.rerun()
                    
                    # 오른쪽 끝: 다음 버튼
                    with col_next:
                        if st.button("다음 ▶", use_container_width=True):
                            if st.session_state.user_page < total_pages:
                                st.session_state.user_page += 1
                                st.rerun()
                else:
                    st.write(f"총 {total_items}명")

                # 데이터 슬라이싱
                current_page = st.session_state.user_page
                start_idx = (current_page - 1) * items_per_page
                end_idx = start_idx + items_per_page
                page_df = target_df.iloc[start_idx:end_idx]

                # '최근활동시간' 제외하고 표시
                display_columns = ['닉네임', 'ID(IP)', '유저타입', '작성글수', '작성댓글수', '총활동수']

                st.dataframe(
                    page_df[display_columns],
                    column_config={
                        "총활동수": st.column_config.NumberColumn(format="%d회"),
                    },
                    hide_index=True,
                    use_container_width=True
                )

else:
    st.info("데이터 로딩 중... (데이터가 없거나 R2 연결을 확인해주세요)")
