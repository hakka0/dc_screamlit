import streamlit as st
import pandas as pd
import boto3
import io
import math
from botocore.config import Config

# --- [설정] 페이지 기본 설정 및 CSS 해킹 ---
st.set_page_config(page_title="갤러리 대시보드", layout="wide")

# [CSS 추가] 데이터프레임 우측 상단 툴바 제거 & 탭 포커스 유지 튜닝
st.markdown("""
    <style>
        /* 데이터프레임 툴바(검색, CSV다운로드 등) 숨기기 */
        [data-testid="stElementToolbar"] {
            display: none;
        }
        /* 버튼 정렬 미세 조정 */
        div.stButton > button {
            width: 100%;
        }
    </style>
""", unsafe_allow_html=True)

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

        # [Tab 3] 전체 유저 일람 (검색 개선 & 메뉴 숨김)
        with tab3:
            st.subheader("🔍 유저 검색 및 전체 목록")

            # 1. 유저 데이터 집계
            user_list_df = filtered_df.groupby(['닉네임', 'ID(IP)', '유저타입']).agg({
                '작성글수': 'sum',
                '작성댓글수': 'sum',
                '총활동수': 'sum'
            }).reset_index()
            user_list_df = user_list_df.sort_values(by='닉네임', ascending=True)

            # 2. [수정됨] 검색 UI 개선 (라디오 버튼 + 텍스트 입력)
            col_search_type, col_search_input = st.columns([1, 4])
            
            with col_search_type:
                search_type = st.radio(
                    "검색 기준",
                    ["닉네임", "ID"],
                    horizontal=True,
                    label_visibility="collapsed" # 공간 절약을 위해 라벨 숨김
                )
            
            with col_search_input:
                search_text = st.text_input(
                    "검색어 입력",
                    placeholder=f"{search_type}을(를) 입력하세요 (일부만 입력해도 검색됨)",
                    label_visibility="collapsed"
                )

            # 3. 검색 필터링 로직 (부분 일치 검색)
            target_df = user_list_df
            if search_text:
                if search_type == "닉네임":
                    # str.contains로 부분 일치 검색 (case=False: 대소문자 구분 안 함)
                    target_df = user_list_df[user_list_df['닉네임'].astype(str).str.contains(search_text, case=False, na=False)]
                else:
                    target_df = user_list_df[user_list_df['ID(IP)'].astype(str).str.contains(search_text, case=False, na=False)]

            # 4. 페이지네이션 UI
            if target_df.empty:
                st.info("검색 결과가 없습니다.")
            else:
                items_per_page = 15
                total_items = len(target_df)
                total_pages = math.ceil(total_items / items_per_page)

                if 'user_page' not in st.session_state:
                    st.session_state.user_page = 1
                
                # 검색 결과가 바뀌어서 페이지가 줄어들면 1페이지로 리셋
                if st.session_state.user_page > total_pages:
                    st.session_state.user_page = 1

                # 상단 페이지 컨트롤러
                if total_pages > 1:
                    col_info, col_prev, col_next = st.columns([8.5, 0.75, 0.75])

                    with col_info:
                        st.markdown(f"<div style='padding-top: 5px;'><b>{st.session_state.user_page}</b> / {total_pages} 페이지 (총 {total_items}명)</div>", unsafe_allow_html=True)
                    
                    with col_prev:
                        if st.button("◀ 이전", use_container_width=True):
                            if st.session_state.user_page > 1:
                                st.session_state.user_page -= 1
                                st.rerun()
                    
                    with col_next:
                        if st.button("다음 ▶", use_container_width=True):
                            if st.session_state.user_page < total_pages:
                                st.session_state.user_page += 1
                                st.rerun()
                else:
                    st.write(f"총 {total_items}명")

                # 데이터 출력
                current_page = st.session_state.user_page
                start_idx = (current_page - 1) * items_per_page
                end_idx = start_idx + items_per_page
                page_df = target_df.iloc[start_idx:end_idx]

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
