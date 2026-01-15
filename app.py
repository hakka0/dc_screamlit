import streamlit as st
import pandas as pd
import boto3
import io
import math
import altair as alt
import random
import concurrent.futures
from botocore.config import Config

# --- 페이지 기본 설정 ---
st.set_page_config(page_title="ProjectMX Dashboard", layout="wide")

# --- [핵심 수정] CSS 주입: 메뉴바 숨기기 및 UI 개선 ---
st.markdown("""
    <style>
        /* 1. 상단 헤더 (햄버거 메뉴, Share 버튼, Deploy 버튼 등) 숨기기 */
        header[data-testid="stHeader"] {
            visibility: hidden;
        }

        /* 2. 하단 푸터 (Made with Streamlit) 숨기기 - 필요 없으면 주석 처리 */
        footer {
            visibility: hidden;
        }

        /* 3. 각 요소별 툴바(우측 상단 +버튼 등) 숨기기 */
        [data-testid="stElementToolbar"] { display: none; }
        
        /* 4. 라디오 버튼 스타일링 (기존 코드 유지) */
        div[role="radiogroup"] label > div:first-child { display: none !important; }
        div[role="radiogroup"] label {
            background-color: #ffffff;
            padding: 10px 20px !important;
            border-radius: 8px !important;
            border: 1px solid #e0e0e0;
            margin-right: 10px;
            transition: all 0.2s;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05);
            display: flex;
            justify-content: center;
            align-items: center;
            width: auto; 
            min-width: 100px;
        }
        div[role="radiogroup"] label div[data-testid="stMarkdownContainer"] > p {
            text-align: center;
            margin: 0;
            width: 100%;
            display: block;
        }
        div[role="radiogroup"] label:hover {
            border-color: #333;
            background-color: #f8f9fa;
        }
        div[role="radiogroup"] label:has(input:checked) {
            background-color: #333333 !important;
            border-color: #333333 !important;
            color: white !important;
        }
        div[role="radiogroup"] label:has(input:checked) p {
            color: white !important;
            font-weight: bold;
        }
        div[data-testid="stSelectbox"] > div > div { min-height: 46px; }
    </style>
""", unsafe_allow_html=True)

st_header_col, st_space, st_date_col, st_time_col = st.columns([5, 1, 2, 3])

with st_header_col:
    st.title("📊 블루 아카이브 갤러리 대시보드")

# --- Cloudflare R2에서 데이터 가져오기 (멀티스레딩 적용) ---
@st.cache_data(ttl=300, show_spinner=False)
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

    if 'Contents' not in response:
        return pd.DataFrame()

    files = [f for f in response['Contents'] if f['Key'].endswith('.xlsx')]
    if not files:
        return pd.DataFrame()

    def fetch_and_parse(file_info):
        file_key = file_info['Key']
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=file_key)
            data = obj['Body'].read()
            return pd.read_excel(io.BytesIO(data), engine='openpyxl')
        except Exception:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(fetch_and_parse, files))
    
    all_dfs = [df for df in results if df is not None]
    
    if not all_dfs:
        return pd.DataFrame()

    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df['수집시간'] = pd.to_datetime(final_df['수집시간'])

    # 총활동수 계산 방식 변경 (단순 합산)
    final_df['총활동수'] = final_df['작성글수'] + final_df['작성댓글수']
    
    return final_df

# --- 데이터 처리 ---
loading_messages = [
    "☁️ 저 구름 너머엔 무엇이 있을까요?",
    "🏃‍♂️ 데이터가 좀 많네요. 기다려 주세요.",
    "🔍 놓친 데이터가 존재하는지 확인 중 입니다.",
    "💾 이 더미 데이터는 뭘까요?",
    "🤖 삐삐쀼쀼"
]

loading_text = random.choice(loading_messages)

with st.spinner(loading_text):
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

    # --- [메인 메뉴] ---
    selected_tab = st.radio(
        "메뉴 선택", 
        ["📈 데이터 상세", "🏆 유저 랭킹", "👥 유저 검색"],
        horizontal=True,
        key="main_menu",
        label_visibility="collapsed"
    )
    
    st.markdown(" ") 

    if filtered_df.empty:
        st.warning(f"⚠️ {selected_date} 해당 시간대에 데이터가 없습니다.")
    else:
        # --- [Tab 1] 시간대별 그래프 ---
        if selected_tab == "📈 데이터 상세":
            total_posts = filtered_df['작성글수'].sum()
            total_comments = filtered_df['작성댓글수'].sum()
            active_users = len(filtered_df.groupby(['닉네임', 'ID(IP)', '유저타입']))

            col1, col2, col3 = st.columns(3)
            col1.metric("📝 총 게시글", f"{total_posts:,}개")
            col2.metric("💬 총 댓글", f"{total_comments:,}개")
            col3.metric("👥 액티브 유저", f"{active_users:,}명")
            
            st.markdown("---")
            st.subheader("📊 시간대별 활동 그래프")

            trend_stats = df.groupby('수집시간')[['작성글수', '작성댓글수']].sum().reset_index()
            trend_users = df.groupby(['수집시간', '닉네임', 'ID(IP)', '유저타입']).size().reset_index().groupby('수집시간').size().reset_index(name='액티브수')
            full_trend_df = pd.merge(trend_stats, trend_users, on='수집시간', how='left').fillna(0)

            chart_data = full_trend_df.melt(
                '수집시간', 
                var_name='활동유형', 
                value_name='카운트'
            )
            zoom_start = pd.to_datetime(selected_date)
            zoom_end = zoom_start + pd.Timedelta(hours=23, minutes=59)

            # 그래프 조작성 개선 (X축 스케일 바인딩)
            zoom_selection = alt.selection_interval(bind='scales', encodings=['x'])

            chart = alt.Chart(chart_data).mark_line(point=True).encode(
                x=alt.X(
                    '수집시간', 
                    axis=alt.Axis(format='%m월 %d일 %H시', title='시간', tickCount=10),
                    scale=alt.Scale(domain=[zoom_start, zoom_end])
                ),
                y=alt.Y(
                    '카운트', 
                    title='활동 수',
                    scale=alt.Scale(zero=True)
                ),
                color=alt.Color(
                    '활동유형', 
                    legend=alt.Legend(title="지표"),
                    scale=alt.Scale(
                        domain=['액티브수', '작성글수', '작성댓글수'],
                        range=['red', 'green', 'blue']
                    )
                ),
                tooltip=[
                    alt.Tooltip('수집시간', format='%Y-%m-%d %H:%M'),
                    alt.Tooltip('활동유형'),
                    alt.Tooltip('카운트')
                ]
            ).properties(
                height=450,
            ).add_params(
                zoom_selection
            )

            st.altair_chart(chart, use_container_width=True)
            st.caption(f"💡 그래프를 **좌우로 드래그**하면 다른 날짜의 데이터도 볼 수 있습니다. (마우스 휠로 줌인/줌아웃 가능)")


        # --- [Tab 2] 활동왕 랭킹 ---
        elif selected_tab == "🏆 유저 랭킹":
            st.subheader("🔥 Top 20")
            ranking_df = filtered_df.groupby(['닉네임', 'ID(IP)', '유저타입'])[['총활동수', '작성글수', '작성댓글수']].sum().reset_index()
            top_users = ranking_df.sort_values(by='총활동수', ascending=False).head(20)
            
            top_users = top_users.rename(columns={
                '유저타입': '계정타입'
            })
            
            st.dataframe(
                top_users,
                column_config={
                    "총활동수": st.column_config.ProgressColumn(format="%d", min_value=0, max_value=int(top_users['총활동수'].max()) if not top_users.empty else 100),
                },
                hide_index=True, use_container_width=True
            )

        # --- [Tab 3] 전체 유저 일람 ---
        elif selected_tab == "👥 유저 검색":
            st.subheader("🔍 유저 검색 및 전체 목록")

            user_list_df = filtered_df.groupby(['닉네임', 'ID(IP)', '유저타입']).agg({
                '작성글수': 'sum',
                '작성댓글수': 'sum',
                '총활동수': 'sum'
            }).reset_index()
            user_list_df = user_list_df.sort_values(by='닉네임', ascending=True)

            col_search_type, col_search_input = st.columns([1.2, 4])
            
            def clear_search_box():
                if 'user_search_box' in st.session_state:
                    st.session_state.user_search_box = None

            with col_search_type:
                st.markdown("**검색 기준**")
                search_type = st.radio(
                    "검색 기준 라벨",
                    ["닉네임", "ID(IP)"],
                    horizontal=True,
                    on_change=clear_search_box,
                    label_visibility="collapsed"
                )

            with col_search_input:
                if search_type == "닉네임":
                    st.markdown("**닉네임 검색** (자동완성)")
                    options = user_list_df['닉네임'].unique().tolist()
                    placeholder_text = "닉네임을 입력하세요"
                else:
                    st.markdown("**ID(IP) 검색** (자동완성)")
                    options = user_list_df['ID(IP)'].unique().tolist()
                    placeholder_text = "ID(IP)를 입력하세요"

                search_query = st.selectbox(
                    label="검색어 입력",
                    options=options,
                    index=None,
                    placeholder=placeholder_text,
                    key="user_search_box",
                    label_visibility="collapsed"
                )

            target_df = user_list_df
            if search_query:
                if search_type == "닉네임":
                    target_df = user_list_df[user_list_df['닉네임'] == search_query]
                else:
                    target_df = user_list_df[user_list_df['ID(IP)'] == search_query]

            if target_df.empty:
                st.info("검색 결과가 없습니다.")
            else:
                items_per_page = 15
                total_items = len(target_df)
                total_pages = math.ceil(total_items / items_per_page)

                if 'user_page' not in st.session_state:
                    st.session_state.user_page = 1
                if st.session_state.user_page > total_pages:
                    st.session_state.user_page = 1

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

                current_page = st.session_state.user_page
                start_idx = (current_page - 1) * items_per_page
                end_idx = start_idx + items_per_page
                page_df = target_df.iloc[start_idx:end_idx]
                
                page_df = page_df.rename(columns={
                    '유저타입': '계정타입'
                })
                
                display_columns = ['닉네임', 'ID(IP)', '계정타입', '작성글수', '작성댓글수', '총활동수']

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
