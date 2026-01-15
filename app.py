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

# --- CSS 주입: 완벽한 표 디자인 구현 ---
st.markdown("""
    <style>
        /* 기본 UI 정리 */
        [data-testid="stElementToolbar"] { display: none; }
        header[data-testid="stHeader"] { visibility: hidden; }
        footer { visibility: hidden; }
        
        /* 라디오 버튼 스타일 */
        div[role="radiogroup"] label > div:first-child { display: none !important; }
        div[role="radiogroup"] label {
            background-color: #ffffff;
            padding: 10px 20px !important;
            border-radius: 8px !important;
            border: 1px solid #e0e0e0;
            margin-right: 10px;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05);
        }
        div[role="radiogroup"] label:has(input:checked) {
            background-color: #333 !important;
            border-color: #333 !important;
            color: white !important;
        }
        div[role="radiogroup"] label:has(input:checked) p {
            color: white !important;
            font-weight: bold;
        }

        /* ------------------------------------------------------- */
        /* [Fake Table] 표 디자인 CSS (세로줄 포함) */
        /* ------------------------------------------------------- */
        
        /* 1. 헤더 스타일 */
        .table-header {
            background-color: #f0f2f6;
            border-top: 1px solid #d5d8dc;
            border-bottom: 1px solid #d5d8dc;
            padding: 12px 0;
            font-weight: 700;
            color: #31333F;
            font-size: 14px;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        /* 2. 데이터 행 스타일 */
        .table-row {
            border-bottom: 1px solid #e6e9ef;
            padding: 6px 0;
            display: flex;
            align-items: center;
            transition: background-color 0.1s;
        }
        .table-row:hover {
            background-color: #f9f9f9;
        }

        /* 3. 셀 내용 스타일 (세로줄 구현) */
        .table-cell {
            font-size: 14px;
            color: #444;
            display: flex;
            align-items: center;
            justify-content: center; /* 가운데 정렬 */
            height: 100%;
            border-right: 1px solid #e6e9ef; /* 세로 구분선 */
            padding: 0 5px;
        }
        
        /* 마지막 셀은 오른쪽 테두리 제거 */
        .table-cell:last-child {
            border-right: none;
        }

        /* 4. 버튼 스타일링 (닉네임) */
        div[data-testid="stVerticalBlock"] div[data-testid="stHorizontalBlock"] button {
            background-color: transparent !important;
            border: none !important;
            padding: 0 !important;
            color: #2E7D32 !important; /* 초록색 */
            font-weight: 600 !important;
            box-shadow: none !important;
            margin: 0 !important;
            height: auto !important;
            width: 100%;
            text-align: center !important; /* 닉네임 가운데 정렬 */
        }
        div[data-testid="stVerticalBlock"] div[data-testid="stHorizontalBlock"] button:hover {
            text-decoration: underline !important;
            color: #1B5E20 !important;
        }
        div[data-testid="stVerticalBlock"] div[data-testid="stHorizontalBlock"] button:active,
        div[data-testid="stVerticalBlock"] div[data-testid="stHorizontalBlock"] button:focus {
            outline: none !important;
            box-shadow: none !important;
            color: #1B5E20 !important;
        }
        
        /* Streamlit 컬럼 간격 최소화 보정 */
        [data-testid="column"] {
            padding: 0 !important;
        }
    </style>
""", unsafe_allow_html=True)

st_header_col, st_space, st_date_col, st_time_col = st.columns([5, 1, 2, 3])

with st_header_col:
    st.title("📊 블루 아카이브 갤러리 대시보드")

# --- Cloudflare R2 데이터 로드 ---
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

    final_df['총활동수'] = final_df['작성글수'] + final_df['작성댓글수']
    return final_df

# --- 유저 상세 정보 모달 ---
@st.dialog("👤 유저 상세 활동 분석")
def show_user_detail_modal(nick, user_id, user_type, raw_df, target_date):
    st.subheader(f"{nick} ({user_type})")
    st.caption(f"ID(IP): {user_id} | 기준일: {target_date}")

    user_daily_df = raw_df[
        (raw_df['수집시간'].dt.date == target_date) & 
        (raw_df['닉네임'] == nick) & 
        (raw_df['ID(IP)'] == user_id)
    ]

    if user_daily_df.empty:
        st.warning("선택하신 날짜에 활동 데이터가 없습니다.")
        return

    user_trend = user_daily_df.groupby('수집시간')[['작성글수', '작성댓글수']].sum().reset_index()
    chart_data = user_trend.melt('수집시간', var_name='활동유형', value_name='카운트')
    
    zoom_start = pd.to_datetime(target_date)
    zoom_end = zoom_start + pd.Timedelta(hours=23, minutes=59)
    zoom_selection = alt.selection_interval(bind='scales', encodings=['x'])

    chart = alt.Chart(chart_data).mark_line(point=True).encode(
        x=alt.X(
            '수집시간', 
            axis=alt.Axis(format='%H시', title='시간', tickCount=12),
            scale=alt.Scale(domain=[zoom_start, zoom_end])
        ),
        y=alt.Y('카운트', title='활동 수', scale=alt.Scale(zero=True, domainMin=0)),
        color=alt.Color(
            '활동유형', 
            legend=alt.Legend(title="활동"),
            scale=alt.Scale(domain=['작성글수', '작성댓글수'], range=['green', 'blue'])
        ),
        tooltip=[
            alt.Tooltip('수집시간', format='%H시 %M분'),
            alt.Tooltip('활동유형'),
            alt.Tooltip('카운트')
        ]
    ).properties(
        height=350,
        title=f"{nick}님의 시간대별 활동 추이"
    ).add_params(
        zoom_selection
    )

    st.altair_chart(chart, use_container_width=True)
    
    u_posts = user_daily_df['작성글수'].sum()
    u_comments = user_daily_df['작성댓글수'].sum()
    st.info(f"📝 총 게시글: {u_posts}개 / 💬 총 댓글: {u_comments}개")

# --- 메인 실행 ---
loading_messages = ["☁️ 데이터 로딩 중...", "🏃‍♂️ 열심히 가져오는 중...", "🔍 분석 중...", "💾 잠시만요...", "🤖 삐삐쀼쀼"]
loading_text = random.choice(loading_messages)

with st.spinner(loading_text):
    df = load_data_from_r2()

if not df.empty:
    min_date = df['수집시간'].dt.date.min()
    max_date = df['수집시간'].dt.date.max()

    with st_date_col:
        selected_date = st.date_input("📅 날짜 선택", value=max_date, min_value=min_date, max_value=max_date)

    with st_time_col:
        start_hour, end_hour = st.slider("⏰ 시간대 선택", 0, 24, (0, 24), step=1, format="%d시")

    day_filtered_df = df[df['수집시간'].dt.date == selected_date]
    
    if end_hour == 24:
        filtered_df = day_filtered_df[day_filtered_df['수집시간'].dt.hour >= start_hour]
    else:
        filtered_df = day_filtered_df[
            (day_filtered_df['수집시간'].dt.hour >= start_hour) & 
            (day_filtered_df['수집시간'].dt.hour < end_hour)
        ]

    st.markdown("---")

    selected_tab = st.radio(
        "메뉴 선택", ["📈 데이터 상세", "🏆 유저 랭킹", "👥 유저 검색"],
        horizontal=True, key="main_menu", label_visibility="collapsed"
    )
    
    st.markdown(" ") 

    if filtered_df.empty:
        st.warning(f"⚠️ {selected_date} 해당 시간대에 데이터가 없습니다.")
    else:
        # --- [Tab 1] 데이터 상세 ---
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
            chart_data = full_trend_df.melt('수집시간', var_name='활동유형', value_name='카운트')
            
            zoom_start = pd.to_datetime(selected_date)
            zoom_end = zoom_start + pd.Timedelta(hours=23, minutes=59)
            zoom_selection = alt.selection_interval(bind='scales', encodings=['x'])

            chart = alt.Chart(chart_data).mark_line(point=True).encode(
                x=alt.X('수집시간', axis=alt.Axis(format='%m월 %d일 %H시', title='시간', tickCount=10), scale=alt.Scale(domain=[zoom_start, zoom_end])),
                y=alt.Y('카운트', title='활동 수', scale=alt.Scale(zero=True)),
                color=alt.Color('활동유형', legend=alt.Legend(title="지표"), scale=alt.Scale(domain=['액티브수', '작성글수', '작성댓글수'], range=['red', 'green', 'blue'])),
                tooltip=[alt.Tooltip('수집시간', format='%Y-%m-%d %H:%M'), alt.Tooltip('활동유형'), alt.Tooltip('카운트')]
            ).properties(height=450).add_params(zoom_selection)

            st.altair_chart(chart, use_container_width=True)
            st.caption(f"💡 그래프를 **좌우로 드래그**하면 다른 날짜의 데이터도 볼 수 있습니다.")

        # --- [Tab 2] 유저 랭킹 (CSS Fake Table) ---
        elif selected_tab == "🏆 유저 랭킹":
            st.subheader("🔥 Top 20")
            st.caption("닉네임을 클릭하면 상세 정보를 볼 수 있습니다.")

            ranking_df = filtered_df.groupby(['닉네임', 'ID(IP)', '유저타입'])[['총활동수', '작성글수', '작성댓글수']].sum().reset_index()
            top_users = ranking_df.sort_values(by='총활동수', ascending=False).head(20)

            # [헤더 출력]
            col_widths = [1, 3, 2.5, 1.5, 1.5, 2]
            header_cols = st.columns(col_widths)
            headers = ["순위", "닉네임", "ID(IP)", "계정", "활동", "글 / 댓"]
            
            for col, text in zip(header_cols, headers):
                col.markdown(f"<div class='table-header'>{text}</div>", unsafe_allow_html=True)

            # [데이터 출력]
            for idx, (index, row) in enumerate(top_users.iterrows()):
                cols = st.columns(col_widths)
                
                # 순위
                cols[0].markdown(f"<div class='table-cell'><b>{idx+1}</b></div>", unsafe_allow_html=True)
                
                # 닉네임 (버튼) - CSS로 가운데 정렬 및 초록색 처리됨
                if cols[1].button(f"{row['닉네임']}", key=f"rank_{idx}", use_container_width=True):
                    show_user_detail_modal(row['닉네임'], row['ID(IP)'], row['유저타입'], df, selected_date)
                
                # 나머지 데이터 (세로줄 div 포함)
                cols[2].markdown(f"<div class='table-cell'>{row['ID(IP)']}</div>", unsafe_allow_html=True)
                cols[3].markdown(f"<div class='table-cell'>{row['유저타입']}</div>", unsafe_allow_html=True)
                cols[4].markdown(f"<div class='table-cell'><b>{row['총활동수']}</b></div>", unsafe_allow_html=True)
                cols[5].markdown(f"<div class='table-cell' style='border-right: none;'>{row['작성글수']} / {row['작성댓글수']}</div>", unsafe_allow_html=True)
                
                # 행 구분선
                st.markdown("<div style='border-bottom: 1px solid #e0e0e0; margin-bottom: 0px;'></div>", unsafe_allow_html=True)


        # --- [Tab 3] 유저 검색 (CSS Fake Table) ---
        elif selected_tab == "👥 유저 검색":
            st.subheader("🔍 유저 검색")
            st.caption("닉네임을 클릭하면 상세 정보를 볼 수 있습니다.")

            user_list_df = filtered_df.groupby(['닉네임', 'ID(IP)', '유저타입']).agg({'작성글수': 'sum', '작성댓글수': 'sum', '총활동수': 'sum'}).reset_index().sort_values(by='닉네임')

            col_search_type, col_search_input = st.columns([1.2, 4])
            
            def clear_search_box():
                if 'user_search_box' in st.session_state: st.session_state.user_search_box = None

            with col_search_type:
                search_type = st.radio("검색 기준", ["닉네임", "ID(IP)"], horizontal=True, on_change=clear_search_box, label_visibility="collapsed")
            with col_search_input:
                options = user_list_df['닉네임'].unique().tolist() if search_type == "닉네임" else user_list_df['ID(IP)'].unique().tolist()
                placeholder = "닉네임 입력" if search_type == "닉네임" else "ID(IP) 입력"
                search_query = st.selectbox("검색어", options, index=None, placeholder=placeholder, key="user_search_box", label_visibility="collapsed")

            target_df = user_list_df
            if search_query:
                target_df = target_df[target_df['닉네임'] == search_query] if search_type == "닉네임" else target_df[target_df['ID(IP)'] == search_query]

            if target_df.empty:
                st.info("검색 결과가 없습니다.")
            else:
                items_per_page = 15
                total_items = len(target_df)
                total_pages = math.ceil(total_items / items_per_page)

                if 'user_page' not in st.session_state: st.session_state.user_page = 1
                if st.session_state.user_page > total_pages: st.session_state.user_page = 1

                if total_pages > 1:
                    c1, c2, c3 = st.columns([8.5, 0.75, 0.75])
                    c1.markdown(f"<div style='padding-top: 5px;'><b>{st.session_state.user_page}</b> / {total_pages} 페이지</div>", unsafe_allow_html=True)
                    if c2.button("◀", use_container_width=True) and st.session_state.user_page > 1:
                        st.session_state.user_page -= 1
                        st.rerun()
                    if c3.button("▶", use_container_width=True) and st.session_state.user_page < total_pages:
                        st.session_state.user_page += 1
                        st.rerun()
                
                st.markdown("---")
                start_idx = (st.session_state.user_page - 1) * items_per_page
                end_idx = start_idx + items_per_page
                page_df = target_df.iloc[start_idx:end_idx]

                # [헤더 출력]
                col_widths = [2.5, 2, 1.5, 1.5, 2]
                header_cols = st.columns(col_widths)
                headers = ["닉네임", "ID(IP)", "계정", "활동", "글 / 댓"]
                for col, text in zip(header_cols, headers):
                    col.markdown(f"<div class='table-header'>{text}</div>", unsafe_allow_html=True)

                # [데이터 출력]
                for idx, (index, row) in enumerate(page_df.iterrows()):
                    cols = st.columns(col_widths)
                    
                    if cols[0].button(f"{row['닉네임']}", key=f"search_{idx}", use_container_width=True):
                        show_user_detail_modal(row['닉네임'], row['ID(IP)'], row['유저타입'], df, selected_date)
                    
                    cols[1].markdown(f"<div class='table-cell'>{row['ID(IP)']}</div>", unsafe_allow_html=True)
                    cols[2].markdown(f"<div class='table-cell'>{row['유저타입']}</div>", unsafe_allow_html=True)
                    cols[3].markdown(f"<div class='table-cell'><b>{row['총활동수']}</b></div>", unsafe_allow_html=True)
                    cols[4].markdown(f"<div class='table-cell' style='border-right: none;'>{row['작성글수']} / {row['작성댓글수']}</div>", unsafe_allow_html=True)
                    
                    st.markdown("<div style='border-bottom: 1px solid #e0e0e0; margin-bottom: 0px;'></div>", unsafe_allow_html=True)

else:
    st.info("데이터 로딩 중... (데이터가 없거나 R2 연결을 확인해주세요)")
