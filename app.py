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

# --- CSS 주입 ---
st.markdown("""
    <style>
        [data-testid="stElementToolbar"] { display: none; }
        header[data-testid="stHeader"] { visibility: hidden; }
        footer { visibility: hidden; }
        
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
        div[data-testid="stSelectbox"] > div > div { min-height: 46px; }
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
    zoom_selection = alt.selection_interval(bind='scales')

    # [모달 그래프 Y축 설정]
    # 1. 해당 유저의 현재 날짜 데이터만 필터링하여 최대값 계산
    current_view_data = chart_data[chart_data['수집시간'].dt.date == target_date]
    if not current_view_data.empty:
        current_max = current_view_data['카운트'].max()
        # 최대값이 너무 작으면 최소 5로 설정, 아니면 10% 여백
        y_max = 5 if pd.isna(current_max) or current_max < 5 else current_max * 1.1
    else:
        y_max = 5

    chart = alt.Chart(chart_data).mark_line(point=True).encode(
        x=alt.X(
            '수집시간', 
            axis=alt.Axis(format='%H시', title='시간', tickCount=12),
            scale=alt.Scale(domain=[zoom_start, zoom_end])
        ),
        # [핵심] domainMin=0으로 0 이하 방지, domain=[0, y_max]로 초기 화면 스케일 최적화
        y=alt.Y('카운트', title='활동 수', scale=alt.Scale(domain=[0, y_max], domainMin=0)),
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
            zoom_selection = alt.selection_interval(bind='scales')

            # [핵심 수정: 메인 그래프 Y축 설정]
            # 1. 전체 데이터(chart_data) 중 '선택된 날짜'의 데이터만 추출
            daily_view_data = chart_data[chart_data['수집시간'].dt.date == selected_date]
            
            # 2. 해당 날짜의 최대값(Max) 계산
            if not daily_view_data.empty:
                current_max = daily_view_data['카운트'].max()
                # 3. Y축 상한선 설정 (최대값의 110% 지점, 최소 10)
                y_domain_max = 10 if pd.isna(current_max) or current_max < 10 else current_max * 1.1
            else:
                y_domain_max = 10

            chart = alt.Chart(chart_data).mark_line(point=True).encode(
                x=alt.X('수집시간', axis=alt.Axis(format='%m월 %d일 %H시', title='시간', tickCount=10), scale=alt.Scale(domain=[zoom_start, zoom_end])),
                # [핵심] domain=[0, y_domain_max]로 초기 뷰 고정 + domainMin=0으로 하한선 힌트 제공
                y=alt.Y('카운트', title='활동 수', scale=alt.Scale(domain=[0, y_domain_max], domainMin=0)),
                color=alt.Color('활동유형', legend=alt.Legend(title="지표"), scale=alt.Scale(domain=['액티브수', '작성글수', '작성댓글수'], range=['red', 'green', 'blue'])),
                tooltip=[alt.Tooltip('수집시간', format='%Y-%m-%d %H:%M'), alt.Tooltip('활동유형'), alt.Tooltip('카운트')]
            ).properties(height=450).add_params(zoom_selection)

            st.altair_chart(chart, use_container_width=True)
            st.caption(f"💡 **마우스 휠**로 확대/축소하거나 **드래그**하여 그래프를 자유롭게 움직일 수 있습니다.")

        # --- [Tab 2] 유저 랭킹 ---
        elif selected_tab == "🏆 유저 랭킹":
            st.subheader("🔥 Top 20")
            st.caption("표의 행을 클릭하면 상세 그래프가 나타납니다.")

            ranking_df = filtered_df.groupby(['닉네임', 'ID(IP)', '유저타입'])[['총활동수', '작성글수', '작성댓글수']].sum().reset_index()
            top_users = ranking_df.sort_values(by='총활동수', ascending=False).head(20)
            
            top_users = top_users.rename(columns={'유저타입': '계정타입'})
            
            event = st.dataframe(
                top_users,
                column_config={
                    "총활동수": st.column_config.NumberColumn(format="%d회"),
                },
                hide_index=True,
                use_container_width=True,
                on_select="rerun",
                selection_mode="single-row"
            )

            if len(event.selection.rows) > 0:
                selected_index = event.selection.rows[0]
                row = top_users.iloc[selected_index]
                show_user_detail_modal(row['닉네임'], row['ID(IP)'], row['계정타입'], df, selected_date)


        # --- [Tab 3] 전체 유저 일람 ---
        elif selected_tab == "👥 유저 검색":
            st.subheader("🔍 유저 검색 및 전체 목록")
            st.caption("표의 행을 클릭하면 상세 그래프가 나타납니다.")

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
                    c1.markdown(f"<div style='padding-top: 5px;'><b>{st.session_state.user_page}</b> / {total_pages} 페이지 (총 {total_items}명)</div>", unsafe_allow_html=True)
                    if c2.button("◀", use_container_width=True) and st.session_state.user_page > 1:
                        st.session_state.user_page -= 1
                        st.rerun()
                    if c3.button("▶", use_container_width=True) and st.session_state.user_page < total_pages:
                        st.session_state.user_page += 1
                        st.rerun()
                else:
                    st.write(f"총 {total_items}명")

                current_page = st.session_state.user_page
                start_idx = (current_page - 1) * items_per_page
                end_idx = start_idx + items_per_page
                page_df = target_df.iloc[start_idx:end_idx]
                
                page_df = page_df.rename(columns={'유저타입': '계정타입'})
                display_columns = ['닉네임', 'ID(IP)', '계정타입', '작성글수', '작성댓글수', '총활동수']

                event = st.dataframe(
                    page_df[display_columns],
                    column_config={
                        "총활동수": st.column_config.NumberColumn(format="%d회"),
                    },
                    hide_index=True,
                    use_container_width=True,
                    on_select="rerun",
                    selection_mode="single-row"
                )

                if len(event.selection.rows) > 0:
                    selected_idx = event.selection.rows[0]
                    row = page_df.iloc[selected_idx]
                    show_user_detail_modal(row['닉네임'], row['ID(IP)'], row['계정타입'], df, selected_date)

else:
    st.info("데이터 로딩 중... (데이터가 없거나 R2 연결을 확인해주세요)")
