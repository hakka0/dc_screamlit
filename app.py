import streamlit as st
import pandas as pd
import math
import altair as alt
import random
from datetime import datetime, time, timedelta

# [추가됨] 오라클 DB 및 지갑 해독용 라이브러리
import oracledb
import base64
import os
import zipfile

from st_aggrid import AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode

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
    st.title("블루 아카이브 갤러리 대시보드")


# ==========================================
# [NEW] 오라클 DB 연동 파트
# ==========================================

@st.cache_resource
def setup_oracle_wallet():
    """Streamlit 환경에 Base64로 저장된 지갑 파일의 압축을 풀어 세팅합니다."""
    wallet_dir = "/tmp/oracle_wallet" # Streamlit Cloud에서 임시 쓰기가 가능한 경로
    if not os.path.exists(wallet_dir):
        os.makedirs(wallet_dir)
        wallet_b64 = st.secrets["ORACLE_WALLET_ZIP_B64"]
        zip_path = os.path.join(wallet_dir, "wallet.zip")
        
        # Base64 문자열을 실제 zip 파일로 복원
        with open(zip_path, "wb") as f:
            f.write(base64.b64decode(wallet_b64))
        
        # 압축 해제
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(wallet_dir)
            
    return wallet_dir

@st.cache_data(ttl=3600, show_spinner=False)
def load_data_from_oracle():
    """오라클 DB에서 최근 14일치 데이터를 즉시 쿼리해옵니다."""
    try:
        wallet_dir = setup_oracle_wallet()
        
        connection = oracledb.connect(
            user=st.secrets["ORACLE_DB_USER"],
            password=st.secrets["ORACLE_DB_PASSWORD"],
            dsn=st.secrets["ORACLE_DB_SERVICE"],
            config_dir=wallet_dir,
            wallet_location=wallet_dir,
            wallet_password=st.secrets["ORACLE_WALLET_PASSWORD"]
        )
        
        cutoff_date = datetime.now() - timedelta(days=14)
        cutoff_str = cutoff_date.strftime("%Y-%m-%d %H:%M")
        
        query = """
            SELECT COLLECTION_TIME, NICKNAME, UID_IP, USER_TYPE, POST_COUNT, COMMENT_COUNT, TOTAL_COUNT
            FROM GALLERY_LOG
            WHERE COLLECTION_TIME >= :1
            ORDER BY COLLECTION_TIME ASC
        """
        
        with connection.cursor() as cursor:
            cursor.arraysize = 10000
            
            cursor.execute(query, [cutoff_str])
            columns = [col[0] for col in cursor.description]
            data = cursor.fetchall()
            
        connection.close()
        
        df = pd.DataFrame(data, columns=columns)
        
        if df.empty:
            return pd.DataFrame()
            
        df.rename(columns={
            'COLLECTION_TIME': '수집시간',
            'NICKNAME': '닉네임',
            'UID_IP': 'ID(IP)',
            'USER_TYPE': '유저타입',
            'POST_COUNT': '작성글수',
            'COMMENT_COUNT': '작성댓글수',
            'TOTAL_COUNT': '총활동수'
        }, inplace=True)
        
        df['수집시간'] = pd.to_datetime(df['수집시간'])
        df['작성글수'] = pd.to_numeric(df['작성글수'], errors='coerce').fillna(0).astype(int)
        df['작성댓글수'] = pd.to_numeric(df['작성댓글수'], errors='coerce').fillna(0).astype(int)
        df['총활동수'] = pd.to_numeric(df['총활동수'], errors='coerce').fillna(0).astype(int)
        
        return df
        
    except Exception as e:
        st.error(f"오라클 DB 연결/조회 실패: {e}")
        return pd.DataFrame()


# --- 차트 함수 ---
def create_fixed_chart(chart_data, title_prefix=""):
    base_df = chart_data.pivot(index='수집시간', columns='활동유형', values='카운트').reset_index()
    base_df.columns.name = None 
    
    for col in ['액티브수', '작성글수', '작성댓글수']:
        if col not in base_df.columns:
            base_df[col] = 0
    base_df = base_df.fillna(0)

    x_axis = alt.X('수집시간', axis=alt.Axis(title='시간', format='%H시'))

    tooltip_config = [
        alt.Tooltip('수집시간', title='🕒 시간', format='%H시'),
        alt.Tooltip('액티브수', title='👥 액티브', format=','),
        alt.Tooltip('작성글수', title='📝 작성글', format=','),
        alt.Tooltip('작성댓글수', title='💬 작성댓글', format=',')
    ]

    lines = alt.Chart(chart_data).mark_line(point=True).encode(
        x=x_axis,
        y=alt.Y('카운트', title='활동 수', scale=alt.Scale(domainMin=0, nice=True)),
        color=alt.Color('활동유형', legend=alt.Legend(title="지표"), 
                        scale=alt.Scale(domain=['액티브수', '작성글수', '작성댓글수'], range=['red', 'green', 'blue']))
    )

    nearest = alt.selection_point(nearest=True, on='mouseover', fields=['수집시간'], empty=False)

    selectors = alt.Chart(base_df).mark_point().encode(
        x=x_axis,
        opacity=alt.value(0), 
        tooltip=tooltip_config 
    ).add_params(
        nearest
    )

    rules = alt.Chart(base_df).mark_rule(color='gray').encode(
        x=x_axis,
        opacity=alt.condition(nearest, alt.value(0.5), alt.value(0)),
        tooltip=tooltip_config
    )

    final_chart = (lines + selectors + rules).properties(
        height=400,
        title=f"{title_prefix} 상세 활동 추이"
    )

    return final_chart


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
    
    chart = create_fixed_chart(chart_data, title_prefix=f"{nick}님의")
    st.altair_chart(chart, width="stretch")
    
    u_posts = user_daily_df['작성글수'].sum()
    u_comments = user_daily_df['작성댓글수'].sum()
    st.info(f"📝 총 게시글: {u_posts}개 / 💬 총 댓글: {u_comments}개")

# --- 메인 실행 ---
loading_messages = ["☁️ 오라클 DB 접속 중...", "🏃‍♂️ 빛의 속도로 쿼리 중...", "🔍 분석 중...", "💾 잠시만요...", "🤖 삐삐쀼쀼"]
loading_text = random.choice(loading_messages)

with st.spinner(loading_text):
    # [수정] 오라클 DB에서 데이터 로드
    df = load_data_from_oracle()

if not df.empty:
    min_date = df['수집시간'].dt.date.min()
    max_date = df['수집시간'].dt.date.max()

    with st_date_col:
        selected_date = st.date_input("📅 날짜 선택", value=max_date, min_value=min_date, max_value=max_date)

    with st_time_col:
        start_hour, end_hour = st.slider("⏰ 시간대 필터", 0, 24, (0, 24), step=1, format="%d시")

    day_filtered_df = df[df['수집시간'].dt.date == selected_date]
    
    if end_hour == 24:
        filtered_df = day_filtered_df[day_filtered_df['수집시간'].dt.hour >= start_hour]
        time_filter_end = datetime.combine(selected_date, time.max)
    else:
        filtered_df = day_filtered_df[
            (day_filtered_df['수집시간'].dt.hour >= start_hour) & 
            (day_filtered_df['수집시간'].dt.hour < end_hour)
        ]
        time_filter_end = datetime.combine(selected_date, time(end_hour, 0)) - timedelta(seconds=1)

    time_filter_start = datetime.combine(selected_date, time(start_hour, 0))

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
            
            daily_data = full_trend_df[full_trend_df['수집시간'].dt.date == selected_date]

            visible_data = daily_data[
                (daily_data['수집시간'] >= time_filter_start) & 
                (daily_data['수집시간'] <= time_filter_end)
            ]

            if visible_data.empty:
                st.warning("선택한 구간에 데이터가 없습니다.")
            else:
                chart_data = visible_data.melt('수집시간', var_name='활동유형', value_name='카운트')
                chart = create_fixed_chart(chart_data)
                st.altair_chart(chart, width="stretch", key=f"main_chart_{selected_date}_{start_hour}_{end_hour}")


        # --- [Tab 2] 유저 랭킹 ---
        elif selected_tab == "🏆 유저 랭킹":
            st.subheader("🔥 Top 20")
            st.caption("✅ 체크박스 클릭시 개인용 그래프가 활성화")

            ranking_df = filtered_df.groupby(['닉네임', 'ID(IP)', '유저타입'])[['총활동수', '작성글수', '작성댓글수']].sum().reset_index()
            
            ranking_df['총활동수'] = ranking_df['총활동수'].astype(int)
            ranking_df['작성글수'] = ranking_df['작성글수'].astype(int)
            ranking_df['작성댓글수'] = ranking_df['작성댓글수'].astype(int)

            top_users = ranking_df.sort_values(by='총활동수', ascending=False).head(20)
            top_users = top_users.rename(columns={'유저타입': '계정타입'})
            event = st.dataframe(
                top_users,
                width="stretch",
                hide_index=True,
                on_select="rerun",          # 행을 클릭하면 즉시 반응
                selection_mode="single-row", # 한 줄만 선택 가능
                key="ranking_native_grid"
            )

            # 클릭한 행이 있다면 모달 창 띄우기
            if len(event.selection.rows) > 0:
                selected_index = event.selection.rows[0]
                selected_row = top_users.iloc[selected_index]
                
                nick = selected_row['닉네임']
                uid = selected_row['ID(IP)']
                account_type = selected_row['계정타입']
                
                show_user_detail_modal(nick, uid, account_type, df, selected_date)


        # --- [Tab 3] 유저 검색 ---
        elif selected_tab == "👥 유저 검색":
            st.subheader("🔍 유저 검색 및 전체 목록")
            st.caption("✅ 체크박스 클릭시 개인용 그래프가 활성화")

            user_list_df = filtered_df.groupby(['닉네임', 'ID(IP)', '유저타입']).agg({
                '작성글수': 'sum',
                '작성댓글수': 'sum',
                '총활동수': 'sum'
            }).reset_index()

            user_list_df['총활동수'] = user_list_df['총활동수'].astype(int)
            user_list_df['작성글수'] = user_list_df['작성글수'].astype(int)
            user_list_df['작성댓글수'] = user_list_df['작성댓글수'].astype(int)
            
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
                page_df = target_df.rename(columns={'유저타입': '계정타입'})
                display_columns = ['닉네임', 'ID(IP)', '계정타입', '작성글수', '작성댓글수', '총활동수']
                page_df = page_df[display_columns]
                event = st.dataframe(
                    page_df,
                    width="stretch",
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="search_native_grid"
                )

                # 클릭한 행이 있다면 모달 창 띄우기
                if len(event.selection.rows) > 0:
                    selected_index = event.selection.rows[0]
                    selected_row = page_df.iloc[selected_index]
                    
                    nick = selected_row['닉네임']
                    uid = selected_row['ID(IP)']
                    account_type = selected_row['계정타입']
                    
                    show_user_detail_modal(nick, uid, account_type, df, selected_date)

else:
    st.info("데이터 로딩 중... (데이터가 없거나 DB 연결을 확인해주세요)")
