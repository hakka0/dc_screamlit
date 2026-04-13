import streamlit as st
import pandas as pd
import altair as alt
import random
import extra_streamlit_components as stx
from datetime import datetime, time, timedelta

# 오라클 DB 및 지갑 해독용 라이브러리
import oracledb
import base64
import os
import zipfile

# ==========================================
# 쿠키 매니저 초기화 및 세션 스테이트(st.session_state) 동기화
# ==========================================
cookie_manager = stx.CookieManager()

# 아직 세션 메모리에 북마크가 없다면, 쿠키에서 불러와서 세션에 저장합니다.
if "bookmarks" not in st.session_state:
    saved_bookmarks = cookie_manager.get(cookie="user_bookmarks")
    if saved_bookmarks:
        st.session_state.bookmarks = [b for b in saved_bookmarks.split(",") if b.strip()]
    else:
        st.session_state.bookmarks = []

# --- 페이지 기본 설정 ---
st.set_page_config(page_title="ProjectMX Dashboard", layout="wide")

# --- CSS 주입 (모바일 UI 최적화 포함) ---
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
        
        /* 모바일 UI 1줄 3등분 강제 고정 */
        div[data-testid="stRadio"] > div[role="radiogroup"] {
            flex-wrap: nowrap !important;
            gap: 5px !important;
        }
        
        div[data-testid="stRadio"] > div[role="radiogroup"] > label {
            flex: 1 1 33.3% !important;
            justify-content: center !important;
            padding: 10px 5px !important;
        }
        
        @media (max-width: 450px) {
            div[data-testid="stRadio"] > div[role="radiogroup"] > label p {
                font-size: 13px !important;
                letter-spacing: -0.5px !important;
                word-break: keep-all !important;
            }
        }
    </style>
""", unsafe_allow_html=True)

st_header_col, st_space, st_date_col, st_time_col = st.columns([5, 1, 2, 3])

with st_header_col:
    st.title("블루 아카이브 갤러리 대시보드")


# ==========================================
# 오라클 DB 연동 파트
# ==========================================
@st.cache_resource
def setup_oracle_wallet():
    wallet_dir = "/tmp/oracle_wallet"
    if not os.path.exists(wallet_dir):
        os.makedirs(wallet_dir)
        wallet_b64 = st.secrets["ORACLE_WALLET_ZIP_B64"]
        zip_path = os.path.join(wallet_dir, "wallet.zip")
        
        with open(zip_path, "wb") as f:
            f.write(base64.b64decode(wallet_b64))
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(wallet_dir)
            
    return wallet_dir

@st.cache_data(ttl=3600, show_spinner=False)
def load_data_from_oracle():
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
            # 네트워크 통신 횟수 100배 단축 (로딩 속도 비약적 상승)
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
        title=f"{title_prefix}"
    )

    return final_chart


# --- 유저 상세 정보 모달 ---
@st.dialog("👤 개인 그래프")
def show_user_detail_modal(nick, user_id, user_type, raw_df, target_date):
    is_bookmarked = nick in st.session_state.bookmarks
    
    col1, col2 = st.columns([0.8, 0.2])
    with col1:
        st.subheader(f"[{user_type}] {nick} 님의 활동내역")
    with col2:
        if st.button("🌟 해제" if is_bookmarked else "⭐ 북마크", key=f"bm_modal_{nick}"):
            if is_bookmarked:
                st.session_state.bookmarks.remove(nick)
            else:
                st.session_state.bookmarks.append(nick)
            
            cookie_manager.set("user_bookmarks", ",".join(st.session_state.bookmarks))
            st.rerun() 
    st.divider()
    
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
loading_messages = ["☁️ 키보토스에 접속 중", "🏃‍♂️ 아로나가 달리고 있어요!", "🔍 케이가 분석 중", "💾 잠시만요!", "🤖 삐삐쀼쀼"]
loading_text = random.choice(loading_messages)

with st.spinner(loading_text):
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
        "메뉴 선택", ["📈 시간대 그래프", "🏆 유저 랭킹", "👥 유저 검색"],
        horizontal=True, key="main_menu", label_visibility="collapsed"
    )
    
    st.markdown(" ") 

    if filtered_df.empty:
        st.warning(f"⚠️ {selected_date} 해당 시간대에 데이터가 없습니다.")
    else:
        # ==========================================
        # [Tab 1] 시간대 그래프
        # ==========================================
        if selected_tab == "📈 시간대 그래프":
            total_posts = filtered_df['작성글수'].sum()
            total_comments = filtered_df['작성댓글수'].sum()
            active_users = len(filtered_df.groupby(['닉네임', 'ID(IP)', '유저타입']))

            col1, col2, col3 = st.columns(3)
            col1.metric("📝 총 게시글", f"{total_posts:,}개")
            col2.metric("💬 총 댓글", f"{total_comments:,}개")
            col3.metric("👥 액티브 유저", f"{active_users:,}명")
            
            st.markdown("---")
            st.subheader("각 시간대 데이터")

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


        # ==========================================
        # [Tab 2] 유저 랭킹
        # ==========================================
        elif selected_tab == "🏆 유저 랭킹":
            st.subheader("Top 20")
            st.caption("✅ 맨 앞의 체크박스를 눌러 북마크하거나, 행을 클릭해 상세 📊 그래프를 확인하세요.")

            ranking_df = filtered_df.groupby(['닉네임', 'ID(IP)', '유저타입'])[['총활동수', '작성글수', '작성댓글수']].sum().reset_index()
            
            ranking_df['총활동수'] = ranking_df['총활동수'].astype(int)
            ranking_df['작성글수'] = ranking_df['작성글수'].astype(int)
            ranking_df['작성댓글수'] = ranking_df['작성댓글수'].astype(int)

            top_users = ranking_df.sort_values(by='총활동수', ascending=False).head(20)
            top_users = top_users.rename(columns={'유저타입': '계정타입'})
            
            top_users.insert(0, '그래프', '📊 보기')
            top_users['북마크'] = top_users['닉네임'].apply(lambda x: True if x in st.session_state.bookmarks else False)
            
            top_users = top_users.sort_values(by=['북마크', '총활동수'], ascending=[False, False])
            
            cols = ['북마크', '그래프'] + [c for c in top_users.columns if c not in ['북마크', '그래프']]
            top_users = top_users[cols]
            
            def highlight_yellow(row):
                if row['북마크'] == True:
                    return ['background-color: #FFFACD'] * len(row)
                return [''] * len(row)
            
            styled_top_users = top_users.style.apply(highlight_yellow, axis=1)

            editor_key = "ranking_editor_v6"
            st.data_editor(
                styled_top_users,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "그래프": st.column_config.TextColumn("상세", width="small"),
                    "북마크": st.column_config.CheckboxColumn("⭐ 북마크", default=False)
                },
                disabled=[c for c in top_users.columns if c != '북마크'],
                on_select="rerun",
                selection_mode="single-row",
                key=editor_key
            )

            state = st.session_state.get(editor_key, {})

            edited_rows = state.get("edited_rows", {})
            if edited_rows:
                for str_idx, changes in edited_rows.items():
                    if "북마크" in changes:
                        idx = int(str_idx)
                        if idx < len(top_users):
                            clicked_nick = top_users.iloc[idx]['닉네임']
                            is_checked = changes["북마크"]
                            
                            if is_checked and clicked_nick not in st.session_state.bookmarks:
                                st.session_state.bookmarks.append(clicked_nick)
                            elif not is_checked and clicked_nick in st.session_state.bookmarks:
                                st.session_state.bookmarks.remove(clicked_nick)
                
                cookie_manager.set("user_bookmarks", ",".join(st.session_state.bookmarks))
                st.rerun()

            selection = state.get("selection", {})
            selected_rows = selection.get("rows", [])
            if len(selected_rows) > 0:
                idx = selected_rows[0]
                if idx < len(top_users): 
                    selected_row = top_users.iloc[idx]
                    show_user_detail_modal(selected_row['닉네임'], selected_row['ID(IP)'], selected_row['계정타입'], df, selected_date)
                    
                    
        # ==========================================
        # [Tab 3] 유저 검색
        # ==========================================
        elif selected_tab == "👥 유저 검색":
            st.subheader("전체 유저 목록")
            st.caption("✅ 맨 앞의 체크박스를 눌러 북마크하거나, 행을 클릭해 상세 📊 그래프를 확인하세요.")

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
                page_df.insert(0, '그래프', '📊 보기')
                page_df['북마크'] = page_df['닉네임'].apply(lambda x: True if x in st.session_state.bookmarks else False)
                page_df = page_df.sort_values(by=['북마크', '총활동수'], ascending=[False, False])

                display_columns = ['북마크', '그래프', '닉네임', 'ID(IP)', '계정타입', '작성글수', '작성댓글수', '총활동수']
                page_df = page_df[display_columns]

                def highlight_yellow_search(row):
                    if row['북마크'] == True:
                        return ['background-color: #FFFACD'] * len(row)
                    return [''] * len(row)
                
                styled_page_df = page_df.style.apply(highlight_yellow_search, axis=1)

                editor_key = "search_editor_v6"
                st.data_editor(
                    styled_page_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "그래프": st.column_config.TextColumn("상세", width="small"),
                        "북마크": st.column_config.CheckboxColumn("⭐ 북마크", default=False)
                    },
                    disabled=[c for c in page_df.columns if c != '북마크'],
                    on_select="rerun",
                    selection_mode="single-row",
                    key=editor_key
                )

                state = st.session_state.get(editor_key, {})

                edited_rows = state.get("edited_rows", {})
                if edited_rows:
                    for str_idx, changes in edited_rows.items():
                        if "북마크" in changes:
                            idx = int(str_idx)
                            if idx < len(page_df):
                                clicked_nick = page_df.iloc[idx]['닉네임']
                                is_checked = changes["북마크"]
                                
                                if is_checked and clicked_nick not in st.session_state.bookmarks:
                                    st.session_state.bookmarks.append(clicked_nick)
                                elif not is_checked and clicked_nick in st.session_state.bookmarks:
                                    st.session_state.bookmarks.remove(clicked_nick)
                    
                    cookie_manager.set("user_bookmarks", ",".join(st.session_state.bookmarks))
                    st.rerun()

                selection = state.get("selection", {})
                selected_rows = selection.get("rows", [])
                if len(selected_rows) > 0:
                    idx = selected_rows[0]
                    if idx < len(page_df):
                        selected_row = page_df.iloc[idx]
                        show_user_detail_modal(selected_row['닉네임'], selected_row['ID(IP)'], selected_row['계정타입'], df, selected_date)

else:
    st.info("데이터 로딩 중... (데이터가 없거나 DB 연결을 확인해주세요)")
