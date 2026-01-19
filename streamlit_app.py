# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import traceback
import sys
import yfinance as yf

# ==========================================
# 版本資訊
# ==========================================
APP_VERSION = "v2.0.0 (即時排行重算版)"
UPDATE_LOG = """
- v1.9.2: 修復欄位名稱錯誤。
- v2.0.0: 重大邏輯升級！
  1. 不再沿用昨日排行。
  2. 盤中直接掃描全台股 (上市+上櫃)，依據即時報價計算成交值。
  3. 根據「即時成交值」重新排序，抓出當下真正的 Top 300 進行廣度分析。
"""

# ==========================================
# 參數與 Token
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"
TOP_N = 300              
BREADTH_THRESHOLD = 0.65
EXCLUDE_ETF_PREFIX = "00"

st.set_page_config(page_title="盤中權證進場判斷", layout="wide")

# ==========================================
# 功能函式
# ==========================================

def get_trading_days(api):
    """ 取得交易日 """
    try:
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d"))
        if df.empty: return []
        dates = sorted(df['date'].unique().tolist())
    except Exception:
        return []
    
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    today_str = tw_now.strftime("%Y-%m-%d")
    current_time = tw_now.time()
    
    # 只要是平日且開盤後，強制加入今天
    if 0 <= tw_now.weekday() <= 4 and current_time >= time(8, 45):
        if not dates or today_str > dates[-1]:
            dates.append(today_str)
            
    return dates

def get_all_stock_ids(api):
    """ 從 FinMind 取得全台股代號清單 (利用昨日資料) """
    # 這裡我們只是要「代號列表」，所以抓最近一天的資料即可
    prev_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    # 往前推幾天直到抓到資料
    for i in range(5):
        d_str = (datetime.now() - timedelta(days=i+1)).strftime("%Y-%m-%d")
        try:
            df = api.taiwan_stock_daily(stock_id="", start_date=d_str)
            if not df.empty:
                # 篩選：排除 ETF
                df = df[~df['stock_id'].str.startswith(EXCLUDE_ETF_PREFIX)]
                # 篩選：只留純數字代號
                df = df[df['stock_id'].str.isdigit()]
                return df['stock_id'].unique().tolist()
        except:
            continue
    return []

def fetch_realtime_rank_from_yahoo(stock_ids):
    """ 
    核心函數：從 Yahoo 批次抓取全市場，並計算成交值排行
    """
    if not stock_ids: return [], {}, None

    # 為了保險，我們對每個代號同時生成 .TW 和 .TWO
    # 雖然這樣會多出一倍請求，但能確保抓到上市櫃所有資料
    tickers_map = {}
    all_tickers = []
    
    # 優化：如果有辦法區分上市上櫃更好，但這裡為了簡單暴力，先全部嘗試
    # 由於 yfinance 批次下載會自動忽略無效代號，所以多丟沒關係
    for c in stock_ids:
        tw = f"{c}.TW"
        two = f"{c}.TWO"
        all_tickers.extend([tw, two])
        tickers_map[tw] = c
        tickers_map[two] = c
    
    print(f"準備掃描全市場 {len(stock_ids)} 檔股票 (請求數: {len(all_tickers)})...")
    
    try:
        # 下載全市場即時報價
        data = yf.download(all_tickers, period="1d", group_by='ticker', progress=False, threads=True)
        
        calculated_list = []
        realtime_cache = {} # 存起來等下算 MA5 用
        latest_time = None

        # 解析資料 (這段比較繁瑣因為 yfinance 格式多變)
        # 遍歷下載回來的每一個 ticker column
        # data 的 columns 可能是 MultiIndex (Ticker, PriceType)
        
        # 取得所有有資料的 Ticker
        valid_tickers = data.columns.levels[0] if isinstance(data.columns, pd.MultiIndex) else [data.name]
        
        for t in valid_tickers:
            try:
                # 取出單檔 DataFrame
                if isinstance(data.columns, pd.MultiIndex):
                    df = data[t]
                else:
                    df = data # 只有一檔時
                
                if df.empty or df['Close'].isna().all() or df['Volume'].isna().all():
                    continue

                # 抓最後一筆
                row = df.iloc[-1]
                c = float(row['Close'])
                h = float(row['High'])
                l = float(row['Low'])
                v = float(row['Volume'])
                
                if v <= 0: continue
                
                # 計算時間
                last_ts = df.index[-1]
                if latest_time is None or last_ts > latest_time:
                    latest_time = last_ts

                # === 你的核心公式 ===
                # 成交值 = ((H + L + C) / 3) * Volume / 1,000,000 (百萬)
                avg_p = (h + l + c) / 3.0
                turnover = (avg_p * v) / 1_000_000.0
                
                # 還原純數字代號
                stock_code = tickers_map.get(t, t.split('.')[0])
                
                # 存入列表以便排序
                calculated_list.append({
                    'code': stock_code,
                    'turnover': turnover,
                    'close': c,
                    'high': h,
                    'low': l,
                    'volume': v
                })
                
                # 存入快取
                realtime_cache[stock_code] = {
                    'close': c, 'high': h, 'low': l, 'volume': v
                }
                
            except Exception:
                continue

        # 排序：取成交值前 TOP_N
        df_rank = pd.DataFrame(calculated_list)
        if df_rank.empty:
            return [], {}, None
            
        # 依成交值降冪排序，並去重 (以防 .TW 和 .TWO 都有數據，雖然少見)
        df_rank = df_rank.sort_values('turnover', ascending=False).drop_duplicates('code')
        top_n_df = df_rank.head(TOP_N)
        
        return top_n_df, realtime_cache, latest_time

    except Exception as e:
        print(f"全市場掃描失敗: {e}")
        return [], {}, None

@st.cache_data(ttl=300)
def fetch_data(_api):
    all_days = get_trading_days(_api)
    if len(all_days) < 2:
        st.error(f"歷史資料不足 (API 連線可能異常)。")
        return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    # === 步驟 1: 取得全市場代號清單 ===
    all_ids = get_all_stock_ids(_api)
    if not all_ids:
        st.error("無法取得股票代號清單。")
        return None
        
    # === 步驟 2: 即時掃描全市場並排行 (Yahoo) ===
    # 這裡會花一點時間，因為要下載 2000 檔
    with st.spinner(f"正在即時掃描全市場 {len(all_ids)} 檔股票，計算最新成交值排行..."):
        df_top_n, rt_cache, last_time = fetch_realtime_rank_from_yahoo(all_ids)
    
    if df_top_n is None or df_top_n.empty:
        st.error("全市場即時掃描失敗，無法產生排行。")
        return None

    # 這就是我們今天要分析的「即時母體」
    target_candidates = df_top_n.to_dict('records')
    
    # === 步驟 3: 逐檔計算 MA5 (FinMind History + Yahoo Realtime) ===
    results = []
    detailed_status = []
    
    progress_bar = st.progress(0, text="正在分析 Top 300 技術指標...")
    
    for i, row in enumerate(target_candidates):
        code = row['code']
        current_close = row['close']
        rank = i + 1
        status = "未知"
        
        try:
            # A. 抓歷史資料 (FinMind)
            stock_df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            )
            
            # 清理：確保不含今日
            stock_df = stock_df[stock_df['date'] < d_curr_str]
            
            # B. 拼上今日即時資料 (Yahoo)
            new_row = pd.DataFrame([{
                'date': d_curr_str,
                'close': current_close
            }])
            stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                
            # C. 計算指標
            if len(stock_df) >= 6:
                stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                curr_row = stock_df.iloc[-1]
                prev_row = stock_df.iloc[-2]
                
                results.append({
                    "d_curr_ok": curr_row['close'] > curr_row['MA5'],
                    "d_prev_ok": prev_row['close'] > prev_row['MA5']
                })
                status = "✅ 納入"
            else:
                status = "❌ 剔除 (K線不足)"
                
        except Exception as e:
            status = f"❌ 剔除 ({str(e)})"

        detailed_status.append({
            "排名": rank,
            "代號": code,
            "現價": current_close,
            "成交額(百萬)": row['turnover'],
            "狀態": status
        })
        
        if i % 30 == 0:
            progress_bar.progress((i + 1) / TOP_N, text=f"分析進度: {i+1}/{TOP_N}")
            
    progress_bar.empty()
    res_df = pd.DataFrame(results)
    detail_df = pd.DataFrame(detailed_status)
    
    # === 步驟 4: 大盤斜率 ===
    slope = 0
    try:
        twii_df = _api.taiwan_stock_daily(stock_id="TAIEX", start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        twii_df = twii_df[twii_df['date'] < d_curr_str]
        try:
            twii_rt = yf.download("^TWII", period="1d", progress=False)
            if not twii_rt.empty:
                last_twii = float(twii_rt['Close'].iloc[-1])
                new_row = pd.DataFrame([{'date': d_curr_str, 'close': last_twii}])
                twii_df = pd.concat([twii_df, new_row], ignore_index=True)
        except:
            pass
        twii_df['MA5'] = twii_df['close'].rolling(5).mean()
        slope = twii_df['MA5'].iloc[-1] - twii_df['MA5'].iloc[-2]
    except:
        pass
        
    return {
        "d_curr": d_curr_str,
        "d_prev": d_prev_str,
        "br_curr": res_df['d_curr_ok'].mean() if not res_df.empty else 0,
        "br_prev": res_df['d_prev_ok'].mean() if not res_df.empty else 0,
        "hit_curr": res_df['d_curr_ok'].sum() if not res_df.empty else 0,
        "hit_prev": res_df['d_prev_ok'].sum() if not res_df.empty else 0,
        "valid": len(res_df),
        "slope": slope,
        "detail_df": detail_df,
        "last_time": last_time
    }

# ==========================================
# UI
# ==========================================
def run_streamlit():
    st.title("📈 盤中權證進場判斷 (v2.0 全市場即時排行)")

    with st.sidebar:
        st.subheader("系統狀態")
        st.success("API Token 已載入")
        st.code(f"Version: {APP_VERSION}")
        st.markdown(UPDATE_LOG)

    api = DataLoader()
    api.login_by_token(API_TOKEN)

    if st.button("🔄 立即掃描全市場 (運算約需 30秒)"):
        st.cache_data.clear()

    try:
        # 第一次載入時自動執行
        data = fetch_data(api)
            
        if data is None:
            st.warning("⚠️ 程式執行完畢但未回傳有效數據，請確認 API 連線。")
        else:
            cond1 = (data['br_curr'] >= BREADTH_THRESHOLD) and (data['br_prev'] >= BREADTH_THRESHOLD)
            cond2 = data['slope'] > 0
            final_decision = cond1 and cond2
            time_str = data['last_time'].strftime("%H:%M:%S") if data['last_time'] else "未知"

            st.subheader(f"📅 數據基準日：{data['d_curr']}")
            st.info(f"📊 統計說明：已掃描全市場並依「即時成交值」重算 Top {TOP_N}。 (最新報價: {time_str})")

            c1, c2, c3 = st.columns(3)
            c1.metric("今日廣度 (D)", f"{data['br_curr']:.1%}", f"{data['hit_curr']}/{data['valid']}")
            c2.metric("昨日廣度 (D-1)", f"{data['br_prev']:.1%}", f"{data['hit_prev']}/{data['valid']}")
            c3.metric("大盤 MA5 斜率", f"{data['slope']:.2f}", "正 ✓" if cond2 else "非正 ✗")

            st.divider()
            st.header("💡 進場結論")
            if final_decision:
                st.success(f"✅ 結論：可進場")
            else:
                st.error(f"⛔ 結論：不可進場")
            
            st.subheader(f"📋 即時成交值排行榜 (Top {TOP_N})")
            st.dataframe(
                data['detail_df'], 
                column_config={
                    "排名": st.column_config.NumberColumn(format="%d"),
                    "現價": st.column_config.NumberColumn(format="%.2f"),
                    "成交額(百萬)": st.column_config.NumberColumn(format="$%.2f"),
                },
                use_container_width=True, 
                hide_index=True
            )

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.code(traceback.format_exc())

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        input("按 Enter 結束...")
