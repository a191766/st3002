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
APP_VERSION = "v1.9.0 (時間軸強制校正版)"
UPDATE_LOG = """
- v1.8.0: 嘗試解決無更新問題，但時間視窗過窄導致盤後回退。
- v1.9.0: 移除 14:00 限制。只要是平日 08:45 後，無條件強制鎖定「今天」為 D，確保 D-1 正確對應到上個交易日 (如上週五)。
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
    """ 取得交易日 (強制校正版) """
    # 1. 先抓歷史 (通常只會到 1/16)
    try:
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d"))
        dates = sorted(df['date'].unique().tolist())
    except:
        dates = []
    
    # 2. 強制加入今天
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    today_str = tw_now.strftime("%Y-%m-%d")
    current_time = tw_now.time()
    
    # 邏輯修正：只要是平日 (Mon=0 ~ Fri=4) 且時間晚於 08:45，無論是否收盤，都強制把今天算進去
    if 0 <= tw_now.weekday() <= 4 and current_time >= time(8, 45):
        if not dates or today_str > dates[-1]:
            dates.append(today_str)
            
    return dates

def fetch_yahoo_realtime_batch(codes):
    """ Yahoo Finance 批次下載 """
    if not codes: return {}, None
    
    tw_tickers = [f"{c}.TW" for c in codes]
    two_tickers = [f"{c}.TWO" for c in codes]
    all_tickers = tw_tickers + two_tickers
    
    try:
        # 下載當日數據
        data = yf.download(all_tickers, period="1d", group_by='ticker', progress=False, threads=True)
        realtime_map = {}
        latest_time = None
        
        # 資料解析
        if len(all_tickers) == 1:
             # 單檔處理
             t = all_tickers[0]
             df = data
             if not df.empty and not df['Close'].isna().all():
                 realtime_map[t.split('.')[0]] = float(df['Close'].iloc[-1])
                 latest_time = df.index[-1]
        else:
            # 多檔處理
            for t in all_tickers:
                try:
                    df = data[t]
                    if not df.empty and not df['Close'].isna().all():
                        last_price = float(df['Close'].iloc[-1])
                        last_ts = df.index[-1]
                        if latest_time is None or last_ts > latest_time:
                            latest_time = last_ts
                        realtime_map[t.split('.')[0]] = last_price
                except:
                    continue
                
        return realtime_map, latest_time
    except Exception as e:
        print(f"Yahoo Err: {e}")
        return {}, None

@st.cache_data(ttl=300)
def fetch_data(_api):
    all_days = get_trading_days(_api)
    if len(all_days) < 2:
        st.error(f"歷史資料不足 (抓到的日期: {all_days})。")
        return None

    d_curr_str = all_days[-1]  # 這應該要是今天 (1/19)
    d_prev_str = all_days[-2]  # 這應該要是上週五 (1/16)
    
    # 除錯訊息：讓使用者確認時間軸是否正確
    debug_dates = f"D={d_curr_str}, D-1={d_prev_str}"
    
    # === 步驟 1: 取得「D-1」的排行 ===
    # 因為 D 是盤中，排行不準，所以我們用 D-1 (1/16) 的排行來選股
    df_all = _api.taiwan_stock_daily(stock_id="", start_date=d_prev_str)
    
    # 如果 D-1 抓不到資料 (例如 API 漏資料)，嘗試再往推一天
    if df_all.empty:
        d_prev_str = all_days[-3]
        df_all = _api.taiwan_stock_daily(stock_id="", start_date=d_prev_str)
        
    cols_map = {c.lower(): c for c in df_all.columns}
    def get_col(n): return df_all[cols_map.get(n.lower(), n)]
    
    try:
        df_all['MyClose'] = get_col('Close')
        df_all['MyVol'] = get_col('Volume')
        df_all['MyId'] = get_col('stock_id')
        df_all['turnover_val'] = df_all['MyClose'] * df_all['MyVol']
    except:
        return None

    df_all['MyId'] = df_all['MyId'].astype(str)
    df_all = df_all[df_all['MyId'].str.isdigit()]  
    df_all = df_all[~df_all['MyId'].str.startswith(EXCLUDE_ETF_PREFIX)] 
    
    df_candidates = df_all.sort_values('turnover_val', ascending=False).head(TOP_N).copy()
    target_codes = df_candidates['MyId'].tolist()
    
    # === 步驟 2: Yahoo 批次抓取即時價 (for D) ===
    rt_prices, last_update_time = fetch_yahoo_realtime_batch(target_codes)
    
    # === 步驟 3: 逐檔運算 ===
    results = []
    detailed_status = []
    updated_count = 0
    
    progress_bar = st.progress(0, text="數據整合中...")
    
    for i, (idx, row) in enumerate(df_candidates.iterrows()):
        code = row['MyId']
        rank = i + 1
        status = "未知"
        price_src = "歷史延用"
        
        # 決定 D (今日) 的價格
        if code in rt_prices:
            current_close = rt_prices[code]
            price_src = "Yahoo即時"
            updated_count += 1
        else:
            # 如果抓不到即時，只好先用 D-1 的收盤價 (最壞情況)
            current_close = row['MyClose']
        
        try:
            # 抓歷史資料 (包含 D-1 及之前)
            stock_df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            )
            
            # 清理：確保 stock_df 裡沒有 D (以防 FinMind 偷跑)
            stock_df = stock_df[stock_df['date'] < d_curr_str]
            
            # 手動合成 D (今日)
            new_row = pd.DataFrame([{
                'date': d_curr_str,
                'close': current_close
            }])
            stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                
            # 計算 MA5
            if len(stock_df) >= 6:
                stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                
                # 這裡最關鍵：
                # curr_row 必須是 D (最後一筆)
                # prev_row 必須是 D-1 (倒數第二筆)
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
            "來源": price_src,
            "狀態": status
        })
        
        if i % 20 == 0:
            progress_bar.progress((i + 1) / TOP_N, text=f"計算中... (已更新 {updated_count} 檔)")
            
    progress_bar.empty()
    res_df = pd.DataFrame(results)
    detail_df = pd.DataFrame(detailed_status)
    
    # === 步驟 4: 大盤斜率 (同樣強制更新) ===
    slope = 0
    try:
        twii_df = _api.taiwan_stock_daily(stock_id="TAIEX", start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        twii_df = twii_df[twii_df['date'] < d_curr_str] # 確保不含今日
        
        # 抓大盤即時
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
        "updated_count": updated_count,
        "last_time": last_update_time,
        "debug_dates": debug_dates
    }

# ==========================================
# UI
# ==========================================
def run_streamlit():
    st.title("📈 盤中權證進場判斷 (v1.9 強制校正版)")

    with st.sidebar:
        st.subheader("系統狀態")
        st.success("API Token 已載入")
        st.code(f"Version: {APP_VERSION}")
        st.markdown(UPDATE_LOG)

    api = DataLoader()
    api.login_by_token(API_TOKEN)

    if st.button("🔄 立即重新整理"):
        st.cache_data.clear()

    try:
        with st.spinner("正在強制校正時間軸並抓取數據..."):
            data = fetch_data(api)
            
        if data is None:
            st.stop()

        cond1 = (data['br_curr'] >= BREADTH_THRESHOLD) and (data['br_prev'] >= BREADTH_THRESHOLD)
        cond2 = data['slope'] > 0
        final_decision = cond1 and cond2
        
        time_str = data['last_time'].strftime("%H:%M:%S") if data['last_time'] else "未知"

        # 這裡會顯示程式認定的 D 與 D-1，讓你確認是否修復
        st.subheader(f"📅 數據基準日：{data['d_curr']}")
        st.caption(f"ℹ️ 時間軸確認：{data['debug_dates']} (若 D 為今日，D-1 應為上週五)")

        # 狀態卡片
        st.info(f"""
        📊 **即時資料狀態**
        - 最新報價時間：**{time_str}**
        - 即時更新數：**{data['updated_count']}** / {len(data['detail_df'])} 檔
        """)

        c1, c2, c3 = st.columns(3)
        c1.metric("今日廣度 (D)", f"{data['br_curr']:.1%}", f"{data['hit_curr']}/{data['valid']}")
        c2.metric("昨日廣度 (D-1)", f"{data['br_prev']:.1%}", f"{data['hit_prev']}/{data['valid']}")
        c3.metric("大盤 MA5 斜率", f"{data['slope']:.2f}", "正 ✓" if cond2 else "非正 ✗")

        st.divider()

        st.header("💡 進場結論")
        if final_decision:
            st.success(f"✅ 結論（{data['d_curr']} 的隔日）：可進場")
        else:
            st.error(f"⛔ 結論（{data['d_curr']} 的隔日）：不可進場")
        
        st.write(f"- 廣度連兩天 ≥ 65%：{'✅ 通過' if cond1 else '❌ 未通過'}")
        st.write(f"- 大盤 MA5 斜率 > 0：{'✅ 通過' if cond2 else '❌ 未通過'} (MA5斜率: {data['slope']:.2f})")

        st.divider()
        st.subheader(f"📋 前 {TOP_N} 大個股即時狀況")
        st.dataframe(
            data['detail_df'], 
            column_config={
                "排名": st.column_config.NumberColumn(format="%d"),
                "現價": st.column_config.NumberColumn(format="%.2f"),
            },
            use_container_width=True,
            height=600,
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
