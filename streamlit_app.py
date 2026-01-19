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
APP_VERSION = "v1.9.1 (除錯顯示版)"
UPDATE_LOG = """
- v1.9.0: 強制校正時間軸。
- v1.9.1: 解除靜默失敗。當抓不到排行榜資料時，會明確顯示錯誤訊息，而非留下空白畫面。
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
        # 嘗試抓 0050 的歷史資料來判斷交易日
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d"))
        if df.empty:
            # 如果連 0050 都抓不到，可能是 Token 過期或 API 掛了
            st.error("⚠️ 無法取得歷史交易日歷 (API 回傳空值)，請檢查 Token 或網路狀態。")
            return []
        dates = sorted(df['date'].unique().tolist())
    except Exception as e:
        st.error(f"⚠️ 取得交易日歷失敗: {e}")
        return []
    
    # 強制加入今天 (若為交易時間)
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    today_str = tw_now.strftime("%Y-%m-%d")
    current_time = tw_now.time()
    
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
        data = yf.download(all_tickers, period="1d", group_by='ticker', progress=False, threads=True)
        realtime_map = {}
        latest_time = None
        
        if len(all_tickers) == 1:
             t = all_tickers[0]
             df = data
             if not df.empty and not df['Close'].isna().all():
                 realtime_map[t.split('.')[0]] = float(df['Close'].iloc[-1])
                 latest_time = df.index[-1]
        else:
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
        return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    debug_dates = f"D={d_curr_str}, D-1={d_prev_str}"
    
    # === 步驟 1: 取得「D-1」的排行 ===
    try:
        df_all = _api.taiwan_stock_daily(stock_id="", start_date=d_prev_str)
    except Exception as e:
        st.error(f"❌ API 請求全市場資料失敗: {e}")
        return None
    
    # 如果 D-1 抓不到，嘗試再往推一天 (D-2)
    if df_all.empty:
        st.warning(f"⚠️ {d_prev_str} 查無全市場資料，嘗試回推至 {all_days[-3]}...")
        d_prev_str = all_days[-3]
        df_all = _api.taiwan_stock_daily(stock_id="", start_date=d_prev_str)
        
    if df_all.empty:
        # 這裡就是導致空白畫面的兇手
        st.error(f"❌ 嚴重錯誤：連續兩天 ({all_days[-2]}, {d_prev_str}) 都無法取得全市場排行資料。API 可能異常。")
        return None
        
    cols_map = {c.lower(): c for c in df_all.columns}
    def get_col(n): return df_all[cols_map.get(n.lower(), n)]
    
    try:
        df_all['MyClose'] = get_col('Close')
        df_all['MyVol'] = get_col('Volume')
        df_all['MyId'] = get_col('stock_id')
        df_all['turnover_val'] = df_all['MyClose'] * df_all['MyVol']
    except Exception as e:
        # 這裡也是潛在的空白兇手
        st.error(f"❌ 資料欄位解析失敗: {e}. 抓到的欄位有: {df_all.columns.tolist()}")
        return None

    df_all['MyId'] = df_all['MyId'].astype(str)
    df_all = df_all[df_all['MyId'].str.isdigit()]  
    df_all = df_all[~df_all['MyId'].str.startswith(EXCLUDE_ETF_PREFIX)] 
    
    df_candidates = df_all.sort_values('turnover_val', ascending=False).head(TOP_N).copy()
    target_codes = df_candidates['MyId'].tolist()
    
    # === 步驟 2: Yahoo 批次抓取即時價 ===
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
        
        if code in rt_prices:
            current_close = rt_prices[code]
            price_src = "Yahoo即時"
            updated_count += 1
        else:
            current_close = row['MyClose']
        
        try:
            stock_df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            )
            
            stock_df = stock_df[stock_df['date'] < d_curr_str]
            
            new_row = pd.DataFrame([{
                'date': d_curr_str,
                'close': current_close
            }])
            stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                
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
            "來源": price_src,
            "狀態": status
        })
        
        if i % 20 == 0:
            progress_bar.progress((i + 1) / TOP_N, text=f"計算中... (已更新 {updated_count} 檔)")
            
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
        "updated_count": updated_count,
        "last_time": last_update_time,
        "debug_dates": debug_dates
    }

# ==========================================
# UI
# ==========================================
def run_streamlit():
    st.title("📈 盤中權證進場判斷 (v1.9.1 除錯版)")

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
            # 這裡不使用 st.stop()，而是印出訊息，避免畫面全白
            st.warning("⚠️ 程式執行完畢但未回傳有效數據，請查看上方是否有紅色錯誤訊息。")
        else:
            # 正常顯示區
            cond1 = (data['br_curr'] >= BREADTH_THRESHOLD) and (data['br_prev'] >= BREADTH_THRESHOLD)
            cond2 = data['slope'] > 0
            final_decision = cond1 and cond2
            time_str = data['last_time'].strftime("%H:%M:%S") if data['last_time'] else "未知"

            st.subheader(f"📅 數據基準日：{data['d_curr']}")
            st.caption(f"ℹ️ 時間軸確認：{data['debug_dates']}")
            st.info(f"📊 即時更新數：**{data['updated_count']}** / {len(data['detail_df'])} 檔 (時間: {time_str})")

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
            
            st.dataframe(data['detail_df'], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.code(traceback.format_exc())

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        input("按 Enter 結束...")
