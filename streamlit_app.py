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
APP_VERSION = "v2.2.0 (FinMind排行版)"
UPDATE_LOG = """
- v2.1.0: 盤中/盤後切換邏輯。
- v2.2.0: 邏輯重構。
  1. 盤中：鎖定使用「昨日 FinMind 成交額排行」前 300 名。
  2. 盤後：優先嘗試「今日 FinMind 成交額排行」，若無資料則回退昨日。
  3. 篩選優化：只保留「4碼純數字」股票，精準排除權證、ETF (00開頭)、TDR (91開頭) 等。
"""

# ==========================================
# 參數與 Token
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"
TOP_N = 300              
BREADTH_THRESHOLD = 0.65
EXCLUDE_PREFIXES = ["00", "91"] # 排除 00(ETF), 91(TDR)

st.set_page_config(page_title="盤中權證進場判斷", layout="wide")

# ==========================================
# 功能函式
# ==========================================

def get_current_status():
    """ 判斷目前是盤中還是盤後 """
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    current_time = tw_now.time()
    # 08:45 ~ 13:30 視為盤中
    is_intraday = time(8, 45) <= current_time < time(13, 30)
    return tw_now, is_intraday

def get_trading_days(api):
    """ 取得交易日 """
    try:
        # 嘗試抓 0050 判斷歷史交易日
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d"))
        if df.empty: return []
        dates = sorted(df['date'].unique().tolist())
    except:
        return []
    
    tw_now, is_intraday = get_current_status()
    today_str = tw_now.strftime("%Y-%m-%d")
    
    # 平日且時間過 08:45，強制把今天加入日期列表
    if 0 <= tw_now.weekday() <= 4 and tw_now.time() >= time(8, 45):
        if not dates or today_str > dates[-1]:
            dates.append(today_str)
            
    return dates

def smart_get_column(df, candidates):
    """ 智慧欄位搜尋 """
    cols = df.columns
    lower_map = {c.lower(): c for c in cols}
    for name in candidates:
        if name in cols: return df[name]
        if name.lower() in lower_map: return df[lower_map[name.lower()]]
    return None

def fetch_yahoo_realtime_batch(codes):
    """ Yahoo 批次下載 (極速版) """
    if not codes: return {}, None
    
    # 建立 ticker 清單
    tw_tickers = [f"{c}.TW" for c in codes]
    # 上市大部分是 .TW，少數上櫃可能是 .TWO，但在 Top 300 中大部分是上市
    # 為了保險，我們可以混合檢查，但為了速度，先以 .TW 為主，若找不到再試 .TWO?
    # 更好的策略：直接丟 .TW 和 .TWO 給 Yahoo，它會自動忽略無效的
    all_tickers = tw_tickers + [f"{c}.TWO" for c in codes]
    
    try:
        # 下載
        data = yf.download(all_tickers, period="1d", group_by='ticker', progress=False, threads=True)
        realtime_map = {}
        latest_time = None
        
        # 處理單檔與多檔回傳格式差異
        valid_tickers = []
        if isinstance(data.columns, pd.MultiIndex):
            valid_tickers = data.columns.levels[0]
        elif not data.empty:
            valid_tickers = [data.name] if hasattr(data, 'name') else []
            # 如果只有一檔且沒 name，通常不會發生在 batch 下載
            if len(all_tickers) == 1: valid_tickers = all_tickers

        # 解析
        if len(valid_tickers) == 0 and not data.empty and len(all_tickers) == 1:
             # 單檔特殊處理
             df = data
             if not df.empty:
                 c = float(df['Close'].iloc[-1])
                 realtime_map[codes[0]] = c
                 latest_time = df.index[-1]
        else:
            for t in valid_tickers:
                try:
                    df = data[t] if isinstance(data.columns, pd.MultiIndex) else data
                    if df.empty or df['Close'].isna().all(): continue
                    
                    last_price = float(df['Close'].iloc[-1])
                    last_ts = df.index[-1]
                    if latest_time is None or last_ts > latest_time:
                        latest_time = last_ts
                    
                    # 存入 map (移除 .TW/.TWO)
                    stock_id = t.split('.')[0]
                    realtime_map[stock_id] = last_price
                except: continue
                
        return realtime_map, latest_time
    except:
        return {}, None

@st.cache_data(ttl=300)
def fetch_data(_api):
    all_days = get_trading_days(_api)
    if len(all_days) < 2:
        st.error("歷史資料不足。")
        return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    tw_now, is_intraday = get_current_status()
    
    # === 步驟 1: 決定排行榜來源日期 ===
    # 預設邏輯：
    # 盤中 -> 用 D-1 排行
    # 盤後 -> 用 D 排行 (若抓不到則降級用 D-1)
    
    target_rank_date = d_prev_str # 預設 D-1
    mode_msg = "🚀 盤中模式 (基準:昨日排行)"
    
    if not is_intraday:
        # 盤後嘗試抓 D
        try:
            check_df = _api.taiwan_stock_daily(stock_id="2330", start_date=d_curr_str)
            if not check_df.empty:
                target_rank_date = d_curr_str
                mode_msg = "🐢 盤後模式 (基準:今日排行)"
            else:
                mode_msg = "⚠️ 盤後模式 (FinMind 尚未更新，暫用昨日排行)"
        except:
            pass # 失敗就維持 D-1

    # === 步驟 2: 取得 FinMind 現成排行 ===
    try:
        # 抓取該日全市場資料
        df_rank = _api.taiwan_stock_daily(stock_id="", start_date=target_rank_date)
        
        # 容錯：萬一真的連 D-1 都抓不到 (例如連假後資料缺失)
        if df_rank.empty:
            target_rank_date = all_days[-3]
            df_rank = _api.taiwan_stock_daily(stock_id="", start_date=target_rank_date)
            mode_msg = f"⚠️ 資料異常，回退至 {target_rank_date} 排行"
            
        # 欄位處理
        # FinMind 欄位通常有: stock_id, Trading_money (成交金額), Trading_Volume (成交量), close
        # 注意：不同版本 API 欄位可能大小寫不同
        df_rank['ID'] = smart_get_column(df_rank, ['stock_id', 'code'])
        df_rank['Money'] = smart_get_column(df_rank, ['Trading_money', 'Trading_Money', 'turnover'])
        df_rank['Close'] = smart_get_column(df_rank, ['close', 'Close', 'price'])
        
        # === 關鍵：篩選邏輯 (只留個股) ===
        # 1. 轉字串
        df_rank['ID'] = df_rank['ID'].astype(str)
        # 2. 必須是 4 碼 (排除權證、特別股等 6 碼商品)
        df_rank = df_rank[df_rank['ID'].str.len() == 4]
        # 3. 必須是純數字 (排除特殊商品)
        df_rank = df_rank[df_rank['ID'].str.isdigit()]
        # 4. 排除 ETF (00開頭) 和 TDR (91開頭)
        for prefix in EXCLUDE_PREFIXES:
            df_rank = df_rank[~df_rank['ID'].str.startswith(prefix)]
            
        # 排序：取成交金額 (Money) 前 N 名
        df_candidates = df_rank.sort_values('Money', ascending=False).head(TOP_N)
        
        # 建立候選名單
        target_list = []
        for _, row in df_candidates.iterrows():
            target_list.append({
                'code': row['ID'],
                'hist_close': row['Close'] # 歷史收盤價 (作為備援)
            })
            
    except Exception as e:
        st.error(f"排行資料獲取失敗: {e}")
        return None

    # === 步驟 3: 批次抓取即時價 (Yahoo) ===
    # 無論是盤中還是盤後，都去問一下 Yahoo 看看有沒有最新價
    # 如果是盤後且 FinMind 已更新，其實 Yahoo 抓到的就是收盤價，沒差
    codes = [x['code'] for x in target_list]
    realtime_prices, last_time = fetch_yahoo_realtime_batch(codes)
    
    # === 步驟 4: 逐檔計算 MA5 ===
    results = []
    detailed_status = []
    
    progress_bar = st.progress(0, text=f"分析數據中 ({mode_msg})...")
    
    for i, item in enumerate(target_list):
        code = item['code']
        rank = i + 1
        
        # 決定當前價格：優先用 Yahoo 即時，沒有則用 FinMind 歷史
        current_close = realtime_prices.get(code, item['hist_close'])
        price_src = "Yahoo即時" if code in realtime_prices else "歷史收盤"
        
        try:
            # 抓個股歷史 (FinMind)
            stock_df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            )
            
            # 清理：確保不含今日 (D)，避免重複疊加
            stock_df = stock_df[stock_df['date'] < d_curr_str]
            
            # 合成今日 (D) 資料
            if current_close > 0:
                new_row = pd.DataFrame([{'date': d_curr_str, 'close': current_close}])
                stock_df = pd.concat([stock_df, new_row], ignore_index=True)
            
            if len(stock_df) >= 6:
                stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                curr_row = stock_df.iloc[-1]  # D
                prev_row = stock_df.iloc[-2]  # D-1
                
                results.append({
                    "d_curr_ok": curr_row['close'] > curr_row['MA5'],
                    "d_prev_ok": prev_row['close'] > prev_row['MA5']
                })
                status = "✅ 納入"
            else:
                status = "❌ 剔除 (資料不足)"
                
        except Exception:
            status = "❌ 錯誤"
            
        detailed_status.append({
            "排名": rank,
            "代號": code,
            "現價": current_close,
            "來源": price_src,
            "狀態": status
        })
        
        if i % 30 == 0:
            progress_bar.progress((i + 1) / TOP_N, text=f"分析進度: {i+1}/{TOP_N}")
            
    progress_bar.empty()
    res_df = pd.DataFrame(results)
    detail_df = pd.DataFrame(detailed_status)
    
    # === 大盤斜率 ===
    slope = 0
    try:
        twii_df = _api.taiwan_stock_daily(stock_id="TAIEX", start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        twii_df = twii_df[twii_df['date'] < d_curr_str]
        try:
            # 嘗試抓大盤即時
            twii_rt = yf.download("^TWII", period="1d", progress=False)
            if not twii_rt.empty:
                last_twii = float(twii_rt['Close'].iloc[-1])
                new_row = pd.DataFrame([{'date': d_curr_str, 'close': last_twii}])
                twii_df = pd.concat([twii_df, new_row], ignore_index=True)
        except: pass
        twii_df['MA5'] = twii_df['close'].rolling(5).mean()
        slope = twii_df['MA5'].iloc[-1] - twii_df['MA5'].iloc[-2]
    except: pass
    
    return {
        "d_curr": d_curr_str,
        "br_curr": res_df['d_curr_ok'].mean() if not res_df.empty else 0,
        "br_prev": res_df['d_prev_ok'].mean() if not res_df.empty else 0,
        "hit_curr": res_df['d_curr_ok'].sum() if not res_df.empty else 0,
        "valid": len(res_df),
        "slope": slope,
        "detail_df": detail_df,
        "mode_msg": mode_msg,
        "last_time": last_time
    }

# ==========================================
# UI
# ==========================================
def run_streamlit():
    st.title("📈 盤中權證進場判斷 (v2.2 FinMind排行)")

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
        data = fetch_data(api)
            
        if data is None:
            st.warning("⚠️ 暫無有效數據")
        else:
            cond1 = (data['br_curr'] >= BREADTH_THRESHOLD) and (data['br_prev'] >= BREADTH_THRESHOLD)
            cond2 = data['slope'] > 0
            final_decision = cond1 and cond2
            time_str = data['last_time'].strftime("%H:%M:%S") if data['last_time'] else "未知"

            st.subheader(f"📅 基準日：{data['d_curr']}")
            st.success(f"📌 {data['mode_msg']}")
            
            c1, c2, c3 = st.columns(3)
            c1.metric("今日廣度 (D)", f"{data['br_curr']:.1%}", f"{data['hit_curr']}/{data['valid']}")
            c2.metric("昨日廣度 (D-1)", f"{data['br_prev']:.1%}", f"{data['hit_prev']}/{data['valid']}")
            c3.metric("大盤 MA5 斜率", f"{data['slope']:.2f}", "正 ✓" if cond2 else "非正 ✗")

            st.divider()
            if final_decision:
                st.success(f"✅ 結論：可進場")
            else:
                st.error(f"⛔ 結論：不可進場")
                
            st.caption(f"即時報價時間: {time_str} | 排行來源篩選：4碼個股 (排除 00/91 開頭)")
            st.dataframe(data['detail_df'], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.code(traceback.format_exc())

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        input("按 Enter 結束...")
