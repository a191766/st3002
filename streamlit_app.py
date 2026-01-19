# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone
import traceback
import sys

# ==========================================
# 版本資訊
# ==========================================
APP_VERSION = "v1.5.0"
UPDATE_LOG = """
- v1.3.5: 新增前 300 名詳細檢查清單。
- v1.4.0: 盤中日期判斷邏輯優化。
- v1.5.0: 強制即時模式。使用 Tick 數據強制確認今日開盤，若無全市場快照，自動以「昨日排行 + 今日即時價」進行運算。
"""

# ==========================================
# 參數與 Token 設定
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"
TOP_N = 300              
RANK_DISPLAY_N = 300     
BREADTH_THRESHOLD = 0.65
EXCLUDE_ETF_PREFIX = "00"

st.set_page_config(page_title="盤中權證進場判斷", layout="wide")

# ==========================================
# 功能函式
# ==========================================

def get_trading_days(api):
    """ 
    取得最近交易日 (Tick 強制確認版)
    """
    # 1. 取得歷史日線
    df = api.taiwan_stock_daily(
        stock_id="0050", 
        start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d")
    )
    dates = sorted(df['date'].unique().tolist())
    
    # 2. 判斷「今天」是否有開盤 (使用 Tick 數據，最準確)
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    today_str = tw_now.strftime("%Y-%m-%d")
    
    if dates and today_str > dates[-1]:
        try:
            # 嘗試抓取 2330 (台積電) 今天的第一筆 Tick
            # 只要有一筆成交，就代表今天有開盤
            ticks = api.taiwan_stock_tick(stock_id="2330", date=today_str)
            if not ticks.empty:
                dates.append(today_str)
                # print(f"偵測到今日 ({today_str}) Tick 資料，強制設為基準日。")
        except Exception:
            pass 

    return dates

def smart_get_column(df, target_type):
    """ 智慧欄位對應 """
    mappings = {
        'High': ['High', 'high', 'max', 'Max'],
        'Low': ['Low', 'low', 'min', 'Min'],
        'Close': ['Close', 'close', 'price', 'Price', 'deal_price'], # 加入 deal_price (Tick用)
        'Volume': ['Volume', 'volume', 'Trading_Volume', 'vol'],
        'Id': ['stock_id', 'stock_code', 'code', 'SecurityCode']
    }
    candidates = mappings.get(target_type, [])
    for c in candidates:
        if c in df.columns:
            return df[c]
    raise KeyError(f"找不到 {target_type} 對應的欄位。DataFrame cols: {df.columns.tolist()}")

def get_realtime_price(api, code, date_str):
    """
    取得單一個股的即時價格 (優先用 snapshot，失敗用 tick)
    回傳: (price, high, low, volume) 或 None
    """
    # 1. 嘗試 Snapshot
    try:
        if hasattr(api, 'taiwan_stock_daily_short'):
            df = api.taiwan_stock_daily_short(stock_id=code, start_date=date_str)
            if not df.empty:
                row = df.iloc[0]
                c = row.get('close') or row.get('Price') or row.get('Close')
                h = row.get('high') or row.get('High')
                l = row.get('low') or row.get('Low')
                v = row.get('volume') or row.get('Trading_Volume')
                return float(c), float(h), float(l), float(v)
    except:
        pass

    # 2. 嘗試 Tick (最後手段)
    try:
        df = api.taiwan_stock_tick(stock_id=code, date=date_str)
        if not df.empty:
            # 取最後一筆
            last = df.iloc[-1]
            c = last['deal_price']
            # Tick 比較難算 High/Low/Vol，暫時用最後成交價代替 H/L，Vol 難以累計
            return float(c), float(c), float(c), 0 
    except:
        pass
        
    return None

@st.cache_data(ttl=300)
def fetch_data(_api):
    all_days = get_trading_days(_api)
    
    if len(all_days) < 2:
        st.error("歷史資料不足。")
        return None

    d_curr_str = all_days[-1] 
    d_prev_str = all_days[-2]
    
    # === 步驟 1: 決定「候選名單 (Candidates)」 ===
    # 嘗試抓取今日全市場快照來排名
    use_yesterday_rank = False
    try:
        if hasattr(_api, 'taiwan_stock_daily_short'):
            df_all = _api.taiwan_stock_daily_short(stock_id="", start_date=d_curr_str)
        else:
            df_all = pd.DataFrame() # 強制失敗
    except:
        df_all = pd.DataFrame()

    # 如果今日快照是空的 (API 延遲或流量限制)，改用「昨日排名」
    if df_all.empty:
        # st.warning(f"⚠️ 暫無 {d_curr_str} 全市場快照，將使用「{d_prev_str} 排名」配合「即時價格」運算。")
        use_yesterday_rank = True
        # 抓昨日資料來排名
        df_all = _api.taiwan_stock_daily(stock_id="", start_date=d_prev_str)

    # 欄位處理與過濾
    try:
        df_all['MyClose'] = smart_get_column(df_all, 'Close')
        df_all['MyHigh'] = smart_get_column(df_all, 'High')
        df_all['MyLow'] = smart_get_column(df_all, 'Low')
        df_all['MyVol'] = smart_get_column(df_all, 'Volume')
        df_all['MyId'] = smart_get_column(df_all, 'Id')
    except:
        return None

    df_all['MyId'] = df_all['MyId'].astype(str)
    df_all = df_all[df_all['MyId'].str.isdigit()]  
    df_all = df_all[~df_all['MyId'].str.startswith(EXCLUDE_ETF_PREFIX)] 
    df_all = df_all[df_all['MyId'] != "TAIEX"] 

    # 計算成交金額並取前 N 名
    df_all['avg_price'] = (df_all['MyHigh'] + df_all['MyLow'] + df_all['MyClose']) / 3.0
    df_all['turnover_val'] = (df_all['avg_price'] * df_all['MyVol']) / 1_000_000.0
    
    df_candidates = df_all.sort_values('turnover_val', ascending=False).head(TOP_N).copy()
    
    # === 步驟 2: 逐檔計算即時狀態 ===
    results = []
    detailed_status = []
    
    progress_bar = st.progress(0, text=f"正在分析 {d_curr_str} 數據 (模式: {'昨日排行+即時價' if use_yesterday_rank else '今日快照'})...")
    total_candidates = len(df_candidates)

    for i, (idx, row) in enumerate(df_candidates.iterrows()):
        code = row['MyId']
        rank = i + 1
        note = ""
        status = "未知"
        
        try:
            # A. 抓歷史資料 (為了算 MA5)
            stock_df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=(datetime.now() - timedelta(days=25)).strftime("%Y-%m-%d")
            )
            
            # B. 補上今日即時資料
            # 如果是「使用昨日排名」模式，或者 stock_df 裡還沒有今天的資料
            last_hist_date = ""
            if not stock_df.empty:
                last_hist_date = pd.to_datetime(stock_df['date'].iloc[-1]).strftime("%Y-%m-%d")
            
            current_close = row['MyClose'] # 預設用列表裡的
            
            if last_hist_date < d_curr_str:
                # 嘗試取得最新價格
                rt_data = get_realtime_price(_api, code, d_curr_str)
                
                if rt_data:
                    c, h, l, v = rt_data
                    current_close = c
                    # 拼湊一筆今日資料
                    new_row = pd.DataFrame([{
                        'date': d_curr_str,
                        'close': c,
                        'open': c, 'high': h, 'low': l, 'Trading_Volume': v
                    }])
                    stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                elif not use_yesterday_rank:
                     # 如果是用今日快照排名的，那 row['MyClose'] 本身就是即時的，直接用
                     new_row = pd.DataFrame([{
                        'date': d_curr_str,
                        'close': row['MyClose'],
                        'open': row['MyClose'], 'high': row['MyHigh'], 'low': row['MyLow'], 'Trading_Volume': row['MyVol']
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
                
                # 更新顯示用的收盤價 (讓列表顯示最新價)
                row['MyClose'] = current_close 
            else:
                status = "❌ 剔除"
                note = "資料不足"
                
        except Exception as e:
            status = "❌ 剔除"
            note = f"Err: {str(e)}"
        
        detailed_status.append({
            "排名": rank,
            "代號": code,
            "收盤": row['MyClose'],
            "成交額(百萬)": round(row['turnover_val'], 2),
            "狀態": status,
            "備註": note
        })

        if i % 10 == 0:
            progress_bar.progress((i + 1) / total_candidates, text=f"進度: {rank}/{total_candidates}")
    
    progress_bar.empty()
    
    res_df = pd.DataFrame(results)
    detail_df = pd.DataFrame(detailed_status)
    
    # === 步驟 3: 大盤斜率 ===
    try:
        twii_df = _api.taiwan_stock_daily(
            stock_id="TAIEX", 
            start_date=(datetime.now() - timedelta(days=25)).strftime("%Y-%m-%d")
        )
        # 補即時大盤
        last_hist = pd.to_datetime(twii_df['date'].iloc[-1]).strftime("%Y-%m-%d")
        if last_hist < d_curr_str:
            rt_twii = get_realtime_price(_api, "TAIEX", d_curr_str)
            if rt_twii:
                new_twii = pd.DataFrame([{'date': d_curr_str, 'close': rt_twii[0]}])
                twii_df = pd.concat([twii_df, new_twii], ignore_index=True)

        twii_df['MA5'] = twii_df['close'].rolling(5).mean()
        slope = twii_df['MA5'].iloc[-1] - twii_df['MA5'].iloc[-2]
    except:
        slope = 0
    
    return {
        "d_curr": d_curr_str,
        "d_prev": d_prev_str,
        "br_curr": res_df['d_curr_ok'].mean() if not res_df.empty else 0,
        "br_prev": res_df['d_prev_ok'].mean() if not res_df.empty else 0,
        "hit_curr": res_df['d_curr_ok'].sum() if not res_df.empty else 0,
        "hit_prev": res_df['d_prev_ok'].sum() if not res_df.empty else 0,
        "valid": len(res_df),
        "ma5_t": 0, # 簡化顯示
        "ma5_t_1": 0,
        "slope": slope,
        "detail_df": detail_df,
        "mode": "昨日排行+即時價" if use_yesterday_rank else "今日即時排行"
    }

# ==========================================
# UI
# ==========================================

def run_streamlit():
    st.title("📈 盤中權證進場判斷監控")

    with st.sidebar:
        st.subheader("系統狀態")
        st.success("API Token 已載入")
        st.divider()
        st.subheader("版本資訊")
        st.code(f"Version: {APP_VERSION}")
        st.markdown(UPDATE_LOG)

    api = DataLoader()
    api.login_by_token(API_TOKEN)

    if st.button("🔄 立即重新整理數據"):
        st.cache_data.clear()

    try:
        with st.spinner("正在強制抓取即時 Tick 數據..."):
            data = fetch_data(api)
            
        if data is None:
            st.stop()

        cond1 = (data['br_curr'] >= BREADTH_THRESHOLD) and (data['br_prev'] >= BREADTH_THRESHOLD)
        cond2 = data['slope'] > 0
        final_decision = cond1 and cond2

        # 顯示模式提示
        st.subheader(f"📅 數據基準日：{data['d_curr']}")
        if "昨日排行" in data['mode']:
            st.caption(f"⚠️ 注意：因盤中全市場快照延遲，目前採用「{data['mode']}」模式進行運算，數據仍具高參考價值。")
        else:
            st.caption("✅ 數據來源：全市場即時快照")

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
        
        st.subheader(f"📋 前 {TOP_N} 大成交值個股檢查清單")
        
        st.dataframe(
            data['detail_df'], 
            column_config={
                "排名": st.column_config.NumberColumn(format="%d"),
                "成交額(百萬)": st.column_config.NumberColumn(format="$%.2f"),
                "收盤": st.column_config.NumberColumn(format="%.2f"),
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
        # 本地執行邏輯
        input("\n按 ENTER 結束程式...")
