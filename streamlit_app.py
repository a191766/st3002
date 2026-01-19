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
APP_VERSION = "v1.4.0"
UPDATE_LOG = """
- v1.3.5: 新增前 300 名詳細檢查清單。
- v1.4.0: 修正盤中日期判斷邏輯。新增「即時偵測機制」，確保盤中能正確抓到「今天」作為基準日 (D)，而非上一個收盤日。
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
    取得最近交易日 (含盤中即時判定) 
    修正：先抓歷史日線，再嘗試抓取「今天」的即時報價。若有，則將今天加入列表。
    """
    # 1. 先取得歷史日線 (這部分通常只會更新到昨天或上週五)
    df = api.taiwan_stock_daily(
        stock_id="0050", 
        start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d")
    )
    dates = sorted(df['date'].unique().tolist())
    
    # 2. 判斷「今天」是否有開盤 (解決盤中看不到今日數據的問題)
    # 設定台灣時區 (UTC+8)
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    today_str = tw_now.strftime("%Y-%m-%d")
    
    # 如果歷史資料最新的日期還不是今天，我們就來檢查今天有沒有即時報價
    if dates and today_str > dates[-1]:
        try:
            # 嘗試抓取 0050 今天的即時快照
            # 如果今天有開盤且盤中已經開始，這裡應該會有資料
            if hasattr(api, 'taiwan_stock_daily_short'):
                rt_df = api.taiwan_stock_daily_short(stock_id="0050", start_date=today_str)
            else:
                # 降級相容
                rt_df = api.taiwan_stock_daily(stock_id="0050", start_date=today_str)
            
            if not rt_df.empty:
                # Bingo! 今天有資料，強制把今天加入日期列表
                dates.append(today_str)
                # print(f"偵測到今日 ({today_str}) 即時交易資料，已納入基準日。")
        except Exception:
            pass # 若發生錯誤或抓不到，就維持原狀 (視為今天沒開盤或還沒開始)

    return dates

def smart_get_column(df, target_type):
    """ 智慧欄位對應 """
    mappings = {
        'High': ['High', 'high', 'max', 'Max'],
        'Low': ['Low', 'low', 'min', 'Min'],
        'Close': ['Close', 'close', 'price', 'Price'],
        'Volume': ['Volume', 'volume', 'Trading_Volume', 'vol'],
        'Id': ['stock_id', 'stock_code', 'code', 'SecurityCode']
    }
    candidates = mappings.get(target_type, [])
    for c in candidates:
        if c in df.columns:
            return df[c]
    raise KeyError(f"找不到 {target_type} 對應的欄位。DataFrame cols: {df.columns.tolist()}")

@st.cache_data(ttl=300)
def fetch_data(_api):
    """ 抓取排行與計算廣度 """
    all_days = get_trading_days(_api)
    
    # 防呆：萬一資料庫完全空的 (極低機率)
    if len(all_days) < 2:
        st.error("無法取得足夠的歷史交易日資料，請稍後再試。")
        return None

    d_curr_str = all_days[-1] # 這就會是「今天」(如果有抓到即時資料)
    d_prev_str = all_days[-2]
    
    # 1. 抓取當日(d_curr_str)全市場資料
    try:
        if hasattr(_api, 'taiwan_stock_daily_short'):
            df_all = _api.taiwan_stock_daily_short(stock_id="", start_date=d_curr_str)
        else:
            raise AttributeError("API too old")
    except (AttributeError, Exception):
        df_all = _api.taiwan_stock_daily(stock_id="", start_date=d_curr_str)
    
    if df_all.empty:
        st.warning(f"查無 {d_curr_str} 的全市場資料，可能尚未開盤或資料源延遲。")
        return None

    # 2. 欄位標準化
    try:
        df_all['MyClose'] = smart_get_column(df_all, 'Close')
        df_all['MyHigh'] = smart_get_column(df_all, 'High')
        df_all['MyLow'] = smart_get_column(df_all, 'Low')
        df_all['MyVol'] = smart_get_column(df_all, 'Volume')
        df_all['MyId'] = smart_get_column(df_all, 'Id')
    except KeyError as e:
        st.error(f"資料欄位解析失敗: {e}")
        return None

    # 3. 過濾雜訊
    df_all['MyId'] = df_all['MyId'].astype(str)
    df_all = df_all[df_all['MyId'].str.isdigit()]  
    df_all = df_all[~df_all['MyId'].str.startswith(EXCLUDE_ETF_PREFIX)] 
    df_all = df_all[df_all['MyId'] != "TAIEX"] 

    # 4. 計算成交金額並排序
    df_all['avg_price'] = (df_all['MyHigh'] + df_all['MyLow'] + df_all['MyClose']) / 3.0
    df_all['turnover_val'] = (df_all['avg_price'] * df_all['MyVol']) / 1_000_000.0
    
    df_candidates = df_all.sort_values('turnover_val', ascending=False).head(TOP_N).copy()
    
    results = []
    detailed_status = []
    
    progress_bar = st.progress(0, text=f"正在分析 {d_curr_str} 的前 {TOP_N} 大個股...")
    total_candidates = len(df_candidates)

    # 5. 逐一檢查
    for i, (idx, row) in enumerate(df_candidates.iterrows()):
        code = row['MyId']
        rank = i + 1
        note = ""
        status = "未知"
        
        try:
            # 抓取個股歷史資料 (往前抓 20 天確保均線足夠)
            stock_df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=(datetime.now() - timedelta(days=25)).strftime("%Y-%m-%d")
            )
            
            # 【關鍵】如果 stock_df 最新的日期還停留在昨天 (因為盤中日線還沒出)，
            # 我們需要把「今天的即時資料 (row)」手動補進去，這樣才能算出最新的 MA5
            
            # 檢查 stock_df 最後一筆日期是否小於 d_curr_str
            if not stock_df.empty:
                last_date_in_hist = pd.to_datetime(stock_df['date'].iloc[-1]).strftime("%Y-%m-%d")
                if last_date_in_hist < d_curr_str:
                    # 手動構建今日的 DataFrame row
                    # 注意：這裡要小心欄位名稱對齊，FinMind daily 通常是 date, open, high, low, close, volume...
                    new_row = pd.DataFrame([{
                        'date': d_curr_str,
                        'close': row['MyClose'],
                        'open': row['MyClose'], # 暫用 Close 替代，計算 MA5 沒差
                        'high': row['MyHigh'],
                        'low': row['MyLow'],
                        'Trading_Volume': row['MyVol']
                    }])
                    # 合併
                    stock_df = pd.concat([stock_df, new_row], ignore_index=True)

            if len(stock_df) >= 6:
                stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                curr_row = stock_df.iloc[-1]
                prev_row = stock_df.iloc[-2]
                
                # 再次確認我們比對的是 D 與 D-1
                # 這樣能確保盤中我們是在看「現在」有沒有站上 MA5
                
                results.append({
                    "d_curr_ok": curr_row['close'] > curr_row['MA5'],
                    "d_prev_ok": prev_row['close'] > prev_row['MA5']
                })
                status = "✅ 納入"
            else:
                status = "❌ 剔除"
                note = f"資料不足 (僅 {len(stock_df)} 筆)"
                
        except Exception as e:
            status = "❌ 剔除"
            note = f"運算錯誤: {str(e)}"
        
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
    
    # 大盤 MA5 斜率 (同樣邏輯：若盤中，需補入今日大盤值)
    try:
        twii_df = _api.taiwan_stock_daily(
            stock_id="TAIEX", 
            start_date=(datetime.now() - timedelta(days=25)).strftime("%Y-%m-%d")
        )
        # 嘗試抓大盤即時
        try:
            twii_rt = None
            if hasattr(_api, 'taiwan_stock_daily_short'):
                twii_rt = _api.taiwan_stock_daily_short(stock_id="TAIEX", start_date=d_curr_str)
            else:
                twii_rt = _api.taiwan_stock_daily(stock_id="TAIEX", start_date=d_curr_str)
                
            if not twii_rt.empty:
                # 補入即時大盤資料
                rt_val = twii_rt.iloc[0]
                # 解析即時欄位
                rt_close = rt_val.get('close') or rt_val.get('Price') or rt_val.get('Close')
                
                last_hist = pd.to_datetime(twii_df['date'].iloc[-1]).strftime("%Y-%m-%d")
                if last_hist < d_curr_str and rt_close:
                     new_twii = pd.DataFrame([{'date': d_curr_str, 'close': float(rt_close)}])
                     twii_df = pd.concat([twii_df, new_twii], ignore_index=True)
        except:
            pass # 大盤即時抓不到就用舊的

        twii_df['MA5'] = twii_df['close'].rolling(5).mean()
        ma5_t = twii_df['MA5'].iloc[-1]
        ma5_t_1 = twii_df['MA5'].iloc[-2]
        slope = ma5_t - ma5_t_1
    except:
        slope = 0
        ma5_t = 0
        ma5_t_1 = 0
    
    return {
        "d_curr": d_curr_str,
        "d_prev": d_prev_str,
        "br_curr": res_df['d_curr_ok'].mean() if not res_df.empty else 0,
        "br_prev": res_df['d_prev_ok'].mean() if not res_df.empty else 0,
        "hit_curr": res_df['d_curr_ok'].sum() if not res_df.empty else 0,
        "hit_prev": res_df['d_prev_ok'].sum() if not res_df.empty else 0,
        "valid": len(res_df),
        "ma5_t": ma5_t,
        "ma5_t_1": ma5_t_1,
        "slope": slope,
        "detail_df": detail_df
    }

# ==========================================
# Streamlit UI 介面
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
        with st.spinner("正在分析盤中即時數據 (含即時 K 線合成)..."):
            data = fetch_data(api)
            
        if data is None:
            st.stop()

        cond1 = (data['br_curr'] >= BREADTH_THRESHOLD) and (data['br_prev'] >= BREADTH_THRESHOLD)
        cond2 = data['slope'] > 0
        final_decision = cond1 and cond2

        # 這裡特別標註盤中狀態
        st.subheader(f"📅 數據基準日：{data['d_curr']} (盤中即時)")
        
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
        st.info("💡 點擊欄位標題可排序，輸入「剔除」可查看被排除個股。")
        
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

# ==========================================
# 執行處理
# ==========================================

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        print(f"--- 盤中權證進場判斷監控 {APP_VERSION} ---")
        try:
            api = DataLoader()
            api.login_by_token(API_TOKEN)
            print("API Token 驗證成功。")
        except Exception as e:
            print(f"API 驗證失敗：{e}")
        
        input("\n按 ENTER 結束程式...")
        sys.exit(0)
