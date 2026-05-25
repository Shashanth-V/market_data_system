import asyncio
import json
import os
import sys
from datetime import datetime
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import httpx
import websockets

# Configure Streamlit page layout and theme
st.set_page_config(
    page_title="Real-Time Market Data analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom premium styling
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap');
    
    /* Main Layout */
    * {
        font-family: 'Outfit', sans-serif;
    }
    
    /* Background and Headers */
    .stApp {
        background: linear-gradient(135deg, #0e1117 0%, #161a24 100%);
        color: #f0f2f6;
    }
    
    h1, h2, h3 {
        color: #ffffff !important;
        font-weight: 800 !important;
        letter-spacing: -0.5px;
    }

    /* Glassmorphic Metrics Card */
    .metric-card {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px;
        padding: 24px;
        text-align: center;
        backdrop-filter: blur(10px);
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-4px);
        border-color: rgba(0, 229, 255, 0.4);
    }
    .metric-label {
        font-size: 14px;
        color: #8c9bb4;
        text-transform: uppercase;
        font-weight: 600;
        margin-bottom: 8px;
        letter-spacing: 1px;
    }
    .metric-value {
        font-size: 32px;
        font-weight: 800;
        color: #ffffff;
    }
    .metric-sub {
        font-size: 12px;
        color: #00e5ff;
        margin-top: 6px;
        font-weight: 500;
    }
    
    /* Custom Badges */
    .badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 11px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .badge-bullish {
        background: rgba(0, 230, 118, 0.15);
        color: #00e676;
        border: 1px solid rgba(0, 230, 118, 0.3);
    }
    .badge-bearish {
        background: rgba(255, 23, 68, 0.15);
        color: #ff1744;
        border: 1px solid rgba(255, 23, 68, 0.3);
    }
    .badge-neutral {
        background: rgba(144, 164, 174, 0.15);
        color: #90a4ae;
        border: 1px solid rgba(144, 164, 174, 0.3);
    }
</style>
""", unsafe_allow_html=True)

# Port configurability for backend Docker environments
BACKEND_HOST = os.getenv("BACKEND_HOST", "localhost")
BACKEND_PORT = os.getenv("BACKEND_PORT", "8000")
BACKEND_URL = f"http://{BACKEND_HOST}:{BACKEND_PORT}"
WS_URL = f"ws://{BACKEND_HOST}:{BACKEND_PORT}/api/v1/ws"

# Initialize Session States
if "selected_symbol" not in st.session_state:
    st.session_state.selected_symbol = "BTC-USD"
if "tickers_data" not in st.session_state:
    st.session_state.tickers_data = {} # Latest tick and metrics per symbol
if "ticks_history" not in st.session_state:
    st.session_state.ticks_history = [] # Raw tick stream

def fetch_tracked_symbols():
    """Fetches valid symbols from backend, falling back if offline."""
    try:
        response = httpx.get(f"{BACKEND_URL}/api/v1/market/symbols", timeout=3.0)
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "DOGE-USD"]

def fetch_historical_candles(symbol: str):
    """Fetches historical 1-minute candles from Backend API."""
    try:
        response = httpx.get(f"{BACKEND_URL}/api/v1/market/candles?symbol={symbol}&limit=60", timeout=3.0)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.sidebar.warning(f"Connecting to database: {e}")
    return []

# Sidebar panel
st.sidebar.markdown(f"<h2 style='text-align: center; margin-bottom: 24px;'>📈 Market Terminal</h2>", unsafe_allow_html=True)

symbols = fetch_tracked_symbols()
selected_symbol = st.sidebar.selectbox(
    "Select Trading Pair",
    options=symbols,
    index=symbols.index(st.session_state.selected_symbol) if st.session_state.selected_symbol in symbols else 0
)
st.session_state.selected_symbol = selected_symbol

st.sidebar.markdown("---")
st.sidebar.markdown("### 🛠️ Configuration")
chart_type = st.sidebar.radio("Chart Type", options=["Candlestick", "Line Chart (Close)"])
enable_metrics_smoothing = st.sidebar.checkbox("Smoothing (SMA overlays)", value=True)

st.sidebar.markdown("---")
st.sidebar.markdown(
    """
    <div style='background: rgba(255, 255, 255, 0.03); border: 1px solid rgba(255, 255, 255, 0.08); border-radius: 12px; padding: 16px; font-size: 13px;'>
        <p style='margin-bottom: 4px; font-weight: 600; color: #ffffff;'>⚡ System Diagnostics</p>
        <p style='margin-bottom: 2px; color: #8c9bb4;'>Status: <span style="color: #00e676; font-weight: bold;">CONNECTED</span></p>
        <p style='margin-bottom: 2px; color: #8c9bb4;'>Pipeline: Asynchronous</p>
        <p style='margin-bottom: 0px; color: #8c9bb4;'>Visual Updates: Live WebSockets</p>
    </div>
    """,
    unsafe_allow_html=True
)

# Dashboard main title banner
st.markdown(
    f"""
    <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 30px;">
        <div>
            <h1 style="margin: 0; font-size: 38px;">{st.session_state.selected_symbol} Live Terminal</h1>
            <p style="margin: 4px 0 0 0; color: #8c9bb4; font-size: 16px;">Real-time quantitative indicators and volatility calculations</p>
        </div>
        <div style="text-align: right;">
            <p style="margin: 0; font-size: 12px; color: #8c9bb4; text-transform: uppercase; font-weight: 600; letter-spacing: 1px;">Local Machine Time</p>
            <p style="margin: 4px 0 0 0; font-size: 20px; font-weight: 600; color: #ffffff;">{datetime.now().strftime('%H:%M:%S')}</p>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# Layout: 4 Analytics Cards
metric_cols = st.columns(4)

card_placeholders = [metric_cols[i].empty() for i in range(4)]

# Large Middle layout: Chart and Live Ticker list
chart_col, ticker_col = st.columns([3, 1])

chart_placeholder = chart_col.empty()
ticker_placeholder = ticker_col.empty()

async def websocket_listener():
    """
    Subscribes to the FastAPI WebSockets, processes stream events,
    and refreshes the Streamlit page objects.
    """
    connection_attempts = 0
    max_connection_attempts = 10
    
    while connection_attempts < max_connection_attempts:
        try:
            async with websockets.connect(WS_URL) as ws:
                connection_attempts = 0 # Reset
                
                # Fetch initial database historical context
                candles = fetch_historical_candles(st.session_state.selected_symbol)
                
                while True:
                    # Await message from WebSocket
                    msg_str = await ws.recv()
                    payload = json.loads(msg_str)
                    
                    tick = payload.get("tick", {})
                    metrics = payload.get("metrics", {})
                    sym = tick.get("symbol")
                    
                    if not sym:
                        continue
                        
                    # Save to state
                    st.session_state.tickers_data[sym] = {
                        "tick": tick,
                        "metrics": metrics
                    }
                    
                    # If this is our active selected symbol, update logs
                    if sym == st.session_state.selected_symbol:
                        st.session_state.ticks_history.insert(0, tick)
                        if len(st.session_state.ticks_history) > 30:
                            st.session_state.ticks_history.pop()

                    # Render top analytics cards
                    active_sym_data = st.session_state.tickers_data.get(st.session_state.selected_symbol, {})
                    
                    if active_sym_data:
                        active_tick = active_sym_data["tick"]
                        active_metrics = active_sym_data["metrics"]
                        
                        price = active_tick.get("price", 0.0)
                        volume = active_tick.get("volume", 0.0)
                        sma_5 = active_metrics.get("moving_average_5m") or price
                        sma_15 = active_metrics.get("moving_average_15m") or price
                        volatility = active_metrics.get("volatility_5m") or 0.0
                        trend = active_metrics.get("trend_signal", "NEUTRAL")
                        
                        # High / Low estimation from candles
                        if candles:
                            high_price = max(c["high"] for c in candles)
                            low_price = min(c["low"] for c in candles)
                        else:
                            high_price = price
                            low_price = price

                        high_price = max(high_price, price)
                        low_price = min(low_price, price)
                        
                        trend_badge = f'<span class="badge badge-neutral">Neutral</span>'
                        if trend == "BULLISH":
                            trend_badge = f'<span class="badge badge-bullish">Bullish</span>'
                        elif trend == "BEARISH":
                            trend_badge = f'<span class="badge badge-bearish">Bearish</span>'

                        # Card 1: Latest Price
                        card_placeholders[0].markdown(
                            f"""
                            <div class="metric-card">
                                <div class="metric-label">Latest Price</div>
                                <div class="metric-value" style="color: #00e5ff;">${price:,.4f}</div>
                                <div class="metric-sub">Vol: {volume:.4f}</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

                        # Card 2: 24h Range (Simulated)
                        card_placeholders[1].markdown(
                            f"""
                            <div class="metric-card">
                                <div class="metric-label">24h Range</div>
                                <div class="metric-value">${high_price:,.2f}</div>
                                <div class="metric-sub" style="color: #ff1744;">Low: ${low_price:,.2f}</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

                        # Card 3: Volatility
                        card_placeholders[2].markdown(
                            f"""
                            <div class="metric-card">
                                <div class="metric-label">Rolling Volatility</div>
                                <div class="metric-value" style="color: #ffd700;">{volatility:.6f}</div>
                                <div class="metric-sub">5-Tick window</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

                        # Card 4: Trend Signal
                        card_placeholders[3].markdown(
                            f"""
                            <div class="metric-card">
                                <div class="metric-label">Trend Signal</div>
                                <div class="metric-value">{trend_badge}</div>
                                <div class="metric-sub">SMA(5) vs SMA(15)</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

                    # Update candle context on every iteration or let it append
                    # Fetching candles occasionally prevents memory drift
                    if len(candles) < 60 or datetime.now().second % 15 == 0:
                        candles = fetch_historical_candles(st.session_state.selected_symbol)

                    # Draw Chart
                    if candles:
                        df_candles = pd.DataFrame(candles)
                        df_candles["start_time"] = pd.to_datetime(df_candles["start_time"])
                        
                        fig = go.Figure()
                        
                        if chart_type == "Candlestick":
                            fig.add_trace(go.Candlestick(
                                x=df_candles["start_time"],
                                open=df_candles["open"],
                                high=df_candles["high"],
                                low=df_candles["low"],
                                close=df_candles["close"],
                                name="Market OHLC"
                            ))
                        else:
                            fig.add_trace(go.Scatter(
                                x=df_candles["start_time"],
                                y=df_candles["close"],
                                mode="lines+markers",
                                name="Close Price",
                                line=dict(color="#00e5ff", width=3)
                            ))
                            
                        # Smooth moving averages overlays
                        if enable_metrics_smoothing and len(df_candles) >= 5:
                            # Apply simple moving average over closed candles
                            df_candles["SMA_5"] = df_candles["close"].rolling(window=5).mean()
                            df_candles["SMA_15"] = df_candles["close"].rolling(window=15).mean()
                            
                            fig.add_trace(go.Scatter(
                                x=df_candles["start_time"],
                                y=df_candles["SMA_5"],
                                mode="lines",
                                name="SMA 5-Min",
                                line=dict(color="#ffd700", width=1.5, dash="dash")
                            ))
                            fig.add_trace(go.Scatter(
                                x=df_candles["start_time"],
                                y=df_candles["SMA_15"],
                                mode="lines",
                                name="SMA 15-Min",
                                line=dict(color="#e040fb", width=1.5, dash="dot")
                            ))

                        fig.update_layout(
                            template="plotly_dark",
                            margin=dict(t=0, b=0, l=0, r=0),
                            height=480,
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(0,0,0,0)",
                            xaxis_rangeslider_visible=False,
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                        )
                        # Increment chart render counter to ensure absolute unique element key for Streamlit
                        if "chart_counter" not in st.session_state:
                            st.session_state.chart_counter = 0
                        st.session_state.chart_counter += 1
                        
                        chart_placeholder.plotly_chart(
                            fig, 
                            use_container_width=True, 
                            key=f"plotly_chart_{st.session_state.selected_symbol}_{st.session_state.chart_counter}"
                        )
                    else:
                        chart_placeholder.info("Awaiting market ticks to aggregate 1-minute historical candles...")

                    # Draw Live Ticker Stream logs
                    log_html = """
                    <div style="background: rgba(255,255,255,0.02); border: 1px solid rgba(255,255,255,0.08); border-radius: 16px; padding: 20px; height: 440px; overflow-y: auto;">
                        <h4 style="margin: 0 0 16px 0; font-weight: 600; color: #ffffff;">⚡ Real-Time Ticker</h4>
                    """
                    
                    history = st.session_state.ticks_history
                    if not history:
                        log_html += "<p style='color: #8c9bb4; font-size: 13px;'>Waiting for incoming transactions...</p>"
                    else:
                        for item in history:
                            ts_formatted = item.get("timestamp", "").split("T")[-1][:8]
                            log_html += f"""
                            <div style="display: flex; justify-content: space-between; border-bottom: 1px solid rgba(255,255,255,0.04); padding: 8px 0; font-size: 13px;">
                                <div>
                                    <span style="color: #8c9bb4; font-family: monospace;">[{ts_formatted}]</span>
                                    <span style="color: #ffffff; font-weight: 600; margin-left: 6px;">${item.get('price'):,.4f}</span>
                                </div>
                                <div style="color: #00e5ff; font-family: monospace;">{item.get('volume'):.3f}</div>
                            </div>
                            """
                    log_html += "</div>"
                    
                    # Dedent lines to prevent Markdown from interpreting leading spaces as a code block
                    clean_log_html = "\n".join([line.strip() for line in log_html.split("\n")])
                    ticker_placeholder.markdown(clean_log_html, unsafe_allow_html=True)
                    
                    # Yield thread to streamlit event loop
                    await asyncio.sleep(0.5)
                    
        except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError) as e:
            connection_attempts += 1
            st.warning(f"Connection to backend failed ({e}). Reconnecting in {connection_attempts * 2}s (Attempt {connection_attempts}/10)...")
            await asyncio.sleep(connection_attempts * 2)
            
    st.error("Lost permanent connection to Backend API. Please verify the FastAPI server is running.")

# Run the WebSocket loop inside Streamlit
try:
    asyncio.run(websocket_listener())
except Exception as e:
    st.error(f"Render loop exception: {e}")
