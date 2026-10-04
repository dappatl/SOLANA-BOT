#!/usr/bin/env python3
"""
Solana Memecoin Bot - Web Version (Streamlit)
Paper mode by default. Live trading is extremely risky.
"""

import asyncio
import base64
import json
import logging
import os
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import httpx
import streamlit as st
import websockets
from dotenv import load_dotenv
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed
from solana.rpc.types import TxOpts
import base58

load_dotenv()

# ====================== CONFIG ======================
BUY_AMOUNT_SOL = float(os.getenv("BUY_AMOUNT_SOL", "0.01"))
MAX_OPEN_POSITIONS = 3
TAKE_PROFIT_PCT = 0.70
STOP_LOSS_PCT = 0.25
MIN_INITIAL_BUY_SOL = 0.15
MAX_MARKET_CAP_SOL = 70.0
PRICE_CHECK_INTERVAL = 18
SLIPPAGE_BPS = int(os.getenv("SLIPPAGE_BPS", "600"))
PRIORITY_FEE_LAMPORTS = int(os.getenv("PRIORITY_FEE_LAMPORTS", "200000"))

WS_URL = "wss://pumpdev.io/ws"
JUPITER_QUOTE = "https://lite-api.jup.ag/swap/v1/quote"
JUPITER_SWAP = "https://lite-api.jup.ag/swap/v1/swap"
SOL_MINT = "So11111111111111111111111111111111111111112"
RPC_URL = os.getenv("RPC_URL", "https://api.mainnet-beta.solana.com")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")

# ====================== SESSION STATE ======================
if "log_queue" not in st.session_state:
    st.session_state.log_queue = queue.Queue(maxsize=2000)
if "portfolio" not in st.session_state:
    st.session_state.portfolio = {"cash_sol": 1.0, "positions": {}}
if "bot_running" not in st.session_state:
    st.session_state.bot_running = False
if "paper_mode" not in st.session_state:
    st.session_state.paper_mode = True
if "keypair" not in st.session_state:
    st.session_state.keypair = None
if "rpc" not in st.session_state:
    st.session_state.rpc = None
if "logs" not in st.session_state:
    st.session_state.logs = []

log_queue = st.session_state.log_queue
portfolio = st.session_state.portfolio

# ====================== LOGGING ======================
class SafeQueueHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            try:
                log_queue.put_nowait(msg)
            except queue.Full:
                try:
                    log_queue.get_nowait()
                    log_queue.put_nowait(msg)
                except Exception:
                    pass
        except Exception:
            self.handleError(record)

logger = logging.getLogger("web_bot")
logger.setLevel(logging.INFO)
logger.handlers.clear()
formatter = logging.Formatter("%(asctime)s | %(message)s", "%H:%M:%S")
handler = SafeQueueHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

# ====================== LIVE TRADING ======================
async def jupiter_swap(input_mint: str, output_mint: str, amount: int) -> Optional[str]:
    if st.session_state.paper_mode or st.session_state.keypair is None:
        return None
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            params = {
                "inputMint": input_mint,
                "outputMint": output_mint,
                "amount": amount,
                "slippageBps": SLIPPAGE_BPS,
            }
            r = await client.get(JUPITER_QUOTE, params=params)
            if r.status_code != 200:
                logger.error(f"Quote error: {r.text[:100]}")
                return None
            quote = r.json()

            body = {
                "userPublicKey": str(st.session_state.keypair.pubkey()),
                "quoteResponse": quote,
                "wrapAndUnwrapSol": True,
                "dynamicComputeUnitLimit": True,
                "prioritizationFeeLamports": PRIORITY_FEE_LAMPORTS,
            }
            r2 = await client.post(JUPITER_SWAP, json=body)
            if r2.status_code != 200:
                logger.error(f"Swap build error: {r2.text[:100]}")
                return None
            data = r2.json()
            tx_b64 = data.get("swapTransaction")
            if not tx_b64:
                return None

            tx_bytes = base64.b64decode(tx_b64)
            tx = VersionedTransaction.from_bytes(tx_bytes)
            tx.sign([st.session_state.keypair])

            opts = TxOpts(skip_preflight=False, preflight_commitment=Confirmed)
            result = await st.session_state.rpc.send_raw_transaction(bytes(tx), opts=opts)
            sig = str(result.value)
            logger.info(f"TX → https://solscan.io/tx/{sig}")
            return sig
    except Exception as e:
        logger.error(f"Swap failed: {e}")
        return None

async def live_buy(mint: str, sol_amount: float) -> bool:
    lamports = int(sol_amount * 1_000_000_000)
    return await jupiter_swap(SOL_MINT, mint, lamports) is not None

# ====================== CORE LOGIC ======================
def passes_filters(event: dict) -> bool:
    if event.get("txType") != "create":
        return False
    mint = event.get("mint")
    if not mint:
        return False
    initial = float(event.get("solAmount") or 0)
    mcap = float(event.get("marketCapSol") or 0)
    return initial >= MIN_INITIAL_BUY_SOL and mcap <= MAX_MARKET_CAP_SOL

async def get_price_usd(session, mint: str) -> Optional[float]:
    try:
        async with session.get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}", timeout=7) as r:
            if r.status != 200:
                return None
            data = await r.json()
            pairs = data.get("pairs") or []
            if pairs:
                return float(pairs[0].get("priceUsd") or 0)
    except Exception:
        pass
    return None

async def do_buy(event: dict):
    mint = event["mint"]
    symbol = event.get("symbol", "???")
    name = (event.get("name") or "")[:28]
    mcap = float(event.get("marketCapSol") or 0)

    if len(portfolio["positions"]) >= MAX_OPEN_POSITIONS or mint in portfolio["positions"]:
        return

    if st.session_state.paper_mode:
        if portfolio["cash_sol"] < BUY_AMOUNT_SOL:
            return
        portfolio["cash_sol"] -= BUY_AMOUNT_SOL
        portfolio["positions"][mint] = {
            "symbol": symbol,
            "name": name,
            "cost_sol": BUY_AMOUNT_SOL,
            "entry_time": datetime.now(timezone.utc).strftime("%H:%M:%S"),
            "entry_mcap": mcap,
            "entry_price_usd": None,
        }
        logger.info(f"PAPER BUY  {symbol} | {BUY_AMOUNT_SOL} SOL | mcap {mcap:.1f}")
    else:
        success = await live_buy(mint, BUY_AMOUNT_SOL)
        if success:
            portfolio["positions"][mint] = {
                "symbol": symbol,
                "name": name,
                "cost_sol": BUY_AMOUNT_SOL,
                "entry_time": datetime.now(timezone.utc).strftime("%H:%M:%S"),
                "entry_mcap": mcap,
                "entry_price_usd": None,
            }
            logger.info(f"LIVE BUY   {symbol} | {BUY_AMOUNT_SOL} SOL")

async def do_sell(mint: str, reason: str):
    pos = portfolio["positions"].pop(mint, None)
    if not pos:
        return
    if st.session_state.paper_mode:
        portfolio["cash_sol"] += pos["cost_sol"]
        logger.info(f"PAPER SELL {pos['symbol']} | {reason}")
    else:
        logger.info(f"LIVE SELL attempted {pos['symbol']} | {reason}")

async def check_exits(session):
    for mint, pos in list(portfolio["positions"].items()):
        price = await get_price_usd(session, mint)
        if not price or price <= 0:
            continue
        if pos["entry_price_usd"] is None:
            pos["entry_price_usd"] = price
            continue
        pnl = (price - pos["entry_price_usd"]) / pos["entry_price_usd"]
        if pnl >= TAKE_PROFIT_PCT:
            await do_sell(mint, f"TP +{pnl*100:.0f}%")
        elif pnl <= -STOP_LOSS_PCT:
            await do_sell(mint, f"SL {pnl*100:.0f}%")

async def bot_loop():
    logger.info("Bot engine started")
    async with aiohttp.ClientSession() as session:
        while st.session_state.bot_running:
            try:
                async with websockets.connect(WS_URL, ping_interval=20) as ws:
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    logger.info("Connected to Pump.fun feed")
                    last = 0.0
                    async for raw in ws:
                        if not st.session_state.bot_running:
                            break
                        try:
                            event = json.loads(raw)
                        except Exception:
                            continue
                        if passes_filters(event):
                            await do_buy(event)
                        now = time.time()
                        if now - last > PRICE_CHECK_INTERVAL:
                            await check_exits(session)
                            last = now
            except Exception as e:
                if st.session_state.bot_running:
                    logger.warning(f"Reconnect in 7s: {e}")
                    await asyncio.sleep(7)
    logger.info("Bot engine stopped")

def start_bot():
    if st.session_state.bot_running:
        return
    st.session_state.bot_running = True
    threading.Thread(target=lambda: asyncio.run(bot_loop()), daemon=True).start()

def stop_bot():
    st.session_state.bot_running = False

# ====================== WEBSITE UI ======================
st.set_page_config(page_title="Memecoin Bot", page_icon="🚀", layout="wide")

st.title("🚀 Solana Memecoin Bot")
st.caption("Educational paper + live trading dashboard • Use at your own risk")

# Sidebar
with st.sidebar:
    st.header("Controls")

    mode = st.radio("Mode", ["Paper", "Live (Dangerous)"], index=0)

    if mode.startswith("Live"):
        if not PRIVATE_KEY:
            st.error("No PRIVATE_KEY found in secrets/.env")
            st.session_state.paper_mode = True
        else:
            try:
                if st.session_state.keypair is None:
                    st.session_state.keypair = Keypair.from_bytes(base58.b58decode(PRIVATE_KEY))
                    st.session_state.rpc = AsyncClient(RPC_URL)
                st.session_state.paper_mode = False
                st.warning(f"Live wallet: `{str(st.session_state.keypair.pubkey())[:8]}...`")
            except Exception as e:
                st.error(f"Invalid private key: {e}")
                st.session_state.paper_mode = True
    else:
        st.session_state.paper_mode = True

    col1, col2 = st.columns(2)
    with col1:
        if st.button("▶ Start", use_container_width=True, type="primary"):
            start_bot()
            st.success("Bot started")
    with col2:
        if st.button("⏹ Stop", use_container_width=True):
            stop_bot()
            st.info("Bot stopped")

    st.divider()
    st.subheader("Settings")
    buy_amount = st.number_input("Buy Amount (SOL)", min_value=0.001, value=BUY_AMOUNT_SOL, step=0.005)
    tp = st.number_input("Take Profit %", min_value=5, value=int(TAKE_PROFIT_PCT * 100), step=5)
    sl = st.number_input("Stop Loss %", min_value=5, value=int(STOP_LOSS_PCT * 100), step=5)

    if st.button("Apply Settings"):
        BUY_AMOUNT_SOL = buy_amount
        TAKE_PROFIT_PCT = tp / 100
        STOP_LOSS_PCT = sl / 100
        st.success("Settings applied")

    st.divider()
    st.metric("Paper Cash", f"{portfolio['cash_sol']:.4f} SOL")
    st.metric("Open Positions", len(portfolio["positions"]))

# Main content
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("Activity Log")
    # Collect new logs
    while not log_queue.empty():
        try:
            st.session_state.logs.append(log_queue.get_nowait())
        except Exception:
            break
    # Keep only last 50 lines
    st.session_state.logs = st.session_state.logs[-50:]
    if st.session_state.logs:
        st.code("\n".join(st.session_state.logs), language=None)
    else:
        st.info("Waiting for activity...")

with col_right:
    st.subheader("Open Positions")
    if not portfolio["positions"]:
        st.info("No open positions")
    else:
        for mint, p in portfolio["positions"].items():
            st.markdown(f"**{p['symbol']}** — {p['cost_sol']:.3f} SOL")
            st.caption(f"Entry {p['entry_time']} • mcap ≈ {p['entry_mcap']:.1f}")

# Auto refresh
time.sleep(2)
st.rerun()
