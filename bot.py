import logging
import aiohttp
import hmac
import hashlib
import time
import asyncio
import platform
import json
from typing import Optional, Tuple, List, Dict
import ntplib
import numpy as np
# For colored logs
try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
except ImportError:
    class DummyColor:
        GREEN = RED = RESET = ""
    Fore = Style = DummyColor()

# --- CONFIGURATION ---
#papertrade5 API Credentials (bot5)
API_KEY = '3ydb5HyHDxEDSlPCTqEbIhwiJ9CT2y'
API_SECRET = 'vguUyYXD36zbUO688cBM0ncovU1yPa2kGDTxa5F0CDYiEhS1mbGgkYPN2zm0'
BASE_URL = 'https://cdn-ind.testnet.deltaex.org'
SYMBOLS = ['BTCUSD', 'ETHUSD', 'SOLUSD', 'ADAUSD', 'SHIBUSD']
SYMBOL_TO_PRODUCT_ID = {}
LEVERAGE = 100

# --- STRATEGY PARAMETERS ---
TIMEFRAME = '5m'  # Configurable timeframe for strategy (e.g., '1m', '5m', '15m', '1h', '4h')
RSI_PERIOD = 14
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70

RISK_PER_TRADE = 0.3  # Fraction of balance to risk per trade (e.g., 0.3 = 30%)
MAX_TRADES_PER_DAY = 450

# --- RISK/REWARD CONTROL ---
STOP_LOSS_PCT = 0.01  # Default 1% stop-loss
SYMBOL_STOP_LOSS_PCT = {
    'ETHUSD': 0.03,  # 3% stop-loss for ETHUSD due to volatility
    'SHIBUSD': 0.05, # 5% stop-loss for SHIBUSD due to high volatility
    'ADAUSD': 0.02   # 2% stop-loss for ADAUSD
}
RISK_REWARD_RATIO = 3.0  # 1:4 risk-reward ratio
MAX_STOPLOSS_DOLLARS = 35  # Absolute max $ loss per trade

# Calculate TAKE_PROFIT_PCT for each symbol based on stop-loss
TAKE_PROFIT_PCT = {}
for symbol in SYMBOLS:
    sl_pct = SYMBOL_STOP_LOSS_PCT.get(symbol, STOP_LOSS_PCT)
    TAKE_PROFIT_PCT[symbol] = sl_pct * RISK_REWARD_RATIO

# --- TRAILING LOGIC ---
TRAILING_ENABLED = True  # Enable trailing stop
TRAILING_START_PCT = 0.03  # Start trailing when profit >= 3% of entry price
TRAILING_STEP_PCT = 0.015  # Move SL by 1.5% of entry price when trailing

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# --- TIME SYNC ---
async def get_server_timestamp() -> int:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BASE_URL}/v2/time") as response:
                if response.status == 200:
                    data = await response.json()
                    return int(data.get('result', {}).get('server_time', time.time()))
    except Exception as e:
        logger.error(f"Failed to fetch server time: {str(e)}")
    return int(time.time())

def get_ntp_timestamp() -> int:
    ntp_servers = ['time.google.com', 'pool.ntp.org', 'time.windows.com']
    for server in ntp_servers:
        try:
            ntp = ntplib.NTPClient()
            response = ntp.request(server, timeout=1)
            return int(response.tx_time)
        except Exception:
            continue
    return int(time.time())

async def get_timestamp() -> int:
    timestamp = get_ntp_timestamp()
    server_time = await get_server_timestamp()
    if abs(timestamp - server_time) > 5:
        logger.warning(f"NTP time ({timestamp}) differs from server time ({server_time}), using server time")
        timestamp = server_time
    return timestamp

# --- API SIGNATURE ---
def generate_signature(method: str, timestamp: int, path: str, query_params: str = '', body: str = '') -> str:
    prehash = f"{method}{timestamp}{path}{query_params}{body}"
    return hmac.new(API_SECRET.encode(), prehash.encode(), hashlib.sha256).hexdigest()

async def make_api_request(method: str, path: str, **kwargs) -> dict:
    params = kwargs.get('params', {})
    query_string = ''
    if params:
        query_string = '?' + '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
    url = f"{BASE_URL}{path}{query_string}"
    body = json.dumps(kwargs.get('json', {}), separators=(',', ':')) if kwargs.get('json') else ''
    max_retries = 5
    for attempt in range(max_retries):
        try:
            timestamp = await get_timestamp()
            signature = generate_signature(method, timestamp, path, query_string, body)
            headers = {
                'api-key': API_KEY,
                'timestamp': str(timestamp),
                'signature': signature,
                'Content-Type': 'application/json',
                'User-Agent': 'python-3.11'
            }
            async with aiohttp.ClientSession() as session:
                async with session.request(method, url, headers=headers, data=body, timeout=10) as response:
                    status = response.status
                    raw_response = await response.text()
                    try:
                        data = json.loads(raw_response)
                        if not isinstance(data, dict):
                            logger.error(f"Response is not a dictionary for {method} {path}: {raw_response}")
                            data = {'success': False, 'result': None, 'error': f'Response not a dict: {raw_response}'}
                    except json.JSONDecodeError:
                        logger.error(f"Non-JSON response for {method} {path}: Status={status}, Response={raw_response}")
                        data = {'success': False, 'result': None, 'error': f'Non-JSON response: {raw_response}'}
                    if status == 200 and data.get('success', False):
                        return data
                    if status == 404:
                        logger.warning(f"Resource not found for {method} {path}: {raw_response}")
                        return {'success': False, 'result': None, 'error': 'Not Found'}
                    if status == 429:
                        retry_after = response.headers.get('Retry-After', '5')
                        logger.warning(f"Rate limit hit for {method} {path}, retrying after {retry_after} seconds")
                        await asyncio.sleep(float(retry_after))
                        continue
                    if status == 401 and data.get('error', {}).get('code') == 'expired_signature':
                        logger.warning(f"Expired signature for {method} {path}, retrying...")
                        continue
                    if status == 500:
                        delay = 2 ** attempt
                        logger.warning(f"Server error for {method} {path}, retrying after {delay:.2f} seconds")
                        await asyncio.sleep(delay)
                        continue
                    logger.error(f"API request failed for {method} {path}: Status={status}, Response={raw_response}")
                    return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"API request failed for {method} {path}: {str(e)}")
            if attempt < max_retries - 1:
                delay = 2 ** attempt
                await asyncio.sleep(delay)
            else:
                return {'success': False, 'result': None, 'error': f'Max retries exceeded: {str(e)}'}
    return {'success': False, 'result': None, 'error': 'Max retries exceeded'}

# --- EXCHANGE UTILS ---
async def validate_credentials():
    try:
        logger.info("Validating API credentials...")
        data = await make_api_request("GET", "/v2/wallet/balances")
        if data.get('success') and data.get('result'):
            logger.info("API credentials validated successfully.")
            return True
        else:
            logger.error(f"Failed to validate credentials. Response: {data}")
            return False
    except Exception as e:
        logger.error(f"Exception while validating credentials: {str(e)}")
        return False

async def validate_symbols():
    global SYMBOLS, SYMBOL_TO_PRODUCT_ID
    max_retries = 3
    for attempt in range(max_retries):
        try:
            data = await make_api_request("GET", "/v2/tickers")
            if data.get('success') and data.get('result'):
                futures_symbols = []
                SYMBOL_TO_PRODUCT_ID.clear()
                for item in data['result']:
                    contract_type = item.get('contract_type', '').lower()
                    symbol = item.get('symbol', '')
                    product_id = item.get('product_id')
                    if 'futures' in contract_type or 'perpetual' in contract_type or symbol.endswith('USD') or symbol.endswith('USDT'):
                        futures_symbols.append(symbol)
                        if product_id is not None:
                            SYMBOL_TO_PRODUCT_ID[symbol] = product_id
                valid_symbols = [s for s in SYMBOLS if s in futures_symbols]
                if not valid_symbols:
                    logger.error("No valid symbols found.")
                    return False
                SYMBOLS = valid_symbols
                logger.info(f"Updated trading symbols: {SYMBOLS}")
                return True
            if attempt < max_retries - 1:
                await asyncio.sleep(5 * (attempt + 1))
        except Exception:
            if attempt < max_retries - 1:
                await asyncio.sleep(5 * (attempt + 1))
    return False

async def fetch_balance() -> Tuple[Optional[float], Optional[float]]:
    try:
        data = await make_api_request("GET", "/v2/wallet/balances")
        if data.get('success') and data.get('result'):
            for balance in data['result']:
                symbol = balance.get('currency_symbol') or balance.get('asset_symbol')
                if symbol in ['USDT', 'USD']:
                    total_balance = float(balance.get('balance', 0))
                    available_balance = float(balance.get('available_balance', 0))
                    if total_balance <= 0 or available_balance < 0:
                        logger.warning(f"Balance check failed: Total={total_balance:.2f}, Available={available_balance:.2f} for {symbol}")
                        return None, None
                    return total_balance, available_balance
            logger.warning("No USDT or USD balance found in wallet")
            return None, None
        logger.error(f"Failed to fetch balance: {data.get('error')}")
        return None, None
    except Exception as e:
        logger.error(f"Error fetching balance: {str(e)}")
        return None, None

async def fetch_leverage(symbol) -> Optional[int]:
    try:
        product_id = SYMBOL_TO_PRODUCT_ID.get(symbol)
        if not product_id:
            logger.error(f"No product ID for {symbol}")
            return None
        response = await make_api_request("GET", f"/v2/products/{product_id}")
        if response.get('success') and response.get('result'):
            leverage = int(response['result'].get('max_leverage', LEVERAGE))
            logger.info(f"Fetched leverage for {symbol}: {leverage}x")
            return leverage
        logger.error(f"Failed to fetch leverage for {symbol}: {response.get('error')}")
        return None
    except Exception as e:
        logger.error(f"Error fetching leverage for {symbol}: {str(e)}")
        return None

async def set_leverage(symbol, leverage: int) -> bool:
    try:
        product_id = SYMBOL_TO_PRODUCT_ID.get(symbol)
        if not product_id:
            logger.error(f"No product ID for {symbol}")
            return False
        payload = {'product_id': product_id, 'leverage': leverage}
        response = await make_api_request("POST", "/v2/leverage", json=payload)
        if response.get('success'):
            logger.info(f"Set leverage for {symbol} to {leverage}x")
            return True
        logger.error(f"Failed to set leverage for {symbol}: {response.get('error')}")
        return False
    except Exception as e:
        logger.error(f"Error setting leverage for {symbol}: {str(e)}")
        return False

async def fetch_ohlcv(symbol, resolution=TIMEFRAME, limit=100) -> List[dict]:
    try:
        end = int(time.time())
        seconds_per_candle = {'1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400}
        if resolution not in seconds_per_candle:
            logger.error(f"Unknown resolution {resolution} for OHLCV fetch")
            return []
        available_seconds = end
        max_limit = available_seconds // seconds_per_candle[resolution]
        actual_limit = min(limit, max_limit)
        start = end - (actual_limit * seconds_per_candle[resolution])
        params = {
            'resolution': resolution,
            'symbol': symbol,
            'start': str(start),
            'end': str(end),
            'limit': str(actual_limit)
        }
        data = await make_api_request("GET", "/v2/history/candles", params=params)
        if not data.get('success') or not data.get('result'):
            logger.error(f"Failed to fetch OHLCV for {symbol}: {data.get('error')}")
            return []
        candles = data['result']
        if not isinstance(candles, list):
            logger.error(f"OHLCV data is not a list for {symbol}: {candles}")
            return []
        validated_candles = []
        for candle in candles:
            if not isinstance(candle, dict):
                logger.warning(f"Skipping invalid OHLCV candle for {symbol}: {candle}")
                continue
            required_keys = ['open', 'high', 'low', 'close', 'volume']
            if not all(key in candle for key in required_keys):
                logger.warning(f"Skipping OHLCV candle with missing keys for {symbol}: {candle}")
                continue
            try:
                for key in required_keys:
                    candle[key] = float(candle[key])
                validated_candles.append(candle)
            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping OHLCV candle with invalid values for {symbol}: {candle}, Error: {str(e)}")
                continue
        if not validated_candles:
            logger.error(f"No valid OHLCV candles for {symbol} after validation")
            return []
        logger.info(f"Fetched {len(validated_candles)} {resolution} candles for {symbol}")
        return validated_candles
    except Exception as e:
        logger.error(f"Error fetching OHLCV for {symbol}: {str(e)}")
        return []

# --- INDICATORS ---
def calculate_rsi(closes: list, period: int) -> list:
    rsi = []
    gains = []
    losses = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i-1]
        gains.append(delta if delta > 0 else 0)
        losses.append(-delta if delta < 0 else 0)
    for i in range(len(closes)):
        if i < period:
            rsi.append(50)
            continue
        avg_gain = np.mean(gains[i-period:i]) if i >= period else 0
        avg_loss = np.mean(losses[i-period:i]) if i >= period else 0
        rs = avg_gain / avg_loss if avg_loss != 0 else 100
        rsi.append(100 - (100 / (1 + rs)))
    return rsi

# --- STRATEGY LOGIC ---
async def get_trade_signal(symbol, ohlcv):
    if not ohlcv:
        logger.info(f"{symbol}: No OHLCV data available for signal generation")
        return None

    closes = [c['close'] for c in ohlcv]
    i = len(closes) - 1
    min_candles = RSI_PERIOD + 1
    if i < min_candles:
        logger.info(f"{symbol}: Not enough candles for signal (have {i+1}, need {min_candles})")
        return None

    if closes[i] <= 0:
        logger.error(f"{symbol}: Invalid close price {closes[i]} at index {i}")
        return None

    rsi = calculate_rsi(closes, RSI_PERIOD)
    if not rsi or len(rsi) != len(closes):
        logger.error(f"{symbol}: RSI calculation failed, length mismatch (closes={len(closes)}, rsi={len(rsi)})")
        return None

    # --- Initialize static attributes for tracking trades ---
    if not hasattr(get_trade_signal, "last_trade_dir"):
        get_trade_signal.last_trade_dir = {}
    if not hasattr(get_trade_signal, "last_trade_time"):
        get_trade_signal.last_trade_time = {}

    last_dir = get_trade_signal.last_trade_dir.get(symbol)
    last_time = get_trade_signal.last_trade_time.get(symbol, 0)
    min_reversal_wait = 3

    # RSI Strategy: Long when RSI crosses above oversold, short when RSI crosses below overbought
    rsi_crossover_oversold = i > 0 and rsi[i-1] <= RSI_OVERSOLD and rsi[i] > RSI_OVERSOLD
    rsi_crossunder_overbought = i > 0 and rsi[i-1] >= RSI_OVERBOUGHT and rsi[i] < RSI_OVERBOUGHT

    allow_long = rsi_crossover_oversold and (last_dir != 'long' or i - last_time >= min_reversal_wait)
    allow_short = rsi_crossunder_overbought and (last_dir != 'short' or i - last_time >= min_reversal_wait)

    # Log the trade signal conditions with colors
    color_long = Fore.GREEN if allow_long else Fore.RESET
    color_short = Fore.RED if allow_short else Fore.RESET
    logger.info(
        f"{symbol}: RSI[{i-1}]={rsi[i-1]:.2f}, RSI[{i}]={rsi[i]:.2f}, "
        f"{color_long}Long Signal={allow_long}{Style.RESET_ALL}, "
        f"{color_short}Short Signal={allow_short}{Style.RESET_ALL}"
    )

    # Log whether conditions are fulfilled
    if allow_long:
        logger.info(f"{Fore.GREEN}{symbol}: Long condition fulfilled (RSI crossed above {RSI_OVERSOLD}){Style.RESET_ALL}")
    elif allow_short:
        logger.info(f"{Fore.RED}{symbol}: Short condition fulfilled (RSI crossed below {RSI_OVERBOUGHT}){Style.RESET_ALL}")
    else:
        logger.info(f"{symbol}: No trade conditions fulfilled (RSI={rsi[i]:.2f})")

    # --- Risk/Reward: 1:5 ratio with percentage-based SL and TP ---
    stop_loss_pct = SYMBOL_STOP_LOSS_PCT.get(symbol, STOP_LOSS_PCT)
    take_profit_pct = TAKE_PROFIT_PCT[symbol]

    if allow_long:
        entry = closes[i]
        stop_loss = entry * (1 - stop_loss_pct)
        take_profit = entry * (1 + take_profit_pct)
        max_loss = entry - stop_loss
        if not all(np.isfinite([entry, stop_loss, take_profit, max_loss])):
            logger.error(f"{symbol}: Invalid signal values: entry={entry}, stop_loss={stop_loss}, take_profit={take_profit}, max_loss={max_loss}")
            return None
        # Verify RRR for long
        risk = entry - stop_loss
        reward = take_profit - entry
        actual_rrr = reward / risk if risk != 0 else 0
        logger.info(
            f"{Fore.GREEN}{symbol}: Long signal generated (entry={entry:.8f}, stop_loss={stop_loss:.8f}, take_profit={take_profit:.8f}, RRR={actual_rrr:.2f}){Style.RESET_ALL}"
        )
        get_trade_signal.last_trade_dir[symbol] = 'long'
        get_trade_signal.last_trade_time[symbol] = i
        return {
            'side': 'buy',
            'entry': entry,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'max_loss': max_loss
        }
    elif allow_short:
        entry = closes[i]
        stop_loss = entry * (1 + stop_loss_pct)
        take_profit = entry * (1 - take_profit_pct)
        max_loss = stop_loss - entry
        if not all(np.isfinite([entry, stop_loss, take_profit, max_loss])):
            logger.error(f"{symbol}: Invalid signal values: entry={entry}, stop_loss={stop_loss}, take_profit={take_profit}, max_loss={max_loss}")
            return None
        # Verify RRR for short
        risk = stop_loss - entry
        reward = entry - take_profit
        actual_rrr = reward / risk if risk != 0 else 0
        logger.info(
            f"{Fore.RED}{symbol}: Short signal generated (entry={entry:.8f}, stop_loss={stop_loss:.8f}, take_profit={take_profit:.8f}, RRR={actual_rrr:.2f}){Style.RESET_ALL}"
        )
        get_trade_signal.last_trade_dir[symbol] = 'short'
        get_trade_signal.last_trade_time[symbol] = i
        return {
            'side': 'sell',
            'entry': entry,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'max_loss': max_loss
        }

    logger.info(f"{symbol}: No trade signal generated.")
    return None

# --- ORDER MANAGEMENT ---
async def fetch_positions(symbol):
    try:
        product_id = SYMBOL_TO_PRODUCT_ID.get(symbol)
        if not product_id:
            logger.error(f"No product ID for {symbol}")
            return None
        params = {'product_id': product_id}
        response = await make_api_request("GET", "/v2/positions", params=params)
        if not response.get('success'):
            logger.error(f"Failed to fetch positions for {symbol}: {response.get('error')}")
            return None
        result = response.get('result')
        logger.info(f"{symbol}: Raw positions response: {result}")
        if not result:
            logger.info(f"No positions found for {symbol}")
            return None
        positions = result if isinstance(result, list) else [result]
        for pos in positions:
            pos_size = float(pos.get('size', 0))
            if pos_size != 0:
                position_id = pos.get('id') or pos.get('position_id')
                if not position_id:
                    symbol_field = pos.get('symbol') or symbol
                    product_id_field = pos.get('product_id') or product_id
                    entry_price = pos.get('entry_price')
                    size = pos.get('size')
                    if symbol_field and product_id_field:
                        position_id = f"{symbol_field}_{product_id_field}"
                    elif entry_price and size:
                        position_id = f"{symbol}_pos_{entry_price}_{size}_{int(time.time())}"
                    else:
                        logger.warning(f"{symbol}: Cannot construct position ID. Skipping position: {pos}")
                        continue
                position = {
                    'id': position_id,
                    'entry_price': float(pos.get('entry_price', 0)),
                    'size': pos_size,
                    'side': 'long' if pos_size > 0 else 'short',
                    'product_id': product_id,
                    'symbol': symbol
                }
                logger.info(f"Fetched position for {symbol}: Entry={position['entry_price']:.2f}, Size={position['size']}, Side={position['side']}, ID={position['id']}")
                return position
        logger.info(f"No active positions for {symbol}")
        return None
    except Exception as e:
        logger.error(f"Exception in fetch_positions for {symbol}: {str(e)}")
        return None

async def cancel_pending_orders(symbol):
    product_id = SYMBOL_TO_PRODUCT_ID.get(symbol)
    if not product_id:
        logger.error(f"No product ID for {symbol} (cancel_pending_orders)")
        return
    params = {'product_id': product_id, 'state': 'open'}
    response = await make_api_request("GET", "/v2/orders", params=params)
    if response.get('success') and response.get('result'):
        orders = response['result']
        if not isinstance(orders, list):
            orders = [orders]
        pending_orders = [order for order in orders if order.get('state') == 'open']
        if pending_orders:
            logger.info(f"Pending open orders for {symbol}:")
            for order in pending_orders:
                logger.info(f"OrderID: {order.get('id')}, Side: {order.get('side')}, Size: {order.get('size')}, Price: {order.get('limit_price')}, Type: {order.get('order_type')}, State: {order.get('state')}")
            for order in pending_orders:
                order_id = order.get('id')
                logger.info(f"Cancelling pending order {order_id} for {symbol}")
                await make_api_request("DELETE", "/v2/orders", params={'order_id': order_id})
        else:
            logger.info(f"No open orders to cancel for {symbol}")
    else:
        logger.info(f"No open orders to cancel for {symbol}")

async def cleanup_orders_on_position_close(symbol):
    logger.info(f"{symbol}: Cleaning up all stop/target orders after position close.")
    await cancel_pending_orders(symbol)

async def place_order(symbol, side, contracts, entry, stop_loss, take_profit, is_scalping):
    product_id = SYMBOL_TO_PRODUCT_ID.get(symbol)
    if not product_id:
        logger.error(f"No product ID for {symbol}")
        return None, None, None, None, None

    product_info = await make_api_request("GET", f"/v2/products/{product_id}")
    if not product_info.get('success') or not product_info.get('result'):
        logger.error(f"Failed to fetch product info for {symbol}: {product_info.get('error')}")
        return None, None, None, None, None
    min_order_size = float(product_info['result'].get('min_order_size', 1))
    max_order_size = float(product_info['result'].get('max_order_size', 1e10))
    contract_value = float(product_info['result'].get('contract_value', 1))

    contracts = max(min_order_size, min(contracts, max_order_size))
    if contracts < min_order_size:
        logger.info(f"{symbol}: Order size {contracts} below min_order_size {min_order_size}, skipping order.")
        return None, None, None, None, None

    # Use market order for entry
    market_payload = {
        'product_id': product_id,
        'size': contracts,
        'side': side,
        'order_type': 'market_order',
        'time_in_force': 'ioc'
    }
    logger.info(f"Placing market order for {symbol}: {market_payload}")
    market_response = await make_api_request("POST", "/v2/orders", json=market_payload)
    if not market_response.get('success') or not market_response.get('result'):
        logger.error(f"Failed to place market order for {symbol}: {market_response.get('error')}")
        await asyncio.sleep(2)
        await cancel_pending_orders(symbol)
        return None, None, None, None, None
    order_result = market_response['result']

    # For scalping trades, don't place stop-loss/take-profit on exchange; manage locally
    if is_scalping:
        return order_result, None, None, stop_loss, take_profit

    # Place stop-loss as market order (reduce-only)
    stop_side = 'sell' if side == 'buy' else 'buy'
    stop_payload = {
        'product_id': product_id,
        'size': contracts,
        'side': stop_side,
        'order_type': 'market_order',
        'stop_order_type': 'stop_loss_order',
        'stop_price': str(round(stop_loss, 2)),
        'time_in_force': 'gtc',
        'reduce_only': True
    }
    logger.info(f"Placing stop-loss order for {symbol}: {stop_payload}")
    stop_response = await make_api_request("POST", "/v2/orders", json=stop_payload)
    if not stop_response.get('success') or not stop_response.get('result'):
        logger.error(f"Failed to place stop-loss order for {symbol}: {stop_response.get('error')}")
        await asyncio.sleep(2)
        await cancel_pending_orders(symbol)
        stop_order = None
    else:
        stop_order = stop_response['result']

    # Place take-profit as limit order (reduce-only)
    tp_payload = {
        'product_id': product_id,
        'size': contracts,
        'side': stop_side,
        'order_type': 'limit_order',
        'limit_price': str(round(take_profit, 2)),
        'time_in_force': 'gtc',
        'reduce_only': True
    }
    logger.info(f"Placing take-profit order for {symbol}: {tp_payload}")
    tp_response = await make_api_request("POST", "/v2/orders", json=tp_payload)
    if not tp_response.get('success') or not tp_response.get('result'):
        logger.error(f"Failed to place take-profit order for {symbol}: {tp_response.get('error')}")
        await asyncio.sleep(2)
        await cancel_pending_orders(symbol)
        tp_order = None
    else:
        tp_order = tp_response['result']

    return order_result, stop_order, tp_order, stop_loss, take_profit

async def close_position(symbol, position):
    product_id = SYMBOL_TO_PRODUCT_ID.get(symbol)
    if not product_id or not position:
        logger.error(f"{symbol}: Cannot close position, missing product_id or position.")
        return False

    side = 'sell' if position['side'] == 'long' else 'buy'
    size = abs(int(position['size']))

    # Fetch best price from orderbook
    try:
        orderbook = await make_api_request("GET", f"/v2/l2orderbook/{product_id}")
        if orderbook.get('success') and orderbook.get('result'):
            if side == 'sell' and 'bids' in orderbook['result'] and orderbook['result']['bids']:
                best_bid = float(orderbook['result']['bids'][0][0])
                limit_price = best_bid
            elif side == 'buy' and 'asks' in orderbook['result'] and orderbook['result']['asks']:
                best_ask = float(orderbook['result']['asks'][0][0])
                limit_price = best_ask
            else:
                raise KeyError("Missing bids or asks in orderbook")
        else:
            raise ValueError("Orderbook fetch failed")
    except (KeyError, ValueError) as e:
        logger.warning(f"{symbol}: Failed to fetch orderbook for close: {str(e)}. Falling back to last known price.")
        ohlcv = await fetch_ohlcv(symbol, resolution='1m', limit=1)
        if ohlcv and isinstance(ohlcv, list) and len(ohlcv) > 0:
            limit_price = float(ohlcv[-1]['close'])
            logger.info(f"{symbol}: Using last known price from OHLCV: {limit_price}")
        else:
            logger.error(f"{symbol}: No valid price available to close position.")
            return False

    if not limit_price:
        logger.error(f"{symbol}: No valid price found to close position.")
        return False

    payload = {
        "product_id": product_id,
        "size": size,
        "side": side,
        "order_type": "market_order",
        "time_in_force": "ioc",
        "reduce_only": True
    }
    logger.info(f"{symbol}: Closing position with market order: {payload}")
    response = await make_api_request("POST", "/v2/orders", json=payload)
    if response.get('success'):
        logger.info(f"{symbol}: Position closed successfully.")
        return True
    else:
        logger.error(f"{symbol}: Failed to close position: {response.get('error')}")
        return False

# --- MAIN TRADING LOOP ---
async def trading_loop():
    if not await validate_symbols():
        logger.error("Failed to validate symbols.")
        return

    running_trades = {symbol: [] for symbol in SYMBOLS}
    daily_trades = {symbol: 0 for symbol in SYMBOLS}
    last_reset_time = time.time()

    RISK_PER_TRADE_THRESHOLD = RISK_PER_TRADE
    FEE_RATE = 0.00053
    MAX_POSITION_THRESHOLD = 1.0

    def calculate_position_size(balance: float, entry_price: float, stop_loss: float, symbol: str, max_order_size: float) -> float:
        risk_amount = balance * RISK_PER_TRADE_THRESHOLD
        price_diff = abs(entry_price - stop_loss)
        if price_diff <= 0:
            return 0
        position_size = risk_amount / price_diff
        notional_value = position_size * entry_price
        margin_required = (notional_value / LEVERAGE) * 1.0
        fees = notional_value * FEE_RATE * 2
        if margin_required + fees > balance:
            position_size = ((balance - fees) * LEVERAGE / 1.0) / entry_price
        if notional_value > balance * MAX_POSITION_THRESHOLD:
            position_size = (balance * MAX_POSITION_THRESHOLD) / entry_price
        position_size = min(position_size, max_order_size / entry_price)
        return position_size if position_size * entry_price >= 1 else 0

    async def ensure_sl_tp(symbol, position, trade, min_order_size, contract_value):
        # Use stop_loss and take_profit from trade dict
        stop_loss = trade.get('stop_loss')
        take_profit = trade.get('take_profit')
        entry = position.get('entry_price')
        side = position.get('side')

        # Defensive: If stop_loss or take_profit is None, recalculate using percentages
        if stop_loss is None or take_profit is None or entry is None:
            stop_loss_pct = SYMBOL_STOP_LOSS_PCT.get(symbol, STOP_LOSS_PCT)
            take_profit_pct = TAKE_PROFIT_PCT[symbol]
            ohlcv = await fetch_ohlcv(symbol, resolution=TIMEFRAME, limit=50)
            if not ohlcv or len(ohlcv) < 20:
                logger.info(f"{symbol}: Not enough OHLCV data to calculate SL/TP for running position.")
                return
            closes = [float(c['close']) for c in ohlcv if isinstance(c, dict) and 'close' in c]
            current_price = closes[-1]
            if side == 'long':
                stop_loss = current_price * (1 - stop_loss_pct)
                take_profit = current_price * (1 + take_profit_pct)
            else:
                stop_loss = current_price * (1 + stop_loss_pct)
                take_profit = current_price * (1 - take_profit_pct)
            trade['stop_loss'] = stop_loss
            trade['take_profit'] = take_profit
            logger.info(f"{symbol}: Recalculated SL/TP for running trade: SL={stop_loss:.8f}, TP={take_profit:.8f}")
        else:
            ohlcv = await fetch_ohlcv(symbol, resolution=TIMEFRAME, limit=50)
            if not ohlcv or len(ohlcv) < 20:
                logger.info(f"{symbol}: Not enough OHLCV data to manage SL/TP for running position.")
                return
            closes = [float(c['close']) for c in ohlcv if isinstance(c, dict) and 'close' in c]
            current_price = closes[-1]

        # --- Trailing Stop: Move stop_loss if profit exceeds threshold ---
        if TRAILING_ENABLED and stop_loss is not None and take_profit is not None and entry is not None:
            profit = (current_price - entry) if side == 'long' else (entry - current_price)
            if profit >= entry * TRAILING_START_PCT:
                if side == 'long':
                    new_sl = current_price * (1 - TRAILING_STEP_PCT)
                    if new_sl > stop_loss:
                        trade['stop_loss'] = new_sl
                        logger.info(f"{symbol}: Trailing SL updated for long: New SL={new_sl:.8f}")
                else:
                    new_sl = current_price * (1 + TRAILING_STEP_PCT)
                    if new_sl < stop_loss:
                        trade['stop_loss'] = new_sl
                        logger.info(f"{symbol}: Trailing SL updated for short: New SL={new_sl:.8f}")

        # --- SL/TP execution logic ---
        stop_loss = trade.get('stop_loss')
        take_profit = trade.get('take_profit')
        if stop_loss is None or take_profit is None or entry is None:
            logger.warning(f"{symbol}: SL/TP or entry is None, cannot manage position.")
            return

        # Close position if price crosses SL or TP
        if side == 'long':
            if current_price <= stop_loss:
                logger.info(f"{Fore.RED}{symbol}: Stop-Loss hit at {current_price:.8f} for long position. Closing position with market order.{Style.RESET_ALL}")
                if await close_position(symbol, position):
                    running_trades[symbol].remove(trade)
                    await cancel_pending_orders(symbol)
            elif current_price >= take_profit:
                logger.info(f"{Fore.GREEN}{symbol}: Take-Profit hit at {current_price:.8f} for long position. Closing position with market order.{Style.RESET_ALL}")
                if await close_position(symbol, position):
                    running_trades[symbol].remove(trade)
                    await cancel_pending_orders(symbol)
        else:
            if current_price >= stop_loss:
                logger.info(f"{Fore.RED}{symbol}: Stop-Loss hit at {current_price:.8f} for short position. Closing position with market order.{Style.RESET_ALL}")
                if await close_position(symbol, position):
                    running_trades[symbol].remove(trade)
                    await cancel_pending_orders(symbol)
            elif current_price <= take_profit:
                logger.info(f"{Fore.GREEN}{symbol}: Take-Profit hit at {current_price:.8f} for short position. Closing position with market order.{Style.RESET_ALL}")
                if await close_position(symbol, position):
                    running_trades[symbol].remove(trade)
                    await cancel_pending_orders(symbol)

    while True:
        try:
            current_time = time.time()
            if current_time - last_reset_time >= 24 * 3600:
                daily_trades = {symbol: 0 for symbol in SYMBOLS}
                last_reset_time = current_time
                logger.info("Reset daily trade counters")

            total_capital, available_capital = await fetch_balance()
            if total_capital is None or available_capital is None:
                logger.warning("No available balance, retrying in 1 second...")
                await asyncio.sleep(1)
                continue

            for symbol in SYMBOLS:
                current_position = await fetch_positions(symbol)
                product_id = SYMBOL_TO_PRODUCT_ID.get(symbol)
                product_info = await make_api_request("GET", f"/v2/products/{product_id}")
                if not product_info.get('success') or not product_info.get('result'):
                    logger.error(f"Failed to fetch product info for {symbol}: {product_info.get('error')}")
                    continue
                contract_value = float(product_info['result'].get('contract_value', 1))
                min_order_size = float(product_info['result'].get('min_order_size', 1))
                max_order_size = float(product_info['result'].get('max_order_size', 1e10))

                if current_position:
                    found_trade = None
                    for trade in running_trades[symbol]:
                        pos = trade.get('position')
                        if pos and pos['side'] == current_position['side'] and abs(pos['size']) == abs(current_position['size']):
                            found_trade = trade
                            break
                    if found_trade:
                        logger.info(f"{symbol}: Monitoring existing trade: {found_trade}")
                    else:
                        logger.info(f"{symbol}: New position detected. Adding to running trades for monitoring.")
                        trade = {
                            'position': current_position,
                            'stop_order_id': None,
                            'tp_order_id': None,
                            'stop_loss': None,
                            'take_profit': None,
                            'is_scalping': False,
                            'entry_time': int(time.time())
                        }
                        running_trades[symbol].append(trade)
                        found_trade = trade
                    await ensure_sl_tp(symbol, current_position, found_trade, min_order_size, contract_value)
                    continue
                else:
                    if running_trades[symbol]:
                        logger.info(f"{symbol}: Position closed. Clearing running trades: {running_trades[symbol]}")
                        running_trades[symbol].clear()
                        await cleanup_orders_on_position_close(symbol)

                logger.info(f"{symbol}: No active positions")

                if daily_trades[symbol] >= MAX_TRADES_PER_DAY:
                    logger.info(f"{symbol}: Max daily trades ({MAX_TRADES_PER_DAY}) reached")
                    continue

                ohlcv = await fetch_ohlcv(symbol, resolution=TIMEFRAME, limit=100)
                if len(ohlcv) < RSI_PERIOD + 1:
                    logger.warning(f"{symbol}: Insufficient OHLCV data (fetched {len(ohlcv)} candles, need {RSI_PERIOD + 1})")
                    continue

                signal = await get_trade_signal(symbol, ohlcv)
                if not signal:
                    continue

                if any(running_trades[symbol]):
                    logger.info(f"{symbol}: Trade signal ignored due to existing running trade")
                    continue

                entry = signal['entry']
                stop_loss = signal['stop_loss']
                if stop_loss is None or entry is None or entry == stop_loss or not np.isfinite(entry) or not np.isfinite(stop_loss):
                    logger.info(f"{symbol}: Trade rejected due to invalid entry/stop_loss (entry={entry}, stop_loss={stop_loss})")
                    continue

                max_loss = signal['max_loss']
                if max_loss is None or not np.isfinite(max_loss):
                    logger.info(f"{symbol}: Trade rejected due to invalid max_loss (max_loss={max_loss})")
                    continue

                if max_loss * contract_value > MAX_STOPLOSS_DOLLARS:
                    logger.info(f"{symbol}: Signal rejected: Stop-Loss ${max_loss * contract_value:.2f} exceeds limit ${MAX_STOPLOSS_DOLLARS}")
                    continue

                position_size = calculate_position_size(
                    available_capital, entry, stop_loss, symbol, max_order_size
                )
                contracts = int(position_size / contract_value)
                if contracts < min_order_size:
                    logger.info(f"{symbol}: Order size {contracts} below min_order_size {min_order_size}, skipping trade")
                    continue

                required_margin = contracts * contract_value * entry / LEVERAGE
                if required_margin > available_capital:
                    logger.info(f"{symbol}: Required margin {required_margin:.2f} > available {available_capital:.2f}, skipping trade")
                    continue

                order, stop_order, tp_order, sl, tp = await place_order(
                    symbol, signal['side'], contracts, entry, signal['stop_loss'], signal['take_profit'], signal.get('is_scalping', False)
                )
                if order:
                    trade = {
                        'position': {
                            'entry_price': entry,
                            'size': contracts if signal['side'] == 'buy' else -contracts,
                            'side': signal['side'],
                            'product_id': product_id,
                            'id': f"{symbol}_{product_id}_{int(time.time())}"
                        },
                        'stop_order_id': stop_order.get('id') if stop_order else None,
                        'tp_order_id': tp_order.get('id') if tp_order else None,
                        'stop_loss': sl,
                        'take_profit': tp,
                        'is_scalping': signal.get('is_scalping', False),
                        'entry_time': signal.get('entry_time', int(time.time()))
                    }
                    running_trades[symbol].append(trade)
                    daily_trades[symbol] += 1
                    color = Fore.GREEN if signal['side'] == 'buy' else Fore.RED
                    logger.info(
                        f"{color}{symbol}: New {signal['side']} trade opened: Entry={entry:.8f}, SL={sl:.8f}, TP={tp:.8f}, Scalping={signal.get('is_scalping', False)}{Style.RESET_ALL}"
                    )

        except Exception as e:
            logger.error(f"Trading loop error: {str(e)}")
        await asyncio.sleep(1)

# --- MAIN ---
async def main():
    if not await validate_credentials():
        logger.error("API credentials validation failed.")
        return
    await trading_loop()

if platform.system() == "Emscripten":
    asyncio.ensure_future(main())
else:
    if __name__ == "__main__":
        asyncio.run(main())
