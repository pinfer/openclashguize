#!/usr/bin/env python3

import os
import json
import asyncio
import logging
import requests
import datetime
import signal
import fcntl
import functools
import traceback
from logging.handlers import TimedRotatingFileHandler
from pytz import utc
from binance import AsyncClient, BinanceSocketManager, BinanceAPIException

# ============== 配置区域 ==============
BASE_URL = 'https://fapi.binance.com'
BINANCE_API_KEY = 'AbaDJU8Q9puMgEbnLaZgNusgImwywULFwjIsWMv1ceZyxnSXLtlEWbNHrYcDerSs'
BINANCE_API_SECRET = 'IqLWduWAJzgCUzgtus65Hx2MP2rFSJRg1tR3g06ngmUqv0spArMuBhb38HR4slvM'

# 策略参数
ENTITY_RATIO_THRESHOLD = 0.25
UPPER_SHADOW_RATIO_THRESHOLD = 0.65
LOWER_SHADOW_RATIO_THRESHOLD = 0.2
INTERVALS = ['1h', '4h', '1d', '1w']
CUSTOM_SYMBOLS = ["ETHUSDT", "SOLUSDT", "ADAUSDT", "LINKUSDT", "XRPUSDT"]
INITIAL_INVESTMENT = 2
MAX_ALLOWED_LEVERAGE = 20
MAINTENANCE_MARGIN_RATE = {
    "BTCUSDT": 0.004,
    "DEFAULT": 0.01
}

# Telegram通知配置
BOT_TOKEN = "6360770475:AAHfey7ZNTW9fmdABhrG-Aa6ZwZMWhaPPlU"
CHAT_ID = "-1002286585218"
# 路径配置
DATA_DIR = '/autotraderbot/data'
LOG_DIR = '/autotraderbot/log'
ORDERS_FILE = os.path.join(DATA_DIR, 'sheji_orders.json')
PROCESSED_SIGNALS_FILE = os.path.join(DATA_DIR, 'sheji_signals.txt')
SHEJI_LOG_FILE = os.path.join(LOG_DIR, 'sheji.log')
HEARTBEAT_FILE = os.path.join(DATA_DIR, 'sheji_heartbeat.txt')
HEARTBEAT_INTERVAL = 300  # 心跳间隔（秒）

# ============== 日志配置 ==============
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

class LockedTimedRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, filename, when='midnight', interval=1, backupCount=2, encoding=None, delay=False, utc=False):
        super().__init__(filename, when, interval, backupCount, encoding, delay, utc)
    
    def emit(self, record):
        try:
            with open(self.baseFilename, 'a') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                super().emit(record)
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception:
            self.handleError(record)

def initialize_logger():
    logger = logging.getLogger('SHEJI_TRADER')
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    
    file_handler = LockedTimedRotatingFileHandler(
        filename=SHEJI_LOG_FILE,
        when='midnight',
        interval=1,
        backupCount=2,
        encoding='utf-8',
        utc=True
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

logger = initialize_logger()

async def update_heartbeat():
    try:
        with open(HEARTBEAT_FILE, 'w', encoding='utf-8') as f:
            f.write(datetime.datetime.now(utc).isoformat())
    except Exception as e:
        logger.error(f"更新心跳文件失败: {str(e)}", exc_info=True)

class ShejiTradingSystem:
    def __init__(self):
        self.client = None
        self.active_orders = {}
        self.processed_signals = set()
        self.processed_klines = {}
        self.running = True
        self.symbols_info = {}
        self.is_hedge_mode = None
        self.last_heartbeat = 0
        self._shutdown_event = asyncio.Event()
        logger.debug("交易系统实例初始化完成")

    def _round_to_precision(self, value: float, precision: int) -> float:
        return round(value, precision)

    async def initialize(self):
        try:
            logger.info("开始初始化 Binance 客户端")
            self.client = await AsyncClient.create(
                BINANCE_API_KEY,
                BINANCE_API_SECRET,
                tld='com'
            )
            await self._load_state()
            
            position_mode = await self.client.futures_get_position_mode()
            self.is_hedge_mode = position_mode['dualSidePosition']
            logger.info(f"账户仓位模式: {'对冲模式' if self.is_hedge_mode else '单向模式'}")
            
            exchange_info = await self.client.futures_exchange_info()
            for symbol_info in exchange_info['symbols']:
                symbol = symbol_info['symbol']
                if symbol not in CUSTOM_SYMBOLS:
                    continue
                self.symbols_info[symbol] = {'step_size': 0.001, 'price_precision': 2}
                for f in symbol_info['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        self.symbols_info[symbol]['step_size'] = float(f['stepSize'])
                    elif f['filterType'] == 'PRICE_FILTER':
                        tick_size = float(f['tickSize'])
                        if tick_size < 1e-4:
                            self.symbols_info[symbol]['price_precision'] = 5
                        elif tick_size < 1e-3:
                            self.symbols_info[symbol]['price_precision'] = 4
                        elif tick_size < 1e-2:
                            self.symbols_info[symbol]['price_precision'] = 3
                        else:
                            self.symbols_info[symbol]['price_precision'] = 2
            
            logger.info("交易所连接初始化成功")
            logger.debug(f"Symbols info: {json.dumps(self.symbols_info, indent=2)}")
        except Exception as e:
            logger.error(f"初始化失败: {str(e)}", exc_info=True)
            await self._send_error_alert(e, "初始化阶段")
            raise

    async def _load_state(self):
        try:
            if os.path.exists(ORDERS_FILE):
                with open(ORDERS_FILE, 'r', encoding='utf-8') as f:
                    orders = json.load(f)
                    valid_orders = {}
                    for order_id, order_info in orders.items():
                        if order_info['status'] in ['FILLED', 'CANCELED']:
                            continue
                        try:
                            res = await self.client.futures_get_order(
                                symbol=order_info['symbol'],
                                orderId=order_id
                            )
                            if res['status'] not in ['FILLED', 'CANCELED']:
                                valid_orders[order_id] = order_info
                                logger.debug(f"订单验证通过 | {order_info['symbol']} | ID: {order_id}")
                            else:
                                logger.info(f"过滤已结束订单 | {order_info['symbol']} | ID: {order_id} | 状态: {res['status']}")
                        except BinanceAPIException as e:
                            if e.code == -2013:
                                logger.info(f"订单不存在，跳过加载 | ID: {order_id}")
                            else:
                                logger.error(f"订单验证失败 | ID: {order_id} | 错误: {e.message}")
                    self.active_orders = valid_orders
                    logger.info(f"有效订单加载完成 | 数量: {len(self.active_orders)}")

            if os.path.exists(PROCESSED_SIGNALS_FILE):
                with open(PROCESSED_SIGNALS_FILE, 'r', encoding='utf-8') as f:
                    self.processed_signals = set(line.strip() for line in f)
                logger.debug(f"已加载历史信号 {len(self.processed_signals)} 条")
        except Exception as e:
            logger.error(f"状态加载失败: {str(e)}", exc_info=True)

    async def _save_state(self):
        try:
            with open(ORDERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.active_orders, f, indent=2, ensure_ascii=False)
                logger.debug("订单记录保存成功")

            with open(PROCESSED_SIGNALS_FILE, 'w', encoding='utf-8') as f:
                f.write("\n".join(self.processed_signals))
                logger.debug("信号记录保存成功")
        except Exception as e:
            logger.error(f"状态保存失败: {str(e)}", exc_info=True)

    async def _send_notification(self, message: str):
        try:
            logger.debug(f"发送通知 | 内容: {message[:100]}...")
            response = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": CHAT_ID,
                    "text": message
                },
                timeout=10
            )
            if response.ok:
                logger.debug(f"通知发送成功 | 响应: {response.text}")
            else:
                logger.error(f"Telegram通知失败 | 状态码: {response.status_code} | 响应: {response.text}")
        except Exception as e:
            logger.error(f"通知发送失败 | 错误: {str(e)}", exc_info=True)

    async def _send_error_alert(self, error: Exception, context: str = ""):
        tb = traceback.format_exc()
        error_time = datetime.datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S')
        alert_msg = (
            "🤖 SHEJI系统通知\n"
            "🚨 系统错误\n"
            f"▫️ 时间: {error_time}\n"
            f"▫️ 类型: {error.__class__.__name__}\n"
            f"▫️ 详情: {str(error)[:200]}\n"
            f"▫️ 上下文: {context}\n"
            f"▫️ 追踪:\n{tb}"
        )
        await self._send_notification(alert_msg)

    async def _get_current_price(self, symbol: str) -> float:
        try:
            ticker = await self.client.futures_symbol_ticker(symbol=symbol)
            return float(ticker['price'])
        except Exception as e:
            logger.error(f"获取当前价格失败 | {symbol} | 错误: {str(e)}")
            return None

    async def _get_klines(self, symbol: str, interval: str) -> list:
        try:
            klines = await self.client.futures_klines(
                symbol=symbol,
                interval=interval,
                limit=2
            )
            return klines
        except BinanceAPIException as e:
            logger.error(f"K线获取失败 | {symbol} | {interval} | {e}")
            await self._send_error_alert(e, f"获取K线 {symbol} {interval}")
            return None

    def _analyze_kline(self, prev_kline, curr_kline, symbol: str) -> dict:
        try:
            # 使用prev_kline（已闭合的K线）进行分析
            current_time = datetime.datetime.now(utc).timestamp()
            kline_close_time = prev_kline[6] / 1000  # 使用前一K线的闭合时间
            
            if current_time < kline_close_time:
                logger.debug(f"忽略未闭合K线 | {symbol} | 闭合时间: {datetime.datetime.fromtimestamp(kline_close_time)}")
                return None

            prev_open = float(prev_kline[1])
            prev_close = float(prev_kline[4])
            prev_high = float(prev_kline[2])
            prev_low = float(prev_kline[3])

            entity_size = abs(prev_close - prev_open)
            total_range = prev_high - prev_low
            
            if total_range < 1e-8:
                return None

            entity_ratio = entity_size / total_range
            upper_ratio = (prev_high - max(prev_open, prev_close)) / total_range
            lower_ratio = (min(prev_open, prev_close) - prev_low) / total_range

            if (entity_ratio < ENTITY_RATIO_THRESHOLD and
                upper_ratio > UPPER_SHADOW_RATIO_THRESHOLD and
                lower_ratio < LOWER_SHADOW_RATIO_THRESHOLD and
                prev_close < curr_kline[2]):  # 使用当前K线的高点进行比较
                return self._calculate_trade_params(
                    open_price=prev_close,
                    low_price=prev_low,
                    high_price=prev_high,
                    symbol=symbol
                )
            return None
        except Exception as e:
            logger.error(f"K线分析异常 | {symbol} | {str(e)}", exc_info=True)
            asyncio.create_task(self._send_error_alert(e, f"K线分析 {symbol}"))
            return None

    def _calculate_trade_params(self, **kwargs) -> dict:
        try:
            symbol_info = self.symbols_info.get(kwargs['symbol'], {'price_precision': 4})
            price_precision = symbol_info['price_precision']
            step_size = symbol_info.get('step_size', 0.001)

            entry = self._round_to_precision(kwargs['open_price'], price_precision)
            price_diff = kwargs['high_price'] - kwargs['low_price']
            sl = self._round_to_precision(
                kwargs['low_price'] - price_diff * 0.272,
                price_precision
            )
            min_price_diff = 10 ** (-price_precision)
            
            if sl >= entry:
                sl = entry - min_price_diff
                logger.warning(f"强制调整止损价至有效值: {sl}（原计算值: {kwargs['low_price'] - price_diff*0.272}）")

            tp = self._round_to_precision(
                kwargs['high_price'] - price_diff * 0.382,
                price_precision
            )

            if entry == 0:
                return None

            risk_pct = abs((entry - sl) / entry * 100)
            if risk_pct == 0:
                return None

            reward_pct = abs((tp - entry) / entry * 100)
            rr_ratio = round(reward_pct / risk_pct, 2)

            margin_rate = MAINTENANCE_MARGIN_RATE.get(kwargs['symbol'], MAINTENANCE_MARGIN_RATE["DEFAULT"])
            leverage_calc = int(1 / (margin_rate + (risk_pct / 100)))
            leverage = min(leverage_calc, MAX_ALLOWED_LEVERAGE)

            quantity = (INITIAL_INVESTMENT * leverage_calc) / entry
            quantity = round((quantity // step_size) * step_size, 8)

            required_margin = (quantity * entry) / leverage
            if required_margin > INITIAL_INVESTMENT:
                logger.warning(f"所需保证金不足: {required_margin:.2f} USDT")

            return {
                'symbol': kwargs['symbol'],
                'side': 'BUY',
                'price': entry,
                'quantity': quantity,
                'stop_loss': sl,
                'take_profit': tp,
                'leverage': leverage,
                'risk_pct': round(risk_pct, 2),
                'reward_pct': round(reward_pct, 2),
                'risk_reward': rr_ratio,
                'interval': kwargs.get('interval', 'N/A')
            }
        except Exception as e:
            logger.error(f"参数计算失败 | {kwargs.get('symbol')} | {str(e)}", exc_info=True)
            asyncio.create_task(self._send_error_alert(e, f"参数计算 {kwargs.get('symbol')}"))
            return None

    async def _place_order(self, signal: dict):
        if not signal:
            return
        try:
            logger.debug(f"开始挂单 | {signal['symbol']} | 价格: {signal['price']}")
            await self.client.futures_change_leverage(
                symbol=signal['symbol'],
                leverage=signal['leverage']
            )
            order_params = {
                'symbol': signal['symbol'],
                'side': signal['side'],
                'type': 'LIMIT',
                'price': str(signal['price']),
                'quantity': str(signal['quantity']),
                'timeInForce': 'GTC'
            }
            if self.is_hedge_mode:
                order_params['positionSide'] = 'LONG' if signal['side'] == 'BUY' else 'SHORT'

            order = await self.client.futures_create_order(**order_params)
            self.active_orders[order['orderId']] = {
                **signal,
                'status': 'NEW',
                'orderId': order['orderId'],
                'timestamp': datetime.datetime.now(utc).isoformat()
            }
            await self._save_state()

            msg = (
                "🤖 SHEJI系统通知\n"
                f"🎯 新交易信号 {signal['symbol']}\n"
                f"▫️ 周期: {signal['interval']}\n"
                f"🏷 入场价: {signal['price']:.4f}\n"
                f"🛑 止损: {signal['stop_loss']:.4f} (-{signal['risk_pct']}%)\n"
                f"🎯 止盈: {signal['take_profit']:.4f} (+{signal['reward_pct']}%)\n"
                f"⚖️ 风险比: 1:{signal['risk_reward']}\n"
                f"📈 杠杆: {signal['leverage']}x\n"
                f"📦 数量: {signal['quantity']:.6f}"
            )
            await self._send_notification(msg)
            logger.info(f"挂单成功 | {signal['symbol']} | 订单ID: {order['orderId']}")
        except BinanceAPIException as e:
            error_msg = f"挂单失败 | {signal['symbol']} | 错误: {e.message}"
            logger.error(error_msg)
            await self._send_notification(
                "🤖 SHEJI系统通知\n"
                f"❌ {signal['symbol']} 挂单失败: {e.message}"
            )
        except Exception as e:
            error_msg = f"挂单异常 | {signal['symbol']} | 错误: {str(e)}"
            logger.error(error_msg, exc_info=True)
            await self._send_notification(
                "🤖 SHEJI系统通知\n"
                f"⚠️ {signal['symbol']} 挂单异常: {str(e)}"
            )

    async def _monitor_orders(self):
        logger.info("启动订单监控循环")
        try:
            while not self._shutdown_event.is_set():
                try:
                    for order_id, order_info in list(self.active_orders.items()):
                        try:
                            res = await self.client.futures_get_order(
                                symbol=order_info['symbol'],
                                orderId=order_id
                            )
                            new_status = res['status']
                            old_status = order_info.get('status', 'UNKNOWN')
                            
                            if new_status != old_status:
                                self.active_orders[order_id]['status'] = new_status
                                await self._save_state()
                                
                                logger.info(
                                    f"订单状态同步 | {order_info['symbol']} | ID: {order_id} | "
                                    f"状态变更: {old_status} → {new_status}"
                                )
                                
                                if new_status == 'CANCELED' and order_info.get('type') in ['STOP_LOSS', 'TAKE_PROFIT']:
                                    msg = (
                                        "🤖 SHEJI系统通知\n"
                                        f"👤 订单已被手动取消\n"
                                        f"▫️ 交易对: {order_info['symbol']}\n"
                                        f"▫️ 订单类型: {'止损单' if order_info['type'] == 'STOP_LOSS' else '止盈单'}\n"
                                        f"▫️ 订单ID: {order_id}\n"
                                        f"▫️ 原始价格: {order_info.get('price', 'N/A')}"
                                    )
                                    await self._send_notification(msg)
                                
                                if new_status == old_status == 'NEW' and order_info.get('type') in ['STOP_LOSS', 'TAKE_PROFIT']:
                                    current_price = float(res['stopPrice'] if res.get('stopPrice') else res['price'])
                                    original_price = float(order_info.get('price', 0))
                                    
                                    if abs(current_price - original_price) > 0.00001:
                                        order_info['price'] = current_price
                                        await self._save_state()
                                        
                                        msg = (
                                            "🤖 SHEJI系统通知\n"
                                            f"📝 订单价格已被手动修改\n"
                                            f"▫️ 交易对: {order_info['symbol']}\n"
                                            f"▫️ 订单类型: {'止损单' if order_info['type'] == 'STOP_LOSS' else '止盈单'}\n"
                                            f"▫️ 订单ID: {order_id}\n"
                                            f"▫️ 原始价格: {original_price}\n"
                                            f"▫️ 新价格: {current_price}"
                                        )
                                        await self._send_notification(msg)
                                
                                if new_status == 'FILLED':
                                    if order_info.get('type') == 'TAKE_PROFIT':
                                        msg = (
                                            "🤖 SHEJI系统通知\n"
                                            f"✅ 止盈触发 {order_info['symbol']}\n"
                                            f"▫️ 触发价: {order_info['price']:.4f}\n"
                                            f"▫️ 订单ID: {order_id}"
                                        )
                                        await self._send_notification(msg)
                                    elif order_info.get('type') == 'STOP_LOSS':
                                        msg = (
                                            "🤖 SHEJI系统通知\n"
                                            f"🛑 止损触发 {order_info['symbol']}\n"
                                            f"▫️ 触发价: {order_info['price']:.4f}\n"
                                            f"▫️ 订单ID: {order_id}"
                                        )
                                        await self._send_notification(msg)
                                
                                if new_status == 'FILLED' and old_status != 'FILLED':
                                    await self._on_order_filled(order_info)
                                
                                if new_status in ['FILLED', 'CANCELED']:
                                    self.active_orders.pop(order_id, None)
                                    await self._save_state()
                                    logger.info(f"已清理结束订单 | ID: {order_id}")

                        except BinanceAPIException as e:
                            if e.code == -2013:
                                logger.info(f"交易所订单不存在，执行清理 | ID: {order_id}")
                                if order_info.get('type') in ['STOP_LOSS', 'TAKE_PROFIT']:
                                    msg = (
                                        "🤖 SHEJI系统通知\n"
                                        "⚠️ 止盈止损订单异常消失\n"
                                        f"▫️ 交易对: {order_info['symbol']}\n"
                                        f"▫️ 订单类型: {'止损单' if order_info['type'] == 'STOP_LOSS' else '止盈单'}\n"
                                        f"▫️ 订单ID: {order_id}\n"
                                        f"▫️ 原始价格: {order_info.get('price', 'N/A')}"
                                    )
                                    await self._send_notification(msg)
                                self.active_orders.pop(order_id, None)
                                await self._save_state()
                            else:
                                logger.error(f"订单查询失败 | ID: {order_id} | 错误: {e.message}")

                    symbols_to_check = set(o['symbol'] for o in self.active_orders.values())
                    for symbol in symbols_to_check:
                        positions = await self.client.futures_position_information(symbol=symbol)
                        position_exists = any(abs(float(pos['positionAmt'])) > 0 for pos in positions)

                        if not position_exists:
                            for o_id, o_info in list(self.active_orders.items()):
                                if o_info['symbol'] == symbol and o_info.get('type') in ['STOP_LOSS', 'TAKE_PROFIT']:
                                    try:
                                        await self.client.futures_cancel_order(
                                            symbol=symbol,
                                            orderId=o_id
                                        )
                                        msg = (
                                            "🤖 SHEJI系统通知\n"
                                            f"🗑 因无持仓取消挂单 {symbol}\n"
                                            f"▫️ 订单ID: {o_id}\n"
                                            f"▫️ 订单类型: {'止损单' if o_info['type'] == 'STOP_LOSS' else '止盈单'}"
                                        )
                                        await self._send_notification(msg)
                                        self.active_orders.pop(o_id)
                                    except BinanceAPIException as e:
                                        if e.code == -2011:
                                            self.active_orders.pop(o_id, None)
                            await self._save_state()

                    current_time = datetime.datetime.now(utc).timestamp()
                    if current_time - self.last_heartbeat >= HEARTBEAT_INTERVAL:
                        await update_heartbeat()
                        self.last_heartbeat = current_time

                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    logger.info("订单监控任务收到取消信号")
                    break
                except Exception as e:
                    logger.error(f"订单监控循环异常: {str(e)}", exc_info=True)
                    await asyncio.sleep(10)
        except asyncio.CancelledError:
            logger.info("订单监控任务正常终止")
        except Exception as e:
            logger.error(f"订单监控任务异常退出: {str(e)}", exc_info=True)

    async def _check_signals(self):
        logger.info("启动信号扫描循环")
        try:
            while not self._shutdown_event.is_set():
                try:
                    now = datetime.datetime.now(utc)
                    scan_flag = any([
                        (now.minute == 0) and now.second < 15,
                        (now.hour % 4 == 0) and now.minute == 0 and now.second < 15,
                        now.hour == 0 and now.minute == 0 and now.second < 15,
                        now.weekday() == 0 and now.hour == 0 and now.minute == 0 and now.second < 15
                    ])
                    
                    if scan_flag:
                        await asyncio.sleep(3)  # 关键延迟
                        logger.debug(f"信号扫描触发，开始扫描信号 | 当前时间: {now}")
                        processed_count = 0
                        for symbol in CUSTOM_SYMBOLS:
                            for interval in INTERVALS:
                                klines = await self._get_klines(symbol, interval)
                                if klines and len(klines) >= 2:
                                    prev_kline = klines[0]  # 已闭合的K线
                                    curr_kline = klines[1]  # 当前未闭合的K线
                                    
                                    # 检查prev_kline是否已闭合
                                    kline_close_time = prev_kline[6] / 1000
                                    if datetime.datetime.now(utc).timestamp() < kline_close_time:
                                        continue
                                    
                                    kline_key = f"{symbol}|{interval}|{prev_kline[0]}"
                                    
                                    if kline_key in self.processed_klines:
                                        continue
                                        
                                    signal = self._analyze_kline(prev_kline, curr_kline, symbol)
                                    if signal:
                                        signal['interval'] = interval
                                        # 新增：发送信号检测通知（无论参数是否有效）
                                        signal_msg = (
                                            "🤖 SHEJI系统通知\n"
                                            f"🔔 检测到潜在信号 | {symbol} | {interval}\n"
                                            f"▫️ 形态: 锤子线/满足条件\n"
                                            f"▫️ 建议方向: {signal['side']}\n"
                                            f"▫️ 原始参数: 入场价={signal['price']} | 止损={signal['stop_loss']} | 止盈={signal['take_profit']}"
                                        )
                                        await self._send_notification(signal_msg)
                                        
                                        if kline_key not in self.processed_signals:
                                            logger.info(f"检测到有效信号 | {symbol} | {interval} | 价格: {signal['price']}")
                                            await self._place_order(signal)
                                            self.processed_signals.add(kline_key)
                                            self.processed_klines[kline_key] = True
                                            await self._save_state()
                                            processed_count += 1
                                        else:
                                            logger.info(f"信号已处理 | {symbol} | {interval}")
                                    else:
                                        logger.debug(f"未检测到有效信号 | {symbol} | {interval}")
                                        
                        logger.info(f"本轮扫描触发信号数: {processed_count}")
                        await asyncio.sleep(60)
                    
                    current_time = datetime.datetime.now(utc).timestamp()
                    if current_time - self.last_heartbeat >= HEARTBEAT_INTERVAL:
                        await update_heartbeat()
                        self.last_heartbeat = current_time

                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    logger.info("信号扫描任务收到取消信号")
                    break
                except Exception as e:
                    logger.error(f"信号扫描异常: {str(e)}", exc_info=True)
                    if not self._shutdown_event.is_set():
                        await asyncio.sleep(10)
        except asyncio.CancelledError:
            logger.info("信号扫描任务正常终止")
        except Exception as e:
            logger.error(f"信号扫描任务异常退出: {str(e)}", exc_info=True)

    async def _check_tp_price_reached(self):
        logger.info("启动止盈价格检查循环")
        try:
            while not self._shutdown_event.is_set():
                try:
                    for order_id, order_info in list(self.active_orders.items()):
                        if order_info.get('status') != 'NEW' or not order_info.get('take_profit'):
                            continue

                        current_price = await self._get_current_price(order_info['symbol'])
                        if not current_price:
                            continue

                        if (order_info.get('side') == 'BUY' and 
                            current_price >= float(order_info['take_profit'])):
                            try:
                                await self.client.futures_cancel_order(
                                    symbol=order_info['symbol'],
                                    orderId=order_id
                                )
                                
                                msg = (
                                    "🤖 SHEJI系统通知\n"
                                    "❌ 取消未成交订单 - 已达止盈价\n"
                                    f"▫️ 交易对: {order_info['symbol']}\n"
                                    f"▫️ 订单ID: {order_id}\n"
                                    f"▫️ 当前价格: {current_price}\n"
                                    f"▫️ 止盈价格: {order_info['take_profit']}"
                                )
                                await self._send_notification(msg)
                                
                                self.active_orders.pop(order_id, None)
                                await self._save_state()
                                
                                logger.info(
                                    "已取消未成交订单 | 已达止盈价 | "
                                    f"{order_info['symbol']} | "
                                    f"订单ID: {order_id} | "
                                    f"当前价格: {current_price} | "
                                    f"止盈价: {order_info['take_profit']}"
                                )
                            except BinanceAPIException as e:
                                if e.code != -2011:  # 忽略"Unknown order sent"错误
                                    logger.error(f"取消订单失败 | {order_info['symbol']} | 订单ID: {order_id} | 错误: {e.message}")
                    
                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    logger.info("止盈价格检查任务收到取消信号")
                    break
                except Exception as e:
                    logger.error(f"检查止盈价格时发生错误: {str(e)}", exc_info=True)
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("止盈价格检查任务正常终止")
        except Exception as e:
            logger.error(f"止盈价格检查任务异常退出: {str(e)}", exc_info=True)

    async def _on_order_filled(self, order: dict):
        required_keys = ['orderId', 'symbol', 'price', 'quantity']
        if not all(key in order for key in required_keys):
            logger.error(f"订单数据不完整，缺失关键字段: {order}")
            return

        try:
            order_id = order.get('orderId')
            if not order_id or order_id not in self.active_orders:
                logger.warning(f"无效的订单数据: {order}")
                return

            self.active_orders[order_id]['status'] = 'FILLED'
            positions = await self.client.futures_position_information(symbol=order['symbol'])
            has_position = False
            for position in positions:
                if abs(float(position['positionAmt'])) > 0:
                    has_position = True
                    break
            
            if not has_position:
                self.active_orders.pop(order_id, None)
                await self._save_state()
                return

            symbol_info = self.symbols_info.get(order['symbol'], {'price_precision': 2})
            price_precision = symbol_info['price_precision']
            stop_loss = self._round_to_precision(order['stop_loss'], price_precision)
            take_profit = self._round_to_precision(order['take_profit'], price_precision)
            
            stop_params = {
                'symbol': order['symbol'],
                'side': 'SELL',
                'type': 'STOP_MARKET',
                'stopPrice': str(stop_loss),
                'closePosition': 'true'
            }
            if self.is_hedge_mode:
                stop_params['positionSide'] = 'LONG'

            sl_res = None
            try:
                sl_res = await self.client.futures_create_order(**stop_params)
                logger.info(f"止损单创建成功 | 订单ID: {sl_res['orderId']}")
                self.active_orders[sl_res['orderId']] = {
                    'symbol': order['symbol'],
                    'type': 'STOP_LOSS',
                    'status': 'NEW',
                    'price': stop_loss,
                    'orderId': sl_res['orderId']
                }
            except BinanceAPIException as e:
                logger.error(f"止损单创建失败 | {order['symbol']} | 错误: {e.message}")
                await self._send_notification(
                    "🤖 SHEJI系统通知\n"
                    f"❌ {order['symbol']} 止损单创建失败: {e.message}"
                )

            tp_params = {
                'symbol': order['symbol'],
                'side': 'SELL',
                'type': 'LIMIT',
                'price': str(take_profit),
                'quantity': str(order['quantity']),
                'timeInForce': 'GTC'
            }
            if self.is_hedge_mode:
                tp_params['positionSide'] = 'LONG'

            tp_res = None
            try:
                tp_res = await self.client.futures_create_order(**tp_params)
                logger.info(f"止盈单创建成功 | 订单ID: {tp_res['orderId']}")
                self.active_orders[tp_res['orderId']] = {
                    'symbol': order['symbol'],
                    'type': 'TAKE_PROFIT',
                    'status': 'NEW',
                    'price': take_profit,
                    'orderId': tp_res['orderId']
                }
            except BinanceAPIException as e:
                logger.error(f"止盈单创建失败 | {order['symbol']} | 错误: {e.message}")
                await self._send_notification(
                    "🤖 SHEJI系统通知\n"
                    f"❌ {order['symbol']} 止盈单创建失败: {e.message}"
                )
            
            await self._save_state()
        except Exception as e:
            logger.error(f"订单成交处理失败 | {order.get('symbol', 'UNKNOWN')} | 错误: {str(e)}", exc_info=True)
            await self._send_error_alert(e, f"订单成交处理 {order.get('symbol', 'UNKNOWN')}")

    async def shutdown(self):
        logger.info("开始关闭系统...")
        self._shutdown_event.set()
        
        try:
            await asyncio.sleep(2)  # 等待任务完成
            await self._save_state()
            if self.client:
                await self.client.close_connection()
            logger.info("系统已安全关闭")
        except Exception as e:
            logger.error(f"关闭过程中出现错误: {str(e)}", exc_info=True)

    async def run(self):
        try:
            await self.initialize()
            logger.info("系统启动完成")
            
            tasks = [
                asyncio.create_task(self._monitor_orders()),
                asyncio.create_task(self._check_signals()),
                asyncio.create_task(self._check_tp_price_reached())
            ]
            
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("系统运行被取消")
        except Exception as e:
            logger.error(f"系统运行时错误: {str(e)}", exc_info=True)
            await self._send_error_alert(e, "主循环")
        finally:
            await self.shutdown()

async def async_shutdown(loop, system):
    try:
        logger.info("开始异步关闭流程")
        system.running = False
        system._shutdown_event.set()
        await asyncio.sleep(2)
        
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        await system.shutdown()
        await loop.shutdown_asyncgens()
        logger.info("异步关闭流程完成")
    except Exception as e:
        logger.error(f"异步关闭过程中出现错误: {str(e)}", exc_info=True)

def signal_handler(sig, loop, system):
    logger.info(f"接收到信号 {signal.Signals(sig).name}({sig})")
    asyncio.create_task(async_shutdown(loop, system))

if __name__ == "__main__":
    try:
        logger.info("\n" + "="*60)
        logger.info("Sheji交易系统启动")
        
        system = ShejiTradingSystem()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                functools.partial(signal_handler, sig, loop, system)
            )
        
        loop.run_until_complete(system.run())
    except KeyboardInterrupt:
        logger.info("接收到用户中断信号")
    except Exception as e:
        logger.error(f"系统运行时发生错误: {str(e)}", exc_info=True)
    finally:
        try:
            if loop.is_running():
                loop.run_until_complete(async_shutdown(loop, system))
            loop.close()
        except Exception as e:
            logger.error(f"最终清理过程中出现错误: {str(e)}", exc_info=True)
        finally:
            logger.info("系统完全关闭")