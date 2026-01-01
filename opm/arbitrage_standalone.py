import argparse
import json
import logging
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, InvalidOperation
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import os
import requests

# ------------------------------------------------------------------
# 配置代理（仅在本地调试时使用）
USE_PROXY = os.environ.get("USE_PROXY", "true").lower() == "true"
PROXY_URL = os.environ.get('PROXY_URL', "http://127.0.0.1:8123")

if USE_PROXY:
    os.environ["HTTP_PROXY"] = PROXY_URL
    os.environ["HTTPS_PROXY"] = PROXY_URL
    os.environ["http_proxy"] = PROXY_URL
    os.environ["https_proxy"] = PROXY_URL
    print(f"✓ 代理已启用: {PROXY_URL}")
else:
    print("✓ 代理已禁用（服务器模式）")

if USE_PROXY:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    proxies = {"http": PROXY_URL, "https": PROXY_URL}
    _original_request = requests.Session.request
    
    def patched_request(self, *args, **kwargs):
        if "proxies" not in kwargs:
            kwargs["proxies"] = proxies
        return _original_request(self, *args, **kwargs)
    
    requests.Session.request = patched_request

# ------------------------------------------------------------------
# Opinion SDK 配置
try:
    from opinion_clob_sdk import Client
    
    # Opinion.Trade SDK 配置
    API_KEY = ""
    HOST = "https://proxy.opinion.trade:8443"
    CHAIN_ID = 56
    RPC_URL = "https://bsc-dataseed.binance.org"
    PRIVATE_KEY = "0x" + "1" * 64
    MULTI_SIG_ADDR = "0x0000000000000000000000000000000000000000"
    CONDITIONAL_TOKEN_ADDR = ""
    MULTISEND_ADDR = ""
    
    client = Client(
        host=HOST,
        apikey=API_KEY,
        chain_id=CHAIN_ID,
        rpc_url=RPC_URL,
        private_key=PRIVATE_KEY,
        multi_sig_addr=MULTI_SIG_ADDR,
        conditional_tokens_addr=CONDITIONAL_TOKEN_ADDR,
        multisend_addr=MULTISEND_ADDR,
    )
    
    if USE_PROXY:
        from opinion_api.api_client import ApiClient
        from opinion_api.api.prediction_market_api import PredictionMarketApi
        from opinion_api.api.user_api import UserApi
        
        client.conf.proxy = PROXY_URL
        client.api_client = ApiClient(client.conf)
        client.market_api = PredictionMarketApi(client.api_client)
        client.user_api = UserApi(client.api_client)
except ImportError:
    client = None
    # 注意：此时 logger 可能还未初始化，警告会在首次使用时输出

# 全局变量：JSON 文件路径
PAIRS_JSON_FILE: Optional[Path] = None
# 全局变量：内存中的 pairs 字典，key 为 pair_id
_pairs_dict: Dict[int, Dict] = {}
_pairs_dict_lock = threading.Lock()  # 保护 _pairs_dict 的锁

def _decimal(value) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


# Opinion SDK 辅助函数
def _get_attr(obj, *names):
    """从对象或字典中获取属性值"""
    if obj is None:
        return None
    for name in names:
        if hasattr(obj, name):
            value = getattr(obj, name)
            if value is not None:
                return value
        if isinstance(obj, dict) and name in obj:
            value = obj.get(name)
            if value is not None:
                return value
    return None


def _safe_float(value):
    """安全地将值转换为 float"""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_timestamp(ts_value):
    """标准化时间戳为毫秒"""
    if ts_value is None:
        return None
    if isinstance(ts_value, (int, float)):
        if ts_value <= 0:
            return None
        if ts_value < 1e12:
            return int(ts_value * 1000)
        return int(ts_value)
    return None


def _summarize_orderbook(book):
    """汇总 orderbook 数据，提取最佳买卖价和深度"""
    best_bid = None
    best_ask = None
    best_bid_size = None
    best_ask_size = None
    total_liquidity = 0.0
    if book is None:
        return {
            "best_bid": None,
            "best_bid_size": None,
            "best_ask": None,
            "best_ask_size": None,
            "spread": None,
            "liquidity": None,
            "orderbook_timestamp": None,
        }

    def _iter_orders(side_name):
        entries = getattr(book, side_name, None)
        if entries is None and isinstance(book, dict):
            entries = book.get(side_name)
        return entries or []

    for bid in _iter_orders("bids"):
        price = _safe_float(_get_attr(bid, "price"))
        if price is None:
            continue
        size = _safe_float(_get_attr(bid, "size"))
        if size is not None:
            total_liquidity += price * size
        if best_bid is None or price > best_bid:
            best_bid = price
            best_bid_size = size

    for ask in _iter_orders("asks"):
        price = _safe_float(_get_attr(ask, "price"))
        if price is None:
            continue
        size = _safe_float(_get_attr(ask, "size"))
        if size is not None:
            total_liquidity += price * size
        if best_ask is None or price < best_ask:
            best_ask = price
            best_ask_size = size

    spread = None
    if best_bid is not None and best_ask is not None:
        spread = best_ask - best_bid
    return {
        "best_bid": best_bid,
        "best_bid_size": best_bid_size,
        "best_ask": best_ask,
        "best_ask_size": best_ask_size,
        "spread": spread,
        "liquidity": total_liquidity if total_liquidity > 0 else None,
        "orderbook_timestamp": _normalize_timestamp(_get_attr(book, "timestamp")),
    }

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_FILE = LOG_DIR / "monitor.log"
CHECK_INTERVAL_SECONDS = 5
PROFIT_THRESHOLD = Decimal("0.01")
PM_BOOK_ENDPOINT = "https://clob.polymarket.com/book"

def setup_logger() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("arbitrage_local")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")
    handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)
    return logger
# ... setup_logger 原有代码保持不变 ...

# --- 新增：专门用于埋点的 Logger 配置 ---
TRACK_LOG_FILE = LOG_DIR / "user_track.log"

def setup_tracker() -> logging.Logger:
    """配置专门用于记录用户行为的 Logger"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    tracker = logging.getLogger("user_tracker")
    
    # 防止重复添加 Handler 导致一条日志打印多次
    if tracker.handlers:
        return tracker
        
    tracker.setLevel(logging.INFO)
    
    # 埋点日志只需要简单的格式：时间 | 动作 | 数据
    fmt = logging.Formatter("%(asctime)s | %(message)s")
    
    # 独立的文件 Handler，写入 user_track.log
    handler = logging.FileHandler(TRACK_LOG_FILE, encoding="utf-8")
    handler.setFormatter(fmt)
    tracker.addHandler(handler)
    
    # 注意：这里我们不加 console handler，避免控制台被埋点刷屏
    # tracker.addHandler(logging.StreamHandler()) 
    
    return tracker

def fetch_match_pairs(logger: logging.Logger) -> List[Dict[str, Optional[str]]]:
    """从本地 JSON 文件读取匹配的 pair 数据，提取原始字段"""
    if PAIRS_JSON_FILE is None:
        logger.error("未指定 pairs JSON 文件路径，请使用 --pairs-file 参数指定")
        return []
    
    if not PAIRS_JSON_FILE.exists():
        logger.warning("指定的 JSON 文件不存在：%s", PAIRS_JSON_FILE)
        return []
    
    try:
        with open(PAIRS_JSON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # 如果是数组格式（pairs），提取原始字段
        if isinstance(data, list):
            pairs = []
            # 原始字段列表
            original_fields = [
                "pair_id", "opinion_market_id", "polymarket_event_id",
                "pm_event_slug", "pm_market_slug",
                "pm_yes_token", "pm_no_token",
                "op_yes_token", "op_no_token",
                "op_parent_title", "op_child_title",
                "pm_url", "op_url", "end_time"
            ]
            for item in data:
                # 只提取原始字段，忽略新增的字段（id, title, description, platforms, metrics, action_status, is_monitoring）
                original_pair = {k: item.get(k) for k in original_fields if k in item}
                pairs.append(original_pair)
            logger.info("从 %s 读取 %d 条 pair 数据", PAIRS_JSON_FILE, len(pairs))
            return pairs
        else:
            logger.warning("无法识别的 JSON 文件格式，期望数组格式")
            return []
    except Exception as err:
        logger.warning("读取 JSON 文件失败：%s", err)
        return []


def _format_title(op_parent_title: Optional[str], op_child_title: Optional[str]) -> str:
    """格式化 title：使用 op_parent_title，如果 op_child_title 不同则添加括号"""
    parent = op_parent_title or ""
    child = op_child_title or ""
    if child and child != parent:
        return f"{parent}({child})"
    return parent

def _update_pair_preserve_metrics(pair: Dict, existing_pair: Dict) -> Dict:
    """更新 pair 的基础字段，但保留已有的 metrics 数据"""
    updated_pair = existing_pair.copy()
    updated_pair["id"] = f"pair_{pair.get('pair_id')}"
    updated_pair["title"] = _format_title(pair.get("op_parent_title"), pair.get("op_child_title"))
    updated_pair["description"] = f"{pair.get('op_parent_title', '')} / {pair.get('op_child_title', '')}" if pair.get("op_child_title") != pair.get("op_parent_title") else pair.get("op_parent_title", "")
    # 构建 platforms，包含每个平台的原始标题
    pm_title = pair.get("pm_market_slug") or ""
    op_title = _format_title(pair.get("op_parent_title"), pair.get("op_child_title"))
    updated_pair["platforms"] = [
        {
            "name": "Polymarket",
            "title": pm_title,
            "icon_code": "POLY",
            "url": pair.get("pm_url", "")
        },
        {
            "name": "Opinion",
            "title": op_title,
            "icon_code": "OP",
            "url": pair.get("op_url", "")
        }
    ]
    updated_pair["is_monitoring"] = existing_pair.get("is_monitoring", False)
    # 保留 end_time（如果已有）
    if "end_time" not in updated_pair:
        updated_pair["end_time"] = pair.get("end_time")
    return updated_pair


def _update_pair_with_analysis(pair_data: Dict, result_data: Optional[Dict]) -> Dict:
    """在原始 pair 数据上添加分析结果字段"""
    # 先复制原始数据
    updated_pair = pair_data.copy()
    
    # 转换结束时间戳（毫秒），保留原始字符串值
    end_time_str = pair_data.get("end_time")
    end_timestamp = None
    if end_time_str:
        try:
            from datetime import datetime, timezone
            # 如果已经是数字（时间戳），直接使用
            if isinstance(end_time_str, (int, float)):
                end_timestamp = int(end_time_str)
            # 如果是字符串，尝试解析
            elif isinstance(end_time_str, str):
                # 处理 "Z" 后缀（UTC）
                if end_time_str.endswith("Z"):
                    end_time_str = end_time_str.replace("Z", "+00:00")
                # 尝试解析
                dt = datetime.fromisoformat(end_time_str)
                # 如果是 naive datetime（没有时区信息），假设是 UTC
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                end_timestamp = int(dt.timestamp() * 1000)
        except Exception as e:
            # 转换失败，保留原始字符串值，不设置 end_timestamp
            logging.getLogger("arbitrage_local").warning("转换 end_time 失败：%s，保留原始值 %s", e, end_time_str)
            pass
    
    # 添加基础字段（无论是否有 orderbook 数据）
    updated_pair["id"] = f"pair_{pair_data.get('pair_id')}"
    updated_pair["title"] = _format_title(pair_data.get("op_parent_title"), pair_data.get("op_child_title"))
    updated_pair["description"] = f"{pair_data.get('op_parent_title', '')} / {pair_data.get('op_child_title', '')}" if pair_data.get("op_child_title") != pair_data.get("op_parent_title") else pair_data.get("op_parent_title", "")
    # 只有转换成功时才更新 end_time，否则保留原始值（已经在 copy() 中保留了）
    if end_timestamp is not None:
        updated_pair["end_time"] = end_timestamp
    # 如果 end_timestamp 是 None，不设置 updated_pair["end_time"]，保留原始值
    # 构建 platforms，包含每个平台的原始标题
    pm_title = pair_data.get("pm_market_slug") or ""
    op_title = _format_title(pair_data.get("op_parent_title"), pair_data.get("op_child_title"))
    updated_pair["platforms"] = [
        {
            "name": "Polymarket",
            "title": pm_title,
            "icon_code": "POLY",
            "url": pair_data.get("pm_url", "")
        },
        {
            "name": "Opinion",
            "title": op_title,
            "icon_code": "OP",
            "url": pair_data.get("op_url", "")
        }
    ]
    updated_pair["is_monitoring"] = False
    
    # 如果有分析结果，添加 metrics 和 action_status
    if result_data and result_data.get("processed"):
        # 为两种方案分别计算 metrics
        pm_yes_op_no_result = None
        pm_no_op_yes_result = None
        
        for res in result_data.get("results", []):
            res_type = res.get("type", "")
            if res_type == "pm_yes+op_no":
                pm_yes_op_no_result = res
            elif res_type == "pm_no+op_yes":
                pm_no_op_yes_result = res
        
        def _calculate_metrics(result):
            """计算单个方案的 metrics"""
            if not result:
                return {
                    "spread": 0.0,
                    "spread_state": "unavailable",
                    "available_capital": 0.0,
                    "estimated_profit": 0.0
                }
            
            price_gap = result.get("price_gap")
            capacity = result.get("capacity", Decimal("0"))
            profit = result.get("profit", Decimal("0"))
            
            spread = 0.0
            if price_gap is not None:
                spread = float(price_gap * 100)  # 转换为百分比，可能是负数
            
            spread_state = "unavailable"
            available_capital = 0.0
            estimated_profit = 0.0
            
            if price_gap is not None:
                if profit >= PROFIT_THRESHOLD:
                    spread_state = "profitable"
                    available_capital = float(capacity) if capacity > 0 else 0.0
                    estimated_profit = float(profit) if profit > 0 else 0.0
                else:
                    spread_state = "unavailable"
                    available_capital = float(capacity) if capacity > 0 else 0.0
                    estimated_profit = float(profit) if profit > 0 else 0.0
            
            return {
                "spread": round(spread, 2),
                "spread_state": spread_state,
                "available_capital": round(available_capital, 2),
                "estimated_profit": round(estimated_profit, 2)
            }
        
        # 计算两种方案的 metrics
        pm_yes_op_no_metrics = _calculate_metrics(pm_yes_op_no_result)
        pm_no_op_yes_metrics = _calculate_metrics(pm_no_op_yes_result)
        
        # 确定整体操作状态（如果任一方案可套利，则为 executable）
        action_status = "monitor"
        if pm_yes_op_no_metrics["spread_state"] == "profitable" or pm_no_op_yes_metrics["spread_state"] == "profitable":
            action_status = "executable"
        
        updated_pair["metrics"] = {
            "pm_yes_op_no": pm_yes_op_no_metrics,
            "pm_no_op_yes": pm_no_op_yes_metrics
        }
        updated_pair["action_status"] = action_status
    else:
        # 没有 orderbook 数据，检查是否已有 metrics，如果有就保留，否则设置基础数据
        existing_metrics = updated_pair.get("metrics")
        if existing_metrics and (existing_metrics.get("pm_yes_op_no") or existing_metrics.get("pm_no_op_yes")):
            # 保留已有的 metrics 数据
            pass
        else:
            # 设置基础数据（两种方案都设为默认值）
            updated_pair["metrics"] = {
                "pm_yes_op_no": {
                    "spread": 0.0,
                    "spread_state": "unavailable",
                    "available_capital": 0.0,
                    "estimated_profit": 0.0
                },
                "pm_no_op_yes": {
                    "spread": 0.0,
                    "spread_state": "unavailable",
                    "available_capital": 0.0,
                    "estimated_profit": 0.0
                }
            }
        updated_pair["action_status"] = updated_pair.get("action_status", "monitor")
    
    return updated_pair


def _convert_pairs_to_api_format(pairs: List[Dict]) -> Dict:
    """将 pairs 数组转换为 API 接口格式，过滤掉已结束的 pair"""
    api_list = []
    current_timestamp = int(time.time() * 1000)  # 当前时间戳（毫秒）
    
    for pair in pairs:
        # 检查 end_time，如果已过期则跳过
        end_time = pair.get("end_time")
        if end_time is not None:
            # end_time 可能是时间戳（毫秒）或字符串
            if isinstance(end_time, (int, float)):
                end_timestamp = int(end_time)
            elif isinstance(end_time, str):
                try:
                    from datetime import datetime, timezone
                    # 尝试解析字符串格式
                    if end_time.endswith("Z"):
                        end_time = end_time.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(end_time)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    end_timestamp = int(dt.timestamp() * 1000)
                except Exception:
                    # 解析失败，跳过该 pair
                    continue
            else:
                # 未知格式，跳过
                continue
            
            # 如果已过期，跳过该 pair
            if end_timestamp < current_timestamp:
                continue
        
        metrics = pair.get("metrics", {})
        
        # 获取两种方案的数据，如果没有则使用默认值
        pm_yes_op_no = metrics.get("pm_yes_op_no", {
            "spread": 0.0,
            "spread_state": "unavailable",
            "available_capital": 0.0,
            "estimated_profit": 0.0
        })
        pm_no_op_yes = metrics.get("pm_no_op_yes", {
            "spread": 0.0,
            "spread_state": "unavailable",
            "available_capital": 0.0,
            "estimated_profit": 0.0
        })
        
        api_item = {
            "id": pair.get("id", f"pair_{pair.get('pair_id')}"),
            "title": pair.get("title", ""),
            "description": pair.get("description", ""),
            "end_time": end_time,
            "platforms": pair.get("platforms", []),
            "metrics": {
                "pm_yes_op_no": pm_yes_op_no,
                "pm_no_op_yes": pm_no_op_yes
            },
            "action_status": pair.get("action_status", "monitor"),
            "is_monitoring": pair.get("is_monitoring", False)
        }
        api_list.append(api_item)
    
    return {
        "code": 200,
        "msg": "success",
        "data": {
            "total": len(api_list),
            "list": api_list
        }
    }


def save_cache(updated_pairs: List[Dict], logger: logging.Logger) -> None:
    """保存更新后的 pairs 数据到 JSON 文件"""
    if PAIRS_JSON_FILE is None:
        logger.warning("PAIRS_JSON_FILE 未设置，无法保存缓存")
        return
    try:
        with open(PAIRS_JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(updated_pairs, f, ensure_ascii=False, indent=2)
        logger.info("✓ 数据已保存到 %s，共 %d 条记录（包含原始字段和分析结果）", PAIRS_JSON_FILE, len(updated_pairs))
    except Exception as err:
        logger.warning("保存缓存失败：%s", err)


# 不再保存到本地文件，只保留在内存中


def load_cache() -> Dict:
    """从内存中的 _pairs_dict 读取并转换为 API 格式"""
    with _pairs_dict_lock:
        pairs_list = list(_pairs_dict.values())
    return _convert_pairs_to_api_format(pairs_list)


class APIHandler(BaseHTTPRequestHandler):
    """API 请求处理器"""

    def do_OPTIONS(self):
        """处理跨域预检请求"""
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_POST(self):
        if self.path == "/api/track":
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                post_data = self.rfile.read(content_length)
                event_data = json.loads(post_data.decode('utf-8'))
                
                # --- 修改点：使用专属的 tracker 记录日志 ---
                action = event_data.get('action', 'unknown')
                details = event_data.get('data', {})
                
                # 记录到 user_track.log
                # 格式化一下，方便后续提取
                msg = f"{action} | {json.dumps(details, ensure_ascii=False)}"
                setup_tracker().info(msg)
                # ---------------------------------------

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "ok"}).encode("utf-8"))
            except Exception as e:
                # 错误日志还是记在系统日志里比较好排查
                logging.getLogger("arbitrage_local").error(f"埋点失败: {e}")
                self.send_response(500)
                self.end_headers()
            return
        
        self.send_response(404)
        self.end_headers()
    def do_GET(self):
        """处理 GET 请求"""
        request_path = urlparse(self.path).path

        # 1. 前端页面：/ 或 /index.html -> 返回本地 index.html
        if request_path in ("/", "/index.html"):
            try:
                # index.html 和 arbitrage_local.py 放在同一目录
                index_path = Path(__file__).resolve().parent / "index.html"
                if not index_path.exists():
                    self.send_response(404)
                    self.send_header("Content-Type", "text/plain; charset=utf-8")
                    self.end_headers()
                    self.wfile.write("index.html not found".encode("utf-8"))
                    return

                content = index_path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return
            except Exception as err:
                self.send_response(500)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                msg = f"Error loading index.html: {err}"
                self.wfile.write(msg.encode("utf-8"))
                return

        # 2. API 接口：/api/arbitrage
        if request_path == "/api/arbitrage" or request_path == "/api/arbitrage/":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")  # 允许跨域
            self.end_headers()

            cache_data = load_cache()
            response = json.dumps(cache_data, ensure_ascii=False).encode("utf-8")
            self.wfile.write(response)
            return

        # 3. 静态资源（例如图标）
        static_dir = Path(__file__).resolve().parent
        requested_name = request_path.lstrip("/")
        if requested_name:
            alias_map = {"po.png": "pm.png"}  # 兼容历史命名
            requested_name = alias_map.get(requested_name, requested_name)
            candidate = (static_dir / requested_name).resolve()

            logging.getLogger("arbitrage_local").warning("加载静态文件 %s: %s", candidate.name, static_dir)
            # 禁止目录穿越，只允许当前目录下的静态文件
            if candidate.parent == static_dir and candidate.is_file():
                try:
                    content = candidate.read_bytes()
                    mime = "image/png" if candidate.suffix.lower() == ".png" else "application/octet-stream"
                    self.send_response(200)
                    self.send_header("Content-Type", mime)
                    self.send_header("Content-Length", str(len(content)))
                    self.end_headers()
                    self.wfile.write(content)
                    return
                except Exception as err:
                    logging.getLogger("arbitrage_local").warning("加载静态文件失败 %s: %s", candidate.name, err)

        # 3. 其他路径：404
        self.send_response(404)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        error_response = json.dumps({
            "code": 404,
            "msg": "Not Found",
            "data": None
        }).encode("utf-8")
        self.wfile.write(error_response)

    def log_message(self, format, *args):
        """重写日志方法，使用更简洁的格式"""
        pass
def run_api_server(port: int = 8080, host: str = "0.0.0.0", logger: Optional[logging.Logger] = None):
    """启动 API 服务器"""
    if logger:
        logger.info("启动 API 服务器: http://%s:%d/api/arbitrage", host, port)
    else:
        print(f"API 服务器已启动: http://{host}:{port}/api/arbitrage")
    server_address = (host, port)
    httpd = HTTPServer(server_address, APIHandler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        if logger:
            logger.info("API 服务器已停止")
        else:
            print("\nAPI 服务器已停止")
        httpd.server_close()


def _fetch_opinion_snapshot(token_id: str) -> Dict[str, Optional[Decimal]]:
    if client is None:
        logging.getLogger("arbitrage_local").warning("opinion_clob_sdk 未安装，无法获取 Opinion orderbook")
        return {"best_bid": None, "best_ask": None}
    try:
        response = client.get_orderbook(str(token_id))
        result = getattr(response, "result", {})
        summary = _summarize_orderbook(result)
        return {
            "best_bid": _decimal(summary.get("best_bid")),
            "best_ask": _decimal(summary.get("best_ask")),
            "best_ask_size": _decimal(summary.get("best_ask_size")),
        }
    except Exception:
        return {"best_bid": None, "best_ask": None}


def _level_get_attr(level: Any, name: str) -> Optional[Any]:
    if isinstance(level, dict):
        return level.get(name)
    return getattr(level, name, None)


def _parse_levels(levels: list) -> List[Tuple[Decimal, Decimal]]:
    parsed = []
    for level in levels:
        price = _decimal(_level_get_attr(level, "price"))
        size = _decimal(_level_get_attr(level, "size"))
        if price is None or size is None:
            continue
        parsed.append((price, size))
    return sorted(parsed, key=lambda item: item[0])


def _extract_asks(result: Any) -> List[Any]:
    asks = []
    if isinstance(result, dict):
        asks = result.get("asks") or []
    else:
        asks = getattr(result, "asks", None)
        if asks is None:
            asks = getattr(result, "ask", None)
        if asks is None:
            asks = []
    return asks


def _fetch_opinion_levels(token: Optional[str], timeout: float = 2.0) -> List[Tuple[Decimal, Decimal]]:
    """获取 Opinion orderbook，带超时控制"""
    if not token or client is None:
        return []
    
    start_time = time.time()
    result_container = {"response": None, "error": None}
    
    def _fetch():
        try:
            result_container["response"] = client.get_orderbook(str(token))
        except Exception as err:
            result_container["error"] = err
    
    # 使用线程执行，并设置超时
    thread = threading.Thread(target=_fetch, daemon=True)
    thread.start()
    thread.join(timeout=timeout)
    elapsed = time.time() - start_time
    
    if thread.is_alive():
        # 线程仍在运行，说明超时了
        logging.getLogger("arbitrage_local").warning(
            "拉取 Opinion orderbook %s 超时（%.3f秒，超时限制%.1f秒）", token, elapsed, timeout
        )
        return []
    
    if result_container["error"]:
        logging.getLogger("arbitrage_local").warning(
            "拉取 Opinion orderbook %s 失败（耗时%.3f秒）：%s", token, elapsed, result_container["error"]
        )
        return []
    
    try:
        response = result_container["response"]
        if response is None:
            logging.getLogger("arbitrage_local").debug(
                "拉取 Opinion orderbook %s 返回 None（耗时%.3f秒）", token, elapsed
            )
            return []
        errno = getattr(response, "errno", None)
        if errno not in (None, 0):
            errmsg = getattr(response, "errmsg", None)
            logging.getLogger("arbitrage_local").warning(
                "Opinion orderbook %s 返回 errno=%s errmsg=%s（耗时%.3f秒）", token, errno, errmsg, elapsed
            )
            return []
        result = getattr(response, "result", {})
        asks = _extract_asks(result)
        parsed = _parse_levels(asks)
        logging.getLogger("arbitrage_local").info(
            "拉取 Opinion orderbook %s 成功（耗时%.3f秒，%d个价格档位）", token, elapsed, len(parsed)
        )
        return parsed
    except Exception as err:
        logging.getLogger("arbitrage_local").warning(
            "解析 Opinion orderbook %s 失败（耗时%.3f秒）：%s", token, elapsed, err
        )
    return []


def _match_levels(
    pm_levels: List[Tuple[Decimal, Decimal]],
    op_levels: List[Tuple[Decimal, Decimal]],
) -> Tuple[Decimal, Decimal, Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
    """
    模拟吃单计算套利空间
    逻辑：从最优价格开始，逐档吃单，只要 pm_price + op_price < 1 就继续
    """
    # 确保已经排序（价格从低到高）
    pm_sorted = sorted(pm_levels, key=lambda x: x[0]) if pm_levels else []
    op_sorted = sorted(op_levels, key=lambda x: x[0]) if op_levels else []
    
    if not pm_sorted or not op_sorted:
        pm_best = pm_sorted[0][0] if pm_sorted else None
        op_best = op_sorted[0][0] if op_sorted else None
        price_gap = Decimal("1") - (pm_best + op_best) if (pm_best and op_best) else None
        return Decimal("0"), Decimal("0"), pm_best, op_best, price_gap
    
    pm_best_price = pm_sorted[0][0]
    op_best_price = op_sorted[0][0]
    
    total_capacity = Decimal("0")
    total_profit = Decimal("0")
    pm_idx = 0
    op_idx = 0
    max_iterations = len(pm_sorted) + len(op_sorted)  # 防止无限循环
    iteration = 0
    
    while pm_idx < len(pm_sorted) and op_idx < len(op_sorted) and iteration < max_iterations:
        iteration += 1
        pm_price, pm_size = pm_sorted[pm_idx]
        op_price, op_size = op_sorted[op_idx]
        
        # 如果价格和 >= 1，没有套利空间，停止
        if pm_price + op_price >= Decimal("1"):
            break
        
        # 简单策略：每次吃 min(pm_size, op_size) 的数量
        qty = min(pm_size, op_size)
        if qty <= 0:
            # 如果某个档位数量为0，跳过
            if pm_size <= 0:
                pm_idx += 1
            if op_size <= 0:
                op_idx += 1
            continue
        
        # 计算投入和收益
        investment = qty * (pm_price + op_price)  # 总投入
        value = qty * Decimal("1")  # 最终价值（yes + no = 1）
        profit = value - investment  # 利润
        
        total_capacity += investment
        total_profit += profit
        
        # 更新剩余数量
        pm_remaining = pm_size - qty
        op_remaining = op_size - qty
        
        # 更新列表中的数量
        pm_sorted[pm_idx] = (pm_price, pm_remaining)
        op_sorted[op_idx] = (op_price, op_remaining)
        
        # 如果某个档位吃完了，移到下一个
        if pm_remaining <= 0:
            pm_idx += 1
        if op_remaining <= 0:
            op_idx += 1
    
    if iteration >= max_iterations:
        logging.getLogger("arbitrage_local").warning(
            "_match_levels 达到最大迭代次数 %d，可能存在问题", max_iterations
        )
    
    price_gap = Decimal("1") - (pm_best_price + op_best_price)
    return total_capacity, total_profit, pm_best_price, op_best_price, price_gap


def _fetch_pm_levels(token_id: Optional[str], logger: logging.Logger) -> List[Tuple[Decimal, Decimal]]:
    if not token_id:
        return []
    start_time = time.time()
    try:
        resp = requests.get(PM_BOOK_ENDPOINT, params={"token_id": token_id}, timeout=2)
        resp.raise_for_status()
        data = resp.json()
        asks = data.get("asks") or []
        parsed = _parse_levels(asks)
        elapsed = time.time() - start_time
        logger.info(
            "拉取 Polymarket orderbook %s 成功（耗时%.3f秒，%d个价格档位）", token_id, elapsed, len(parsed)
        )
        return parsed
    except Exception as err:
        elapsed = time.time() - start_time
        logger.warning("拉取 Polymarket orderbook %s 失败（耗时%.3f秒）：%s", token_id, elapsed, err)
    return []


def analyze(logger: logging.Logger) -> None:
    """分析套利机会，不依赖数据库"""
    try:
        logger.info("=== analyze 函数开始执行 ===")
        pairs = fetch_match_pairs(logger)
        if not pairs:
            logger.info("当前无有效匹配，稍后重试。")
            return
        logger.info("准备分析 %d 条匹配记录", len(pairs))
        total_pairs = len(pairs)
        # 使用线程安全的计数器
        processed_count = [0]  # 使用列表以便在闭包中修改
        arbitrage_count = [0]
        count_lock = threading.Lock()
        
        # 不再从文件加载，只使用内存数据
        
        # 初始化内存中的 pairs_dict（如果不存在）
        logger.info("初始化内存中的 pairs_dict")
        with _pairs_dict_lock:
            added_count = 0
            for pair in pairs:
                pair_id = pair.get("pair_id")
                if pair_id and pair_id not in _pairs_dict:
                    _pairs_dict[pair_id] = pair
                    added_count += 1
            logger.info("初始化完成，新增 %d 个 pairs，内存中共有 %d 个 pairs", added_count, len(_pairs_dict))

        def fetch_orderbooks_for_pair(pair_data):
            """串行拉取单个 pair 的 orderbook 数据，总超时 10 秒"""
            pair, pair_id = pair_data
            yes_token = pair.get("pm_yes_token")
            no_token = pair.get("pm_no_token")
            op_yes_token = pair.get("op_yes_token")
            op_no_token = pair.get("op_no_token")
            
            start_time = time.time()
            max_total_time = 10.0  # 总超时 10 秒
            orderbook_results = {}
            
            # 串行拉取 4 个 orderbook
            tasks = [
                ("pm_yes", yes_token),
                ("pm_no", no_token),
                ("op_yes", op_yes_token),
                ("op_no", op_no_token),
            ]
            
            for key, token in tasks:
                elapsed = time.time() - start_time
                if elapsed >= max_total_time:
                    logger.warning("Pair=%d 拉取超时（%.2f秒），跳过 %s", pair_id, elapsed, key)
                    orderbook_results[key] = []
                    break
                
                if not token:
                    orderbook_results[key] = []
                    continue
                
                try:
                    remaining_time = max_total_time - elapsed
                    if key.startswith("pm_"):
                        orderbook_results[key] = _fetch_pm_levels(token, logger)
                    else:
                        # Opinion 请求使用剩余时间作为超时，但最多 2 秒
                        timeout = min(2.0, remaining_time)
                        orderbook_results[key] = _fetch_opinion_levels(token, timeout=timeout)
                except Exception as err:
                    logger.warning("Pair=%d 获取 %s orderbook 失败: %s", pair_id, key, err)
                    orderbook_results[key] = []
            
            # 确保所有 key 都有结果
            for key, _ in tasks:
                if key not in orderbook_results:
                    orderbook_results[key] = []
            
            elapsed = time.time() - start_time
            logger.info("Pair=%d 所有 orderbook 拉取完成，总耗时%.3f秒", pair_id, elapsed)
            
            return pair_id, orderbook_results
        
        def calculate_arbitrage(pair_data, orderbook_results):
            """计算单个 pair 的套利机会"""
            pair, pair_id = pair_data
            logger.info("Pair=%d 开始计算套利", pair_id)
            
            op_url = pair.get("op_url") or "opinion_url"
            pm_url = pair.get("pm_url") or "polymarket_url"
            op_parent_title = pair.get("op_parent_title") or ""
            op_child_title = pair.get("op_child_title") or ""
            
            pm_yes_levels = orderbook_results.get("pm_yes", [])
            pm_no_levels = orderbook_results.get("pm_no", [])
            op_yes_levels = orderbook_results.get("op_yes", [])
            op_no_levels = orderbook_results.get("op_no", [])
            
            logger.info("Pair=%d orderbook 数据: pm_yes=%d档位, pm_no=%d档位, op_yes=%d档位, op_no=%d档位", 
                       pair_id, len(pm_yes_levels), len(pm_no_levels), len(op_yes_levels), len(op_no_levels))
            
            if not pm_yes_levels and not pm_no_levels:
                logger.warning("Pair=%d PM orderbook 为空", pair_id)
                return {"processed": False, "reason": "PM orderbook 为空"}
            if not op_yes_levels and not op_no_levels:
                logger.warning("Pair=%d OP orderbook 为空", pair_id)
                return {"processed": False, "reason": "OP orderbook 为空"}
            
            calc_start = time.time()
            arbitrage_found = False
            results = []
            
            if pm_yes_levels and op_no_levels:
                logger.info("Pair=%d 开始计算 pm_yes+op_no 方案", pair_id)
                try:
                    capacity, profit, pm_price, op_price, price_gap = _match_levels(
                        pm_yes_levels, op_no_levels
                    )
                    logger.info("Pair=%d pm_yes+op_no 计算结果: capacity=%s profit=%s pm_price=%s op_price=%s price_gap=%s", 
                               pair_id, capacity, profit, pm_price, op_price, price_gap)
                    results.append({
                        "type": "pm_yes+op_no",
                        "pm_price": pm_price,
                        "op_price": op_price,
                        "price_gap": price_gap,
                        "profit": profit,
                        "capacity": capacity,
                    })
                    if profit >= PROFIT_THRESHOLD:
                        arbitrage_found = True
                        results[-1]["arbitrage"] = True
                        logger.info("Pair=%d pm_yes+op_no 发现套利机会！profit=%s", pair_id, profit)
                except Exception as err:
                    logger.error("Pair=%d 计算 pm_yes+op_no 失败：%s", pair_id, err, exc_info=True)
            
            if pm_no_levels and op_yes_levels:
                logger.info("Pair=%d 开始计算 pm_no+op_yes 方案", pair_id)
                try:
                    capacity, profit, pm_price, op_price, price_gap = _match_levels(
                        pm_no_levels, op_yes_levels
                    )
                    logger.info("Pair=%d pm_no+op_yes 计算结果: capacity=%s profit=%s pm_price=%s op_price=%s price_gap=%s", 
                               pair_id, capacity, profit, pm_price, op_price, price_gap)
                    results.append({
                        "type": "pm_no+op_yes",
                        "pm_price": pm_price,
                        "op_price": op_price,
                        "price_gap": price_gap,
                        "profit": profit,
                        "capacity": capacity,
                    })
                    if profit >= PROFIT_THRESHOLD:
                        arbitrage_found = True
                        results[-1]["arbitrage"] = True
                        logger.info("Pair=%d pm_no+op_yes 发现套利机会！profit=%s", pair_id, profit)
                except Exception as err:
                    logger.error("Pair=%d 计算 pm_no+op_yes 失败：%s", pair_id, err, exc_info=True)
            
            elapsed_calc = time.time() - calc_start
            logger.info("Pair=%d 套利计算完成，计算耗时%.3f秒，结果数=%d", pair_id, elapsed_calc, len(results))
            
            return {
                "processed": True,
                "pair_id": pair_id,
                "op_url": op_url,
                "pm_url": pm_url,
                "op_parent_title": op_parent_title,
                "op_child_title": op_child_title,
                "arbitrage_found": arbitrage_found,
                "results": results,
            }
        
        def process_calculation(pair_id, pair, orderbook_results):
            """处理单个 pair 的计算和保存"""
            logger.info("Pair=%d 工作线程开始处理", pair_id)
            try:
                logger.info("Pair=%d 调用 calculate_arbitrage", pair_id)
                result = calculate_arbitrage((pair, pair_id), orderbook_results)
                logger.info("Pair=%d calculate_arbitrage 返回: processed=%s", pair_id, result.get("processed") if result else None)
                
                # 更新内存中的 pairs_dict
                logger.info("Pair=%d 开始更新内存中的 pairs_dict", pair_id)
                with _pairs_dict_lock:
                    existing_pair = _pairs_dict.get(pair_id, pair)
                    if result and result.get("processed"):
                        updated_pair = _update_pair_with_analysis(pair, result)
                        logger.info("Pair=%d 使用分析结果更新 pair", pair_id)
                    else:
                        existing_metrics = existing_pair.get("metrics")
                        if existing_metrics and (existing_metrics.get("pm_yes_op_no") or existing_metrics.get("pm_no_op_yes")):
                            updated_pair = _update_pair_preserve_metrics(pair, existing_pair)
                            logger.info("Pair=%d 保留已有 metrics 数据", pair_id)
                        else:
                            updated_pair = _update_pair_with_analysis(pair, None)
                            logger.info("Pair=%d 设置基础数据", pair_id)
                    _pairs_dict[pair_id] = updated_pair
                logger.info("Pair=%d 内存更新完成", pair_id)
                
                # 记录结果
                if result and result.get("processed"):
                    op_url = result["op_url"]
                    pm_url = result["pm_url"]
                    op_parent_title = result["op_parent_title"]
                    op_child_title = result["op_child_title"]
                    
                    for res in result["results"]:
                        logger.info(
                            "Pair=%d (%s): pm_price=%s op_price=%s price_gap=%s profit=%s capacity=%s | %s / %s",
                            pair_id,
                            res["type"],
                            res["pm_price"],
                            res["op_price"],
                            res["price_gap"],
                            res["profit"],
                            res["capacity"],
                            op_parent_title,
                            op_child_title,
                        )
                        if res.get("arbitrage"):
                            logger.info(
                                "✓ ARBITRAGE pair=%d: %s | pm_price=%s op_price=%s op_parent=%s op_child=%s capacity=%s price_gap=%s expected_profit=%s | %s / %s",
                                pair_id,
                                res["type"],
                                res["pm_price"],
                                res["op_price"],
                                op_parent_title,
                                op_child_title,
                                res["capacity"],
                                res["price_gap"],
                                res["profit"],
                                pm_url,
                                op_url,
                            )
                    logger.info("Pair=%d 处理完成，返回 True", pair_id)
                    return True
                logger.info("Pair=%d 处理完成，返回 False（未处理）", pair_id)
                return False
            except Exception as err:
                logger.error("Pair=%d 处理失败：%s", pair_id, err, exc_info=True)
                return False
        
        # 创建队列用于传递拉取的数据
        logger.info("创建队列和事件对象")
        orderbook_queue = queue.Queue()
        stop_event = threading.Event()
        logger.info("队列和事件对象创建完成")
        
        def calculation_worker():
            """计算工作线程，持续从队列获取数据并计算"""
            worker_name = threading.current_thread().name
            logger.debug("计算工作线程 %s 启动", worker_name)
            while not stop_event.is_set() or not orderbook_queue.empty():
                try:
                    # 从队列获取数据，超时 1 秒，避免一直阻塞
                    item = orderbook_queue.get(timeout=1.0)
                    if item is None:  # 停止信号
                        logger.debug("计算工作线程 %s 收到停止信号", worker_name)
                        break
                    
                    pair_id, pair, orderbook_results = item
                    logger.info("计算工作线程 %s 从队列获取到 Pair=%d，开始处理", worker_name, pair_id)
                    if process_calculation(pair_id, pair, orderbook_results):
                        logger.info("计算工作线程 %s 完成 Pair=%d 的处理", worker_name, pair_id)
                        with count_lock:
                            processed_count[0] += 1
                        # 检查是否有套利机会
                        with _pairs_dict_lock:
                            pair_data = _pairs_dict.get(pair_id)
                            if pair_data:
                                metrics = pair_data.get("metrics", {})
                                if (metrics.get("pm_yes_op_no", {}).get("spread_state") == "profitable" or
                                    metrics.get("pm_no_op_yes", {}).get("spread_state") == "profitable"):
                                    with count_lock:
                                        arbitrage_count[0] += 1
                    
                    orderbook_queue.task_done()
                except queue.Empty:
                    continue
                except Exception as err:
                    logger.warning("计算工作线程异常：%s", err)
        
        # 启动计算工作线程（持续运行）
        logger.info("启动计算工作线程（持续运行，10个线程）")
        calc_threads = []
        for i in range(10):
            thread = threading.Thread(target=calculation_worker, daemon=True, name=f"calc_worker_{i}")
            thread.start()
            calc_threads.append(thread)
        
        try:
            # 死循环滚动拉取订单簿
            logger.info("开始滚动拉取订单簿（死循环）")
            cycle_count = 0
            
            while True:
                cycle_count += 1
                logger.info("=== 开始第 %d 轮拉取 ===", cycle_count)
                
                for idx, pair in enumerate(pairs, 1):
                    pair_id = pair.get("pair_id")
                    op_parent_title = pair.get("op_parent_title") or ""
                    op_child_title = pair.get("op_child_title") or ""
                    logger.info("拉取 Pair=%d (%d/%d): %s / %s", pair_id, idx, total_pairs, op_parent_title, op_child_title)
                    
                    if idx % 10 == 0 or idx == total_pairs or idx == 1:
                        logger.info("拉取进度: %d/%d (%.1f%%)", idx, total_pairs, 100.0 * idx / total_pairs)
                    
                    try:
                        # 串行拉取订单簿（10秒超时）
                        pair_id, orderbook_results = fetch_orderbooks_for_pair((pair, pair_id))
                        
                        # 拉取完成后立即放入队列，等待计算线程处理
                        orderbook_queue.put((pair_id, pair, orderbook_results))
                        logger.info("Pair=%d 订单簿拉取完成，已放入队列等待计算（队列大小：%d）", pair_id, orderbook_queue.qsize())
                        
                    except Exception as err:
                        logger.warning("拉取 Pair=%d 的 orderbook 失败：%s", pair_id, err)
                        # 即使拉取失败，也放入队列（使用空的 orderbook_results）
                        orderbook_queue.put((pair_id, pair, {}))
                
                logger.info("第 %d 轮拉取完成，等待 %d 秒后开始下一轮", cycle_count, CHECK_INTERVAL_SECONDS)
                time.sleep(CHECK_INTERVAL_SECONDS)
                
        except KeyboardInterrupt:
            logger.info("收到停止信号，正在停止...")
        finally:
            # 停止计算线程
            stop_event.set()
            # 等待队列处理完成
            orderbook_queue.join()
            # 等待所有计算线程完成
            for thread in calc_threads:
                thread.join(timeout=5.0)
            logger.info("所有任务已完成")
        
        logger.info("分析完成: 总计 %d 条，已处理 %d 条，发现 %d 个套利机会", total_pairs, processed_count[0], arbitrage_count[0])
    except Exception as err:
        logger.error("analyze 函数执行失败：%s", err, exc_info=True)
        raise


def start_loop(api_port: int = 8080, api_host: str = "0.0.0.0"):
    logger = setup_logger()

    def runner():
        while True:
            analyze(logger)
            time.sleep(CHECK_INTERVAL_SECONDS)

    # 启动套利分析线程
    arbitrage_thread = threading.Thread(
        target=runner, daemon=True, name="arbitrage_local_monitor"
    )
    arbitrage_thread.start()
    
    # 启动 API 服务器线程（默认启动）
    api_thread = threading.Thread(
        target=lambda: run_api_server(port=api_port, host=api_host, logger=logger),
        daemon=True,
        name="api_server"
    )
    api_thread.start()
    logger.info("API 服务器线程已启动，端口: %d", api_port)
    
    return arbitrage_thread


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="本地套利监控（从 JSON 文件读取 pair 数据，可选启动 API 服务器）")
    parser.add_argument(
        "--pairs-file",
        type=str,
        required=True,
        help="pairs JSON 文件路径（例如：strategy/pairs.json）",
    )
    parser.add_argument(
        "--api-port",
        type=int,
        default=8080,
        help="API 服务器端口（默认: 8080）",
    )
    parser.add_argument(
        "--api-host",
        type=str,
        default="0.0.0.0",
        help="API 服务器地址（默认: 0.0.0.0）",
    )
    args = parser.parse_args()
    
    # 设置 JSON 文件路径
    PAIRS_JSON_FILE = Path(args.pairs_file).resolve()
    
    logger = setup_logger()
    logger.info("使用 pairs 文件：%s", PAIRS_JSON_FILE)
    logger.info("API 服务器将启动在 http://%s:%d/api/arbitrage", args.api_host, args.api_port)
    
    start_loop(api_port=args.api_port, api_host=args.api_host)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        logging.getLogger("arbitrage_local").info("套利监控收到退出信号。")
