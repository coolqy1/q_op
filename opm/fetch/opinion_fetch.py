import json
import logging
import os
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from db.opinion_market import (
    load_token_market_map,
    load_token_set,
    persist_opinion_markets,
)
from db.opinion_orderbook import persist_orderbook_snapshot
from opinion_clob_sdk import Client
from opinion_clob_sdk.model import TopicStatusFilter, TopicType

# ------------------------------------------------------------------
# 配置代理（仅在本地调试时使用）
USE_PROXY = os.environ.get("USE_PROXY", "True").lower() == "true"
PROXY_URL = os.environ.get('PROXY_URL', "http://127.0.0.1:8123")

if USE_PROXY:
    os.environ["HTTP_PROXY"] = PROXY_URL
    os.environ["HTTPS_PROXY"] = PROXY_URL
    os.environ["http_proxy"] = PROXY_URL
    os.environ["https_proxy"] = PROXY_URL
    print(f"✓ 代理已启用: {PROXY_URL}")
else:
    print("✓ 代理已禁用（服务器模式）")

    # 默认情况不设置代理

if USE_PROXY:
    import requests
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
# Opinion.Trade SDK 配置
API_KEY = ""
HOST = "https://proxy.opinion.trade:8443"
CHAIN_ID = 56
RPC_URL = "https://bsc-dataseed.binance.org"
PRIVATE_KEY = "0x" + "1" * 64
MULTI_SIG_ADDR = "0x0000000000000000000000000000000000000000"
CONDITIONAL_TOKEN_ADDR = ""
MULTISEND_ADDR = ""
MARKET_FETCH_LIMIT = int(os.environ.get("MARKET_FETCH_LIMIT", "20"))
ORDERBOOK_WORKERS = int(os.environ.get("ORDERBOOK_WORKERS", "5"))

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

# ------------------------------------------------------------------
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
LOG_FILE = os.path.join(LOG_DIR, "opinion_fetch.log")
DATA_LOG_FILE = os.path.join(LOG_DIR, "market_fetch.jsonl")

LOGGER = logging.getLogger("opinion_fetch")

def ensure_log_dir():
    os.makedirs(LOG_DIR, exist_ok=True)

def setup_logger():
    ensure_log_dir()
    if LOGGER.handlers:
        return LOGGER

    formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    LOGGER.setLevel(logging.DEBUG)
    LOGGER.addHandler(console)
    LOGGER.addHandler(file_handler)
    return LOGGER

def log_fetch_summary(market_count, token_id_count):
    ensure_log_dir()
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "market_count": market_count,
        "token_id_count": token_id_count,
    }
    try:
        with open(DATA_LOG_FILE, "a", encoding="utf-8") as fh:
            json.dump(entry, fh, ensure_ascii=False)
            fh.write("\n")
        LOGGER.info(
            "已抓取 %d 条市场候选项，涉及 %d 个 token，并写入日志：%s",
            market_count,
            token_id_count,
            DATA_LOG_FILE,
        )
    except OSError as err:
        LOGGER.warning("写入市场日志失败：%s", err)


def log_market_response(topic_type, page, response):
    """打印 Opinion.Trade get_markets 的原始响应"""
    result = getattr(response, "result", None)
    LOGGER.info(
        "市场响应 [%s page %s] errno=%s errmsg=%s result=%s",
        topic_type.name,
        page,
        getattr(response, "errno", None),
        getattr(response, "errmsg", None),
        _format_response_result(result),
    )


def _format_response_result(result):
    """尝试把 result 转为 JSON，可递归处理常见类型"""
    if result is None:
        return "null"
    try:
        return json.dumps(_serialize(result), ensure_ascii=False)
    except Exception:
        return str(result)


def _serialize(obj):
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (list, tuple, set)):
        return [_serialize(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if hasattr(obj, "__dict__"):
        data = {}
        for key, value in obj.__dict__.items():
            if not key.startswith("_"):
                data[key] = _serialize(value)
        return data
    return str(obj)

# ------------------------------------------------------------------
def _get_attr(obj, *names):
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

def _collect_token_ids(source, token_set):
    if source is None:
        return
    yes_token_id = _get_attr(source, "yesTokenId", "yes_token_id")
    no_token_id = _get_attr(source, "noTokenId", "no_token_id")
    if yes_token_id:
        token_set.add(str(yes_token_id))
    if no_token_id:
        token_set.add(str(no_token_id))

def _is_resolved(obj):
    status_enum = _get_attr(obj, "statusEnum", "status_enum")
    if status_enum and str(status_enum).strip().lower() == "resolved":
        return True
    status_value = _get_attr(obj, "status", "statusValue", "status_value")
    if status_value and str(status_value).strip().lower() == "resolved":
        return True
    return False

def _normalize_timestamp(ts_value):
    if ts_value is None:
        return None
    if isinstance(ts_value, (int, float)):
        if ts_value <= 0:
            return None
        if ts_value < 1e12:
            return int(ts_value * 1000)
        return int(ts_value)
    return None

def _safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _summarize_orderbook(book):
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


def _collect_market_candidates():
    pending_entries = []
    token_ids = set()
    parent_map = {}
    parent_children_flag = {}
    topic_types = [TopicType.BINARY, TopicType.CATEGORICAL]
    for topic_type in topic_types:
        page = 1
        while True:
            response = client.get_markets(
                topic_type=topic_type,
                status=TopicStatusFilter.ACTIVATED,
                page=page,
                limit=MARKET_FETCH_LIMIT,
            )
            log_market_response(topic_type, page, response)
            if response.errno != 0:
                if page == 1:
                    raise RuntimeError(f"获取 {topic_type.name} 市场失败: {response.errmsg}")
                break
            markets = response.result.list
            if not markets:
                break
            for market in markets:
                if _is_resolved(market):
                    continue
                _collect_token_ids(market, token_ids)
                market_id = str(_get_attr(market, "marketId", "market_id") or "")
                key = (topic_type.name, market_id)
                child_markets = _get_attr(market, "child_markets", "childMarkets")
                parent_map[key] = {"topic_type": topic_type.name, "market": market}
                parent_children_flag[key] = bool(child_markets)
                if topic_type == TopicType.CATEGORICAL:
                    child_markets = _get_attr(market, "child_markets", "childMarkets")
                    if child_markets:
                        for child in child_markets:
                            if _is_resolved(child):
                                continue
                            _collect_token_ids(child, token_ids)
                            pending_entries.append((topic_type.name, market, child))
                        continue
                pending_entries.append((topic_type.name, market, None))
            if len(markets) < MARKET_FETCH_LIMIT:
                break
            page += 1
    return pending_entries, token_ids, parent_map, parent_children_flag

def fetch_market_sources():
    """返回当前可用的市场候选项和需要拉取的 token_id"""
    return _collect_market_candidates()


def _fetch_orderbook_for_token(token_id, mapped_market_id=None):
    token_key = str(token_id)
    fetch_time = datetime.now(timezone.utc)
    try:
        response = client.get_orderbook(token_key)
    except Exception as err:
        LOGGER.warning("拉取订单簿 %s 失败: %s", token_key, err)
        return token_key, None, None, None, None

    errno = getattr(response, "errno", None)
    errmsg = getattr(response, "errmsg", None)
    result = getattr(response, "result", None)
    formatted = _format_response_result(result)
    summary = None
    if errno == 0 and result is not None:
        summary = _summarize_orderbook(result)
        market_ref = mapped_market_id or _get_attr(result, "market", "marketId", "market_id")
        persist_orderbook_snapshot(
            token_id=token_key,
            market_id=market_ref,
            metrics=summary,
            raw_response=result,
            fetch_time=fetch_time,
            logger=LOGGER,
        )
    return token_key, errno, errmsg, formatted, summary


def fetch_orderbooks(token_ids, token_market_map=None):
    """按 token_id 列并发拉取订单簿快照并打印"""
    if not token_ids:
        LOGGER.info("当前未发现 token_id，跳过订单簿抓取")
        return

    unique_ids = [str(tid) for tid in set(token_ids) if tid]
    if not unique_ids:
        LOGGER.info("过滤后未剩余有效 token_id，跳过订单簿抓取")
        return
    
    market_map = {str(k): v for k, v in (token_market_map or {}).items() if k is not None}
    LOGGER.info("准备拉取 %d 个 token 订单簿，最大并发 %d", len(unique_ids), ORDERBOOK_WORKERS)
    max_workers = max(1, min(ORDERBOOK_WORKERS, len(unique_ids)))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_token = {
            executor.submit(
                _fetch_orderbook_for_token,
                token_key,
                market_map.get(token_key),
            ): token_key
            for token_key in unique_ids
        }
        for future in as_completed(future_to_token):
            token_id = future_to_token[future]
            try:
                token_key, errno, errmsg, formatted, summary = future.result()
                LOGGER.info(
                    "订单簿 [%s] errno=%s errmsg=%s result=%s",
                    token_key,
                    errno,
                    errmsg,
                    formatted,
                )
                if summary:
                    LOGGER.info(
                        "订单簿 [%s] 摘要 %s",
                        token_key,
                        json.dumps(summary, ensure_ascii=False),
                    )
            except Exception as err:
                LOGGER.warning("处理 token_id=%s 的订单簿结果失败：%s", token_id, err)


def fetch_markets():
    LOGGER.info("开始拉取市场数据（limit=%s）", MARKET_FETCH_LIMIT)
    start = datetime.now(timezone.utc)
    pending_entries, token_ids, parent_map, parent_children_flag = fetch_market_sources()
    LOGGER.info(
        "已经抓取到 %d 个市场候选项，涉及 %d 个 token_id",
        len(pending_entries),
        len(token_ids),
    )
    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    LOGGER.info("共获取 %d 条市场数据，耗时 %.2f 秒", len(pending_entries), elapsed)
    log_fetch_summary(len(pending_entries), len(token_ids))
    persist_opinion_markets(
        pending_entries,
        parent_map,
        parent_children_flag,
        datetime.now(timezone.utc),
        logger=LOGGER,
    )
    return pending_entries


def fetch_latest_orderbooks():
    """按数据库里现有的 token_id 抓取订单簿并打印"""
    token_market_map = load_token_market_map()
    if not token_market_map:
        LOGGER.info("数据库中未找到 token -> market 映射，跳过订单簿抓取")
        return
    fetch_orderbooks(list(token_market_map.keys()), token_market_map)


def start_background_task(func, interval_seconds, name):
    def runner():
        while True:
            try:
                LOGGER.info("开始 %s", name)
                func()
            except Exception as err:
                LOGGER.exception("%s 任务失败：%s", name, err)
            time.sleep(interval_seconds)

    thread = threading.Thread(target=runner, daemon=True, name=name)
    thread.start()

if __name__ == "__main__":
    setup_logger()
    fetch_markets()
    start_background_task(fetch_markets, 600, "fetch_markets")
    start_background_task(fetch_latest_orderbooks, 300, "fetch_orderbooks")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        LOGGER.info("收到退出信号，结束循环")
