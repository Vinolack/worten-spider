import ssl
import certifi
import os
import sys
import time
import json
import uuid
import random
import string
import logging
import threading
from logging.handlers import QueueHandler
import psutil
import pandas as pd
import multiprocessing
import subprocess
import configloader
import paths as path_utils
import browser_runtime
from state_store import StateStore, default_state_db
from excel_schemas import SELLER_COLUMNS, SELLER_SHEET, seller_failure_row
from excel_io import read_url_rows, write_single_sheet_excel
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from typing import List, Dict, Optional, Any
from urllib.parse import urlsplit, urlunsplit, urljoin

# Selenium
import seleniumwire.undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

# --- 全局配置与补丁 ---
ssl._create_default_https_context = ssl._create_unverified_context
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

def resource_path(relative_path):
    return path_utils.resource_path(relative_path)

def get_exe_dir():
    return path_utils.get_exe_dir()

c = configloader.config()
LOG_LEVEL = configloader.get_log_level(c)
CHROME_FOR_TESTING_PATH = resource_path("cft/chrome-win64/chrome.exe")
DRIVER_FOR_TESTING_PATH = resource_path("cft/chromedriver-win64/chromedriver.exe")
BASE_URL = "https://www.worten.pt"
exe_folder = get_exe_dir()

# 支持环境变量传递文件路径
INPUT_FILE = os.path.join(exe_folder, "input_links.xlsx")
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_FILE = os.path.join(exe_folder, f"worten_seller_data_{timestamp}.xlsx")

MAX_RETRIES = 3
URL_RETRY_LIMIT = 5
CF_BYPASS_PORT = int(c.get_key('cf_bypass_port') or 3000)
MAX_WORKERS = int(c.get_key('MAX_WORKER') or 4)
DEFAULT_MAX_URLS_PER_DRIVER_MIN = 15
DEFAULT_MAX_URLS_PER_DRIVER_MAX = 20

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - [Process %(process)d] - %(message)s')
logging.getLogger('seleniumwire').setLevel(logging.WARNING)

PAGE_NAVIGATION_TIMEOUT = 100
ELEMENT_WAIT_TIMEOUT = 60

# --- 核心工具函数 ---

def setup_log_queue_handler(log_queue):
    if log_queue is not None:
        try:
            qh = QueueHandler(log_queue)
            root = logging.getLogger()
            if root.handlers:
                for h in root.handlers[:]:
                    root.removeHandler(h)
            root.addHandler(qh)
            root.setLevel(LOG_LEVEL)
        except Exception:
            pass

import requests

def get_cf_cookie_from_api(port: int, proxy_str: Optional[str] = None) -> Optional[Dict]:
    return browser_runtime.get_cf_cookie_from_api(c, port, proxy_str)

def close_cookie_pup(driver: uc.Chrome):
    return browser_runtime.close_cookie_pup(driver)

def read_urls_from_excel(filename: str) -> Optional[List[Dict[str, Any]]]:
    try:
        rows = read_url_rows(filename, include_pages=False)
        if rows is None:
            logging.error(f"错误: Excel文件 '{filename}' 中未找到名为 'url' 的列。")
        return rows
    except Exception as e:
        logging.error(f"读取Excel文件 '{filename}' 时发生错误: {e}")
        return None

def save_data_to_multiple_sheets(seller_data: List[Dict], filename: str):
    normalized_rows = []
    for row in seller_data:
        normalized = dict(row)
        if '链接' not in normalized and '店铺链接' in normalized:
            normalized['链接'] = normalized.get('店铺链接')
        normalized_rows.append(normalized)
    write_single_sheet_excel(normalized_rows, filename, SELLER_COLUMNS, SELLER_SHEET)
    logging.info(f"已将 {len(normalized_rows)} 条卖家数据保存到 {filename}。")

def parse_price(price_str: str) -> Optional[float]:
    if not isinstance(price_str, str): return None
    try:
        price_str = price_str.replace('€', '').replace('.', '').replace(',', '.').strip()
        price_str = ''.join(price_str.split())
        return float(price_str)
    except: return None

def wait_for_safe_cpu(threshold: float = 85.0, check_interval: int = 5):
    """监控 CPU 使用率"""
    browser_runtime.wait_for_safe_cpu(threshold, check_interval)

def force_kill_driver(driver):
    """彻底清理 Driver 及其相关的 Chrome 进程"""
    browser_runtime.force_kill_driver(driver)

def navigate_with_retries(driver: uc.Chrome, url: str, max_attempts: int = 3, backoff_base: int = 2) -> bool:
    return browser_runtime.navigate_with_retries(driver, url, max_attempts, backoff_base)

def create_chrome_driver(session_data: Dict, chrome_init_lock=None) -> Optional[uc.Chrome]:
    """统一创建并初始化 Chrome Driver """
    lock = chrome_init_lock or multiprocessing.Lock()
    return browser_runtime.create_chrome_driver(
        session_data,
        CHROME_FOR_TESTING_PATH,
        DRIVER_FOR_TESTING_PATH,
        BASE_URL,
        lock,
        max_retries=MAX_RETRIES,
        connection_timeout=20,
        sleep_after_base_get=True,
    )

# --- 会话管理 ---

def get_fresh_session(session_lock=None):
    """按需请求一个新会话。session_lock 用于跨进程序列化 cf_bypass API 调用。"""
    if session_lock:
        with session_lock:
            return browser_runtime.create_session_data(c, CF_BYPASS_PORT)
    return browser_runtime.create_session_data(c, CF_BYPASS_PORT)

# --- 任务发现 (带进度增量) ---

def discovery_process_with_progress(initial_urls, discovery_completed_event, log_queue, total_estimated, total_increment_queue, state_db_path=None, run_id=None):
    setup_log_queue_handler(log_queue)
    logging.info("--- [发现进程] 启动 ---")
    total_estimated.value = 0 
    state = StateStore(state_db_path) if state_db_path and run_id else None
    count = 0
    try:
        if state:
            state.mark_discovery_started(run_id)
        for item in initial_urls:
            url = item.get('url')
            if url:
                task = {'url': url, 'type': 'product_page'}
                if not state or state.add_task(run_id, task, 'more_seller', max_attempts=URL_RETRY_LIMIT + 1):
                    count += 1
                    if total_increment_queue:
                        total_increment_queue.put(1) # 发送增量信号
        if state:
            state.mark_discovery_finished(run_id)
        logging.info(f"[发现进程] 已写入 {count} 个初始任务。")
    except Exception as e:
        logging.error(f"[发现进程] 任务发现失败: {e}")
        if state:
            state.mark_discovery_failed(run_id, str(e))
        raise
    finally:
        discovery_completed_event.set()

# --- 业务逻辑  ---

def scrape_other_sellers_logic(driver: uc.Chrome, product_url: str) -> List[Dict]:
    """
    在商品页面抓取 '其他卖家' 信息。
    包含了点击 '查看更多卖家' 的逻辑。
    """
    other_sellers_list = []
    
    # 1. 导航 
    if not navigate_with_retries(driver, product_url, max_attempts=3):
        logging.error(f"页面导航彻底失败: {product_url}")
        return [{"ERROR": "page_load_failed"}]

    # 404 检测
    try:
        time.sleep(random.uniform(1, 3))
        err404 = driver.find_elements(By.CSS_SELECTOR, ".error404__title")
        if err404 and err404[0].is_displayed():
            logging.info(f"检测到 404 页面: {product_url}")
            return [{"ERROR": "404"}] # 标记为 404
    except: pass
    
    close_cookie_pup(driver)

    # 2. 尝试寻找并点击 “查看更多卖家”
    clicked_more_sellers = False
    for attempt in range(MAX_RETRIES):
        try:
            other_sellers_link_selector = "span[class='h-underline']"
            other_sellers_link_bth = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, other_sellers_link_selector))
            )
            driver.execute_script("arguments[0].click();", other_sellers_link_bth)
            clicked_more_sellers = True
            logging.debug("   -> 已点击 '其他卖家' 链接，等待列表加载...")
            break

        except TimeoutException:
            logging.debug("   -> 未找到 '其他卖家' 链接，重试...")
            time.sleep(random.uniform(2,5))
            if attempt == MAX_RETRIES - 1:
                return [] # 没有更多卖家
    
    # 3. 等待真实的卖家卡片加载（排除骨架屏/Loading状态）
    try:
        # 使用 CSS 伪类 :not 排除 loading 卡片
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "article.seller-card:not(.seller-card--loading)"))
        )
        time.sleep(1) # 给前端框架 1 秒钟的缓冲时间完成最终 DOM 渲染
    except TimeoutException:
        if clicked_more_sellers:
            logging.warning("   -> 点击后未加载出真实卖家列表（或一直在loading）。")
        else:
            logging.debug("   -> 页面上没有真实卖家列表。")
        return []

    # 4. 抓取所有卖家卡片
    try:
        # 获取真实卡片总数（必须排除 loading）
        initial_cards = driver.find_elements(By.CSS_SELECTOR, "article.seller-card:not(.seller-card--loading)")
        cards_count = len(initial_cards)
        
        for i in range(cards_count):
            seller_info = {
                "店铺名称": "N/A", "链接": "N/A",
                "店铺运费": "N/A", "送货时间": "N/A"
            }
            try:
                # 重新获取真实卡片
                fresh_cards = driver.find_elements(By.CSS_SELECTOR, "article.seller-card:not(.seller-card--loading)")
                if i >= len(fresh_cards):
                    break
                    
                card = fresh_cards[i]

                # 强制将当前卡片滚动到页面中央
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
                time.sleep(0.5) 

                # --- 1. 判断并提取：店铺名称和链接 ---
                name_elements_t2 = card.find_elements(By.CSS_SELECTOR, ".seller-card__name")
                link_elements_t1 = card.find_elements(By.CSS_SELECTOR, ".seller-card__seller a")
                
                if name_elements_t2:
                    # 匹配到 Worten 自营
                    seller_info['店铺名称'] = name_elements_t2[0].get_attribute('textContent').strip()
                    seller_info['链接'] = BASE_URL
                    seller_info['店铺运费'] = '€0.00'
                elif link_elements_t1:
                    # 匹配第三方卖家 Marketplace
                    seller_info['店铺名称'] = link_elements_t1[0].get_attribute('textContent').strip()
                    href = link_elements_t1[0].get_attribute('href')
                    seller_info['链接'] = urljoin(BASE_URL, href) if href else "N/A"
                    
                    # 提取第三方运费
                    shipping_elements = card.find_elements(By.CSS_SELECTOR, ".seller-card__shipping--price")
                    if shipping_elements:
                        shipping_text = shipping_elements[0].get_attribute('textContent').strip()
                        shipping_value = parse_price(shipping_text)
                        seller_info['店铺运费'] = f"€{shipping_value:.2f}" if shipping_value is not None else shipping_text
                else:
                    html_content = card.get_attribute('outerHTML')
                    logging.warning(f"第 {i+1} 个卡片未能提取到名称，其实际HTML为: {html_content[:800]}")
                    seller_info['店铺名称'] = "提取失败(请查看日志)"
                
                # --- 2. 提取：送货时间 ---
                delivery_elements = card.find_elements(By.CSS_SELECTOR, "span.neu-11")
                if delivery_elements:
                    seller_info['送货时间'] = delivery_elements[-1].get_attribute('textContent').strip()
                    
                other_sellers_list.append(seller_info)

            except Exception as e:
                logging.warning(f"   -> 处理第 {i+1} 个卖家卡片时发生系统错误: {str(e)}")
                continue

    except Exception as e:
        logging.error(f"   -> 抓取卖家信息整体出错: {e}")
        
    return other_sellers_list

# --- Worker ---

class ScraperWorker:
    def __init__(self, discovery_completed_event, log_queue=None, increment_queue=None, state_db_path=None, run_id=None, stop_flag=None, session_lock=None, chrome_init_lock=None):
        self.discovery_completed_event = discovery_completed_event
        self.log_queue = log_queue
        self.increment_queue = increment_queue
        self.state_db_path = state_db_path
        self.run_id = run_id
        self.stop_flag = stop_flag
        self.state = StateStore(state_db_path) if state_db_path and run_id else None
        self.worker_id = str(uuid.uuid4())[:8]
        self.driver = None
        self.processed_count = 0
        self.consecutive_failures = 0
        self.current_max_urls = random.randint(DEFAULT_MAX_URLS_PER_DRIVER_MIN, DEFAULT_MAX_URLS_PER_DRIVER_MAX)
        self.session_lock = session_lock
        self.chrome_init_lock = chrome_init_lock

    def setup_driver(self):
        for i in range(MAX_RETRIES):
            session = get_fresh_session(self.session_lock)
            if not session:
                logging.warning(f"[Worker {self.worker_id}] 获取会话超时，重试...")
                time.sleep(5 * (i + 1))  # 指数退避
                continue
            
            self.driver = create_chrome_driver(session, self.chrome_init_lock)
            if self.driver:
                self.processed_count = 0
                return True
            else:
                logging.warning(f"[Worker {self.worker_id}] 会话不可用，重试...")
        
        logging.error(f"[Worker {self.worker_id}] 连续启动失败，Worker 将稍后继续尝试。")
        return False

    def teardown_driver(self):
        if self.driver:
            force_kill_driver(self.driver)
            self.driver = None

    def run(self):
        setup_log_queue_handler(self.log_queue)
        if not self.state:
            logging.error(f"[Worker {self.worker_id}] 状态存储不可用，Worker 退出。")
            return

        # 错开启动：避免所有 Worker 同时请求 Session 和创建 Driver
        startup_delay = random.uniform(1, max(3, MAX_WORKERS))
        logging.info(f"[Worker {self.worker_id}] 启动，延迟 {startup_delay:.1f}s 后开始...")
        time.sleep(startup_delay)

        task_key = None
        try:
            while True:
                if self.stop_flag is not None and self.stop_flag.value:
                    break

                task = self.state.claim_next_task(self.run_id, self.worker_id)
                if not task:
                    if self.state.is_discovery_finished(self.run_id):
                        break
                    time.sleep(2)
                    continue

                task_key = task.get('task_key')
                if self.stop_flag is not None and self.stop_flag.value:
                    if task_key:
                        self.state.release_task(self.run_id, task_key, self.worker_id, consume_attempt=False, error='任务已停止')
                    task_key = None
                    break

                if self.driver is None:
                    if not self.setup_driver():
                        if task_key:
                            self.state.release_task(self.run_id, task_key, self.worker_id, consume_attempt=False, error='driver_setup_failed')
                        task_key = None
                        logging.error(f"[Worker {self.worker_id}] 无法创建 Driver，释放当前任务并暂停后继续。")
                        time.sleep(10)
                        continue

                if self.processed_count >= self.current_max_urls:
                    logging.info(f"[Worker {self.worker_id}] 轮换 Driver...")
                    self.teardown_driver()
                    if not self.setup_driver():
                        if task_key:
                            self.state.release_task(self.run_id, task_key, self.worker_id, consume_attempt=False, error='driver_rotation_failed')
                        task_key = None
                        logging.error(f"[Worker {self.worker_id}] 轮换 Driver 失败，释放当前任务并暂停后继续。")
                        time.sleep(10)
                        continue

                # 处理任务并发送进度信号
                success = self.process_task(task)
                task_key = None
                if self.increment_queue:
                    self.increment_queue.put(1) # 进度+1

                # 释放 Chrome 内存：清除 cookies 和请求缓存
                if self.driver:
                    try:
                        self.driver.delete_all_cookies()
                    except Exception:
                        pass
                    browser_runtime.clear_driver_requests(self.driver)

                if success: self.consecutive_failures = 0
                else: self.consecutive_failures += 1
                self.processed_count += 1

                if self.consecutive_failures >= 3:
                    self.teardown_driver()
                    self.consecutive_failures = 0
        except Exception as exc:
            logging.error(f"[Worker {self.worker_id}] Worker 异常退出: {exc}")
            if task_key:
                self.state.release_task(self.run_id, task_key, self.worker_id, consume_attempt=False, error=str(exc))
            raise
        finally:
            self.teardown_driver()

    def record_task_result(self, task_key, result_group, rows, status='succeeded', error=None):
        result_rows = list(rows)
        if self.state and task_key:
            saved = self.state.complete_task(
                self.run_id,
                task_key,
                result_group,
                result_rows,
                status=status,
                error=error,
                owner=self.worker_id,
            )
            if not saved:
                logging.warning(f"[Worker {self.worker_id}] 任务 lease 已失效，丢弃本地结果: {task_key}")
                return False
        return True

    def record_task_failure(self, task_key, result_group, rows, error):
        result_rows = list(rows)
        if self.state and task_key:
            saved = self.state.fail_task(
                self.run_id,
                task_key,
                result_group,
                result_rows,
                error,
                owner=self.worker_id,
            )
            if not saved:
                logging.warning(f"[Worker {self.worker_id}] 任务 lease 已失效，丢弃失败结果: {task_key}")
                return False
        return True

    def process_task(self, task):
        url = task['url']
        task_key = task.get('task_key')
        try:
            # 执行抓取逻辑
            other_sellers_info = scrape_other_sellers_logic(self.driver, url)
            
            # 检查是否有特定错误标记
            if other_sellers_info and "ERROR" in other_sellers_info[0]:
                if other_sellers_info[0]["ERROR"] == "404":
                    row = seller_failure_row(url, '失效链接 (404)')
                    return self.record_task_result(task_key, 'seller', [row], status='invalid', error='404')
                if other_sellers_info[0]["ERROR"] == "page_load_failed":
                    row = seller_failure_row(url, '抓取失败')
                    self.record_task_failure(task_key, 'seller', [row], 'page_load_failed')
                    return False
                row = seller_failure_row(url, '抓取失败')
                self.record_task_failure(task_key, 'seller', [row], str(other_sellers_info[0].get('ERROR')))
                return False # 其他错误视为失败

            task_rows = []
            if not other_sellers_info:
                # 空列表，可能是页面没有其他卖家，也可能是加载失败。
                task_rows.append(seller_failure_row(url, '无更多卖家'))
            else:
                for seller in other_sellers_info:
                    task_rows.append({
                        '初始链接': url,
                        '店铺名称': seller.get('店铺名称', 'N/A'),
                        '链接': seller.get('链接', 'N/A'),
                        '店铺运费': seller.get('店铺运费', 'N/A'),
                        '送货时间': seller.get('送货时间', 'N/A')
                    })
            saved = self.record_task_result(task_key, 'seller', task_rows)
            if saved:
                logging.info(f" 成功处理: {url}，抓取到 {len(other_sellers_info)} 个卖家。")
            return saved

        except Exception as e:
            logging.error(f"[Worker {self.worker_id}] 任务异常 {url}: {e}")
            row = seller_failure_row(url, '抓取失败')
            self.record_task_failure(task_key, 'seller', [row], str(e))
            return False

# --- 进度管理进程 ---

def progress_manager(processed_count, total_estimated, increment_queue, total_increment_queue, stop_flag):
    while not stop_flag.value:
        try:
            while not increment_queue.empty():
                increment_queue.get_nowait()
                processed_count.value += 1
            while not total_increment_queue.empty():
                total_increment_data = total_increment_queue.get_nowait()
                total_estimated.value += total_increment_data
            time.sleep(0.2)
        except: pass

# --- 主函数 ---

def main(progress_callback=None, stop_check_callback=None, input_file=None, output_file=None, state_db_path=None):
    multiprocessing.freeze_support()
    os.environ["WDM_DEFAULT_TIMEOUT"] = "90"
    input_file = input_file or INPUT_FILE
    output_file = output_file or OUTPUT_FILE
    state_db_path = state_db_path or default_state_db()
    state = StateStore(state_db_path)
    
    logging.info(f"--- Worten 更多卖家爬虫启动 (Workers: {MAX_WORKERS}) ---")
    initial_urls = read_urls_from_excel(input_file)
    if not initial_urls:
        logging.error("未找到输入链接。")
        return {'status': 'failed', 'message': '未找到输入链接'}
    run_id, resumed = state.create_or_resume_run('more_seller', input_file, output_file)
    if resumed:
        logging.info(f"继续未完成任务: run_id={run_id}, output_file={output_file}")
        recovered = state.recover_running_tasks(run_id, '程序重新启动，任务重新排队')
        if recovered:
            logging.info(f"已恢复 {recovered} 个上次运行遗留的进行中任务。")
    else:
        state.recover_stale_tasks(run_id)

    def _log_listener(q):
        root = logging.getLogger()
        while True:
            try:
                record = q.get()
                if record is None: break
                root.handle(record)
            except: break

    with multiprocessing.Manager() as manager:
        log_queue = manager.Queue()
        listener_thread = threading.Thread(target=_log_listener, args=(log_queue,), daemon=True)
        listener_thread.start()

        stop_flag = manager.Value('b', False)
        discovery_completed_event = manager.Event()

        # 跨进程序列化锁：防止所有 Worker 同时请求 Session 和创建 Chrome
        session_lock = manager.Lock()
        chrome_init_lock = manager.Lock()
        
        # 数据存储 — 结果直接存入 SQLite，不再需要 manager.list 累积内存

        # 进度管理变量
        processed_count = manager.Value('i', 0)
        total_estimated = manager.Value('i', 0)
        increment_queue = manager.Queue()
        total_increment_queue = manager.Queue()
        start_time = manager.Value('d', time.time())

        # 启动管理进程
        pm_p = multiprocessing.Process(target=progress_manager, args=(processed_count, total_estimated, increment_queue, total_increment_queue, stop_flag))
        pm_p.start()

        # 进度回调线程
        def progress_updater():
            last_queue_log = 0
            while not stop_flag.value:
                if progress_callback:
                    elapsed = time.time() - start_time.value
                    rate = processed_count.value / (elapsed / 60) if elapsed > 0 else 0
                    progress = state.progress(run_id)
                    progress_callback({
                        'processed': progress['processed'],
                        'total': progress['total'] or total_estimated.value,
                        'rate': rate,
                        'message': f"正在分析卖家信息: {progress['processed']}/{progress['total'] or total_estimated.value}"
                    })

                now = time.time()
                if now - last_queue_log >= 10:
                    stats = state.queue_stats(run_id)
                    discovery = state.discovery_status(run_id)
                    logging.info(
                        f"[队列状态] discovery={discovery}, total={stats['total']}, "
                        f"pending={stats['pending']}, running={stats['running']}, "
                        f"active_workers={stats['active_workers']}/{MAX_WORKERS}, succeeded={stats['succeeded']}, "
                        f"failed_final={stats['failed_final']}, invalid={stats['invalid']}"
                    )
                    last_queue_log = now

                if stop_check_callback and stop_check_callback():
                    stop_flag.value = True
                time.sleep(2)

        updater_t = threading.Thread(target=progress_updater, daemon=True)
        updater_t.start()

        # 启动组件
        discovery_p = None
        try:
            logging.info("按需请求 Session：跳过备用 Session 生产和预热。")

            # 启动发现进程 (带进度)
            discovery_p = multiprocessing.Process(target=discovery_process_with_progress, args=(initial_urls, discovery_completed_event, log_queue, total_estimated, total_increment_queue, state_db_path, run_id))
            discovery_p.start()

            # 启动 Workers
            with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(ScraperWorker(discovery_completed_event, log_queue, increment_queue, state_db_path, run_id, stop_flag, session_lock, chrome_init_lock).run) for _ in range(MAX_WORKERS)]
                wait(futures)
                for future in futures:
                    future.result()
        finally:
            stop_flag.value = True
            try:
                log_queue.put(None)
            except Exception:
                pass
            if discovery_p is not None:
                discovery_p.join(timeout=5)
                if discovery_p.is_alive():
                    discovery_p.terminate()
            pm_p.join(timeout=5)
            if pm_p.is_alive():
                pm_p.terminate()
        
        discovery = state.discovery_status(run_id)
        if discovery == 'failed' or (isinstance(discovery, dict) and discovery.get('status') == 'failed'):
            message = discovery.get('error') if isinstance(discovery, dict) else '任务发现失败'
            state.set_run_status(run_id, 'failed', message)
            raise RuntimeError(message)
        rows_by_group = state.grouped_result_rows(run_id)
        save_data_to_multiple_sheets(rows_by_group.get('seller', []), output_file)
        if state.has_incomplete_tasks(run_id):
            message = '仍有未完成任务，稍后可再次点击开始/继续任务'
            state.set_run_status(run_id, 'failed', message)
            raise RuntimeError(message)
        state.set_run_status(run_id, 'completed')
        
        if progress_callback: 
            elapsed_time = time.time() - start_time.value
            final_rate = processed_count.value / (elapsed_time / 60) if elapsed_time > 0 else 0
            progress = state.progress(run_id)
            progress_callback({'processed': progress['processed'], 'total': progress['total'], 'rate': final_rate, 'message': '任务完成！'})
        return {'status': 'completed', 'run_id': run_id, 'resumed': resumed, 'output_file': output_file}

    if os.name == 'nt':
        try: subprocess.run("taskkill /F /T /IM chrome*", shell=True, stderr=subprocess.DEVNULL)
        except: pass

if __name__ == '__main__':
    main()