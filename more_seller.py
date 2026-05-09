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
import queue
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
NODE_SCRIPT_PATH = resource_path("index.js")
BASE_URL = "https://www.worten.pt"
exe_folder = get_exe_dir()

# 支持环境变量传递文件路径
INPUT_FILE = os.path.join(exe_folder, "input_links.xlsx")
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_FILE = os.path.join(exe_folder, f"worten_seller_data_{timestamp}.xlsx")

MAX_RETRIES = 3
URL_RETRY_LIMIT = 5
MAX_WORKERS = int(c.get_key('MAX_WORKER') or 4)
DEFAULT_MAX_URLS_PER_DRIVER_MIN = 15
DEFAULT_MAX_URLS_PER_DRIVER_MAX = 20
SESSION_LIFESPAN_SECONDS = 10 * 60
MIN_SESSION_USABLE_TIME_SECONDS = 4 * 60

CHROME_INIT_LOCK = multiprocessing.Lock()
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

def create_chrome_driver(session_data: Dict) -> Optional[uc.Chrome]:
    """统一创建并初始化 Chrome Driver """
    return browser_runtime.create_chrome_driver(
        session_data,
        CHROME_FOR_TESTING_PATH,
        DRIVER_FOR_TESTING_PATH,
        BASE_URL,
        CHROME_INIT_LOCK,
        max_retries=MAX_RETRIES,
        connection_timeout=20,
        sleep_after_base_get=True,
    )

# --- 会话生产 ---

def session_producer(session_queue, url_queue, node_script_path, stop_flag, port, num_producers, log_queue):
    setup_log_queue_handler(log_queue)
    shutdown_buffer = num_producers if num_producers > 1 else 2
    while not stop_flag.value:
        try:
            if session_queue.qsize() < int(MAX_WORKERS / 2 + shutdown_buffer):
                session_data = browser_runtime.create_session_data(c, port)
                if session_data and "cookies" in session_data:
                    session_queue.put(session_data)
                    logging.info(f"[生产者{port}] 会话就绪。库存: {session_queue.qsize()}")
                time.sleep(2)
            else: time.sleep(2)
        except: time.sleep(10)

def get_fresh_session(session_queue):
    return browser_runtime.get_fresh_session(session_queue, SESSION_LIFESPAN_SECONDS, MIN_SESSION_USABLE_TIME_SECONDS)

# --- 任务发现 (带进度增量) ---

def discovery_process_with_progress(initial_urls, url_queue, session_queue, discovery_completed_event, log_queue, total_estimated, total_increment_queue, state_db_path=None, run_id=None):
    setup_log_queue_handler(log_queue)
    logging.info("--- [发现进程] 启动 ---")
    total_estimated.value = 0 
    state = StateStore(state_db_path) if state_db_path and run_id else None
    
    count = 0
    for item in initial_urls:
        url = item.get('url')
        if url:
            task = {'url': url, 'type': 'product_page'}
            if not state or state.add_task(run_id, task, 'more_seller', max_attempts=URL_RETRY_LIMIT + 1):
                url_queue.put(task)
                count += 1
                if total_increment_queue:
                    total_increment_queue.put(1) # 发送增量信号
    
    logging.info(f"[发现进程] 已分发 {count} 个初始任务。")
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
    def __init__(self, url_queue, more_seller_info_data, results_lock, session_queue, discovery_completed_event, log_queue=None, increment_queue=None, state_db_path=None, run_id=None):
        self.url_queue = url_queue
        self.more_seller_info_data = more_seller_info_data
        self.results_lock = results_lock
        self.session_queue = session_queue
        self.discovery_completed_event = discovery_completed_event
        self.log_queue = log_queue
        self.increment_queue = increment_queue
        self.state_db_path = state_db_path
        self.run_id = run_id
        self.state = StateStore(state_db_path) if state_db_path and run_id else None
        self.worker_id = str(uuid.uuid4())[:8]
        self.driver = None
        self.processed_count = 0
        self.consecutive_failures = 0
        self.current_max_urls = random.randint(DEFAULT_MAX_URLS_PER_DRIVER_MIN, DEFAULT_MAX_URLS_PER_DRIVER_MAX)

    def setup_driver(self):
        for i in range(MAX_RETRIES):
            session = get_fresh_session(self.session_queue)
            if not session:
                logging.warning(f"[Worker {self.worker_id}] 获取会话超时，重试...")
                time.sleep(5)
                continue
            
            self.driver = create_chrome_driver(session)
            if self.driver:
                self.processed_count = 0
                return True
            else:
                logging.warning(f"[Worker {self.worker_id}] 会话不可用，重试...")
        
        logging.error(f"[Worker {self.worker_id}] 连续启动失败，Worker 退出。")
        return False

    def teardown_driver(self):
        if self.driver:
            force_kill_driver(self.driver)
            self.driver = None

    def run(self):
        setup_log_queue_handler(self.log_queue)
        while True:
            try:
                task = self.url_queue.get(block=True, timeout=2)
            except queue.Empty:
                if self.discovery_completed_event.is_set(): break
                else: continue

            if self.driver is None:
                if not self.setup_driver():
                    self.url_queue.put(task)
                    break

            if self.processed_count >= self.current_max_urls:
                logging.info(f"[Worker {self.worker_id}] 轮换 Driver...")
                self.teardown_driver()
                if not self.setup_driver():
                    self.url_queue.put(task)
                    break

            # 处理任务并发送进度信号
            success = self.process_task(task)
            if self.increment_queue:
                self.increment_queue.put(1) # 进度+1

            if success: self.consecutive_failures = 0
            else: self.consecutive_failures += 1
            self.processed_count += 1

            if self.consecutive_failures >= 3:
                self.teardown_driver()
                self.consecutive_failures = 0
        self.teardown_driver()

    def process_task(self, task):
        url = task['url']
        task_key = task.get('task_key')
        if self.state and not self.state.claim_task(self.run_id, task, self.worker_id):
            return True
        try:
            # 执行抓取逻辑
            other_sellers_info = scrape_other_sellers_logic(self.driver, url)
            
            # 检查是否有特定错误标记
            if other_sellers_info and "ERROR" in other_sellers_info[0]:
                if other_sellers_info[0]["ERROR"] == "404":
                    row = seller_failure_row(url, '失效链接 (404)')
                    with self.results_lock:
                        self.more_seller_info_data.append(row)
                    if self.state and task_key:
                        self.state.complete_task(self.run_id, task_key, 'seller', [row], status='invalid', error='404')
                    return True # 视为成功处理（虽然是无效链接）
                if other_sellers_info[0]["ERROR"] == "page_load_failed":
                    row = seller_failure_row(url, '抓取失败')
                    with self.results_lock:
                        self.more_seller_info_data.append(row)
                    if self.state and task_key:
                        self.state.fail_task(self.run_id, task_key, 'seller', [row], 'page_load_failed')
                    return False
                row = seller_failure_row(url, '抓取失败')
                with self.results_lock:
                    self.more_seller_info_data.append(row)
                if self.state and task_key:
                    self.state.fail_task(self.run_id, task_key, 'seller', [row], str(other_sellers_info[0].get('ERROR')))
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
            with self.results_lock:
                for seller_record in task_rows:
                    self.more_seller_info_data.append(seller_record)
            if self.state and task_key:
                self.state.complete_task(self.run_id, task_key, 'seller', task_rows)
                
            logging.info(f" 成功处理: {url}，抓取到 {len(other_sellers_info)} 个卖家。")
            return True

        except Exception as e:
            logging.error(f"[Worker {self.worker_id}] 任务异常 {url}: {e}")
            row = seller_failure_row(url, '抓取失败')
            with self.results_lock:
                self.more_seller_info_data.append(row)
            if self.state and task_key:
                self.state.fail_task(self.run_id, task_key, 'seller', [row], str(e))
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
    cf_port = int(c.get_key('cf_bypass_port') or 3000)
    num_producers = int(c.get_key('num_session_producers') or 1)
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

        url_queue = manager.Queue()
        session_queue = manager.Queue()
        stop_flag = manager.Value('b', False)
        discovery_completed_event = manager.Event()
        
        more_seller_info_data = manager.list()
        results_lock = manager.Lock()

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
                if stop_check_callback and stop_check_callback():
                    stop_flag.value = True
                time.sleep(2)

        updater_t = threading.Thread(target=progress_updater, daemon=True)
        updater_t.start()

        # 启动组件
        for task in state.load_unfinished_tasks(run_id):
            url_queue.put(task)
        producers = [multiprocessing.Process(target=session_producer, args=(session_queue, url_queue, NODE_SCRIPT_PATH, stop_flag, cf_port, num_producers, log_queue)) for _ in range(num_producers)]
        for p in producers: p.start()
        time.sleep(10)

        # 启动发现进程 (带进度)
        discovery_p = multiprocessing.Process(target=discovery_process_with_progress, args=(initial_urls, url_queue, session_queue, discovery_completed_event, log_queue, total_estimated, total_increment_queue, state_db_path, run_id))
        discovery_p.start()

        # 启动 Workers
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(ScraperWorker(url_queue, more_seller_info_data, results_lock, session_queue, discovery_completed_event, log_queue, increment_queue, state_db_path, run_id).run) for _ in range(MAX_WORKERS)]
            wait(futures)
            for future in futures:
                future.result()

        # 收尾
        stop_flag.value = True
        log_queue.put(None)
        discovery_p.join(timeout=5)
        for p in producers: p.join(timeout=5)
        pm_p.join(timeout=5)
        
        rows_by_group = state.grouped_result_rows(run_id)
        save_data_to_multiple_sheets(rows_by_group.get('seller', list(more_seller_info_data)), output_file)
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