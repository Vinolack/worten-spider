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
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

def get_exe_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

c = configloader.config()
CHROME_FOR_TESTING_PATH = resource_path("cft/chrome-win64/chrome.exe")
DRIVER_FOR_TESTING_PATH = resource_path("cft/chromedriver-win64/chromedriver.exe")
NODE_SCRIPT_PATH = resource_path("index.js")
BASE_URL = "https://www.worten.pt"
exe_folder = get_exe_dir()

# 支持环境变量传递文件路径
INPUT_FILE = os.environ.get('WORTEN_INPUT_FILE') or os.path.join(exe_folder, "input_links.xlsx")
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_FILE = os.environ.get('WORTEN_OUTPUT_FILE') or os.path.join(exe_folder, f"worten_seller_data_{timestamp}.xlsx")

MAX_RETRIES = 3
URL_RETRY_LIMIT = 5
MAX_WORKERS = int(c.get_key('MAX_WORKER') or 4)
DEFAULT_MAX_URLS_PER_DRIVER_MIN = 15
DEFAULT_MAX_URLS_PER_DRIVER_MAX = 20
SESSION_LIFESPAN_SECONDS = 10 * 60
MIN_SESSION_USABLE_TIME_SECONDS = 4 * 60

CHROME_INIT_LOCK = multiprocessing.Lock()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Process %(process)d] - %(message)s')
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
            root.setLevel(logging.INFO)
        except Exception:
            pass

def get_cf_cookie_from_nodejs(node_script_path: str, port: int, proxy: Optional[str] = None) -> Optional[Dict]:
    """执行 Node.js 脚本以获取 Cloudflare cookie"""
    command = ['node', node_script_path, proxy if proxy else 'null', str(port)]
    try:
        startupinfo = None
        creationflags = 0
        if sys.platform == 'win32':
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = subprocess.SW_HIDE
            creationflags = 0x08000000 
        result = subprocess.run(
            command, capture_output=True, text=True, encoding='utf-8', check=True, timeout=90, 
            startupinfo=startupinfo, creationflags=creationflags
        )
        return json.loads(result.stdout.strip())
    except Exception as e:
        logging.error(f"Node.js execution failed: {e}")
        return None

def close_cookie_pup(driver: uc.Chrome):
    try:
        cookie_pup_selector = "button[class='button--md button--primary button--black button'] span"
        cookie_close_bth = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, cookie_pup_selector))
        )
        driver.execute_script("arguments[0].click();", cookie_close_bth)
        return True
    except:
        return False

def read_urls_from_excel(filename: str) -> Optional[List[Dict[str, Any]]]:
    try:
        df = pd.read_excel(filename, engine='openpyxl')
        if 'url' not in df.columns:
            logging.error(f"错误: Excel文件 '{filename}' 中未找到名为 'url' 的列。")
            return None
        return df[['url']].dropna(subset=['url']).to_dict('records')
    except Exception as e:
        logging.error(f"读取Excel文件 '{filename}' 时发生错误: {e}")
        return None

def save_data_to_multiple_sheets(seller_data: List[Dict], filename: str):
    if not seller_data:
        logging.warning("没有任何数据可保存。")
        return
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df_sellers = pd.DataFrame(seller_data)
            # 定义需要的列顺序
            seller_columns = ['初始链接', '店铺名称', '链接', '店铺运费', '送货时间']
            
            # 兼容性处理
            if '链接' not in df_sellers.columns and '店铺链接' in df_sellers.columns:
                df_sellers.rename(columns={'店铺链接': '链接'}, inplace=True)
            
            # 重新索引
            df_sellers = df_sellers.reindex(columns=seller_columns)
            
            df_sellers.to_excel(writer, sheet_name='跟卖链接数据 (Sellers)', index=False)
            logging.info(f"已将 {len(df_sellers)} 条卖家数据保存到 {filename}。")
    except Exception as e:
        logging.error(f"保存数据失败: {e}")

def parse_price(price_str: str) -> Optional[float]:
    if not isinstance(price_str, str): return None
    try:
        price_str = price_str.replace('€', '').replace('.', '').replace(',', '.').strip()
        price_str = ''.join(price_str.split())
        return float(price_str)
    except: return None

def wait_for_safe_cpu(threshold: float = 85.0, check_interval: int = 5):
    """监控 CPU 使用率"""
    if psutil is None: return
    try:
        while True:
            cpu_usage = psutil.cpu_percent(interval=1)
            if cpu_usage < threshold: break
            logging.warning(f"系统 CPU 负载过高 ({cpu_usage}%)，暂停创建 Driver {check_interval}秒...")
            time.sleep(check_interval)
    except: pass

def force_kill_driver(driver):
    """彻底清理 Driver 及其相关的 Chrome 进程"""
    if not driver: return
    try: driver.quit()
    except: pass
    pids_to_kill = []
    try:
        if hasattr(driver, 'service') and driver.service.process:
            pids_to_kill.append(driver.service.process.pid)
        if hasattr(driver, 'browser_pid') and driver.browser_pid:
            pids_to_kill.append(driver.browser_pid)
    except: pass
    for pid in pids_to_kill:
        try:
            proc = psutil.Process(pid)
            for child in proc.children(recursive=True): child.kill()
            proc.kill()
        except: pass

def navigate_with_retries(driver: uc.Chrome, url: str, max_attempts: int = 3, backoff_base: int = 2) -> bool:
    for attempt in range(1, max_attempts + 1):
        try:
            driver.get(url)
            return True
        except WebDriverException as e:
            error_msg = str(e)
            if "ERR_TUNNEL_CONNECTION_FAILED" in error_msg or "ERR_PROXY_CONNECTION_FAILED" in error_msg:
                logging.error(f"代理隧道建立失败 (IP已废): {error_msg}")
                return False
            logging.warning(f"WebDriver 错误 (尝试 {attempt}/{max_attempts}): {e}")
        
        if attempt < max_attempts:
            time.sleep(backoff_base ** (attempt - 1))
    return False

def create_chrome_driver(session_data: Dict) -> Optional[uc.Chrome]:
    """统一创建并初始化 Chrome Driver """
    if not session_data: return None
    wait_for_safe_cpu(threshold=80.0)
    cookies = session_data.get('cookies', [])
    user_agent = session_data.get('headers', {}).get("user-agent")
    proxy_wire = session_data.get('proxy_for_selenium_wire')
    driver = None
    for attempt in range(MAX_RETRIES): 
        try:
            sw_opts = {'proxy': {'http': proxy_wire, 'https': proxy_wire, 'no_proxy': 'localhost,127.0.0.1'}, 'verify_ssl': False}
            opts = uc.ChromeOptions()
            opts.page_load_strategy = 'eager'
            opts.add_argument('--headless=new')
            opts.add_argument('--disable-features=UseEcoQoSForBackgroundProcess')
            opts.add_argument('--ignore-certificate-errors')
            opts.add_argument('--no-sandbox')
            opts.add_argument('--disable-dev-shm-usage')
            opts.add_argument('--no-zygote') # 减少进程孵化开销
            opts.add_argument('--disable-gpu-sandbox')
            opts.add_argument('--disable-gpu')
            opts.add_argument('--disable-popup-blocking')
            opts.add_argument('--disable-extensions')
            opts.add_argument('--disable-background-networking')
            opts.add_argument('--disable-sync')
            opts.add_argument('--disable-translate')
            opts.add_argument('--disable-default-apps')
            opts.add_argument('--no-first-run')
            opts.add_argument('--disable-software-rasterizer')
            opts.add_argument('--renderer-process-limit=1') 
            if user_agent: opts.add_argument(f'--user-agent={user_agent}')
            
            with CHROME_INIT_LOCK:
                wait_for_safe_cpu(threshold=80.0, check_interval=random.randint(3,5))
                driver = uc.Chrome(browser_executable_path=CHROME_FOR_TESTING_PATH, driver_executable_path=DRIVER_FOR_TESTING_PATH, options=opts, seleniumwire_options=sw_opts, version_main=142)
            
            driver.set_page_load_timeout(60) 
            driver.get(BASE_URL)
            time.sleep(random.uniform(2, 4))
            driver.delete_all_cookies()
            for ck in cookies:
                if 'sameSite' in ck and ck['sameSite'] not in ['Strict', 'Lax', 'None']: del ck['sameSite']
                try: driver.add_cookie(ck)
                except: pass 
            return driver

        except Exception as e:
            logging.warning(f"创建 Driver 尝试 {attempt+1}/{MAX_RETRIES} 失败: {e}")
            if driver:
                force_kill_driver(driver)
                driver = None
            time.sleep(2) 

    logging.error("创建 Driver 彻底失败.")
    if driver: force_kill_driver(driver)
    return None

# --- 会话生产 ---

def session_producer(session_queue, url_queue, node_script_path, stop_flag, port, num_producers, log_queue):
    setup_log_queue_handler(log_queue)
    shutdown_buffer = num_producers if num_producers > 1 else 2
    while not stop_flag.value:
        try:
            if session_queue.qsize() < int(MAX_WORKERS / 2 + shutdown_buffer):
                session_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
                full_user = f"{c.get_key('PROXY_USER_BASE')}{session_id}"
                proxy_node = f"{c.get_key('PROXY_HOST')}:{c.get_key('PROXY_PORT')}:{full_user}:{c.get_key('PROXY_PASS')}"
                proxy_wire = f"http://{full_user}:{c.get_key('PROXY_PASS')}@{c.get_key('PROXY_HOST')}:{c.get_key('PROXY_PORT')}"
                
                session_data = get_cf_cookie_from_nodejs(node_script_path, port, proxy_node)
                if session_data and "cookies" in session_data:
                    session_data['proxy_for_selenium_wire'] = proxy_wire
                    session_data['created_at'] = time.time()
                    session_queue.put(session_data)
                    logging.info(f"[生产者{port}] 会话就绪。库存: {session_queue.qsize()}")
                time.sleep(2)
            else: time.sleep(2)
        except: time.sleep(10)

def get_fresh_session(session_queue):
    while True:
        try:
            sd = session_queue.get(timeout=60)
            if (time.time() - sd.get('created_at', 0)) < (SESSION_LIFESPAN_SECONDS - MIN_SESSION_USABLE_TIME_SECONDS): return sd
        except queue.Empty: return None

# --- 任务发现 (带进度增量) ---

def discovery_process_with_progress(initial_urls, url_queue, session_queue, discovery_completed_event, log_queue, total_estimated, total_increment_queue):
    setup_log_queue_handler(log_queue)
    logging.info("--- [发现进程] 启动 ---")
    total_estimated.value = 0 
    
    count = 0
    for item in initial_urls:
        url = item.get('url')
        if url:
            url_queue.put({'url': url, 'type': 'product_page'})
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
        return []

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
    
    # 3. 等待卖家卡片加载
    try:
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "article.seller-card"))
        )
    except TimeoutException:
        if clicked_more_sellers:
            logging.warning("   -> 点击后未加载出卖家列表。")
        else:
            logging.debug("   -> 页面上没有卖家列表。")
        return []

    # 4. 抓取所有卖家卡片
    try:
        seller_cards = driver.find_elements(By.CSS_SELECTOR, "article.seller-card")
        
        for card in seller_cards:
            seller_info = {
                "店铺名称": "N/A", "链接": "N/A",
                "店铺运费": "N/A", "送货时间": "N/A"
            }
            try:
                # Type 2 (Worten 自营)
                name_elements_t2 = card.find_elements(By.CSS_SELECTOR, ".seller-card__name")
                if name_elements_t2:
                    seller_info['店铺名称'] = name_elements_t2[0].text.strip()
                    seller_info['链接'] = BASE_URL
                    seller_info['店铺运费'] = '€0.00' 
                else:
                    # Type 1 (Marketplace)
                    # 尝试多种选择器获取名称
                    for attempt in range(5):
                        name_elements_t1 = card.find_elements(By.CSS_SELECTOR, "a[class*='seller-card__link'] span")
                        if not name_elements_t1:
                            name_elements_t1 = card.find_elements(By.CSS_SELECTOR, "div.seller-card__seller > a > span")
                                                   
                        if name_elements_t1:
                            seller_info['店铺名称'] = name_elements_t1[0].text.strip()
                            break
                        time.sleep(random.uniform(1,2))

                    link_elements_t1 = card.find_elements(By.CSS_SELECTOR, "div.seller-card__seller > a")
                    if link_elements_t1:
                        href = link_elements_t1[0].get_attribute('href')
                        seller_info['链接'] = urljoin(BASE_URL, href) if href else "N/A"

                    # Shipping cost
                    for attempt in range(5):
                        shipping_elements_t1 = card.find_elements(By.CSS_SELECTOR, "span[class*='seller-card__shipping--price'] span[class*='price__numbers']")
                        if shipping_elements_t1:
                            shipping_value = parse_price(shipping_elements_t1[0].text.strip())
                            seller_info['店铺运费'] = f"€{shipping_value:.2f}" if shipping_value is not None else "N/A"
                            break
                        time.sleep(random.uniform(1,2))

                    # Delivery time
                    for attempt in range(5):
                        delivery_elements = card.find_elements(By.CSS_SELECTOR, "span[class='neu-07'] span[class='neu-11']")
                        if delivery_elements:
                            seller_info['送货时间'] = delivery_elements[0].text.strip()
                            break
                        time.sleep(random.uniform(1,2))
                        
                other_sellers_list.append(seller_info)

            except Exception as e:
                pass # 单个卡片失败忽略

    except Exception as e:
        logging.error(f"   -> 抓取卖家信息整体出错: {e}")
        
    return other_sellers_list

# --- Worker ---

class ScraperWorker:
    def __init__(self, url_queue, more_seller_info_data, results_lock, session_queue, discovery_completed_event, log_queue=None, increment_queue=None):
        self.url_queue = url_queue
        self.more_seller_info_data = more_seller_info_data
        self.results_lock = results_lock
        self.session_queue = session_queue
        self.discovery_completed_event = discovery_completed_event
        self.log_queue = log_queue
        self.increment_queue = increment_queue
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
        try:
            # 执行抓取逻辑
            other_sellers_info = scrape_other_sellers_logic(self.driver, url)
            
            # 检查是否有特定错误标记
            if other_sellers_info and "ERROR" in other_sellers_info[0]:
                if other_sellers_info[0]["ERROR"] == "404":
                    with self.results_lock:
                        self.more_seller_info_data.append({'初始链接': url, '店铺名称': '失效链接 (404)'})
                    return True # 视为成功处理（虽然是无效链接）
                return False # 其他错误视为失败

            if not other_sellers_info:
                # 空列表，可能是页面没有其他卖家，也可能是加载失败。
                with self.results_lock:
                    self.more_seller_info_data.append({'初始链接': url, '店铺名称': '无更多卖家'})
            else:
                for seller in other_sellers_info:
                    seller_record = {
                        '初始链接': url,
                        '店铺名称': seller.get('店铺名称', 'N/A'),
                        '链接': seller.get('链接', 'N/A'),
                        '店铺运费': seller.get('店铺运费', 'N/A'),
                        '送货时间': seller.get('送货时间', 'N/A')
                    }
                    with self.results_lock:
                        self.more_seller_info_data.append(seller_record)
                
            logging.info(f" 成功处理: {url}，抓取到 {len(other_sellers_info)} 个卖家。")
            return True

        except Exception as e:
            logging.error(f"[Worker {self.worker_id}] 任务异常 {url}: {e}")
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

def main(progress_callback=None, stop_check_callback=None):
    multiprocessing.freeze_support()
    os.environ["WDM_DEFAULT_TIMEOUT"] = "90"
    cf_port = int(c.get_key('cf_bypass_port') or 3000)
    num_producers = int(c.get_key('num_session_producers') or 1)
    
    logging.info(f"--- Worten 更多卖家爬虫启动 (Workers: {MAX_WORKERS}) ---")
    initial_urls = read_urls_from_excel(INPUT_FILE)
    if not initial_urls:
        logging.error("未找到输入链接。")
        return

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
                    progress_callback({
                        'processed': processed_count.value,
                        'total': total_estimated.value,
                        'rate': rate,
                        'message': f'正在分析卖家信息: {processed_count.value}/{total_estimated.value}'
                    })
                if stop_check_callback and stop_check_callback():
                    stop_flag.value = True
                time.sleep(2)

        updater_t = threading.Thread(target=progress_updater, daemon=True)
        updater_t.start()

        # 启动组件
        producers = [multiprocessing.Process(target=session_producer, args=(session_queue, url_queue, NODE_SCRIPT_PATH, stop_flag, cf_port, num_producers, log_queue)) for _ in range(num_producers)]
        for p in producers: p.start()
        time.sleep(10)

        # 启动发现进程 (带进度)
        discovery_p = multiprocessing.Process(target=discovery_process_with_progress, args=(initial_urls, url_queue, session_queue, discovery_completed_event, log_queue, total_estimated, total_increment_queue))
        discovery_p.start()

        # 启动 Workers
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(ScraperWorker(url_queue, more_seller_info_data, results_lock, session_queue, discovery_completed_event, log_queue, increment_queue).run) for _ in range(MAX_WORKERS)]
            wait(futures)

        # 收尾
        stop_flag.value = True
        log_queue.put(None)
        discovery_p.join(timeout=5)
        for p in producers: p.join(timeout=5)
        pm_p.join(timeout=5)
        
        save_data_to_multiple_sheets(list(more_seller_info_data), OUTPUT_FILE)
        
        if progress_callback: 
            elapsed_time = time.time() - start_time.value
            final_rate = processed_count.value / (elapsed_time / 60) if elapsed_time > 0 else 0
            progress_callback({'processed': processed_count.value, 'total': total_estimated.value, 'rate': final_rate, 'message': '任务完成！'})

    if os.name == 'nt':
        try: subprocess.run("taskkill /F /T /IM chrome*", shell=True, stderr=subprocess.DEVNULL)
        except: pass

if __name__ == '__main__':
    main()