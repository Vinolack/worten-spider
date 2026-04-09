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

INPUT_FILE = os.environ.get('WORTEN_INPUT_FILE') or os.path.join(exe_folder, "price_check_links.xlsx")
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_FILE = os.environ.get('WORTEN_OUTPUT_FILE') or os.path.join(exe_folder, f"worten_price_data_{timestamp}.xlsx")

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

def save_data_to_excel(product_data: List[Dict], filename: str):
    """
    将价格数据保存到Excel文件中。
    """
    if not product_data:
        logging.warning("没有任何数据可保存。")
        return
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df_product = pd.DataFrame(product_data)
            product_columns = ['商品链接', '价格', '运费', '销售和发货方']
            df_product = df_product.reindex(columns=product_columns)
            df_product.to_excel(writer, sheet_name='商品价格数据 (Product Prices)', index=False)
            logging.info(f"已将 {len(df_product)} 条数据保存到 {filename}。")
    except Exception as e:
        logging.error(f"保存失败: {e}")

def parse_price(price_str: str) -> Optional[float]:
    if not isinstance(price_str, str): return None
    try:
        price_str = price_str.replace('€', '').replace('.', '').replace(',', '.').strip()
        price_str = ''.join(price_str.split())
        return float(price_str)
    except: return None

def wait_for_safe_cpu(threshold: float = 85.0, check_interval: int = 5):
    """
    监控 CPU 使用率。如果超过阈值，则阻塞等待，直到 CPU 降温。
    """
    if psutil is None: return
    try:
        while True:
            cpu_usage = psutil.cpu_percent(interval=1)
            if cpu_usage < threshold: break
            logging.warning(f"系统 CPU 负载过高 ({cpu_usage}%)，暂停创建 Driver {check_interval}秒...")
            time.sleep(check_interval)
    except: pass

def force_kill_driver(driver):
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
            driver.delete_all_cookies()
            for ck in cookies:
                if 'sameSite' in ck and ck['sameSite'] not in ['Strict', 'Lax', 'None']: del ck['sameSite']
                try: driver.add_cookie(ck)
                except: pass 
            return driver
        except:
            if driver: force_kill_driver(driver)
            time.sleep(2) 
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

# --- 任务发现 ---

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

# --- 业务逻辑 ---

def scrape_product_price_details(driver: uc.Chrome, product_url: str) -> Optional[Dict]:
    details = {}
    if not navigate_with_retries(driver, product_url, max_attempts=2):
        return {"_status": "page_load_failed"}
    try:
        # 404 检测
        try:
            time.sleep(random.uniform(2, 4))
            err404 = driver.find_elements(By.CSS_SELECTOR, ".error404__title")
            if err404 and err404[0].is_displayed():
                logging.info(f"页面显示 404 标题，判定为失效链接: {product_url}")
                return {"_status": "invalid"}
        except: pass

        # 等待核心元素
        title_selector = "h1.product-header__title"
        try:
            WebDriverWait(driver, PAGE_NAVIGATION_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, title_selector))
            )
        except TimeoutException:
            logging.error(f"[FAILED] 页面加载超时(无标题): {product_url}")
            return {"_status": "page_load_failed"}

        # 2. 提取数据
        # --- Price ---
        try: 
            price_elem = driver.find_element(By.CSS_SELECTOR, "span.price--lg span.price__numbers--bold")
            price_val = parse_price(price_elem.text.strip())
            details["价格"] = f"€{price_val:.2f}" if price_val is not None else "N/A"
        except: details["价格"] = "N/A"
        
        # --- Shipping ---
        shipping_found = False
        for _ in range(10):
            try:
                shipping_elem = driver.find_element(By.CSS_SELECTOR, ".add-07, .bold.notranslate.bold")
                if shipping_elem.is_displayed():
                    details["运费"] = shipping_elem.text.strip().replace(',', '.')
                    shipping_found = True
                    break
            except: pass
            time.sleep(1)
        if not shipping_found: details["运费"] = "N/A"

        try:
            seller_elem = driver.find_element(By.CSS_SELECTOR, "a[class*='product-price-info__link'] span")
            details["销售和发货方"] = seller_elem.text.strip()
        except: details["销售和发货方"] = "Worten"
        return details
    except Exception as e:
        return {"_status": "page_load_failed", "_error": str(e)}

# --- Worker (核心修改点：继承进度功能) ---

class ScraperWorker:
    def __init__(self, url_queue, all_product_data, results_lock, session_queue, discovery_completed_event, log_queue=None, increment_queue=None):
        self.url_queue = url_queue
        self.all_product_data = all_product_data
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
                time.sleep(2)
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
            for attempt in range(URL_RETRY_LIMIT + 1):
                data = scrape_product_price_details(self.driver, url)
                if isinstance(data, dict) and data.get('_status'):
                    status = data.get('_status')
                    if status == 'invalid':
                        with self.results_lock: self.all_product_data.append({'商品链接': url, '价格': '失效链接'})
                        return True
                    elif status == 'page_load_failed':
                        if attempt == URL_RETRY_LIMIT:
                            with self.results_lock:
                                self.all_product_data.append({'商品链接': url, '价格': '抓取失败', '运费': '抓取失败'})
                            return False
                        continue # 重试

                # 检查运费 (Partial Success check)
                if not data.get("运费") or data.get("运费") == "N/A":
                    logging.warning(f"[Worker {self.worker_id}] 运费为空，重试...")
                    if attempt == URL_RETRY_LIMIT:
                        with self.results_lock: self.all_product_data.append({'商品链接': url, '价格': '抓取失败'})
                        return False
                    continue
                
                data['商品链接'] = url
                with self.results_lock: self.all_product_data.append(data)
                return True
        except Exception as e:
            logging.error(f"Worker Error: {e}")
            return False
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
    
    logging.info(f"--- 价格检查启动 (Workers: {MAX_WORKERS}) ---")
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
        
        all_product_data = manager.list()
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
                        'message': f'正在检查价格: {processed_count.value}/{total_estimated.value}'
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

        discovery_p = multiprocessing.Process(target=discovery_process_with_progress, args=(initial_urls, url_queue, session_queue, discovery_completed_event, log_queue, total_estimated, total_increment_queue))
        discovery_p.start()

        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(ScraperWorker(url_queue, all_product_data, results_lock, session_queue, discovery_completed_event, log_queue, increment_queue).run) for _ in range(MAX_WORKERS)]
            wait(futures)

        # 收尾
        stop_flag.value = True
        log_queue.put(None)
        discovery_p.join(timeout=5)
        for p in producers: p.join(timeout=5)
        pm_p.join(timeout=5)
        
        save_data_to_excel(list(all_product_data), OUTPUT_FILE)
        if progress_callback: 
            # 计算最终的平均速率
            elapsed_time = time.time() - start_time.value
            final_rate = processed_count.value / (elapsed_time / 60) if elapsed_time > 0 else 0
            progress_callback({'processed': processed_count.value, 'total': total_estimated.value, 'rate': final_rate, 'message': '任务完成！'})

    if os.name == 'nt':
        try: subprocess.run("taskkill /F /T /IM chrome*", shell=True, stderr=subprocess.DEVNULL)
        except: pass

if __name__ == '__main__':
    main()