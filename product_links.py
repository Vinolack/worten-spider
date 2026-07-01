import ssl
import certifi
import os
import time
import json
import uuid
import random
import logging
import threading
import multiprocessing
import subprocess
import queue
from concurrent.futures import ProcessPoolExecutor, wait
from logging.handlers import QueueHandler
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import seleniumwire.undetected_chromedriver as uc 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

import configloader
import paths as path_utils
import browser_runtime
from state_store import StateStore, default_state_db
from excel_schemas import PRODUCT_LINK_COLUMNS, PRODUCT_LINK_SHEET
from excel_io import read_url_rows, write_single_sheet_excel
from url_classifier import (
    append_page_param,
    extract_seller_id,
    is_allowed_worten_product_url,
    parse_pages_to_scrape as parse_pages_to_scrape_shared,
    seller_search_url,
)

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

INPUT_FILE = os.path.join(exe_folder, "input_links.xlsx")
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_FILE = os.path.join(exe_folder, f"worten_product_links_{timestamp}.xlsx")

MAX_RETRIES = 3
URL_RETRY_LIMIT = 1
CF_BYPASS_PORT = int(c.get_key('cf_bypass_port') or 3000)
SELLER_SCRAPED_PAGE_COUNT = int(c.get_key('SELLER_SCRAPED_PAGE_COUNT'))
MAX_URLS_PER_DISCOVERY = 15
MAX_WORKERS = int(c.get_key('MAX_WORKER') or 4)

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - [Process %(process)d] - %(message)s')
logging.getLogger('seleniumwire').setLevel(logging.WARNING)

def is_safe_worten_listing_url(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or '').lower()
    return parsed.scheme == 'https' and (host == 'worten.pt' or host.endswith('.worten.pt'))


LISTING_LINK_SELECTOR = ".listing-content__list li a"


def setup_log_queue_handler(log_queue):
    if log_queue is not None:
        try:
            qh = QueueHandler(log_queue)
            root = logging.getLogger()
            if root.handlers:
                for handler in root.handlers[:]:
                    root.removeHandler(handler)
            root.addHandler(qh)
            root.setLevel(LOG_LEVEL)
        except Exception:
            pass


def read_urls_from_excel(filename: str) -> Optional[List[Dict[str, Any]]]:
    try:
        rows = read_url_rows(filename, include_pages=True)
        if rows is None:
            logging.error(f"错误: Excel文件 '{filename}' 中未找到名为 'url' 的列。")
        return rows
    except FileNotFoundError:
        logging.error(f"错误: 输入文件 '{filename}' 未找到。")
        return None
    except Exception as exc:
        logging.error(f"读取Excel文件 '{filename}' 时发生错误: {exc}")
        return None


def deduplicate_product_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    seen = set()
    deduplicated = []
    for row in rows:
        url = str(row.get('商品链接') or '').strip()
        if not url:
            continue
        full_url = urljoin(BASE_URL, url)
        if not is_allowed_worten_product_url(full_url) or full_url in seen:
            continue
        seen.add(full_url)
        deduplicated.append({'商品链接': full_url})
    return deduplicated


def save_data_to_excel(product_links: List[Dict[str, Any]], filename: str) -> None:
    rows = deduplicate_product_rows(product_links)
    write_single_sheet_excel(rows, filename, PRODUCT_LINK_COLUMNS, PRODUCT_LINK_SHEET)
    logging.info(f"已将 {len(rows)} 条商品链接保存到 {filename}。")


def navigate_with_retries(driver: uc.Chrome, url: str, max_attempts: int = 3, backoff_base: int = 2) -> bool:
    return browser_runtime.navigate_with_retries(driver, url, max_attempts, backoff_base)


def force_kill_driver(driver):
    browser_runtime.force_kill_driver(driver)


def get_fresh_session(session_lock=None):
    if session_lock:
        with session_lock:
            return browser_runtime.create_session_data(c, CF_BYPASS_PORT)
    return browser_runtime.create_session_data(c, CF_BYPASS_PORT)


def create_chrome_driver(session_data: Dict, chrome_init_lock=None) -> Optional[uc.Chrome]:
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


def parse_pages_to_scrape(pages_value):
    return parse_pages_to_scrape_shared(pages_value, SELLER_SCRAPED_PAGE_COUNT)


def _close_listing_cookie_popup(driver: uc.Chrome) -> None:
    try:
        button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".checkYes.button")))
        driver.execute_script("arguments[0].click();", button)
    except Exception:
        pass


def scrape_listing_product_links(driver: uc.Chrome, listing_url: str) -> Optional[List[Dict[str, str]]]:
    if not is_safe_worten_listing_url(str(listing_url or '').strip()):
        logging.warning(f"[商品链接发现] 拒绝导航不安全的列表页地址: {listing_url}")
        return None

    if not navigate_with_retries(driver, listing_url, max_attempts=2):
        logging.warning(f"[商品链接发现] 列表页导航失败: {listing_url}")
        return None

    try:
        _close_listing_cookie_popup(driver)
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, LISTING_LINK_SELECTOR)))
        except TimeoutException:
            logging.warning(f"[商品链接发现] 商品列表选择器超时，列表页失败: {listing_url}")
            return None

        rows = []
        for element in driver.find_elements(By.CSS_SELECTOR, LISTING_LINK_SELECTOR):
            href = element.get_attribute('href')
            if not href:
                continue
            full_url = urljoin(BASE_URL, href)
            if is_allowed_worten_product_url(full_url):
                rows.append({'商品链接': full_url})
            else:
                logging.debug(f"[商品链接发现] 跳过非商品链接: {full_url}")
        return deduplicate_product_rows(rows)
    except Exception as exc:
        logging.error(f"[商品链接发现] 列表页处理失败: {listing_url} - {exc}")
        return None


def _record_direct_product_link(state, run_id, url, total_increment_queue, increment_queue):
    full_url = urljoin(BASE_URL, str(url).strip())
    if not is_allowed_worten_product_url(full_url):
        return False

    task = {'url': full_url, 'type': 'product_link'}
    if state.add_task(run_id, task, 'product_links', max_attempts=1):
        if total_increment_queue:
            total_increment_queue.put(1)
        state.complete_task(run_id, task['task_key'], 'product_links', [{'商品链接': full_url}])
        if increment_queue:
            increment_queue.put(1)
        return True

    task_key = task.get('task_key')
    if not task_key:
        return False
    with state.connect() as conn:
        row = conn.execute(
            "SELECT status FROM tasks WHERE run_id = ? AND task_key = ?",
            (run_id, task_key),
        ).fetchone()
        if row and row['status'] not in ('succeeded', 'failed_final', 'invalid', 'cancelled'):
            payload = json.dumps([{'商品链接': full_url}], ensure_ascii=False)
            conn.execute(
                """
                UPDATE tasks
                SET status = 'succeeded', result_group = 'product_links', result_json = ?,
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = ?, finished_at = ?
                WHERE run_id = ? AND task_key = ?
                """,
                (payload, datetime.utcnow().isoformat(timespec='seconds'), datetime.utcnow().isoformat(timespec='seconds'), run_id, task_key),
            )
            if increment_queue:
                increment_queue.put(1)
            return True
    return False


def _task_result_info(state, run_id: str, task_key: str):
    with state.connect() as conn:
        row = conn.execute(
            "SELECT status, result_json FROM tasks WHERE run_id = ? AND task_key = ?",
            (run_id, task_key),
        ).fetchone()
    if not row or row['status'] not in ('succeeded', 'failed_final', 'invalid', 'cancelled'):
        return None, None
    try:
        row_count = len(json.loads(row['result_json'] or '[]'))
    except Exception:
        row_count = None
    return row['status'], row_count


def discovery_process_with_progress(
    initial_urls,
    discovery_completed_event,
    log_queue,
    total_estimated,
    total_increment_queue,
    increment_queue,
    stop_flag=None,
    session_lock=None,
    chrome_init_lock=None,
    state_db_path=None,
    run_id=None,
):
    setup_log_queue_handler(log_queue)
    logging.info("--- [商品链接发现进程] 启动 ---")
    state = StateStore(state_db_path) if state_db_path and run_id else None

    def should_stop() -> bool:
        return bool(stop_flag is not None and stop_flag.value)

    try:
        if not state:
            raise RuntimeError("StateStore 未初始化，发现进程无法运行")
        state.mark_discovery_started(run_id)
        total_estimated.value = 0

        for index, item in enumerate(initial_urls, start=1):
            if should_stop():
                raise InterruptedError("任务已停止")
            raw_url = str(item.get('url') or '').strip()
            if not raw_url:
                continue

            normalized_url = urljoin(BASE_URL, raw_url)
            if is_allowed_worten_product_url(normalized_url):
                _record_direct_product_link(state, run_id, normalized_url, total_increment_queue, increment_queue)
                continue

            seller_id = extract_seller_id(normalized_url)
            target_url = seller_search_url(seller_id) if seller_id else normalized_url
            if not is_safe_worten_listing_url(target_url):
                logging.warning(f"[商品链接发现] 拒绝不安全的列表页地址: {raw_url}")
                task = {'url': target_url, 'type': 'listing_page', 'source_url': raw_url, 'page': None}
                inserted = state.add_task(run_id, task, 'product_links', max_attempts=1)
                if inserted:
                    if total_increment_queue:
                        total_increment_queue.put(1)
                    state.fail_task(run_id, task['task_key'], 'product_links', [], '不安全的列表页地址')
                    if increment_queue:
                        increment_queue.put(1)
                else:
                    status, _ = _task_result_info(state, run_id, task.get('task_key'))
                    if status is None and task.get('task_key'):
                        state.fail_task(run_id, task['task_key'], 'product_links', [], '不安全的列表页地址')
                        if increment_queue:
                            increment_queue.put(1)
                continue

            pages = list(parse_pages_to_scrape(item.get('pages_to_scrape')))
            logging.info(f"[商品链接发现进程] 正在展开 ({index}/{len(initial_urls)}): {target_url} (页数: {pages})")

            for page_num in pages:
                if should_stop():
                    raise InterruptedError("任务已停止")
                page_url = append_page_param(target_url, page_num)
                task = {'url': page_url, 'type': 'listing_page', 'source_url': raw_url, 'page': page_num}
                inserted = state.add_task(run_id, task, 'product_links', max_attempts=URL_RETRY_LIMIT + 1)
                if inserted:
                    if total_increment_queue:
                        total_increment_queue.put(1)
                    continue

                status, row_count = _task_result_info(state, run_id, task.get('task_key'))
                if status in ('succeeded', 'failed_final', 'invalid', 'cancelled'):
                    continue

        state.mark_discovery_finished(run_id)
        logging.info("--- [商品链接发现进程] 全部完成 ---")
    except InterruptedError as exc:
        logging.info(f"--- [商品链接发现进程] 已停止: {exc} ---")
    except Exception as exc:
        logging.error(f"--- [商品链接发现进程] 失败: {exc} ---")
        if state:
            state.mark_discovery_failed(run_id, str(exc))
        raise
    finally:
        discovery_completed_event.set()


class ScraperWorker:
    def __init__(
        self,
        log_queue=None,
        increment_queue=None,
        state_db_path=None,
        run_id=None,
        stop_flag=None,
        session_lock=None,
        chrome_init_lock=None,
    ):
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
        self.session_lock = session_lock
        self.chrome_init_lock = chrome_init_lock

    def should_stop(self) -> bool:
        return bool(self.stop_flag is not None and self.stop_flag.value)

    def setup_driver(self) -> bool:
        for attempt in range(MAX_RETRIES):
            if self.should_stop():
                return False
            session = get_fresh_session(self.session_lock)
            if not session:
                time.sleep(5 * (attempt + 1))
                continue
            self.driver = create_chrome_driver(session, self.chrome_init_lock)
            if self.driver:
                self.processed_count = 0
                return True
            logging.warning(f"[Worker {self.worker_id}] 会话不可用，重试...")
        logging.error(f"[Worker {self.worker_id}] Driver 启动失败。")
        return False

    def teardown_driver(self) -> None:
        if self.driver:
            try:
                force_kill_driver(self.driver)
            finally:
                self.driver = None

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

    def process_task(self, task) -> bool:
        task_key = task.get('task_key')
        task_type = task.get('type')
        url = task.get('url')

        if task_type == 'product_link':
            full_url = urljoin(BASE_URL, str(url or '').strip())
            if is_allowed_worten_product_url(full_url):
                return self.record_task_result(task_key, 'product_links', [{'商品链接': full_url}])
            return self.record_task_failure(task_key, 'product_links', [], '无效商品链接')

        if task_type != 'listing_page':
            logging.warning(f"[Worker {self.worker_id}] 不支持的任务类型: {task_type} {url}")
            return self.record_task_failure(task_key, 'product_links', [], f'不支持的任务类型: {task_type}')

        if not is_safe_worten_listing_url(str(url or '').strip()):
            logging.warning(f"[Worker {self.worker_id}] 拒绝不安全的列表页任务: {url}")
            return self.record_task_failure(task_key, 'product_links', [], '不安全的列表页地址')

        rows = scrape_listing_product_links(self.driver, url)
        if rows is None:
            return self.record_task_failure(task_key, 'product_links', [], '列表页抓取失败')

        saved = self.record_task_result(task_key, 'product_links', rows)
        if saved:
            logging.info(f"[Worker {self.worker_id}] 成功处理列表页: {url}，发现 {len(rows)} 个商品链接。")
        return saved

    def _cleanup_driver_state(self) -> None:
        if not self.driver:
            return
        try:
            self.driver.delete_all_cookies()
        except Exception:
            pass
        try:
            browser_runtime.clear_driver_requests(self.driver)
        except Exception:
            pass

    def run(self):
        setup_log_queue_handler(self.log_queue)
        if not self.state:
            logging.error(f"[Worker {self.worker_id}] 状态存储不可用，Worker 退出。")
            return

        startup_delay = random.uniform(1, max(3, MAX_WORKERS))
        logging.info(f"[Worker {self.worker_id}] 启动，延迟 {startup_delay:.1f}s 后开始...")
        time.sleep(startup_delay)

        task_key = None
        try:
            while True:
                if self.should_stop():
                    break

                task = self.state.claim_next_task(self.run_id, self.worker_id)
                if not task:
                    if self.state.is_discovery_finished(self.run_id):
                        break
                    time.sleep(2)
                    continue

                task_key = task.get('task_key')
                if self.should_stop():
                    if task_key:
                        self.state.release_task(self.run_id, task_key, self.worker_id, consume_attempt=False, error='任务已停止')
                    task_key = None
                    break

                if task.get('type') != 'listing_page':
                    if self.process_task(task) and self.increment_queue:
                        self.increment_queue.put(1)
                    task_key = None
                    continue

                if task.get('type') == 'listing_page' and self.driver is None:
                    if not self.setup_driver():
                        if task_key:
                            saved = self.record_task_failure(task_key, 'product_links', [], 'driver_setup_failed')
                            if saved and self.increment_queue:
                                self.increment_queue.put(1)
                        task_key = None
                        logging.error(f"[Worker {self.worker_id}] 无法创建 Driver，当前任务标记失败。")
                        time.sleep(10)
                        continue

                if self.processed_count >= MAX_URLS_PER_DISCOVERY:
                    logging.info(f"[Worker {self.worker_id}] 轮换 Driver...")
                    self.teardown_driver()
                    if not self.setup_driver():
                        if task_key:
                            saved = self.record_task_failure(task_key, 'product_links', [], 'driver_rotation_failed')
                            if saved and self.increment_queue:
                                self.increment_queue.put(1)
                        task_key = None
                        logging.error(f"[Worker {self.worker_id}] 轮换 Driver 失败，当前任务标记失败。")
                        time.sleep(10)
                        continue

                success = self.process_task(task)
                task_key = None
                if success and self.increment_queue:
                    self.increment_queue.put(1)
                self._cleanup_driver_state()
                self.processed_count += 1
                if success:
                    self.consecutive_failures = 0
                else:
                    self.consecutive_failures += 1
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


def progress_manager(processed_count, total_estimated, increment_queue, total_increment_queue, stop_flag):
    def drain_progress_items():
        while True:
            try:
                increment_queue.get_nowait()
                processed_count.value += 1
            except queue.Empty:
                break
        while True:
            try:
                total_increment_data = total_increment_queue.get_nowait()
                total_estimated.value += total_increment_data
            except queue.Empty:
                break

    while True:
        try:
            drain_progress_items()
            if stop_flag.value:
                drain_progress_items()
                break
            time.sleep(0.2)
        except Exception:
            if stop_flag.value:
                break
            time.sleep(0.2)


def main(progress_callback=None, stop_check_callback=None, input_file=None, output_file=None, state_db_path=None):
    multiprocessing.freeze_support()
    os.environ["WDM_DEFAULT_TIMEOUT"] = "90"
    input_file = input_file or INPUT_FILE
    output_file = output_file or OUTPUT_FILE
    state_db_path = state_db_path or default_state_db()
    state = StateStore(state_db_path)

    logging.info("--- Worten 商品链接提取启动 ---")
    initial_urls = read_urls_from_excel(input_file)
    if not initial_urls:
        logging.error("未找到输入链接。")
        return {'status': 'failed', 'message': '未找到输入链接'}

    run_id, resumed = state.create_or_resume_run('product_links', input_file, output_file)
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
                if record is None:
                    break
                root.handle(record)
            except Exception:
                break

    with multiprocessing.Manager() as manager:
        log_queue = manager.Queue()
        listener_thread = threading.Thread(target=_log_listener, args=(log_queue,), daemon=True)
        listener_thread.start()

        stop_flag = manager.Value('b', False)
        stop_requested = manager.Value('b', False)
        discovery_completed_event = manager.Event()
        session_lock = manager.Lock()
        chrome_init_lock = manager.Lock()

        processed_count = manager.Value('i', 0)
        total_estimated = manager.Value('i', 0)
        increment_queue = manager.Queue()
        total_increment_queue = manager.Queue()
        start_time = manager.Value('d', time.time())

        pm_p = multiprocessing.Process(target=progress_manager, args=(processed_count, total_estimated, increment_queue, total_increment_queue, stop_flag))
        pm_p.start()

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
                        'message': f"正在提取商品链接: {progress['processed']}/{progress['total'] or total_estimated.value}"
                    })

                now = time.time()
                if now - last_queue_log >= 10:
                    stats = state.queue_stats(run_id)
                    discovery = state.discovery_status(run_id)
                    logging.info(
                        f"[队列状态] discovery={discovery}, total={stats['total']}, "
                        f"pending={stats['pending']}, running={stats['running']}, "
                        f"active_workers={stats['active_workers']}/{MAX_WORKERS}, "
                        f"succeeded={stats['succeeded']}, failed_final={stats['failed_final']}"
                    )
                    last_queue_log = now

                if stop_check_callback and stop_check_callback():
                    stop_requested.value = True
                    stop_flag.value = True
                time.sleep(2)

        updater_t = threading.Thread(target=progress_updater, daemon=True)
        updater_t.start()

        discovery_p = None
        discovery_exitcode = None
        try:
            logging.info("按需请求 Session：跳过备用 Session 生产和预热。")
            if state.discovery_status(run_id) == 'finished':
                logging.info("[主进程] 发现阶段已完成，跳过发现进程。")
                discovery_completed_event.set()
            else:
                discovery_p = multiprocessing.Process(
                    target=discovery_process_with_progress,
                    args=(
                        initial_urls,
                        discovery_completed_event,
                        log_queue,
                        total_estimated,
                        total_increment_queue,
                        increment_queue,
                        stop_flag,
                        session_lock,
                        chrome_init_lock,
                        state_db_path,
                        run_id,
                    ),
                )
                discovery_p.start()
                discovery_p.join()
                discovery_exitcode = discovery_p.exitcode

            if stop_requested.value:
                message = '任务已停止，稍后可再次点击开始/继续任务'
                state.set_run_status(run_id, 'cancelled', message)
                raise RuntimeError(message)
            if state.discovery_status(run_id) == 'failed':
                state.set_run_status(run_id, 'failed', '任务发现失败')
                raise RuntimeError('任务发现失败')
            if discovery_exitcode not in (0, None):
                message = '商品链接发现进程异常退出'
                state.set_run_status(run_id, 'failed', message)
                raise RuntimeError(message)

            if state.has_incomplete_tasks(run_id):
                with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [
                        executor.submit(
                            ScraperWorker(
                                log_queue,
                                increment_queue,
                                state_db_path,
                                run_id,
                                stop_flag,
                                session_lock,
                                chrome_init_lock,
                            ).run
                        )
                        for _ in range(MAX_WORKERS)
                    ]
                    wait(futures)
                    for future in futures:
                        future.result()
            else:
                logging.info("[主进程] 没有待抓取的列表页任务，跳过 Worker 阶段。")
        finally:
            stop_flag.value = True
            try:
                log_queue.put(None)
            except Exception:
                pass
            if discovery_p is not None and discovery_p.is_alive():
                discovery_p.terminate()
            pm_p.join(timeout=5)
            if pm_p.is_alive():
                pm_p.terminate()
            try:
                listener_thread.join(timeout=5)
            except Exception:
                pass

        if stop_requested.value:
            message = '任务已停止，稍后可再次点击开始/继续任务'
            state.set_run_status(run_id, 'cancelled', message)
            raise RuntimeError(message)
        if state.discovery_status(run_id) == 'failed':
            state.set_run_status(run_id, 'failed', '任务发现失败')
            raise RuntimeError('任务发现失败')
        if discovery_exitcode not in (0, None):
            message = '商品链接发现进程异常退出'
            state.set_run_status(run_id, 'failed', message)
            raise RuntimeError(message)

        rows_by_group = state.grouped_result_rows(run_id)
        product_rows = rows_by_group.get('product_links', [])
        save_data_to_excel(product_rows, output_file)
        stats = state.queue_stats(run_id)
        failed_final = stats.get('failed_final', 0)
        if failed_final:
            message = f"商品链接提取完成，但有 {failed_final} 个店铺/列表页抓取失败；失败项已记录，可查看日志或断点数据库。"
            logging.warning(message)
            state.add_event(run_id, 'WARNING', message)
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
        try:
            subprocess.run("taskkill /F /T /IM chrome*", shell=True, stderr=subprocess.DEVNULL)
        except Exception:
            pass


if __name__ == '__main__':
    main()
