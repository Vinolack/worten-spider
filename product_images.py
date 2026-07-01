import ipaddress
import logging
import multiprocessing
import os
import queue
import random
import socket
import threading
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, wait
from datetime import datetime
from logging.handlers import QueueHandler
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import certifi
import ssl
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

import browser_runtime
import configloader
import paths as path_utils
from excel_io import read_url_rows, write_single_sheet_excel
from excel_schemas import PRODUCT_IMAGE_COLUMNS, PRODUCT_IMAGE_SHEET
from state_store import StateStore, default_state_db
from url_classifier import is_allowed_worten_product_url
from product_info import (
    BASE_URL,
    DEFAULT_MAX_URLS_PER_DRIVER_MAX,
    DEFAULT_MAX_URLS_PER_DRIVER_MIN,
    IMAGE_TRANSFER_FAILED,
    IMAGE_UPLOAD_FAILED,
    build_requests_proxy_url,
    close_cookie_pup,
    convert_avif_to_jpg,
    create_chrome_driver,
    download_image,
    force_kill_driver,
    get_fresh_session,
    navigate_with_retries,
    normalize_image_url,
    upload_to_image_host,
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
exe_folder = get_exe_dir()

INPUT_FILE = os.path.join(exe_folder, "input_links.xlsx")
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_FILE = os.path.join(exe_folder, f"worten_product_images_{timestamp}.xlsx")

MAX_RETRIES = 3
URL_RETRY_LIMIT = 5
MAX_WORKERS = int(c.get_key('MAX_WORKER') or 4)
PAGE_NAVIGATION_TIMEOUT = 60
IMAGE_WAIT_TIMEOUT = 30
IMAGE_SELECTOR = "img.product-gallery__slider-image"
ALLOWED_IMAGE_HOSTS = ("worten.pt", "worten-static.pt", "wortenimages.pt")
MODE = 'product_images'

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - [Process %(process)d] - %(message)s')
logging.getLogger('seleniumwire').setLevel(logging.ERROR)


class WorkerPoisonedException(Exception):
    pass


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
        rows = read_url_rows(filename, include_pages=False)
        if rows is None:
            logging.error(f"错误: Excel文件 '{filename}' 中未找到名为 'url' 的列。")
        return rows
    except FileNotFoundError:
        logging.error(f"错误: 输入文件 '{filename}' 未找到。")
        return None
    except Exception as exc:
        logging.error(f"读取Excel文件 '{filename}' 时发生错误: {exc}")
        return None


def is_safe_image_url(url: str) -> bool:
    try:
        parsed = urlparse(str(url or '').strip())
        if parsed.scheme != 'https' or parsed.username or parsed.password:
            return False
        host = (parsed.hostname or '').lower()
        if not host:
            return False
        if host == 'localhost' or host.endswith('.localhost'):
            return False
        try:
            ipaddress.ip_address(host)
            return False
        except ValueError:
            pass
        if not any(host == allowed or host.endswith(f'.{allowed}') for allowed in ALLOWED_IMAGE_HOSTS):
            return False
        try:
            resolved = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
        except OSError:
            return False
        for result in resolved:
            address = result[4][0]
            ip = ipaddress.ip_address(address)
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified:
                return False
        return True
    except Exception:
        return False


def product_image_failure_row(url: str, reason: str) -> Dict[str, str]:
    row = {'商品链接': url}
    for index in range(1, 6):
        row[f'图{index}'] = reason
    return {column: row.get(column, '') for column in PRODUCT_IMAGE_COLUMNS}


def empty_product_image_row(url: str) -> Dict[str, str]:
    row = {'商品链接': url}
    for index in range(1, 6):
        row[f'图{index}'] = ''
    return row


def save_data_to_excel(product_image_rows: List[Dict[str, Any]], filename: str) -> None:
    rows = [{column: row.get(column, '') for column in PRODUCT_IMAGE_COLUMNS} for row in product_image_rows]
    write_single_sheet_excel(rows, filename, PRODUCT_IMAGE_COLUMNS, PRODUCT_IMAGE_SHEET)
    logging.info(f"已将 {len(rows)} 条商品图片数据保存到 {filename}。")


def normalize_product_input(raw_url: str) -> str:
    return urljoin(BASE_URL, str(raw_url or '').strip())


def _record_failed_discovery_task(state, run_id, task, reason, total_increment_queue, increment_queue):
    inserted = state.add_task(run_id, task, MODE, max_attempts=1)
    if inserted and total_increment_queue:
        total_increment_queue.put(1)
    if inserted or task.get('task_key'):
        saved = state.fail_task(run_id, task['task_key'], MODE, [product_image_failure_row(task['url'], reason)], reason)
        if saved and increment_queue:
            increment_queue.put(1)
    return inserted


def discovery_process_with_progress(
    initial_urls,
    discovery_completed_event,
    log_queue,
    total_estimated,
    total_increment_queue,
    increment_queue,
    stop_flag=None,
    state_db_path=None,
    run_id=None,
):
    setup_log_queue_handler(log_queue)
    logging.info("--- [商品图片发现进程] 启动 ---")
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

            full_url = normalize_product_input(raw_url)
            task = {'url': full_url, 'type': 'product_page'}
            if is_allowed_worten_product_url(full_url):
                inserted = state.add_task(run_id, task, MODE, max_attempts=URL_RETRY_LIMIT + 1)
                if inserted and total_increment_queue:
                    total_increment_queue.put(1)
                logging.debug(f"[商品图片发现进程] 已加入商品图片任务 ({index}/{len(initial_urls)}): {full_url}")
            else:
                logging.warning(f"[商品图片发现进程] 无效商品链接，已记录失败: {raw_url}")
                _record_failed_discovery_task(state, run_id, task, '无效商品链接', total_increment_queue, increment_queue)

        state.mark_discovery_finished(run_id)
        logging.info("--- [商品图片发现进程] 全部完成 ---")
    except InterruptedError as exc:
        logging.info(f"--- [商品图片发现进程] 已停止: {exc} ---")
    except Exception as exc:
        logging.error(f"--- [商品图片发现进程] 失败: {exc} ---")
        if state:
            state.mark_discovery_failed(run_id, str(exc))
        raise
    finally:
        discovery_completed_event.set()


def _first_srcset_url(srcset: str) -> str:
    if not srcset:
        return ''
    first_candidate = srcset.split(',', 1)[0].strip()
    return first_candidate.split(' ', 1)[0].strip()


def collect_gallery_image_urls(driver, product_url: str) -> List[Dict[str, str]]:
    image_urls = []
    seen = set()
    for img in driver.find_elements(By.CSS_SELECTOR, IMAGE_SELECTOR):
        raw_src = (
            img.get_attribute('src')
            or img.get_attribute('data-src')
            or img.get_attribute('data-original')
            or _first_srcset_url(img.get_attribute('srcset') or '')
        )
        normalized_url = normalize_image_url(raw_src)
        if not normalized_url or normalized_url in seen:
            continue
        seen.add(normalized_url)
        image_urls.append({'raw_src': raw_src or '', 'normalized_url': normalized_url})
        if raw_src and raw_src != normalized_url:
            logging.info(f"[图片URL规范化] product_url={product_url}, raw_src={raw_src}, normalized_url={normalized_url}")
        if len(image_urls) >= 5:
            break
    return image_urls


def process_gallery_image(image_info: Dict[str, str], product_url: str, proxy_url: Optional[str] = None) -> str:
    raw_src = image_info.get('raw_src') or ''
    url = image_info.get('normalized_url') or ''
    if not is_safe_image_url(url):
        logging.warning(f"图片地址不安全，跳过下载: product_url={product_url}, raw_src={raw_src}, normalized_url={url}")
        return '图片地址不安全'

    image_content, filename_or_failure, content_type = download_image(url, proxy_url=proxy_url)
    if image_content is None:
        logging.warning(f"图片下载失败，已标记为 {filename_or_failure}: product_url={product_url}, raw_src={raw_src}, normalized_url={url}")
        return filename_or_failure

    filename = filename_or_failure
    converted_image = convert_avif_to_jpg(image_content, content_type, url, product_url=product_url)
    if converted_image is None:
        logging.warning(f"图片转换失败，已标记为 {IMAGE_TRANSFER_FAILED}: product_url={product_url}, raw_src={raw_src}, normalized_url={url}, filename={filename}, content_type={content_type or 'unknown'}")
        return IMAGE_TRANSFER_FAILED

    image_content, filename, content_type = converted_image
    uploaded_url, upload_failure_reason = upload_to_image_host(image_content, filename, source_url=url, content_type=content_type, product_url=product_url)
    if uploaded_url:
        return uploaded_url

    failure_value = upload_failure_reason or IMAGE_UPLOAD_FAILED
    logging.warning(f"图片上传失败，已标记为 {failure_value}: product_url={product_url}, raw_src={raw_src}, normalized_url={url}, filename={filename}, content_type={content_type or 'unknown'}")
    return failure_value


def scrape_product_images(driver, product_url: str, proxy_url: Optional[str] = None) -> Dict[str, Any]:
    if not is_allowed_worten_product_url(product_url):
        return {**product_image_failure_row(product_url, '无效商品链接'), '_status': 'invalid'}

    if not navigate_with_retries(driver, product_url, max_attempts=5):
        logging.error(f"[FAILED] 商品图片页面导航失败: {product_url}")
        return {**product_image_failure_row(product_url, '页面加载失败'), '_status': 'page_load_failed'}

    try:
        time.sleep(random.uniform(1, 2))
        err404 = driver.find_elements(By.CSS_SELECTOR, ".error404__title")
        if err404 and err404[0].is_displayed():
            logging.info(f"页面显示 404 标题，判定为失效链接: {product_url}")
            return {**product_image_failure_row(product_url, '失效链接'), '_status': 'invalid'}
    except Exception:
        pass

    close_cookie_pup(driver)

    try:
        WebDriverWait(driver, IMAGE_WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, IMAGE_SELECTOR))
        )
    except TimeoutException:
        try:
            driver.execute_script("window.stop();")
        except Exception:
            pass
        logging.error(f"[FAILED] 商品图片加载失败: {product_url} (等待图库图片超时)。")
        return {**product_image_failure_row(product_url, '未找到图片'), '_status': 'no_images'}

    row = empty_product_image_row(product_url)
    image_urls = collect_gallery_image_urls(driver, product_url)
    if not image_urls:
        return {**product_image_failure_row(product_url, '未找到图片'), '_status': 'no_images'}

    for image_index, image_info in enumerate(image_urls[:5], start=1):
        row[f'图{image_index}'] = process_gallery_image(image_info, product_url, proxy_url=proxy_url)
    row['_status'] = 'ok'
    return row


class ScraperWorker:
    def __init__(
        self,
        discovery_completed_event,
        log_queue=None,
        increment_queue=None,
        state_db_path=None,
        run_id=None,
        stop_flag=None,
        session_lock=None,
        chrome_init_lock=None,
    ):
        self.discovery_completed_event = discovery_completed_event
        self.log_queue = log_queue
        self.increment_queue = increment_queue
        self.state_db_path = state_db_path
        self.run_id = run_id
        self.stop_flag = stop_flag
        self.state = StateStore(state_db_path) if state_db_path and run_id else None
        self.session_lock = session_lock
        self.chrome_init_lock = chrome_init_lock
        self.worker_id = str(uuid.uuid4())[:8]
        self.driver = None
        self.proxy_for_requests = None
        self.processed_count = 0
        self.consecutive_failures = 0
        self.current_max_urls = random.randint(DEFAULT_MAX_URLS_PER_DRIVER_MIN, DEFAULT_MAX_URLS_PER_DRIVER_MAX)

    def should_stop(self) -> bool:
        return bool(self.stop_flag is not None and self.stop_flag.value)

    def setup_driver(self) -> bool:
        logging.debug(f"[Worker {self.worker_id}] 准备启动 Driver...")
        for attempt in range(MAX_RETRIES):
            if self.should_stop():
                return False
            session = get_fresh_session(self.session_lock)
            if not session:
                logging.warning(f"[Worker {self.worker_id}] 获取会话超时，正在重试 ({attempt + 1}/{MAX_RETRIES})...")
                time.sleep(5 * (attempt + 1))
                continue
            self.driver = create_chrome_driver(session, self.chrome_init_lock)
            if self.driver:
                self.proxy_for_requests = session.get('proxy_for_selenium_wire') or build_requests_proxy_url()
                self.processed_count = 0
                time.sleep(random.uniform(1, 2))
                return True
            logging.warning(f"[Worker {self.worker_id}] 当前会话/代理不可用，将丢弃并获取新会话重试...")
        logging.error(f"[Worker {self.worker_id}] 连续 {MAX_RETRIES} 次启动 Driver 失败。")
        return False

    def teardown_driver(self) -> None:
        if self.driver:
            force_kill_driver(self.driver)
            self.driver = None
        self.proxy_for_requests = None

    def handle_driver_unavailable(self, task_key, product_url, reason):
        if self.should_stop():
            if task_key:
                self.state.release_task(self.run_id, task_key, self.worker_id, consume_attempt=False, error='任务已停止')
            return False
        if task_key:
            return self.record_task_failure(task_key, MODE, [product_image_failure_row(product_url, reason)], reason)
        return False

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
        if task_type != 'product_page':
            return self.record_task_failure(task_key, MODE, [product_image_failure_row(url, '不支持的任务类型')], f'不支持的任务类型: {task_type}')

        try:
            row = scrape_product_images(self.driver, url, proxy_url=self.proxy_for_requests)
            status = row.pop('_status', 'ok')
            if status in ('page_load_failed', 'invalid', 'no_images'):
                reason = row.get('图1') or '抓取失败'
                return self.record_task_failure(task_key, MODE, [{column: row.get(column, '') for column in PRODUCT_IMAGE_COLUMNS}], reason)

            saved = self.record_task_result(task_key, MODE, [{column: row.get(column, '') for column in PRODUCT_IMAGE_COLUMNS}])
            if saved:
                logging.info(f"[Worker {self.worker_id}] 成功处理商品图片: {url}")
            return saved
        except Exception as exc:
            logging.error(f"[Worker {self.worker_id}] 商品图片任务失败 {url}: {exc}")
            return self.record_task_failure(task_key, MODE, [product_image_failure_row(url, '抓取失败')], str(exc))

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
                        logging.info(f"[Worker {self.worker_id}] 发现已结束且无可领取任务，退出。")
                        break
                    time.sleep(2)
                    continue

                task_key = task.get('task_key')
                if self.should_stop():
                    if task_key:
                        self.state.release_task(self.run_id, task_key, self.worker_id, consume_attempt=False, error='任务已停止')
                    task_key = None
                    break

                if task.get('type') != 'product_page' or not is_allowed_worten_product_url(str(task.get('url') or '').strip()):
                    if self.process_task(task) and self.increment_queue:
                        self.increment_queue.put(1)
                    task_key = None
                    continue

                if self.driver is None:
                    if not self.setup_driver():
                        saved = self.handle_driver_unavailable(task_key, task.get('url'), 'driver_setup_failed')
                        if saved and self.increment_queue:
                            self.increment_queue.put(1)
                        task_key = None
                        if self.should_stop():
                            break
                        logging.error(f"[Worker {self.worker_id}] 无法创建 Driver，当前任务标记失败。")
                        time.sleep(10)
                        continue

                if self.processed_count >= self.current_max_urls:
                    logging.info(f"[Worker {self.worker_id}] 轮换 Driver...")
                    self.teardown_driver()
                    if not self.setup_driver():
                        saved = self.handle_driver_unavailable(task_key, task.get('url'), 'driver_rotation_failed')
                        if saved and self.increment_queue:
                            self.increment_queue.put(1)
                        task_key = None
                        if self.should_stop():
                            break
                        logging.error(f"[Worker {self.worker_id}] 轮换 Driver 失败，当前任务标记失败。")
                        time.sleep(10)
                        continue

                if self.should_stop():
                    if task_key:
                        self.state.release_task(self.run_id, task_key, self.worker_id, consume_attempt=False, error='任务已停止')
                    task_key = None
                    break

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
                    logging.error(f"[Worker {self.worker_id}] 连续失败3次，强制重启。")
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

    logging.info("--- Worten 商品图片处理启动 ---")
    initial_urls = read_urls_from_excel(input_file)
    if not initial_urls:
        logging.error("未找到输入链接。")
        if progress_callback:
            progress_callback({'processed': 0, 'total': 0, 'rate': 0, 'message': '未找到输入链接'})
        return {'status': 'failed', 'message': '未找到输入链接'}

    run_id, resumed = state.create_or_resume_run(MODE, input_file, output_file)
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
                    total = progress['total'] or total_estimated.value
                    progress_callback({
                        'processed': progress['processed'],
                        'total': total,
                        'rate': rate,
                        'message': f"正在处理商品图片: {progress['processed']}/{total}",
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
                message = '商品图片发现进程异常退出'
                state.set_run_status(run_id, 'failed', message)
                raise RuntimeError(message)

            if state.has_incomplete_tasks(run_id):
                with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [
                        executor.submit(
                            ScraperWorker(
                                discovery_completed_event,
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
                logging.info("[主进程] 没有待处理的商品图片任务，跳过 Worker 阶段。")
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
            message = '商品图片发现进程异常退出'
            state.set_run_status(run_id, 'failed', message)
            raise RuntimeError(message)

        rows_by_group = state.grouped_result_rows(run_id)
        product_image_rows = rows_by_group.get(MODE, [])
        save_data_to_excel(product_image_rows, output_file)
        stats = state.queue_stats(run_id)
        failed_final = stats.get('failed_final', 0)
        if failed_final:
            message = f"商品图片处理完成，但有 {failed_final} 个链接处理失败；失败项已写入结果。"
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


if __name__ == '__main__':
    main()
