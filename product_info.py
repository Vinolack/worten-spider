import ssl
import certifi
import os
import time
import uuid
import random
import logging
import threading
import warnings
from io import BytesIO
from logging.handlers import QueueHandler
import requests
import pandas as pd
try:
    from PIL import Image
    import pillow_avif  # Registers AVIF support for Pillow.
except ImportError:
    Image = None
from urllib3.exceptions import InsecureRequestWarning
import multiprocessing
import subprocess
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from typing import List, Dict, Optional, Any, Tuple
from urllib.parse import unquote, urlsplit, urlunsplit, urljoin
from curl_cffi import requests as cffi_requests

# Selenium
import seleniumwire.undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.common.exceptions import WebDriverException

import configloader
import paths as path_utils
import browser_runtime
from state_store import StateStore, default_state_db
from excel_schemas import PRODUCT_COLUMNS, SELLER_COLUMNS, PRODUCT_SHEET, SHOP_SHEET, SELLER_SHEET, product_failure_row, seller_failure_row
from excel_io import read_url_rows, write_multi_sheet_excel

# --- 全局配置与补丁 ---

# SSL Context
ssl._create_default_https_context = ssl._create_unverified_context
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

# 资源路径
def resource_path(relative_path):
    return path_utils.resource_path(relative_path)

def get_exe_dir():
    return path_utils.get_exe_dir()

# 配置加载
c = configloader.config()
LOG_LEVEL = configloader.get_log_level(c)
CHROME_FOR_TESTING_PATH = resource_path("cft/chrome-win64/chrome.exe")
DRIVER_FOR_TESTING_PATH = resource_path("cft/chromedriver-win64/chromedriver.exe")
BASE_URL = "https://www.worten.pt"
exe_folder = get_exe_dir()

INPUT_FILE = os.path.join(exe_folder, "input_links.xlsx")
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_FILE = os.path.join(exe_folder, f"worten_data_{timestamp}.xlsx")

IMAGE_HOST_UPLOAD_URL = c.get_key('IMAGE_HOST_UPLOAD_URL')
IMAGE_TOKEN = c.get_key('IMAGE_TOKEN')
SELLER_SCRAPED_PAGE_COUNT = int(c.get_key('SELLER_SCRAPED_PAGE_COUNT'))
MAX_RETRIES = 3
URL_RETRY_LIMIT = 5
CF_BYPASS_PORT = int(c.get_key('cf_bypass_port') or 3000)

MAX_WORKERS = int(c.get_key('MAX_WORKER') or 4)
# 默认每个 Driver 处理多少个 URL 后重启
DEFAULT_MAX_URLS_PER_DRIVER_MIN = 15
DEFAULT_MAX_URLS_PER_DRIVER_MAX = 20

# 锁与日志
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - [Process %(process)d] - %(message)s')
logging.getLogger('seleniumwire').setLevel(logging.ERROR)

PAGE_NAVIGATION_TIMEOUT = 60
ELEMENT_WAIT_TIMEOUT = 60

class WorkerPoisonedException(Exception):
    pass

# --- 核心工具函数 ---

def setup_log_queue_handler(log_queue):
    """辅助函数：为子进程配置 QueueHandler"""
    if log_queue is not None:
        try:
            qh = QueueHandler(log_queue)
            root = logging.getLogger()
            # 清除子进程继承的 handler，避免重复打印
            if root.handlers:
                for h in root.handlers[:]:
                    root.removeHandler(h)
            root.addHandler(qh)
            root.setLevel(LOG_LEVEL)
        except Exception:
            pass

def get_cf_cookie_from_api(port: int, proxy_str: Optional[str] = None) -> Optional[Dict]:
    """请求 Cloudflare Bypass API 获取 cookie"""
    return browser_runtime.get_cf_cookie_from_api(c, port, proxy_str)

def close_cookie_pup(driver: uc.Chrome):
    return browser_runtime.close_cookie_pup(driver)

def read_urls_from_excel(filename: str) -> Optional[List[Dict[str, Any]]]:
    """从Excel文件中读取URL列表。"""
    try:
        rows = read_url_rows(filename, include_pages=True)
        if rows is None:
            logging.error(f"错误: Excel文件 '{filename}' 中未找到名为 'url' 的列。")
        return rows
    except FileNotFoundError:
        logging.error(f"错误: 输入文件 '{filename}' 未找到。")
        return None
    except Exception as e:
        logging.error(f"读取Excel文件 '{filename}' 时发生未知错误: {e}")
        return None
    
def save_data_to_multiple_sheets(seller_data: List[Dict], shop_data: List[Dict], product_data: List[Dict], filename: str):
    """
    将不同类型的数据保存到同一个Excel文件的多个工作表中。
    """
    write_multi_sheet_excel(filename, [
        {"name": SELLER_SHEET, "rows": seller_data, "columns": SELLER_COLUMNS},
        {"name": SHOP_SHEET, "rows": shop_data, "columns": PRODUCT_COLUMNS, "text_columns": ["EAN", "SKU"]},
        {"name": PRODUCT_SHEET, "rows": product_data, "columns": PRODUCT_COLUMNS, "text_columns": ["EAN", "SKU"]},
    ])
    logging.info(f"已将 {len(seller_data)} 条卖家数据保存到工作表 '{SELLER_SHEET}'。")
    logging.info(f"已将 {len(shop_data)} 条店铺商品详细数据保存到工作表 '{SHOP_SHEET}'。")
    logging.info(f"已将 {len(product_data)} 条商品详细数据保存到工作表 '{PRODUCT_SHEET}'。")
    logging.info(f"数据已成功保存到文件: {filename}")

def wait_for_safe_cpu(threshold: float = 85.0, check_interval: int = 5):
    browser_runtime.wait_for_safe_cpu(threshold, check_interval)

def force_kill_driver(driver):
    browser_runtime.force_kill_driver(driver)

def parse_price(price_str: str) -> Optional[float]:
    """将价格字符串转换为浮点数。"""
    if not isinstance(price_str, str):
        return None
    try:
        # 移除货币符号, 移除千位分隔符, 将逗号小数改为点号小数, 去除空格
        price_str = price_str.replace('€', '').replace('.', '').replace(',', '.').strip()
        price_str = ''.join(price_str.split())
        return float(price_str)
    except (ValueError, TypeError):
        return None

def navigate_with_retries(driver: uc.Chrome, url: str, max_attempts: int = 3, backoff_base: int = 2) -> bool:
    return browser_runtime.navigate_with_retries(driver, url, max_attempts, backoff_base)
       
# ---  Image Download and Upload Functions ---
IMAGE_DOWNLOAD_FAILED = "download fail"
IMAGE_TRANSFER_FAILED = "transfer fail"
IMAGE_UPLOAD_FAILED = "upload fail"
IMAGE_SOURCE_INVALID = "图片源文件失效"


def build_requests_proxy_url(session_id: Optional[str] = None) -> Optional[str]:
    return browser_runtime.build_requests_proxy_url(c, session_id)


def _image_extension_from_content(content: bytes, content_type: str, fallback_url: str) -> str:
    content_type = (content_type or '').split(';', 1)[0].strip().lower()
    content_type_map = {
        'image/jpeg': '.jpg',
        'image/jpg': '.jpg',
        'image/png': '.png',
        'image/gif': '.gif',
        'image/webp': '.webp',
        'image/avif': '.avif',
        'image/svg+xml': '.svg',
    }
    if content_type in content_type_map:
        return content_type_map[content_type]
    if content.startswith(b'\xff\xd8\xff'):
        return '.jpg'
    if content.startswith(b'\x89PNG\r\n\x1a\n'):
        return '.png'
    if content.startswith((b'GIF87a', b'GIF89a')):
        return '.gif'
    if content.startswith(b'RIFF'):
        return '.webp'
    if content.startswith((b'\x00\x00\x00\x18ftypavif', b'\x00\x00\x00\x1cftypavif')):
        return '.avif'

    path = urlsplit(fallback_url).path
    ext = os.path.splitext(path)[1].lower()
    if not ext or len(ext) > 10 or not ext[1:].isalnum():
        ext = '.jpg'
    return ext


def _filename_for_image(content: bytes, content_type: str, fallback_url: str) -> str:
    return f"{uuid.uuid4()}{_image_extension_from_content(content, content_type, fallback_url)}"


def _is_avif_image(content: bytes, content_type: str) -> bool:
    content_type = (content_type or '').split(';', 1)[0].strip().lower()
    return content_type == 'image/avif' or content.startswith((
        b'\x00\x00\x00\x18ftypavif',
        b'\x00\x00\x00\x1cftypavif',
    ))


def convert_avif_to_jpg(image_content: bytes, content_type: str, source_url: str, product_url: str = '') -> Optional[Tuple[bytes, str, str]]:
    if not _is_avif_image(image_content, content_type):
        return image_content, _filename_for_image(image_content, content_type, source_url), content_type
    if Image is None:
        logging.error(f"AVIF转JPG失败: Pillow/pillow-avif-plugin 未安装, product_url={product_url}, source_url={source_url}")
        return None

    try:
        with Image.open(BytesIO(image_content)) as image:
            if image.mode in ('RGBA', 'LA') or (image.mode == 'P' and 'transparency' in image.info):
                rgba_image = image.convert('RGBA')
                background = Image.new('RGB', rgba_image.size, (255, 255, 255))
                background.paste(rgba_image, mask=rgba_image.split()[-1])
                image = background
            else:
                image = image.convert('RGB')

            output = BytesIO()
            try:
                image.save(output, format='JPEG', quality=90, optimize=True)
                filename = f"{uuid.uuid4()}.jpg"
                logging.info(f"AVIF图片已转为JPG: product_url={product_url}, source_url={source_url}, filename={filename}")
                return output.getvalue(), filename, 'image/jpeg'
            finally:
                output.close()
    except Exception as e:
        logging.error(f"AVIF转JPG失败: product_url={product_url}, source_url={source_url}, error={e}")
        return None


def _strip_image_size_suffix(url: str) -> str:
    parts = list(urlsplit(url))
    if parts[2].endswith('_zoom'):
        parts[2] = parts[2][:-5]
    return urlunsplit(parts)


def normalize_image_url(raw_url: str) -> Optional[str]:
    raw_url = (raw_url or '').strip()
    if not raw_url:
        return None
    if raw_url.startswith('//'):
        raw_url = f"https:{raw_url}"

    url = urljoin(BASE_URL, raw_url)
    parsed = urlsplit(url)
    path = unquote(parsed.path)
    if path.startswith('/i/http://') or path.startswith('/i/https://'):
        url = path[3:]
        if parsed.query:
            url = f"{url}?{parsed.query}"

    return _strip_image_size_suffix(url)


def _image_download_headers() -> Dict[str, str]:
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'Accept-Language': 'pt-PT,pt;q=0.9,en;q=0.8',
        'Referer': BASE_URL,
    }


class NonImageContentError(requests.exceptions.RequestException):
    pass


def _image_download_failure_value(error: Optional[Exception]) -> str:
    if isinstance(error, NonImageContentError):
        return IMAGE_SOURCE_INVALID
    return IMAGE_DOWNLOAD_FAILED


def _looks_like_image(content: bytes, content_type: str) -> bool:
    # Some bad image endpoints return HTTP 200 and even image/jpeg while serving a non-image body.
    signatures = (
        b'\xff\xd8\xff',
        b'\x89PNG\r\n\x1a\n',
        b'GIF87a',
        b'GIF89a',
        b'RIFF',
        b'\x00\x00\x00\x18ftypavif',
        b'\x00\x00\x00\x1cftypavif',
    )
    return any(content.startswith(signature) for signature in signatures)


def _is_retryable_network_error(error: Exception) -> bool:
    return isinstance(error, (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.SSLError,
        requests.exceptions.ChunkedEncodingError,
        requests.exceptions.ContentDecodingError,
    ))


def _request_image(
    url: str,
    headers: Dict[str, str],
    timeout: int,
    proxies: Optional[Dict[str, str]],
    verify: Any,
    attempts: int,
    label: str,
) -> Tuple[Optional[bytes], Optional[str], Optional[Exception], bool]:
    last_error = None
    saw_ssl_error = False
    for attempt in range(attempts):
        try:
            kwargs = {
                'headers': headers,
                'timeout': timeout,
                'proxies': proxies,
                'verify': verify,
                'impersonate': 'chrome110'
            }
            if verify is False:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore', InsecureRequestWarning)
                    response = cffi_requests.get(url, **kwargs)
            else:
                response = cffi_requests.get(url, **kwargs)
            
            response.raise_for_status()
            content = response.content
            content_type = response.headers.get('Content-Type', '')
            if not _looks_like_image(content, content_type):
                raise NonImageContentError(f"NotImageContent:{content_type or 'unknown'}")
            return content, content_type, None, saw_ssl_error
        except requests.exceptions.SSLError as e:
            last_error = e
            saw_ssl_error = True
            logging.info(f"[图片{label}重试 {attempt+1}/{attempts}] {url} - SSLError")
        except requests.exceptions.RequestException as e:
            last_error = e
            if not _is_retryable_network_error(e):
                logging.warning(f"[图片{label}失败-不重试] {url} - {e}")
                break
            logging.info(f"[图片{label}重试 {attempt+1}/{attempts}] {url} - {e.__class__.__name__}")
        if attempt < attempts - 1:
            time.sleep(2 ** attempt)
    return None, None, last_error, saw_ssl_error


def download_image(url: str, timeout: int = 30, proxy_url: Optional[str] = None) -> Tuple[Optional[bytes], str, str]:
    headers = _image_download_headers()
    verify_bundle = certifi.where()

    content, content_type, last_error, saw_ssl_error = _request_image(
        url, headers, timeout, proxies=None, verify=verify_bundle, attempts=MAX_RETRIES, label='本地下载'
    )
    if content is not None:
        return content, _filename_for_image(content, content_type or '', url), content_type or ''
    if last_error and not _is_retryable_network_error(last_error):
        failure_value = _image_download_failure_value(last_error)
        logging.error(f"图片下载失败: {url} - {last_error}, output_value={failure_value}")
        return None, failure_value, ''

    proxy_url = proxy_url or build_requests_proxy_url()
    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
    if proxies:
        content, content_type, proxy_error, proxy_ssl_error = _request_image(
            url, headers, timeout, proxies=proxies, verify=verify_bundle, attempts=MAX_RETRIES, label='代理下载'
        )
        if content is not None:
            return content, _filename_for_image(content, content_type or '', url), content_type or ''
        last_error = proxy_error or last_error
        saw_ssl_error = saw_ssl_error or proxy_ssl_error
    else:
        logging.info(f"图片下载未配置代理，跳过代理重试: {url}")

    if saw_ssl_error:
        host = urlsplit(url).netloc
        logging.warning(f"[图片SSL兜底] 证书校验失败，尝试关闭校验下载: host={host}, url={url}")
        content, content_type, fallback_error, _ = _request_image(
            url, headers, timeout, proxies=None, verify=False, attempts=1, label='SSL兜底本地下载'
        )
        if content is not None:
            return content, _filename_for_image(content, content_type or '', url), content_type or ''
        last_error = fallback_error or last_error

        if proxies:
            content, content_type, fallback_proxy_error, _ = _request_image(
                url, headers, timeout, proxies=proxies, verify=False, attempts=1, label='SSL兜底代理下载'
            )
            if content is not None:
                return content, _filename_for_image(content, content_type or '', url), content_type or ''
            last_error = fallback_proxy_error or last_error

    error_name = last_error.__class__.__name__ if last_error else "UnknownError"
    failure_value = _image_download_failure_value(last_error)
    logging.error(f"图片下载失败: {url} - {error_name}, output_value={failure_value}")
    return None, failure_value, ''

def _extract_uploaded_url(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    direct_url = payload.get('url')
    if isinstance(direct_url, str) and direct_url.strip():
        return direct_url.strip()

    data = payload.get('data')
    if isinstance(data, dict):
        data_url = data.get('url')
        if isinstance(data_url, str) and data_url.strip():
            return data_url.strip()
    return None


def _extract_upload_failure_message(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    message = payload.get('message')
    if isinstance(message, str) and message.strip():
        return message.strip()

    data = payload.get('data')
    if isinstance(data, dict):
        data_message = data.get('message')
        if isinstance(data_message, str) and data_message.strip():
            return data_message.strip()
    return None


def _response_preview(response: requests.Response, limit: int = 500) -> str:
    try:
        return response.text[:limit].replace('\n', ' ').replace('\r', ' ')
    except Exception:
        return '<response text unavailable>'


def upload_to_image_host(image_content: bytes, filename: str, source_url: str = '', content_type: str = '', product_url: str = '') -> Tuple[Optional[str], Optional[str]]:
    last_failure_reason = None
    try:
        image_file = BytesIO(image_content)
        try:
            response = requests.post(
                IMAGE_HOST_UPLOAD_URL,
                files={'image': (filename, image_file)},
                data={'token': IMAGE_TOKEN},
                timeout=10
            )
        finally:
            image_file.close()

        if response.ok:
            try:
                payload = response.json()
            except ValueError:
                logging.error(f"上传返回非JSON: {response.status_code}, product_url={product_url}, filename={filename}, content_type={content_type or 'unknown'}, source_url={source_url}, body={_response_preview(response)}")
                payload = None

            original_url = _extract_uploaded_url(payload)
            if original_url:
                parts = list(urlsplit(original_url))
                parts[1] = "gbcm-imagehost.vshare.dev" # 替换域名
                return urlunsplit(parts), None
            last_failure_reason = _extract_upload_failure_message(payload) or last_failure_reason

            logging.error(f"上传响应缺少url字段: {response.status_code}, failure_reason={last_failure_reason or IMAGE_UPLOAD_FAILED}, product_url={product_url}, filename={filename}, content_type={content_type or 'unknown'}, source_url={source_url}, body={_response_preview(response)}")
        else:
            logging.error(f"上传失败 HTTP {response.status_code}, product_url={product_url}, filename={filename}, content_type={content_type or 'unknown'}, source_url={source_url}, body={_response_preview(response)}")
    except Exception as e:
        logging.error(f"上传异常: product_url={product_url}, filename={filename}, content_type={content_type or 'unknown'}, source_url={source_url}, error={str(e)}")
    return None, last_failure_reason or IMAGE_UPLOAD_FAILED

def scrape_sellers_from_page(driver: uc.Chrome, product_url: str) -> List[Dict]:
    """
    在跟卖页面上抓取所有卖家信息
    """
    all_sellers_for_this_product = []
    
    # 使用稳健的导航逻辑
    if not navigate_with_retries(driver, product_url):
        logging.error(f"无法加载卖家页面: {product_url}")
        return []
        
    try:
        # 1. 等待真实的卖家卡片加载（排除 .seller-card--loading 骨架屏）
        WebDriverWait(driver, ELEMENT_WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "article.seller-card:not(.seller-card--loading)"))
        )
        time.sleep(1) # 给前端框架 1 秒钟的缓冲时间完成最终 DOM 渲染
        
        # 2. 获取真实卡片总数
        initial_cards = driver.find_elements(By.CSS_SELECTOR, "article.seller-card:not(.seller-card--loading)")
        cards_count = len(initial_cards)
        
        if cards_count == 0:
            logging.warning(f"🟡 在 {driver.current_url} 的卖家页面上未找到任何真实卖家卡片。")
            return []

        logging.info(f"在页面上找到了 {cards_count} 个卖家卡片。")

        # 3. 索引循环遍历
        for i in range(cards_count):
            seller_info = {
                "初始链接": product_url, "店铺名称": "N/A", "链接": "N/A",
                "店铺运费": "N/A", "送货时间": "N/A"
            }
            try:
                # 重新获取DOM，防止 StaleElementReferenceException
                fresh_cards = driver.find_elements(By.CSS_SELECTOR, "article.seller-card:not(.seller-card--loading)")
                if i >= len(fresh_cards):
                    break
                card = fresh_cards[i]

                # 强制滚动到视野中心触发懒加载
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
                time.sleep(0.5)

                # --- 提取名称和链接 ---
                name_elements_t2 = card.find_elements(By.CSS_SELECTOR, ".seller-card__name")
                link_elements_t1 = card.find_elements(By.CSS_SELECTOR, ".seller-card__seller a")
                
                if name_elements_t2:
                    # Worten 自营
                    seller_info['店铺名称'] = name_elements_t2[0].get_attribute('textContent').strip()
                    seller_info['链接'] = BASE_URL
                    seller_info['店铺运费'] = '0'
                elif link_elements_t1:
                    # Marketplace 第三方
                    seller_info['店铺名称'] = link_elements_t1[0].get_attribute('textContent').strip()
                    href = link_elements_t1[0].get_attribute('href')
                    seller_info['链接'] = urljoin(BASE_URL, href) if href else "N/A"

                    # 提取运费
                    shipping_elements = card.find_elements(By.CSS_SELECTOR, ".seller-card__shipping--price")
                    if shipping_elements:
                        # 兼容原有的替换逻辑
                        seller_info['店铺运费'] = shipping_elements[0].get_attribute('textContent').strip().replace(',', '.')
                else:
                    logging.warning(f"第 {i+1} 个卡片未能提取到名称，可能结构有变异。")
                    seller_info['店铺名称'] = "提取失败"
                
                # --- 提取送货时间 ---
                delivery_elements = card.find_elements(By.CSS_SELECTOR, "span.neu-11")
                if delivery_elements:
                    seller_info['送货时间'] = delivery_elements[-1].get_attribute('textContent').strip()

                all_sellers_for_this_product.append(seller_info)
                logging.debug(f"   > 成功提取信息: {seller_info['店铺名称']}")

            except Exception as e:
                logging.warning(f"   > 处理第 {i+1} 个卡片时发生错误: {e}")
                continue
                
    except TimeoutException:
        logging.warning(f"🟡 在 {driver.current_url} 加载真实卖家卡片超时。")
    except Exception as e:
        logging.error(f"在 scrape_sellers_from_page 中发生错误: {e}")
        
    return all_sellers_for_this_product

def scrape_product_details(driver: uc.Chrome, product_url: str, proxy_url: Optional[str] = None) -> Optional[Dict]:
    """
    访问单个商品页面，验证页面有效性，然后抓取其详细信息。
    """
    logging.debug(f"   -> 正在抓取商品详情: {product_url}")
    details = {}
    
    title_selector = "h1[class='product-header__title'] span"

    if not navigate_with_retries(driver, product_url, max_attempts=5):
        logging.error(f"[FAILED] 页面导航失败: {product_url} ")
        return {"_status": "page_load_failed"}

    # 额外检测：404 页面
    try:
        time.sleep(random.uniform(2,4))
        err404 = driver.find_elements(By.CSS_SELECTOR, ".error404__title")
        if err404 and err404[0].is_displayed():
            logging.info(f"页面显示 404 标题，判定为失效链接: {product_url}")
            return {"_status": "invalid"}
    except Exception:
        pass
    
    close_cookie_pup(driver)

    # 等待页面核心元素出现
    for attempt in range(2):
        try:
            WebDriverWait(driver, PAGE_NAVIGATION_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, title_selector))
            )
            break  
        except TimeoutException:
            driver.execute_script("window.stop();")
            if attempt == 1:
                logging.error(f"[FAILED] 页面导航失败: {product_url} (等待核心元素超时)。")
                return {"_status": "page_load_failed"}
        

    # Handle adult content pop-up (成人弹窗处理)
    try:
        close_button_selector = ".checkYes.button.button--primary.button--black.button--md"
        close_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, close_button_selector))
        )
        driver.execute_script("arguments[0].click();", close_btn)
    except TimeoutException:
        pass

    # --- 解析数据逻辑 ---
    try:
        # 1. Title
        title_element = driver.find_element(By.CSS_SELECTOR, title_selector)
        details["标题"] = title_element.text.strip() if title_element else "N/A"

        # 2. Rating
        try:
            rating_el = driver.find_element(By.CSS_SELECTOR, "div.rating--s.rating.product-header__rating > span.rating__star-value.semibold > span")
            details["产品评分"] = rating_el.text.strip()
        except NoSuchElementException:
            details["产品评分"] = "N/A"

        # 3. Category
        category_selector = "ol.breadcrumbs__wrapper span.breadcrumbs__item__name"
        category_elements = driver.find_elements(By.CSS_SELECTOR, category_selector)
        if category_elements:
            cat_texts = [el.text.strip() for el in category_elements if el.text.strip()]
            details["类目"] = "/".join(cat_texts)

        # --- 4. Images ---
        image_urls = []
        img_elements = driver.find_elements(By.CSS_SELECTOR, "img.product-gallery__slider-image")
        for img in img_elements:
            raw_src = img.get_attribute('src')
            normalized_url = normalize_image_url(raw_src)
            if normalized_url:
                image_urls.append({'raw_src': raw_src, 'normalized_url': normalized_url})
                if raw_src and raw_src != normalized_url:
                    logging.info(f"[图片URL规范化] product_url={product_url}, raw_src={raw_src}, normalized_url={normalized_url}")

        for image_index, image_info in enumerate(image_urls[:5], start=1):
            image_key = f"图{image_index}"
            raw_src = image_info['raw_src']
            url = image_info['normalized_url']
            image_content, filename_or_failure, content_type = download_image(url, proxy_url=proxy_url)
            if image_content is None:
                details[image_key] = filename_or_failure
                logging.warning(f"图片下载失败，已标记为 {filename_or_failure}: product_url={product_url}, raw_src={raw_src}, normalized_url={url}")
                continue

            filename = filename_or_failure
            converted_image = convert_avif_to_jpg(image_content, content_type, url, product_url=product_url)
            if converted_image is None:
                details[image_key] = IMAGE_TRANSFER_FAILED
                logging.warning(f"图片转换失败，已标记为 {IMAGE_TRANSFER_FAILED}: product_url={product_url}, raw_src={raw_src}, normalized_url={url}, filename={filename}, content_type={content_type or 'unknown'}")
                continue

            image_content, filename, content_type = converted_image
            uploaded_url, upload_failure_reason = upload_to_image_host(image_content, filename, source_url=url, content_type=content_type, product_url=product_url)
            if uploaded_url:
                details[image_key] = uploaded_url
            else:
                details[image_key] = upload_failure_reason or IMAGE_UPLOAD_FAILED
                logging.warning(f"图片上传失败，已标记为 {details[image_key]}: product_url={product_url}, raw_src={raw_src}, normalized_url={url}, filename={filename}, content_type={content_type or 'unknown'}")

        # 5. Price, Seller, Shipping
        try: details["价格"] = driver.find_element(By.CSS_SELECTOR, "span[class='price--lg price--mixed price--B price'] span[class='price__numbers--bold price__numbers notranslate raised-decimal price__numbers--bold price__numbers']").text.strip()
        except: details["价格"] = "N/A"
        
        try: details["销售和发货方"] = driver.find_element(By.CSS_SELECTOR, "a[class='product-price-info__link font-m bold button--md button--link button--black button product-price-info__link font-m bold w-app-link product-price-info__link font-m bold button--md button--link button--black button product-price-info__link font-m bold'] span").text.strip()
        except: details["销售和发货方"] = "N/A"
        
        # 运费获取
        shipping_found = False
        for _ in range(10): 
            shipping_text = None
            try:
                shipping_elem = driver.find_element(By.CSS_SELECTOR, ".add-07")
                if shipping_elem.is_displayed(): shipping_text = shipping_elem.text.strip()
            except: pass

            if not shipping_text:
                try:
                    shipping_elem = driver.find_element(By.CSS_SELECTOR, ".bold.notranslate.bold")
                    if shipping_elem.is_displayed(): shipping_text = shipping_elem.text.strip()
                except: pass

            if shipping_text:
                details["运费"] = shipping_text.replace(',', '.')
                shipping_found = True
                break
            time.sleep(1) # 缩短内部等待

        if not shipping_found:
            details["运费"] = "N/A"
        
        # 6. EAN/SKU/Desc/Brand  
        details["EAN"], details["SKU"], details["品牌"] = "N/A", "N/A", "N/A"
        time.sleep(random.uniform(2,4)) # Wait before interacting with modal
        try:
            # 打开模态框
            for attempt in range(3):
                try:
                    tech_bth = WebDriverWait(driver, 60).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, 'div[class="technical-specifications"] button[class="action-list--vertical-spacious action-list"]'))
                    )
                    driver.execute_script("arguments[0].click();", tech_bth)
                    break
                except TimeoutException:
                    # 尝试备用路径
                    try:
                        sub_tech_bth = WebDriverWait(driver, 60).until(
                            EC.element_to_be_clickable((By.XPATH, '//span[normalize-space()="Características técnicas"]'))
                        )
                        driver.execute_script("arguments[0].click();", sub_tech_bth)
                        break
                    except TimeoutException:
                        if attempt == 2:
                            break
                    time.sleep(2) # Wait before retrying

            # Wait for modal to appear
            WebDriverWait(driver, ELEMENT_WAIT_TIMEOUT).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, ".table-specifications"))
            )
            rows = driver.find_elements(By.CSS_SELECTOR, ".table-specifications__row")
            for row in rows:
                try:
                    key = row.find_element(By.CSS_SELECTOR, "p.table__subtitle").text.strip()
                    value = row.find_element(By.CSS_SELECTOR, ".table-specifications__right-container span").text.strip()
                    if key == "EAN": details["EAN"] = value
                    elif key == "Referência": details["SKU"] = value
                    elif key == "Marca": details["品牌"] = value
                except:
                    continue
            
            # Close modal
            for attempt in range(3):
                try:
                    modal_close_selector = "div[aria-hidden='false'] button[class='button--md button--tertiary button--black button--icon-right button'] span"
                    modal_close_bth = WebDriverWait(driver, 30).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, modal_close_selector))
                    )
                    time.sleep(random.uniform(2,4)) # Allow modal to fully load
                    driver.execute_script("arguments[0].click();", modal_close_bth)
                    break
                except TimeoutException:
                    if attempt == 2:
                        raise
                    time.sleep(2) # Wait before retrying
    
        except TimeoutException:
            logging.warning("未找到技术规格模态框。")
        except Exception as e:
            logging.error(f"抓取 EAN/SKU/品牌时出错: {e}")

        # --- 7. Description ---
        details["描述"] = "N/A"
        time.sleep(random.uniform(2,4)) # Wait before interacting with modal
        try:
            try:
                # Open description modal
                description_selector = 'div[class="about-product"] button[class="action-list--vertical-spacious action-list"]'
                desc_bth = WebDriverWait(driver, 50).until(   
                    EC.element_to_be_clickable((By.CSS_SELECTOR, description_selector))
                )
                driver.execute_script("arguments[0].click();", desc_bth)
            except TimeoutException:
                # 尝试备用路径
                description_selector = '//span[normalize-space()="Sobre o produto"]'
                sub_desc_bth = WebDriverWait(driver, 30).until(   
                    EC.element_to_be_clickable((By.XPATH, description_selector))
                )
                driver.execute_script("arguments[0].click();", sub_desc_bth)

            # Wait for modal to appear
            WebDriverWait(driver, 30).until(
                EC.visibility_of_element_located((By.XPATH, "//div[@aria-hidden='false']//h2[@id='modalTitle']"))
            )
            try:
                desc_part1 = driver.find_element(By.CSS_SELECTOR, ".font-m.bold.h-mb-1").text.strip()
            except NoSuchElementException:
                desc_part1 = ""

            try:
                desc_part2 = driver.find_element(By.CSS_SELECTOR, "div.rich-text-wrapper div.ql-editor").text.strip()
            except NoSuchElementException:
                desc_part2 = ""

            details["描述"] = "\n".join(filter(None, [desc_part1, desc_part2]))
            #  Close modal
            close_desc_selector = "//div[@class='about-product']//header[@class='neu-01-bg modal__header']//span[1]"
            close_desc_bth = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, close_desc_selector))
            )
            time.sleep(random.uniform(2,4)) # Allow modal to close
            driver.execute_script("arguments[0].click();", close_desc_bth)
            
        except TimeoutException:
            logging.warning("未找到描述模态框。")     
        except Exception as e:
            logging.error(f"关闭描述模态框时出错: {e}")
        
        return details

    except Exception as e:
        logging.error(f"解析页面元素时出错 {product_url}: {e}")
        return details

def scrape_other_sellers_on_product_page(driver: uc.Chrome) -> List[Dict]:
    """
    在商品页面抓取其他卖家信息 (辅助获取最低价及前三个铺货对比数据)
    """
    logging.debug("   -> 正在查找 '其他卖家' 链接...")
    other_sellers_list = []
    other_sellers_link_selector = "span[class='h-underline']"
    
    try:
        # 点击“其他卖家”
        other_sellers_link_bth = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, other_sellers_link_selector))
        )
        driver.execute_script("arguments[0].click();", other_sellers_link_bth)
        logging.debug("   -> 已点击 '其他卖家' 链接，等待页面加载...")

        # 等待排除骨架屏的真实卡片
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "article.seller-card:not(.seller-card--loading)"))
        )
        time.sleep(1)
        logging.debug("   -> 已打开 '其他卖家' 页面，开始抓取...")
        
        initial_cards = driver.find_elements(By.CSS_SELECTOR, "article.seller-card:not(.seller-card--loading)")
        cards_count = len(initial_cards)
        
        if cards_count <= 1:
            logging.debug("   -> '其他卖家' 页面只有一个或没有卖家，无需抓取。")
            return []

        for i in range(1, cards_count):
            seller_details = {'name': "N/A", 'price': "N/A", 'shipping': "N/A"}
            try:
                fresh_cards = driver.find_elements(By.CSS_SELECTOR, "article.seller-card:not(.seller-card--loading)")
                if i >= len(fresh_cards):
                    break
                card = fresh_cards[i]

                # 强制滚动触发加载
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
                time.sleep(0.5)

                # --- 提取价格 ---
                price_elem = card.find_elements(By.CSS_SELECTOR, "div.seller-card__buybox-container > span > span > span")
                if price_elem:
                    price_val = parse_price(price_elem[0].get_attribute('textContent').strip())
                    seller_details['price'] = f"€{price_val:.2f}" if price_val is not None else "N/A"
                
                # --- 提取名称和运费 ---
                name_elements_t2 = card.find_elements(By.CSS_SELECTOR, "span.seller-card__name")
                link_elements_t1 = card.find_elements(By.CSS_SELECTOR, "div.seller-card__seller > a > span")
                
                if name_elements_t2: # Worten 自营
                    seller_details['name'] = name_elements_t2[0].get_attribute('textContent').strip()
                    shipping_elems = card.find_elements(By.CSS_SELECTOR, "span.seller-card__shipping--price")
                    if shipping_elems:
                        seller_shipping_text = shipping_elems[0].get_attribute('textContent').strip().replace(',', '.')
                        seller_details['shipping'] = ''.join(seller_shipping_text.split())
                    else:
                        seller_details['shipping'] = "0"
                        
                elif link_elements_t1: # Marketplace 第三方
                    seller_details['name'] = link_elements_t1[0].get_attribute('textContent').strip()
                    shipping_elems = card.find_elements(By.CSS_SELECTOR, "span.seller-card__shipping--price")
                    if shipping_elems:
                        seller_shipping_text = shipping_elems[0].get_attribute('textContent').strip().replace(',', '.')
                        seller_details['shipping'] = ''.join(seller_shipping_text.split())

                other_sellers_list.append(seller_details)

            except Exception as e:
                logging.warning(f"   -> 处理第 {i+1} 个'其他卖家'卡片时出错跳过: {e}")
                continue

    except TimeoutException:
        logging.debug("   -> 未找到 '其他卖家' 链接或等待卡片超时，跳过。")
        return []
    except Exception as e:
        logging.error(f"   -> 抓取 '其他卖家' 信息时出错: {e}")
        
    return other_sellers_list

def create_chrome_driver(session_data: Dict, chrome_init_lock=None) -> Optional[uc.Chrome]:
    """
    统一创建并初始化 Chrome Driver。
    chrome_init_lock: Manager.Lock，用于跨进程序列化 Chrome 创建，防止同时启动过多实例导致超时。
    """
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
    """按需请求一个新会话。session_lock 用于跨进程序列化 cf_bypass API 调用，防止并发请求过多。"""
    if session_lock:
        with session_lock:
            return browser_runtime.create_session_data(c, CF_BYPASS_PORT)
    return browser_runtime.create_session_data(c, CF_BYPASS_PORT)


def discovery_process_with_progress(initial_urls: List[Dict], discovery_completed_event, log_queue, total_estimated, total_increment_queue,
                                   session_lock=None, chrome_init_lock=None,
                                   state_db_path=None, run_id=None):
    """
    支持进度跟踪的发现进程。任务只写入 StateStore。
    session_lock/chrome_init_lock: Manager.Lock，跨进程共享的锁，用于序列化会话和Driver创建。
    """
    setup_log_queue_handler(log_queue)
    logging.info("--- [发现进程] 启动 ---")
    state = StateStore(state_db_path) if state_db_path and run_id else None
    driver = None
    current_session_count = 0
    MAX_URLS_PER_DISCOVERY = 15

    def add_discovered_task(task, max_attempts=URL_RETRY_LIMIT + 1):
        if not state:
            raise RuntimeError("StateStore 未初始化，无法写入发现任务")
        inserted = state.add_task(run_id, task, 'product_info', max_attempts=max_attempts)
        if inserted and total_increment_queue:
            total_increment_queue.put(1)
        return inserted

    try:
        if not state:
            raise RuntimeError("StateStore 未初始化，发现进程无法运行")
        state.mark_discovery_started(run_id)
        total_estimated.value = 0 # 初始归零，配合增量队列使用
        expansion_tasks = []

        # 1. 快速分类
        for item in initial_urls:
            url = item['url']
            if ('marketplace-see-more-offers' in url and 'product_id' in url) or ('produtos/' in url):
                task = {'url': url, 'type': 'product_page' if 'produtos/' in url else 'seller_page'}
                add_discovered_task(task)
            elif 'seller_id' in url:
                expansion_tasks.append({'url': url, 'type': 'shop_page', 'pages': item.get('pages_to_scrape')})
            else:
                expansion_tasks.append({'url': url, 'type': 'category_page', 'pages': item.get('pages_to_scrape')})

        logging.info(f"[发现进程] 待展开任务数: {len(expansion_tasks)}")
        if not expansion_tasks:
            logging.info("--- [发现进程] 无需展开任务 ---")
            state.mark_discovery_finished(run_id)
            return

        # 2. 处理展开任务
        for i, task in enumerate(expansion_tasks):
            def ensure_driver_ready():
                nonlocal driver, current_session_count
                if driver is None or current_session_count >= MAX_URLS_PER_DISCOVERY:
                    if driver:
                        try: driver.quit()
                        except: pass
                    driver = None
                    for attempt in range(3):
                        logging.info(f"[发现进程] 获取新会话 (尝试 {attempt+1})...")
                        session_data = get_fresh_session(session_lock)
                        if not session_data:
                            time.sleep(5); continue
                        driver = create_chrome_driver(session_data, chrome_init_lock)
                        if driver: break
                    if not driver: return False
                    current_session_count = 0
                return True

            try:
                if not ensure_driver_ready(): continue

                url = task['url']
                # --- 更稳健的页码解析 ---
                pages_str = str(task['pages']) if task['pages'] else ""
                if pages_str and pages_str.lower() != 'nan':
                    try:
                        pages = [int(p.strip()) for p in pages_str.replace('，', ',').split(',') if p.strip().isdigit()]
                    except:
                        pages = range(1, SELLER_SCRAPED_PAGE_COUNT + 1)
                else:
                    pages = range(1, SELLER_SCRAPED_PAGE_COUNT + 1)

                # 修正 URL 构建逻辑
                target_url = url
                if task['type'] == 'shop_page':
                    seller_id = url.split('seller_id=')[-1]
                    target_url = f"https://www.worten.pt/search?query=*&facetFilters=seller_id:{seller_id}"

                logging.info(f"[发现进程] 正在展开 ({i+1}/{len(expansion_tasks)}): {target_url} (页数: {list(pages)})")

                for page_num in pages:
                    sep = '&' if '?' in target_url else '?'
                    p_url = f"{target_url}{sep}page={page_num}"

                    # 导航
                    if not ensure_driver_ready(): break
                    nav_ok = navigate_with_retries(driver, p_url, max_attempts=2)

                    # 即使导航失败也不要直接 break 整个店铺，尝试下一页
                    if not nav_ok:
                        logging.warning(f"[发现进程] 页 {page_num} 导航失败，跳过该页。")
                        fail_task = {'url': p_url, 'type': 'shop_page', 'source_url': url, 'page': page_num}
                        if add_discovered_task(fail_task, max_attempts=1):
                            row = product_failure_row(p_url, '列表页导航失败')
                            state.fail_task(run_id, fail_task['task_key'], 'shop', [row], '列表页导航失败')
                        continue

                    current_session_count += 1

                    # 处理弹窗
                    try:
                        btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".checkYes.button")))
                        driver.execute_script("arguments[0].click();", btn)
                    except: pass

                    # 提取链接
                    found_links = False # 每一页重置
                    try:
                        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".listing-content__list li a")))
                        links = driver.find_elements(By.CSS_SELECTOR, ".listing-content__list li a")

                        count = 0
                        for l in links:
                            href = l.get_attribute('href')
                            if href:
                                child_task = {'url': urljoin(BASE_URL, href), 'type': 'product_page', 'source_url': p_url}
                                if state.add_task(run_id, child_task, 'product_info', max_attempts=URL_RETRY_LIMIT + 1):
                                    count += 1

                        if count > 0:
                            found_links = True
                            if total_increment_queue:
                                total_increment_queue.put(count)
                                logging.info(f"[发现进程] 页 {page_num} 发现 {count} 个任务，已更新总数。")

                    except TimeoutException:
                        logging.warning(f"[发现进程] 页 {page_num} 没找到商品列表，判定为该任务末页。")

                    # 如果这一页确实没货（排除超时情况），则停止该店铺的后续翻页
                    if not found_links:
                        break

            except Exception as e:
                logging.error(f"[发现进程] 任务 {task['url']} 发生错误: {e}")
                continue

        state.mark_discovery_finished(run_id)
        logging.info("--- [发现进程] 全部完成 ---")
    except Exception as e:
        logging.error(f"--- [发现进程] 失败: {e} ---")
        if state:
            state.mark_discovery_failed(run_id, str(e))
        raise
    finally:
        if driver:
            try: driver.quit()
            except: pass
        discovery_completed_event.set()

# --- 抓取 Worker  ---

def build_failed_product_record(url: str, reason: str) -> Dict[str, str]:
    return product_failure_row(url, reason)


def build_failed_seller_record(url: str, reason: str) -> Dict[str, str]:
    return seller_failure_row(url, reason)


class ScraperWorker:
    def __init__(self, discovery_completed_event, log_queue=None, increment_queue=None,
                 state_db_path=None, run_id=None, stop_flag=None,
                 session_lock=None, chrome_init_lock=None):
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
        # 基础上限
        self.current_max_urls = random.randint(DEFAULT_MAX_URLS_PER_DRIVER_MIN, DEFAULT_MAX_URLS_PER_DRIVER_MAX)

    def setup_driver(self):
        logging.debug(f"[Worker {self.worker_id}] 准备启动 Driver...")

        for i in range(MAX_RETRIES):
            session = get_fresh_session(self.session_lock)
            if not session:
                logging.warning(f"[Worker {self.worker_id}] 获取会话超时，正在重试 ({i+1}/{MAX_RETRIES})...")
                time.sleep(5 * (i + 1))  # 指数退避：5s, 10s, 15s
                continue
            
            self.driver = create_chrome_driver(session, self.chrome_init_lock)
            
            if self.driver:
                self.proxy_for_requests = session.get('proxy_for_selenium_wire') or build_requests_proxy_url()
                self.processed_count = 0
                time.sleep(random.uniform(2,4))
                return True # 成功启动
            else:
                logging.warning(f"[Worker {self.worker_id}] 当前会话/代理不可用，将丢弃并获取新会话重试...")
        
        logging.error(f"[Worker {self.worker_id}] 连续 {MAX_RETRIES} 次启动 Driver 失败。Worker 将稍后继续尝试。")
        return False

    def teardown_driver(self):
        if self.driver:
            force_kill_driver(self.driver)
            self.driver = None
        self.proxy_for_requests = None

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
                        logging.info(f"[Worker {self.worker_id}] 发现已结束且无可领取任务，退出。")
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

                success = self.process_task(task)
                task_key = None

                # 释放 Chrome 内存：清除 cookies 和请求缓存
                if self.driver:
                    try:
                        self.driver.delete_all_cookies()
                    except Exception:
                        pass
                    browser_runtime.clear_driver_requests(self.driver)

                if success:
                    self.consecutive_failures = 0
                else:
                    self.consecutive_failures += 1

                self.processed_count += 1

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
        ttype = task['type']
        task_key = task.get('task_key')

        try:
            if ttype == 'seller_page':
                # from __main__ import scrape_sellers_from_page
                data = scrape_sellers_from_page(self.driver, url)
                if data:
                    return self.record_task_result(task_key, 'seller', data)

                row = build_failed_seller_record(url, '抓取失败')
                self.record_task_failure(task_key, 'seller', [row], '抓取失败')
                return False

            elif ttype == 'product_page':
                # from __main__ import scrape_product_details, scrape_other_sellers_on_product_page, parse_price

                details = scrape_product_details(self.driver, url, proxy_url=self.proxy_for_requests)
                if not details or details.get('_status') == 'page_load_failed':
                    row = build_failed_product_record(url, '抓取失败')
                    self.record_task_failure(task_key, 'product', [row], '抓取失败')
                    return False

                if details.get('_status') == 'invalid':
                    row = build_failed_product_record(url, '失效链接')
                    return self.record_task_result(task_key, 'product', [row], status='invalid', error='失效链接') # 视为处理完成（无效链接）

                # 抓取其他卖家
                others = scrape_other_sellers_on_product_page(self.driver)

                # 计算最低价
                prices = []
                p1 = parse_price(details.get("价格"))
                if p1: prices.append(p1)
                for o in others:
                    p2 = parse_price(o.get('price'))
                    if p2: prices.append(p2)
                if prices:
                    details["当前售价（最低）"] = f"€{min(prices):.2f}"

                # 合并数据
                final = {'商品链接': url, **details}
                for i, s in enumerate(others[:3]):
                    final[f'店铺{i+1}'] = s.get('name')
                    final[f'售价{i+1}'] = s.get('price')
                    final[f'运费{i+1}'] = s.get('shipping')

                saved = self.record_task_result(task_key, 'product', [final])
                if saved:
                    logging.info(f" 成功抓取商品: {url}")
                return saved

        except Exception as e:
            logging.error(f"[Worker {self.worker_id}] 任务失败 {url}: {e}")
            if ttype == 'seller_page':
                row = build_failed_seller_record(url, '抓取失败')
                result_group = 'seller'
            elif ttype == 'product_page':
                row = build_failed_product_record(url, '抓取失败')
                result_group = 'product'
            else:
                row = build_failed_product_record(url, '抓取失败')
                result_group = 'product'
            self.record_task_failure(task_key, result_group, [row], str(e))
            return False
        return False


class ScraperWorkerWithProgress(ScraperWorker):
    """支持进度跟踪的Worker类"""
    def __init__(self, discovery_completed_event, log_queue=None, increment_queue=None,
                 state_db_path=None, run_id=None, stop_flag=None,
                 session_lock=None, chrome_init_lock=None):
        super().__init__(discovery_completed_event, log_queue, increment_queue,
                        state_db_path, run_id, stop_flag,
                        session_lock, chrome_init_lock)

    def process_task(self, task):
        # 无论任务成功与否，都应该计入处理总数
        url = task.get('url', 'Unknown URL')
        task_type = task.get('type', 'Unknown Type')
        
        logging.info(f"[Worker {self.worker_id}] 开始处理任务: {task_type} - {url}")
        
        try:
            result = super().process_task(task)
            logging.info(f"[Worker {self.worker_id}] 任务处理结果: {result} - {url}")
            return result
        except Exception as e:
            logging.error(f"[Worker {self.worker_id}] 任务处理异常: {e} - {url}")
            raise
        finally:
            # 发送增量信号到进度管理进程
            if self.increment_queue:
                try:
                    self.increment_queue.put(1)  # 发送增量1
                    logging.debug(f"[Worker {self.worker_id}] 发送增量信号 (任务: {url})")
                except Exception as e:
                    logging.error(f"[Worker {self.worker_id}] 发送增量信号失败: {e}")


def progress_manager(processed_count, total_estimated, increment_queue, total_increment_queue, stop_flag):
    """专门的进度管理进程"""
    setup_log_queue_handler(None)  # 进度管理进程不需要日志队列
    
    logging.info("[进度管理进程] 启动")
    
    while not stop_flag.value:
        try:
            # 接收已处理任务增量信号
            if not increment_queue.empty():
                increment_data = increment_queue.get_nowait()
                processed_count.value += 1
                logging.debug(f"[进度管理进程] 收到处理增量，当前已处理: {processed_count.value}")
            
            # 接收总任务数增量信号
            if not total_increment_queue.empty():
                total_increment_data = total_increment_queue.get_nowait()
                total_estimated.value += total_increment_data
                logging.debug(f"[进度管理进程] 收到总任务增量，当前总数: {total_estimated.value}")
            
            time.sleep(0.1)  # 短暂休眠，避免CPU占用过高
        except Exception as e:
            logging.error(f"[进度管理进程] 错误: {e}")
            time.sleep(1)
    
    logging.info("[进度管理进程] 结束")

# --- 主程序入口 ---

def main(progress_callback=None, stop_check_callback=None, input_file=None, output_file=None, state_db_path=None):
    """
    主函数，支持进度回调和停止检查
    
    Args:
        progress_callback: 进度回调函数，接收字典参数 {'processed': int, 'total': int, 'rate': float, 'message': str}
        stop_check_callback: 停止检查回调函数，返回bool值表示是否应该停止
    """
    multiprocessing.freeze_support()
    os.environ["WDM_DEFAULT_TIMEOUT"] = "90"
    input_file = input_file or INPUT_FILE
    output_file = output_file or OUTPUT_FILE
    state_db_path = state_db_path or default_state_db()
    state = StateStore(state_db_path)
    
    logging.info(f"--- Worten 全速抓取启动 (Workers: {MAX_WORKERS}) ---")
    
    # 1. 读取 Excel
    initial_urls = []
    try:
        initial_urls = read_urls_from_excel(input_file)
    except Exception:
        # Fallback for testing
        df = pd.read_excel(input_file)
        initial_urls = df[['url', 'pages_to_scrape']].to_dict('records')

    if not initial_urls:
        logging.error("没有输入链接，退出。")
        if progress_callback:
            progress_callback({'processed': 0, 'total': 0, 'rate': 0, 'message': '没有输入链接'})
        return {'status': 'failed', 'message': '没有输入链接'}
    run_id, resumed = state.create_or_resume_run('product_info', input_file, output_file)
    if resumed:
        logging.info(f"继续未完成任务: run_id={run_id}, output_file={output_file}")
        recovered = state.recover_running_tasks(run_id, '程序重新启动，任务重新排队')
        if recovered:
            logging.info(f"已恢复 {recovered} 个上次运行遗留的任务。")
    else:
        state.recover_stale_tasks(run_id)



















    # 2. 初始化多进程管理器
    def _log_listener(q: multiprocessing.Queue):
        """在主进程中运行的日志监听器：从队列读取 LogRecord 并交给根 logger 处理。"""
        root = logging.getLogger()
        while True:
            try:
                record = q.get()
            except Exception:
                break
            if record is None:
                break
            try:
                # record 已经是 LogRecord（由 QueueHandler 放入），直接由根 logger 处理
                root.handle(record)
            except Exception:
                import sys, traceback
                print("Error in log listener:", file=sys.stderr)
                traceback.print_exc()

    with multiprocessing.Manager() as manager:
        # 使用 manager.Queue() 在 Windows spawn 模式下可安全在进程间传递
        log_queue = manager.Queue()

        listener_thread = threading.Thread(target=_log_listener, args=(log_queue,), daemon=True)
        listener_thread.start()

        stop_flag = manager.Value('b', False)
        discovery_completed_event = manager.Event()

        # 跨进程序列化锁：防止所有 Worker 同时请求 Session 和创建 Chrome
        session_lock = manager.Lock()
        chrome_init_lock = manager.Lock()

        # 数据存储 — 结果直接存入 SQLite，不再需要 manager.list 累积内存

        # 进度跟踪 - 使用专门的进度管理进程
        processed_count = manager.Value('i', 0)  # 已处理任务数
        total_estimated = manager.Value('i', 0)  # 总任务数
        increment_queue = manager.Queue()  # 已处理任务增量信号队列
        total_increment_queue = manager.Queue()  # 总任务数增量信号队列
        start_time = manager.Value('d', time.time())

        # 启动专门的进度管理进程
        progress_manager_process = multiprocessing.Process(
            target=progress_manager,
            args=(processed_count, total_estimated, increment_queue, total_increment_queue, stop_flag)
        )
        progress_manager_process.start()
        logging.info("[主进程] 进度管理进程已启动")

        # 进度更新线程
        def progress_updater():
            """定期更新进度信息"""
            logging.info(f"[进度线程] 启动，初始状态: processed={processed_count.value}, total={total_estimated.value}")
            last_queue_log = 0
            while not stop_flag.value:
                try:
                    if progress_callback:
                        elapsed_time = time.time() - start_time.value
                        rate = processed_count.value / (elapsed_time / 60) if elapsed_time > 0 else 0

                        progress = state.progress(run_id)
                        progress_data = {
                            'processed': progress['processed'],
                            'total': progress['total'] or total_estimated.value,
                            'rate': rate,
                            'message': f"已处理 {progress['processed']} 个任务"
                        }

                        logging.info(f"[进度线程] 更新进度: {progress_data}")
                        progress_callback(progress_data)

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
                    time.sleep(2)  # 每2秒更新一次
                except Exception as e:
                    logging.error(f"进度更新出错: {e}")
                    break
            logging.info("[进度线程] 结束")

        progress_thread = threading.Thread(target=progress_updater, daemon=True)
        progress_thread.start()

        discovery_p = None
        try:
            logging.info("按需请求 Session：跳过备用 Session 生产和预热。")

            # 4. 启动发现进程 (独立的后台进程)
            if state.discovery_status(run_id) == 'finished':
                logging.info("[主进程] 发现阶段已完成，跳过发现进程。")
                discovery_completed_event.set()
            else:
                logging.info(f"[主进程] 启动发现进程，输入URL数量: {len(initial_urls)}")
                discovery_p = multiprocessing.Process(
                    target=discovery_process_with_progress,
                    args=(initial_urls, discovery_completed_event, log_queue, total_estimated, total_increment_queue, session_lock, chrome_init_lock, state_db_path, run_id)
                )
                discovery_p.start()
                logging.info(f"[主进程] 发现进程已启动，PID: {discovery_p.pid}")

            # 5. 启动抓取 Worker 池 (立即开始，不等发现结束)
            logging.info(f"启动抓取 Workers... (Worker数量: {MAX_WORKERS})")
            with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                for i in range(MAX_WORKERS):
                    worker_instance = ScraperWorkerWithProgress(
                        discovery_completed_event,
                        log_queue=log_queue, increment_queue=increment_queue,
                        state_db_path=state_db_path, run_id=run_id, stop_flag=stop_flag,
                        session_lock=session_lock, chrome_init_lock=chrome_init_lock
                    )
                    logging.info(f"[主进程] 创建 Worker {i+1}/{MAX_WORKERS}: {worker_instance.worker_id}")
                    futures.append(executor.submit(worker_instance.run))

                wait(futures)
                for future in futures:
                    future.result()
                logging.info("[主进程] 所有 Worker 已完成")

            logging.info("所有抓取任务完成。")
        finally:
            stop_flag.value = True
            try:
                log_queue.put(None)
            except Exception:
                pass
            if discovery_p is not None:
                discovery_p.join(timeout=5)
                if discovery_p.is_alive(): discovery_p.terminate()

            # 等待进度管理进程结束
            progress_manager_process.join(timeout=5)
            if progress_manager_process.is_alive(): progress_manager_process.terminate()
            try:
                listener_thread.join(timeout=5)
            except Exception:
                pass

        # 7. 保存
        if state.discovery_status(run_id) == 'failed':
            state.set_run_status(run_id, 'failed', '任务发现失败')
            raise RuntimeError('任务发现失败')

        logging.info("正在保存数据...")
        rows_by_group = state.grouped_result_rows(run_id)
        save_data_to_multiple_sheets(
            rows_by_group.get('seller', []),
            rows_by_group.get('shop', []),
            rows_by_group.get('product', []),
            output_file,
        )
        if state.has_incomplete_tasks(run_id):
            message = '仍有未完成任务，稍后可再次点击开始/继续任务'
            state.set_run_status(run_id, 'failed', message)
            raise RuntimeError(message)
        state.set_run_status(run_id, 'completed')

        # 最终进度更新
        if progress_callback:
            elapsed_time = time.time() - start_time.value
            rate = processed_count.value / (elapsed_time / 60) if elapsed_time > 0 else 0
            progress = state.progress(run_id)
            final_message = f"任务完成！总共处理 {progress['processed']} 个任务"
            logging.info(f"[主进程] 最终进度: {progress['processed']}/{progress['total']}, 耗时: {elapsed_time:.1f}秒")
            progress_callback({
                'processed': progress['processed'],
                'total': progress['total'],
                'rate': rate,
                'message': final_message
            })
        return {'status': 'completed', 'run_id': run_id, 'resumed': resumed, 'output_file': output_file}

    # 8. 强制清理僵尸进程
    if os.name == 'nt':
        try: subprocess.run("taskkill /F /T /IM chrome*", shell=True, stderr=subprocess.DEVNULL)
        except: pass

if __name__ == '__main__':
    main()