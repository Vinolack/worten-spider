import ssl
import certifi
import os
import sys
import time
import json
import uuid
import random
import psutil
import string
import logging
import threading
from logging.handlers import QueueHandler
import requests
import pandas as pd
import multiprocessing
import subprocess
import queue
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from typing import List, Dict, Optional, Any
from urllib.parse import urlsplit, urlunsplit, urljoin

# Selenium
import seleniumwire.undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.common.exceptions import WebDriverException

import configloader

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

# 配置加载
c = configloader.config()
CHROME_FOR_TESTING_PATH = resource_path("cft/chrome-win64/chrome.exe")
DRIVER_FOR_TESTING_PATH = resource_path("cft/chromedriver-win64/chromedriver.exe")
NODE_SCRIPT_PATH = resource_path("index.js")
BASE_URL = "https://www.worten.pt"
exe_folder = get_exe_dir()

INPUT_FILE = os.environ.get('WORTEN_INPUT_FILE') or os.path.join(exe_folder, "input_links.xlsx")
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_FILE = os.environ.get('WORTEN_OUTPUT_FILE') or os.path.join(exe_folder, f"worten_data_{timestamp}.xlsx")

IMAGE_PATH = resource_path("product_image")
if not os.path.exists(IMAGE_PATH):
    os.makedirs(IMAGE_PATH)

IMAGE_HOST_UPLOAD_URL = c.get_key('IMAGE_HOST_UPLOAD_URL')
IMAGE_TOKEN = c.get_key('IMAGE_TOKEN')
SELLER_SCRAPED_PAGE_COUNT = int(c.get_key('SELLER_SCRAPED_PAGE_COUNT'))
MAX_RETRIES = 3
URL_RETRY_LIMIT = 5

MAX_WORKERS = int(c.get_key('MAX_WORKER') or 4)
# 默认每个 Driver 处理多少个 URL 后重启
DEFAULT_MAX_URLS_PER_DRIVER_MIN = 15
DEFAULT_MAX_URLS_PER_DRIVER_MAX = 20

SESSION_LIFESPAN_SECONDS = 10 * 60
MIN_SESSION_USABLE_TIME_SECONDS = 4 * 60

# 锁与日志
CHROME_INIT_LOCK = multiprocessing.Lock()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Process %(process)d] - %(message)s')
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
            root.setLevel(logging.INFO)
        except Exception:
            pass

def get_cf_cookie_from_api(port: int, proxy_str: Optional[str] = None) -> Optional[Dict]:
    """请求 Cloudflare Bypass API 获取 cookie"""
    api_host = c.get_key('cf_host') 
    api_url = f"http://{api_host}:{port}/cf-clearance-scraper"
    
    payload = {
        "url": "https://www.worten.pt/",
        "mode": "waf-session"
    }
    
    # 解析传入的 proxy_str (格式预期为 "ip:port:账号:密码")
    if proxy_str and proxy_str != 'null':
        parts = proxy_str.split(':')
        if len(parts) == 4:
            host, proxy_port, username, password = parts
            payload["proxy"] = {
                "host": host,
                "port": int(proxy_port),
                "username": username,
                "password": password
            }
        else:
            logging.error(f"代理格式错误，预期为 ip:port:user:pass, 实际收到: {proxy_str}")
            return None

    try:
        response = requests.post(
            api_url,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=90
        )
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        err_msg = str(e)
        if hasattr(e, 'response') and e.response is not None:
            err_msg = f"{e.response.status_code} - {e.response.text}"
        logging.error(f"请求 CF API 失败 [端口 {port}]: {err_msg}")
        return None

def close_cookie_pup(driver: uc.Chrome):
    try:
        cookie_pup_selector = "button[class='button--md button--primary button--black button'] span"
        cookie_close_bth = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, cookie_pup_selector))
        )
        driver.execute_script("arguments[0].click();", cookie_close_bth)
        return True
    except TimeoutException:
        return False
    except Exception:
        return False

def read_urls_from_excel(filename: str) -> Optional[List[Dict[str, Any]]]:
    """从Excel文件中读取URL列表。"""
    try:
        df = pd.read_excel(filename, engine='openpyxl')
        if 'url' not in df.columns:
            logging.error(f"错误: Excel文件 '{filename}' 中未找到名为 'url' 的列。")
            return None
        if 'pages_to_scrape' not in df.columns:
            logging.info("在Excel中未找到 'pages_to_scrape' 列，将为所有店铺链接使用默认分页逻辑。")
            df['pages_to_scrape'] = None
        else:
            df['pages_to_scrape'] = df['pages_to_scrape'].apply(lambda x: str(x) if pd.notna(x) and str(x).strip() != 'nan' else None)

        return df[['url', 'pages_to_scrape']].dropna(subset=['url']).to_dict('records')
    
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
    if not seller_data and not shop_data and not product_data:
        logging.warning("没有任何数据可保存。")
        return

    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            if seller_data:
                df_sellers = pd.DataFrame(seller_data)
                seller_columns = ['初始链接', '店铺名称', '链接', '店铺运费', '送货时间']
                df_sellers = df_sellers.reindex(columns=seller_columns)
                df_sellers.to_excel(writer, sheet_name='跟卖链接数据 (Sellers)', index=False)
                logging.info(f"已将 {len(df_sellers)} 条卖家数据保存到工作表 '跟卖链接数据 (Sellers)'。")
            
            if shop_data:
                df_shop = pd.DataFrame(shop_data)
                shop_columns = [
                    '商品链接', '产品评分', 'EAN', 'SKU', '品牌', '类目', 
                    '标题', '描述', '销售和发货方', '运费', '当前售价（最低）',
                    '图1', '图2', '图3', '图4', '图5',
                    '店铺1', '售价1', '运费1', '店铺2', '售价2', '运费2', '店铺3', '售价3', '运费3',
                ]
                
                # 强制将 EAN 和 SKU 列转换为字符串，以保留前导零
                for col in ['EAN', 'SKU']:
                    if col in df_shop.columns:
                        # .astype(str) 确保所有内容都是字符串
                        # .replace(...) 清理掉 'nan' 或 'None' 字符串，使其变为空单元格
                        df_shop[col] = df_shop[col].astype(str).replace({'nan': pd.NA, 'None': pd.NA})

                df_shop = df_shop.reindex(columns=shop_columns)
                df_shop.to_excel(writer, sheet_name='店铺链接数据 (Shop Products)', index=False)
                logging.info(f"已将 {len(df_shop)} 条店铺商品详细数据保存到工作表 '店铺链接数据 (Shop Products)'。")

            if product_data:
                df_product = pd.DataFrame(product_data)
                product_columns = [
                    '商品链接', '产品评分', 'EAN', 'SKU', '品牌', '类目', 
                    '标题', '描述', '销售和发货方', '运费', '当前售价（最低）',
                    '图1', '图2', '图3', '图4', '图5',
                    '店铺1', '售价1', '运费1', '店铺2', '售价2', '运费2', '店铺3', '售价3', '运费3',
                ]

                for col in ['EAN', 'SKU']:
                    if col in df_product.columns:
                        df_product[col] = df_product[col].astype(str).replace({'nan': pd.NA, 'None': pd.NA, 'N/A': pd.NA})
                        
                df_product = df_product.reindex(columns=product_columns)
                df_product.to_excel(writer, sheet_name='商品链接数据 (Product Links)', index=False)
                logging.info(f"已将 {len(df_product)} 条商品详细数据保存到工作表 '商品链接数据 (Product Links)'。")
                
        logging.info(f"数据已成功保存到文件: {filename}")
    except Exception as e:
        logging.error(f"保存数据到Excel文件 '{filename}' 时发生错误: {e}")

def wait_for_safe_cpu(threshold: float = 85.0, check_interval: int = 5):
    """
    监控 CPU 使用率。如果超过阈值，则阻塞等待，直到 CPU 降温。
    """
    try:
        while True:
            # 检测 CPU 使用率，阻塞 1 秒进行统计
            cpu_usage = psutil.cpu_percent(interval=1)
            
            if cpu_usage < threshold:
                break  # CPU 负载正常，放行
            
            logging.warning(f"系统 CPU 负载过高 ({cpu_usage}%)，暂停创建 Driver {check_interval}秒以等待冷却...")
            time.sleep(check_interval)
    except Exception as e:
        logging.warning(f"CPU 监控出错 (忽略): {e}")

def force_kill_driver(driver):
    """
    彻底清理 Driver 及其相关的 Chrome 进程。
    先尝试优雅 quit()，如果失败或进程残留，则通过 PID 强制 kill。
    """
    if not driver:
        return

    # 1. 尝试优雅退出
    try:
        driver.quit()
    except Exception:
        pass  # quit 报错是常态，忽略

    # 2. 检查是否还有残留进程
    # driver.service.process.pid (chromedriver.exe 的 PID)
    # driver.browser_pid (chrome.exe 的 PID，UC特有属性)
    
    pids_to_kill = []
    
    # 获取 ChromeDriver 的 PID
    try:
        if hasattr(driver, 'service') and driver.service.process:
            pids_to_kill.append(driver.service.process.pid)
    except: pass

    # 获取 Chrome 浏览器的 PID
    try:
        if hasattr(driver, 'browser_pid') and driver.browser_pid:
            pids_to_kill.append(driver.browser_pid)
    except: pass

    # 使用 psutil 强制查杀
    if psutil:
        for pid in pids_to_kill:
            try:
                proc = psutil.Process(pid)
                # 杀掉进程及其所有子进程
                for child in proc.children(recursive=True):
                    try: 
                        child.kill()
                    except: pass
                proc.kill()
            except psutil.NoSuchProcess:
                pass 
            except Exception as e:
                logging.warning(f"强制清理进程 {pid} 失败: {e}")

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
    """
    尝试加载 URL。
    遇到加载超时时，尝试强制停止加载。如果不抛出其他错误，则视为"勉强成功"，
    交由后续的元素查找逻辑去验证页面是否真的可用。
    """
    for attempt in range(1, max_attempts + 1):
        try:
            driver.get(url)
            return True # 完美加载
        # except TimeoutException:
        #     # 加载超时，尝试强制停止加载
        #     logging.warning(f"导航超时 (尝试 {attempt}/{max_attempts}) - 尝试停止加载...")
        #     try:
        #         driver.execute_script("window.stop();")
        #         if url in driver.current_url or "worten" in driver.current_url:
        #             return True
        #     except: pass

        except WebDriverException as e:
            error_msg = str(e)
            # --- 检测隧道连接失败 ---
            if "ERR_TUNNEL_CONNECTION_FAILED" in error_msg or "ERR_PROXY_CONNECTION_FAILED" in error_msg:
                logging.error(f"代理隧道建立失败 (IP已废): {error_msg}")
                # 直接返回 False，不再进行剩余的重试。
                return False
            logging.warning(f"WebDriver 错误 (尝试 {attempt}/{max_attempts}): {e}")

        # 如果还有重试机会，则等待指数退避
        if attempt < max_attempts:
            wait_sec = backoff_base ** (attempt - 1)
            time.sleep(wait_sec)
        
    return False
       
# ---  Image Download and Upload Functions ---
def download_image(url: str, timeout: int = 30) -> Optional[str]:
    image_dir = IMAGE_PATH
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                url,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'},
                timeout=timeout
            )
            response.raise_for_status()
            path = urlsplit(url).path
            ext = os.path.splitext(path)[1]
            filename = f"{uuid.uuid4()}{ext or '.jpg'}"
            save_path = os.path.join(image_dir, filename)
            with open(save_path, 'wb') as f:
                f.write(response.content)
            return save_path
        except requests.exceptions.RequestException as e:
            logging.info(f"[图片下载重试 {attempt+1}/{MAX_RETRIES}] {url} - {e.__class__.__name__}")
            if attempt == MAX_RETRIES - 1:
                logging.error(f"下载失败: {url} - {e}")
                return None
            time.sleep(2 ** attempt)

def upload_to_image_host(file_path: str) -> Optional[str]:
    for attempt in range(MAX_RETRIES):
        try:
            with open(file_path, 'rb') as f:
                response = requests.post(
                    IMAGE_HOST_UPLOAD_URL,
                    files={'image': f},
                    data={'token': IMAGE_TOKEN},
                    timeout=10 * (attempt + 1)
                )
            if response.ok:
                original_url = response.json()['url']
                parts = list(urlsplit(original_url))
                parts[1] = "gbcm-imagehost.vshare.dev" # 替换域名
                return urlunsplit(parts)
            logging.error(f"[Retry {attempt+1}] 上传失败 HTTP {response.status_code}")
        except Exception as e:
            logging.error(f"[Retry {attempt+1}] 上传异常: {str(e)}")
        if attempt < MAX_RETRIES:
            time.sleep(2 ** (attempt + 1))
    return None

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

def scrape_product_details(driver: uc.Chrome, product_url: str) -> Optional[Dict]:
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
        local_image_paths = []
        try:
            img_elements = driver.find_elements(By.CSS_SELECTOR, "img.product-gallery__slider-image")
            for img in img_elements:
                src = img.get_attribute('src')
                if src:
                    image_urls.append(urljoin(BASE_URL, src))
            
            successful_count = 0

            for url in image_urls:
                # 如果已经填满了5张图，提前结束
                if successful_count >= 5:
                    break
                    
                local_path = download_image(url)
                if local_path:
                    uploaded_url = upload_to_image_host(local_path)
                    if uploaded_url:
                        successful_count += 1
                        # 始终填入当前顺位的列，避免留空
                        details[f"图{successful_count}"] = uploaded_url
                        local_image_paths.append(local_path)
                    else:
                        # 如果上传失败，删除本地文件，尝试下一张
                        try: os.remove(local_path)
                        except: pass
                else:
                    logging.warning(f"图片下载失败，跳过: {url}")
        finally:
            # Clean up local images
            for path in local_image_paths:
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except Exception as e:
                    logging.warning(f"无法删除本地图片 {path}: {e}")

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

def create_chrome_driver(session_data: Dict) -> Optional[uc.Chrome]:
    """
    统一创建并初始化 Chrome Driver。
    """
    if not session_data:
        return None
    wait_for_safe_cpu(threshold=80.0, check_interval=random.randint(3,5))
    time.sleep(random.uniform(1, 3))
    cookies = session_data.get('cookies', [])
    user_agent = session_data.get('headers', {}).get("user-agent")
    proxy_for_selenium_wire = session_data.get('proxy_for_selenium_wire')

    driver = None
    
    for attempt in range(MAX_RETRIES): 
        try:
            seleniumwire_options = {
                'proxy': {
                    'http': proxy_for_selenium_wire, 
                    'https': proxy_for_selenium_wire,
                    'no_proxy': 'localhost,127.0.0.1' 
                },
                'verify_ssl': False,
                'connection_timeout': 20
            }
            
            # 每次循环都创建一个全新的 ChromeOptions 对象 
            options = uc.ChromeOptions()
            options.page_load_strategy = 'eager'
            options.add_argument('--headless=new')
            options.add_argument('--disable-features=UseEcoQoSForBackgroundProcess')
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--no-zygote') # 减少进程孵化开销
            options.add_argument('--disable-gpu-sandbox')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-popup-blocking')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-background-networking') # 禁用后台网络活动
            options.add_argument('--disable-sync')               # 禁用同步
            options.add_argument('--disable-translate')          # 禁用翻译
            options.add_argument('--disable-default-apps')       # 禁用默认应用
            options.add_argument('--no-first-run')
            options.add_argument('--disable-software-rasterizer')# 禁用软件光栅化
            options.add_argument('--renderer-process-limit=1')
            
            if user_agent:
                options.add_argument(f'--user-agent={user_agent}')

            # 3. 尝试初始化 Driver
            with CHROME_INIT_LOCK:
                wait_for_safe_cpu(threshold=80.0, check_interval=random.randint(3,5))
                driver = uc.Chrome(
                    browser_executable_path=CHROME_FOR_TESTING_PATH, 
                    driver_executable_path=DRIVER_FOR_TESTING_PATH, 
                    options=options,
                    seleniumwire_options=seleniumwire_options,
                    version_main=142
                )
            driver.set_page_load_timeout(60) 

            # 注入 Cookie 流程
            driver.get(BASE_URL)
            time.sleep(random.uniform(2, 4))
            driver.delete_all_cookies()
            
            for cookie in cookies:
                if 'sameSite' in cookie and cookie['sameSite'] not in ['Strict', 'Lax', 'None']:
                    del cookie['sameSite']
                try:
                    driver.add_cookie(cookie)
                except Exception:
                    pass 
            
            # driver.get(BASE_URL)
            # close_cookie_pup(driver)
            
            logging.debug(f"Driver 创建成功")
            time.sleep(random.uniform(2,4))
            return driver

        except Exception as e:
            logging.warning(f"创建 Driver 尝试 {attempt+1}/2 失败: {e}")
            if driver:
                force_kill_driver(driver)
                driver = None 
            
            # 在重试前稍作等待
            time.sleep(2) 

    logging.error("创建 Driver 彻底失败.")
    if driver:
        force_kill_driver(driver)
    return None

# --- 生产者与会话管理 ---

def session_producer(session_queue: multiprocessing.Queue, url_queue: multiprocessing.Queue, 
                     node_script_path: str, stop_flag, port: int, num_producers: int,log_queue: multiprocessing.Queue):
    """
    会话生产者：持续生成 CF 可用的 Session 并放入 session_queue。
    """
    setup_log_queue_handler(log_queue)
    
    shutdown_buffer = num_producers if num_producers > 1 else 2

    while not stop_flag.value:
        try:
            url_count = url_queue.qsize()
            current_session_count = session_queue.qsize()
            
            target = int(MAX_WORKERS / 2 + shutdown_buffer)
            if url_count == 0:
                 target = shutdown_buffer # 维持最低库存

            if current_session_count < target:
                PROXY_HOST = c.get_key('PROXY_HOST')
                PROXY_PORT = c.get_key('PROXY_PORT')
                PROXY_USER_BASE = c.get_key('PROXY_USER_BASE')
                PROXY_PASS = c.get_key('PROXY_PASS')
                
                session_id = ''.join(random.choices(string.ascii_letters, k=12))
                full_username = f"{PROXY_USER_BASE}-country-PT-sid-{session_id}-stime-60"
                proxy_node = f"{PROXY_HOST}:{PROXY_PORT}:{full_username}:{PROXY_PASS}"
                proxy_wire = f"http://{full_username}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"

                session_data = get_cf_cookie_from_api(port, proxy_node)
                
                if session_data and "cookies" in session_data:
                    session_data['proxy_for_selenium_wire'] = proxy_wire
                    session_data['created_at'] = time.time()
                    session_queue.put(session_data)
                    logging.info(f"[生产者{port}] 生成会话成功。库存: {session_queue.qsize()}")
                    time.sleep(2)
                else:
                    logging.debug(f"[生产者{port}] 生成失败，重试。")
                    time.sleep(5)
            else:
                time.sleep(2)
        except Exception as e:
            logging.error(f"[生产者{port}] 异常: {e}")
            time.sleep(10)

def get_fresh_session(session_queue: multiprocessing.Queue):
    """从队列获取一个新鲜的会话"""
    while True:
        try:
            session_data = session_queue.get(timeout=60)
        except queue.Empty:
            return None

        created_at = session_data.get('created_at', 0)
        age = time.time() - created_at
        max_age = SESSION_LIFESPAN_SECONDS - MIN_SESSION_USABLE_TIME_SECONDS
        
        if age < max_age:
            return session_data
        else:
            logging.info(f"丢弃过期会话 (Age: {int(age)}s)")
            continue


def discovery_process(initial_urls: List[Dict], url_queue: multiprocessing.Queue,
                      session_queue: multiprocessing.Queue, discovery_completed_event, log_queue, total_increment_queue):
    """
    发现进程：处理初始 URL，分类并展开为具体的可抓取页面 URL。
    1. 直接分类为卖家页面或商品页面的，直接放入 url_queue。
    2. 店铺页或类目页的，进行展开处理，生成具体的分页 URL 放入 url_queue。
    """
    # 1. 配置日志
    setup_log_queue_handler(log_queue)
    logging.info("--- [发现进程] 启动 ---")
    
    expansion_tasks = []
    
    # 1. 快速分类
    for item in initial_urls:
        url = item['url']
        if 'marketplace-see-more-offers' in url and 'product_id' in url:
            url_queue.put({'url': url, 'type': 'seller_page'})
        elif 'produtos/' in url:
            url_queue.put({'url': url, 'type': 'product_page'})
        elif 'seller_id' in url:
            expansion_tasks.append({'url': url, 'type': 'shop_page', 'pages': item.get('pages_to_scrape')})
        else:
            expansion_tasks.append({'url': url, 'type': 'category_page', 'pages': item.get('pages_to_scrape')})

    logging.info(f"[发现进程] 待展开任务数: {len(expansion_tasks)}")

    # 2. 处理展开任务
    if expansion_tasks:
        driver = None
        current_session_count = 0
        MAX_URLS_PER_DISCOVERY = 10

        for i, task in enumerate(expansion_tasks):
            # ---------------------------------------------------------
            # 辅助函数：确保 Driver 可用 (获取 Session 或 复用)
            # ---------------------------------------------------------
            def ensure_driver_ready():
                nonlocal driver, current_session_count
                if driver is None or current_session_count >= MAX_URLS_PER_DISCOVERY:
                    if driver:
                        try: driver.quit()
                        except: pass
                    
                    driver = None
                    for attempt in range(3):
                        logging.info(f"[发现进程] 获取会话 (尝试 {attempt+1})...")
                        session_data = get_fresh_session(session_queue)
                        if not session_data:
                            time.sleep(5); continue
                        
                        driver = create_chrome_driver(session_data)
                        if driver: break
                    
                    if not driver:
                        return False
                    current_session_count = 0
                return True

            # ---------------------------------------------------------
            # 任务执行逻辑
            # ---------------------------------------------------------
            try:
                # 1. 准备 Driver
                if not ensure_driver_ready():
                    logging.error(f"[发现进程] Driver 初始化失败，跳过任务 {task['url']}")
                    continue

                url = task['url']
                task_type = task['type']
                pages_str = task['pages']
                
                target_url = url
                if task_type == 'shop_page':
                    seller_id = url.split('seller_id=')[-1]
                    target_url = f"https://www.worten.pt/search?query=*&facetFilters=seller_id:{seller_id}"

                pages = range(1, SELLER_SCRAPED_PAGE_COUNT + 1)
                if pages_str:
                    try: pages = [int(p.strip()) for p in pages_str.split(',') if p.strip().isdigit()]
                    except: pass
                
                logging.info(f"[发现进程] 正在展开 ({i+1}/{len(expansion_tasks)}): {target_url} (页数: {len(pages)})")
                
                # --- 翻页循环 ---
                for page_num in pages:
                    sep = '&' if '?' in target_url else '?'
                    p_url = f"{target_url}{sep}page={page_num}"
                    
                    page_success = False
                    MAX_PAGE_SESSION_RETRIES = 2 # 允许换 2 次 Session 试试
                    
                    for retry_idx in range(MAX_PAGE_SESSION_RETRIES + 1):
                        # 确保有 Driver
                        if not ensure_driver_ready():
                            break

                        # 尝试导航
                        nav_ok = navigate_with_retries(driver, p_url, max_attempts=2)
                        
                        if nav_ok:
                            # 导航成功，跳出重试循环，进行数据抓取
                            page_success = True
                            break
                        else:
                            # 导航失败
                            if retry_idx < MAX_PAGE_SESSION_RETRIES:
                                logging.warning(f"[发现进程] 页 {page_num} 加载失败，销毁当前 Driver，换新 Session 重试...")
                                if driver:
                                    try: driver.quit()
                                    except: pass
                                driver = None # 强制 ensure_driver_ready 创建新的
                                current_session_count = 0 # 重置计数
                            else:
                                logging.error(f"[发现进程] 页 {page_num} 经过 {MAX_PAGE_SESSION_RETRIES+1} 个 Session 尝试后依然失败，放弃此页。")

                    if not page_success:
                        logging.warning(f"[发现进程] 放弃店铺 {url} 的后续翻页。")
                        break

                    current_session_count += 1
                    
                    # 尝试处理弹窗
                    try:
                        btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".checkYes.button")))
                        driver.execute_script("arguments[0].click();", btn)
                    except: pass
                    
                    # 等待列表并提取
                    found_links = False
                    try:
                        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".listing-content__list li a")))
                        links = driver.find_elements(By.CSS_SELECTOR, ".listing-content__list li a")
                        
                        count = 0
                        for l in links:
                            href = l.get_attribute('href')
                            if href:
                                full_url = urljoin(BASE_URL, href)
                                url_queue.put({'url': full_url, 'type': 'product_page'})
                                count += 1
                        
                        # 发送总任务数增量
                        if count > 0 and total_increment_queue:
                            total_increment_queue.put(count)
                            logging.debug(f"[发现进程] 页 {page_num} 总任务增量+{count}")
                        
                        if count > 0: found_links = True
                        logging.info(f"[发现进程] 页 {page_num}: 发现 {count} 个商品")
                        
                    except TimeoutException:
                        logging.warning(f"[发现进程] 页 {page_num} 无结果(超时)。")
                    
                    # 如果这一页没有任何商品，通常意味着到了最后一页，停止翻页
                    if not found_links:
                        break
            
            except Exception as e:
                logging.error(f"[发现进程] 任务出错: {e}")
        
        if driver:
            try: driver.quit()
            except: pass

    logging.info("--- [发现进程] 全部完成 ---")
    discovery_completed_event.set()


def discovery_process_with_progress(initial_urls: List[Dict], url_queue: multiprocessing.Queue,
                                   session_queue: multiprocessing.Queue, discovery_completed_event, log_queue, total_estimated, total_increment_queue):
    """
    支持进度跟踪的发现进程
    """
    # 1. 配置日志
    setup_log_queue_handler(log_queue)
    logging.info("--- [发现进程] 启动 ---")
    
    expansion_tasks = []
    total_estimated.value = 0 # 初始归零，配合增量队列使用
    
    # 1. 快速分类
    for item in initial_urls:
        url = item['url']
        if ('marketplace-see-more-offers' in url and 'product_id' in url) or ('produtos/' in url):
            url_queue.put({'url': url, 'type': 'product_page' if 'produtos/' in url else 'seller_page'})
            if total_increment_queue:
                total_increment_queue.put(1) # 直接链接计 1
        elif 'seller_id' in url:
            expansion_tasks.append({'url': url, 'type': 'shop_page', 'pages': item.get('pages_to_scrape')})
        else:
            expansion_tasks.append({'url': url, 'type': 'category_page', 'pages': item.get('pages_to_scrape')})

    logging.info(f"[发现进程] 待展开任务数: {len(expansion_tasks)}")
    
    if not expansion_tasks:
        logging.info("--- [发现进程] 无需展开任务，通知 Worker 收工 ---")
        discovery_completed_event.set() 
        return
    
    # 2. 处理展开任务
    driver = None
    current_session_count = 0
    MAX_URLS_PER_DISCOVERY = 15

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
                    session_data = get_fresh_session(session_queue)
                    if not session_data:
                        time.sleep(5); continue
                    driver = create_chrome_driver(session_data)
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
                            url_queue.put({'url': urljoin(BASE_URL, href), 'type': 'product_page'})
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

    if driver:
        try: driver.quit()
        except: pass

    logging.info("--- [发现进程] 全部完成 ---")
    discovery_completed_event.set()

# --- 抓取 Worker  ---

class ScraperWorker:
    def __init__(self, url_queue, all_seller_info, all_shop_data, all_product_data,
                 results_lock, session_queue, discovery_completed_event, log_queue=None, increment_queue=None):
        self.url_queue = url_queue
        self.all_seller_info = all_seller_info
        self.all_shop_data = all_shop_data
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
        # 基础上限
        self.current_max_urls = random.randint(DEFAULT_MAX_URLS_PER_DRIVER_MIN, DEFAULT_MAX_URLS_PER_DRIVER_MAX)

    def setup_driver(self):
        logging.debug(f"[Worker {self.worker_id}] 准备启动 Driver...")

        for i in range(MAX_RETRIES):
            session = get_fresh_session(self.session_queue)
            if not session:
                logging.warning(f"[Worker {self.worker_id}] 获取会话超时，正在重试 ({i+1}/{MAX_RETRIES})...")
                time.sleep(5)
                continue
            
            self.driver = create_chrome_driver(session)
            
            if self.driver:
                self.processed_count = 0
                time.sleep(random.uniform(2,4))
                return True # 成功启动
            else:
                logging.warning(f"[Worker {self.worker_id}] 当前会话/代理不可用，将丢弃并获取新会话重试...")
        
        logging.error(f"[Worker {self.worker_id}] 连续 {MAX_RETRIES} 次启动 Driver 失败。Worker 将暂时退出。")
        return False

    def teardown_driver(self):
        if self.driver:
            force_kill_driver(self.driver)
            self.driver = None

    def run(self):
        setup_log_queue_handler(self.log_queue)

        while True:
            # A. 尝试获取任务
            try:
                # 使用较短的超时，以便定期检查退出条件
                task = self.url_queue.get(block=True, timeout=2)
            except queue.Empty:
                # 队列为空
                if self.discovery_completed_event.is_set():
                    logging.info(f"[Worker {self.worker_id}] 队列为空且发现已结束，退出。")
                    break
                else:
                    # 发现还在进行，继续等待
                    continue

            # B. 拿到任务了 -> 检查是否有 Driver，没有才创建
            if self.driver is None:
                if not self.setup_driver():
                    # 如果创建失败，把任务放回队列
                    self.url_queue.put(task)
                    logging.error(f"[Worker {self.worker_id}] 无法创建 Driver，放弃当前任务并退出。")
                    break

            # C. 轮换检查
            if self.processed_count >= self.current_max_urls:
                logging.info(f"[Worker {self.worker_id}] 轮换 Driver...")
                self.teardown_driver()
                if not self.setup_driver():
                    self.url_queue.put(task) # 把任务放回去
                    break

            # D. 执行抓取
            success = self.process_task(task)
            
            if success:
                self.consecutive_failures = 0
            else:
                self.consecutive_failures += 1
            
            self.processed_count += 1
            
            # E. 中毒检查
            if self.consecutive_failures >= 3:
                logging.error(f"[Worker {self.worker_id}] 连续失败3次，强制重启。")
                self.teardown_driver()
                # 下次循环的 B 步骤会自动重新创建
                self.consecutive_failures = 0

        self.teardown_driver()

    def process_task(self, task):
        url = task['url']
        ttype = task['type']
        
        try:
            if ttype == 'seller_page':
                # from __main__ import scrape_sellers_from_page
                data = scrape_sellers_from_page(self.driver, url)
                if data:
                    with self.results_lock:
                        self.all_seller_info.extend(data)
                    return True
            
            elif ttype == 'product_page':
                # from __main__ import scrape_product_details, scrape_other_sellers_on_product_page, parse_price
                
                details = scrape_product_details(self.driver, url)
                if not details or details.get('_status') == 'page_load_failed':
                    # 简单重试逻辑可在此处加强
                    return False
                
                if details.get('_status') == 'invalid':
                    return True # 视为处理完成（无效链接）
                
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
                
                with self.results_lock:
                    self.all_product_data.append(final)
                logging.info(f" 成功抓取商品: {url}, 剩余队列: {self.url_queue.qsize()}")
                return True

        except Exception as e:
            logging.error(f"[Worker {self.worker_id}] 任务失败 {url}: {e}")
            return False
        return False


class ScraperWorkerWithProgress(ScraperWorker):
    """支持进度跟踪的Worker类"""
    def __init__(self, url_queue, all_seller_info, all_shop_data, all_product_data,
                 results_lock, session_queue, discovery_completed_event, log_queue=None, increment_queue=None):
        super().__init__(url_queue, all_seller_info, all_shop_data, all_product_data,
                        results_lock, session_queue, discovery_completed_event, log_queue, increment_queue)

    def process_task(self, task):
        # 无论任务成功与否，都应该计入处理总数
        url = task.get('url', 'Unknown URL')
        task_type = task.get('type', 'Unknown Type')
        
        logging.info(f"[Worker {self.worker_id}] 开始处理任务: {task_type} - {url}")
        
        try:
            result = super().process_task(task)
            logging.info(f"[Worker {self.worker_id}] 任务处理结果: {result} - {url}")
        except Exception as e:
            logging.error(f"[Worker {self.worker_id}] 任务处理异常: {e} - {url}")
            result = False
        
        # 发送增量信号到进度管理进程
        if self.increment_queue:
            try:
                self.increment_queue.put(1)  # 发送增量1
                logging.debug(f"[Worker {self.worker_id}] 发送增量信号 (任务: {url})")
            except Exception as e:
                logging.error(f"[Worker {self.worker_id}] 发送增量信号失败: {e}")
                
        return result

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

def main(progress_callback=None, stop_check_callback=None):
    """
    主函数，支持进度回调和停止检查
    
    Args:
        progress_callback: 进度回调函数，接收字典参数 {'processed': int, 'total': int, 'rate': float, 'message': str}
        stop_check_callback: 停止检查回调函数，返回bool值表示是否应该停止
    """
    multiprocessing.freeze_support()
    os.environ["WDM_DEFAULT_TIMEOUT"] = "90"
    cf_port = int(c.get_key('cf_bypass_port') or 3000)
    num_producers = int(c.get_key('num_session_producers') or 1)
    
    logging.info(f"--- Worten 全速抓取启动 (Workers: {MAX_WORKERS}) ---")
    
    # 1. 读取 Excel
    initial_urls = []
    try:
        # from __main__ import read_urls_from_excel
        initial_urls = read_urls_from_excel(INPUT_FILE)
    except:
        # Fallback for testing
        df = pd.read_excel(INPUT_FILE)
        initial_urls = df[['url', 'pages_to_scrape']].to_dict('records')

    if not initial_urls:
        logging.error("没有输入链接，退出。")
        if progress_callback:
            progress_callback({'processed': 0, 'total': 0, 'rate': 0, 'message': '没有输入链接'})
        return

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

        url_queue = manager.Queue()
        session_queue = manager.Queue()
        stop_flag = manager.Value('b', False)
        discovery_completed_event = manager.Event()
        
        # 数据存储
        all_seller_info = manager.list()
        all_shop_data = manager.list() # 暂时没用到，可根据需求移除
        all_product_data = manager.list()
        results_lock = manager.Lock()
        
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
            while not stop_flag.value:
                try:
                    if progress_callback:
                        elapsed_time = time.time() - start_time.value
                        rate = processed_count.value / (elapsed_time / 60) if elapsed_time > 0 else 0
                        
                        progress_data = {
                            'processed': processed_count.value,
                            'total': total_estimated.value,
                            'rate': rate,
                            'message': f'已处理 {processed_count.value} 个任务'
                        }
                        
                        logging.info(f"[进度线程] 更新进度: {progress_data}")
                        progress_callback(progress_data)
                    time.sleep(2)  # 每2秒更新一次
                except Exception as e:
                    logging.error(f"进度更新出错: {e}")
                    break
            logging.info("[进度线程] 结束")
        
        progress_thread = threading.Thread(target=progress_updater, daemon=True)
        progress_thread.start()

        # 3. 启动 Session 生产者
        producers = []
        for i in range(num_producers):
            p = multiprocessing.Process(
                target=session_producer,
                args=(session_queue, url_queue, NODE_SCRIPT_PATH, stop_flag, cf_port, num_producers, log_queue)
            )
            p.start()
            producers.append(p)
        
        logging.info("等待 15 秒以预热 Session...")
        time.sleep(15)

        # 4. 启动发现进程 (独立的后台进程)
        logging.info(f"[主进程] 启动发现进程，输入URL数量: {len(initial_urls)}")
        discovery_p = multiprocessing.Process(
            target=discovery_process_with_progress,
            args=(initial_urls, url_queue, session_queue, discovery_completed_event, log_queue, total_estimated, total_increment_queue)
        )
        discovery_p.start()
        logging.info(f"[主进程] 发现进程已启动，PID: {discovery_p.pid}")

        # 5. 启动抓取 Worker 池 (立即开始，不等发现结束)
        logging.info(f"启动抓取 Workers... (Worker数量: {MAX_WORKERS})")
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for i in range(MAX_WORKERS):
                worker_instance = ScraperWorkerWithProgress(
                    url_queue, all_seller_info, all_shop_data, all_product_data,
                    results_lock, session_queue, discovery_completed_event,
                    log_queue=log_queue, increment_queue=increment_queue
                )
                logging.info(f"[主进程] 创建 Worker {i+1}/{MAX_WORKERS}: {worker_instance.worker_id}")
                futures.append(executor.submit(worker_instance.run))

            # 等待所有 Worker 完成
            # Worker 会在 (队列空 AND discovery_completed_event set) 时退出
            wait(futures)
            logging.info("[主进程] 所有 Worker 已完成")
        
        logging.info("所有抓取任务完成。")

        # 通知日志监听器退出并等待其结束
        try:
            log_queue.put(None)
        except Exception:
            pass
        try:
            listener_thread.join(timeout=5)
        except Exception:
            pass

        # 6. 清理
        stop_flag.value = True
        discovery_p.join(timeout=5)
        if discovery_p.is_alive(): discovery_p.terminate()
        
        for p in producers:
            p.join(timeout=5)
            if p.is_alive(): p.terminate()
        
        # 等待进度管理进程结束
        progress_manager_process.join(timeout=5)
        if progress_manager_process.is_alive(): progress_manager_process.terminate()

        # 7. 保存
        logging.info("正在保存数据...")
        # from __main__ import save_data_to_multiple_sheets
        save_data_to_multiple_sheets(list(all_seller_info), list(all_shop_data), list(all_product_data), OUTPUT_FILE)
        
        # 最终进度更新
        if progress_callback:
            elapsed_time = time.time() - start_time.value
            rate = processed_count.value / (elapsed_time / 60) if elapsed_time > 0 else 0
            final_message = f'任务完成！总共处理 {processed_count.value} 个任务'
            logging.info(f"[主进程] 最终进度: {processed_count.value}/{total_estimated.value}, 耗时: {elapsed_time:.1f}秒")
            progress_callback({
                'processed': processed_count.value,
                'total': total_estimated.value,
                'rate': rate,
                'message': final_message
            })

    # 8. 强制清理僵尸进程
    if os.name == 'nt':
        try: subprocess.run("taskkill /F /T /IM chrome*", shell=True, stderr=subprocess.DEVNULL)
        except: pass

if __name__ == '__main__':
    main()