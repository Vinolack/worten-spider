import logging
import multiprocessing
import os
import queue
import random
import string
import time
from typing import Dict, Optional

import psutil
import requests
import seleniumwire.undetected_chromedriver as uc
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from paths import get_exe_dir


def seleniumwire_runtime_dir() -> str:
    runtime_dir = os.path.join(get_exe_dir(), "runtime", "seleniumwire")
    os.makedirs(runtime_dir, exist_ok=True)
    return runtime_dir


def get_cf_cookie_from_api(config, port: int, proxy_str: Optional[str] = None) -> Optional[Dict]:
    """Request a Cloudflare-cleared browser session from the configured bypass API."""
    api_host = config.get_key('cf_host')
    api_url = f"http://{api_host}:{port}/cf-clearance-scraper"
    payload = {"url": "https://www.worten.pt/", "mode": "waf-session"}

    if proxy_str and proxy_str != 'null':
        parts = proxy_str.split(':')
        if len(parts) == 4:
            host, proxy_port, username, password = parts
            payload["proxy"] = {
                "host": host,
                "port": int(proxy_port),
                "username": username,
                "password": password,
            }
        else:
            logging.error(f"代理格式错误，预期为 ip:port:user:pass, 实际收到: {proxy_str}")
            return None

    try:
        response = requests.post(api_url, json=payload, headers={'Content-Type': 'application/json'}, timeout=90)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exc:
        err_msg = str(exc)
        if hasattr(exc, 'response') and exc.response is not None:
            err_msg = f"{exc.response.status_code} - {exc.response.text}"
        logging.error(f"请求 CF API 失败 [端口 {port}]: {err_msg}")
        return None


def close_cookie_pup(driver: uc.Chrome) -> bool:
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


def wait_for_safe_cpu(threshold: float = 85.0, check_interval: int = 5) -> None:
    if psutil is None:
        return
    try:
        while True:
            cpu_usage = psutil.cpu_percent(interval=1)
            if cpu_usage < threshold:
                break
            logging.warning(f"系统 CPU 负载过高 ({cpu_usage}%)，暂停创建 Driver {check_interval}秒...")
            time.sleep(check_interval)
    except Exception:
        pass


def force_kill_driver(driver) -> None:
    if not driver:
        return
    try:
        driver.quit()
    except Exception:
        pass

    pids_to_kill = []
    try:
        if hasattr(driver, 'service') and driver.service.process:
            pids_to_kill.append(driver.service.process.pid)
        if hasattr(driver, 'browser_pid') and driver.browser_pid:
            pids_to_kill.append(driver.browser_pid)
    except Exception:
        pass

    for pid in pids_to_kill:
        try:
            proc = psutil.Process(pid)
            for child in proc.children(recursive=True):
                try:
                    child.kill()
                except Exception:
                    pass
            proc.kill()
        except psutil.NoSuchProcess:
            pass
        except Exception as exc:
            logging.warning(f"强制清理进程 {pid} 失败: {exc}")


def navigate_with_retries(driver: uc.Chrome, url: str, max_attempts: int = 3, backoff_base: int = 2) -> bool:
    for attempt in range(1, max_attempts + 1):
        try:
            driver.get(url)
            return True
        except WebDriverException as exc:
            error_msg = str(exc)
            if "ERR_TUNNEL_CONNECTION_FAILED" in error_msg or "ERR_PROXY_CONNECTION_FAILED" in error_msg:
                logging.error(f"代理隧道建立失败 (IP已废): {error_msg}")
                return False
            logging.warning(f"WebDriver 错误 (尝试 {attempt}/{max_attempts}): {exc}")

        if attempt < max_attempts:
            time.sleep(backoff_base ** (attempt - 1))
    return False


def build_requests_proxy_url(config, session_id: Optional[str] = None) -> Optional[str]:
    proxy_host = config.get_key('PROXY_HOST')
    proxy_port = config.get_key('PROXY_PORT')
    proxy_user_base = config.get_key('PROXY_USER_BASE')
    proxy_pass = config.get_key('PROXY_PASS')
    if not all([proxy_host, proxy_port, proxy_user_base, proxy_pass]):
        return None

    sid = session_id or ''.join(random.choices(string.ascii_letters, k=12))
    full_username = f"{proxy_user_base}-country-PT-sid-{sid}-stime-60"
    return f"http://{full_username}:{proxy_pass}@{proxy_host}:{proxy_port}"


def build_proxy_urls(config, session_id: Optional[str] = None) -> Dict[str, str]:
    sid = session_id or ''.join(random.choices(string.ascii_letters, k=12))
    full_username = f"{config.get_key('PROXY_USER_BASE')}-country-PT-sid-{sid}-stime-60"
    return {
        "session_id": sid,
        "proxy_node": f"{config.get_key('PROXY_HOST')}:{config.get_key('PROXY_PORT')}:{full_username}:{config.get_key('PROXY_PASS')}",
        "proxy_wire": f"http://{full_username}:{config.get_key('PROXY_PASS')}@{config.get_key('PROXY_HOST')}:{config.get_key('PROXY_PORT')}",
    }


def create_chrome_driver(
    session_data: Dict,
    chrome_path: str,
    driver_path: str,
    base_url: str,
    chrome_init_lock: multiprocessing.Lock,
    max_retries: int = 3,
    connection_timeout: Optional[int] = None,
    sleep_before_driver: bool = False,
    sleep_after_base_get: bool = False,
    sleep_before_return: bool = False,
) -> Optional[uc.Chrome]:
    if not session_data:
        return None
    wait_for_safe_cpu(threshold=80.0, check_interval=random.randint(3, 5))
    if sleep_before_driver:
        time.sleep(random.uniform(1, 3))

    cookies = session_data.get('cookies', [])
    user_agent = session_data.get('headers', {}).get("user-agent")
    proxy_wire = session_data.get('proxy_for_selenium_wire')
    driver = None

    for attempt in range(max_retries):
        try:
            seleniumwire_options = {
                'proxy': {'http': proxy_wire, 'https': proxy_wire, 'no_proxy': 'localhost,127.0.0.1'},
                'verify_ssl': False,
                'disable_capture': True,
                'request_storage': 'memory',
                'request_storage_base_dir': seleniumwire_runtime_dir(),
                'suppress_connection_errors': True,
            }
            if connection_timeout is not None:
                seleniumwire_options['connection_timeout'] = connection_timeout

            options = uc.ChromeOptions()
            options.page_load_strategy = 'eager'
            for arg in [
                '--headless=new',
                '--disable-features=UseEcoQoSForBackgroundProcess',
                '--ignore-certificate-errors',
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--no-zygote',
                '--disable-gpu-sandbox',
                '--disable-gpu',
                '--disable-popup-blocking',
                '--disable-extensions',
                '--disable-background-networking',
                '--disable-sync',
                '--disable-translate',
                '--disable-default-apps',
                '--no-first-run',
                '--disable-software-rasterizer',
                '--renderer-process-limit=1',
            ]:
                options.add_argument(arg)
            if user_agent:
                options.add_argument(f'--user-agent={user_agent}')

            with chrome_init_lock:
                wait_for_safe_cpu(threshold=80.0, check_interval=random.randint(3, 5))
                driver = uc.Chrome(
                    browser_executable_path=chrome_path,
                    driver_executable_path=driver_path,
                    options=options,
                    seleniumwire_options=seleniumwire_options,
                    version_main=142,
                )
            driver.set_page_load_timeout(60)
            driver.get(base_url)
            if sleep_after_base_get:
                time.sleep(random.uniform(2, 4))
            driver.delete_all_cookies()

            for cookie in cookies:
                if 'sameSite' in cookie and cookie['sameSite'] not in ['Strict', 'Lax', 'None']:
                    del cookie['sameSite']
                try:
                    driver.add_cookie(cookie)
                except Exception:
                    pass

            if sleep_before_return:
                time.sleep(random.uniform(2, 4))
            return driver
        except Exception as exc:
            logging.warning(f"创建 Driver 尝试 {attempt + 1}/{max_retries} 失败: {exc}")
            if driver:
                force_kill_driver(driver)
                driver = None
            time.sleep(2)

    logging.error("创建 Driver 彻底失败.")
    if driver:
        force_kill_driver(driver)
    return None


def get_fresh_session(session_queue, session_lifespan_seconds: int, min_session_usable_time_seconds: int):
    while True:
        try:
            session_data = session_queue.get(timeout=60)
        except queue.Empty:
            return None

        created_at = session_data.get('created_at', 0)
        age = time.time() - created_at
        max_age = session_lifespan_seconds - min_session_usable_time_seconds
        if age < max_age:
            return session_data
        logging.info(f"丢弃过期会话 (Age: {int(age)}s)")


def create_session_data(config, port: int) -> Optional[Dict]:
    proxy = build_proxy_urls(config)
    session_data = get_cf_cookie_from_api(config, port, proxy["proxy_node"])
    if session_data and "cookies" in session_data:
        session_data['proxy_for_selenium_wire'] = proxy["proxy_wire"]
        session_data['created_at'] = time.time()
        return session_data
    return None
