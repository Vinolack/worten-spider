# Worten 爬虫控制台

## 依赖环境

* Python 3.11 
* pip

## 安装与设置

**1. 检查 Chrome 版本**

在当前路径创建 `cft` 文件夹，安装匹配版本的 Chrome for Testing 和 chromedriver，并解压到：

```
cft/
  chrome-win64/chrome.exe
  chromedriver-win64/chromedriver.exe
```

**2. 添加证书**

将 `ca.crt` 证书导入 Chrome：`设置` -> `隐私与安全` -> `管理证书` -> `使用从操作系统导入的本地证书`。

**3. 填写 config.toml**

三个功能共用同一个 `config.toml`：

* `SELLER_SCRAPED_PAGE_COUNT`：店铺/类目链接默认爬取页数
* `IMAGE_HOST_UPLOAD_URL`：图床地址
* `IMAGE_TOKEN`：图床密钥
* `PROXY_HOST` / `PROXY_PORT` / `PROXY_USER_BASE` / `PROXY_PASS`：代理配置
* `MAX_WORKER`：最大并发进程数
* `LOG_LEVEL`：日志等级，可选 `DEBUG` / `INFO` / `WARNING` / `ERROR` / `CRITICAL`，默认 `INFO`；排查进度和细节时可设为 `DEBUG`
* `cf_bypass_port`：cf_bypass 端口
* `cf_host`：cf_bypass 地址
* `num_session_producers`：调用 cf_bypass 容器数

## 执行步骤

```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r .\requirements.txt
```

启动统一 GUI：

```bash
python .\worten_gui.py
```

GUI 顶部通过功能选择切换：商品信息抓取、价格检查、跟卖信息抓取。

## 断点续跑

程序会在输出文件所在目录创建 `worten_runs.sqlite`，用于记录任务状态和结果：

* 程序崩溃或重启后，再次使用相同功能、相同输入文件和相同输出文件启动，会继续未完成任务。
* 已完成任务不会重复抓取。
* Excel 导出失败时，已完成结果仍保存在 SQLite 中，修复输出文件占用/权限问题后可再次点击“开始/继续任务”。

## 程序打包

打包统一入口：

```bash
pyinstaller --noconfirm --onedir --windowed --clean --name "Worten爬虫控制台V3" --add-data "cft;cft" --add-data "config.toml;." --add-data "ca.crt;seleniumwire" worten_gui.py
```

## 验证

```bash
python -m py_compile product_info.py price_check.py more_seller.py worten_gui.py state_store.py excel_schemas.py
```
