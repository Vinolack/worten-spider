# Worten 商品信息抓取工具

## 依赖环境

* Python 11.3 或更高版本
* pip

## 安装与设置

请按照以下步骤设置您的项目环境：

**1. 检查Chrome版本**

在当前路径创建文件夹`cft`，点击[这里](https://googlechromelabs.github.io/chrome-for-testing/)，安装对应版本的chrome和chromedriver，将下载的两个`.zip`文件解压到`cft`中
    
    你的项目/
    │
    ├── cft/
    │   │
    │   ├── chrome-win64/
    │   │   ├── chrome.exe         <-- 这是 CfT 浏览器
    │   │   └── ... (其他文件)
    │   │
    │   └── chromedriver-win64/
    │       └── chromedriver.exe   <-- 这是匹配的 Driver
    │
    ├── product_info.py                    <-- 你的Python脚本
    ├── input_links.xlsx
    └── index.js

**2. 添加证书**

将文件夹中的`ca.crt`证书导入Chrome浏览器中：`设置` -> `隐私与安全` -> `管理证书` -> `使用从操作系统导入的本地证书`

**3. 安装node环境**

1.  安装[node.js](https://nodejs.org/en/download/current)
2.  安装依赖：打开终端，运行以下指令
    bash
    ```
    npm update
    npm install
    ```

**4. 填写config配置文件**
* SELLER_SCRAPED_PAGE_COUNT = 1                             #爬取店铺链接或者类目链接时默认爬取的最大页码
* IMAGE_HOST_UPLOAD_URL = ""                                #图床地址
* IMAGE_TOKEN = ""                                          #图床密钥
* PROXY_HOST = ""                                           #IP池配置
* PROXY_PORT = 7778
* PROXY_USER_BASE = ""
* PROXY_PASS = ""                                           
* MAX_WORKER = 1                                            #最大并发进程数
* cf_bypass_port = 3000                                     #cf_bypass端口
* cf_host = ""                                              #cf_bypass地址
* num_session_producers = 11                                #调用cf_bypass容器数

## 执行步骤

**程序打包**
bash
```
pyinstaller --noconfirm --onedir --windowed --clean --name "Worten商品信息爬虫工具" `
>> --add-data "cft;cft" --add-data "node_modules;node_modules" `
>> --add-data "config.toml;." --add-data "index.js;." `
>> --add-data "request.js;." --add-data "ca.crt;seleniumwire" `
>> worten_gui.py

```

```
pyinstaller --noconfirm --onedir --windowed --clean --name "Worten价格检查爬虫工具" `
>> --add-data "cft;cft" --add-data "node_modules;node_modules" `
>> --add-data "config.toml;." --add-data "index.js;." `
>> --add-data "request.js;." --add-data "ca.crt;seleniumwire" `
>> price_check_gui.py
```

```
pyinstaller --noconfirm --onedir --windowed --clean --name "Worten跟卖信息爬虫工具" `
>> --add-data "cft;cft" --add-data "node_modules;node_modules" `
>> --add-data "config.toml;." --add-data "index.js;." `
>> --add-data "request.js;." --add-data "ca.crt;seleniumwire" `
>> more_seller_gui.py
```