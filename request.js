// request.js
const axios = require('axios');
const fs = require('fs');
const toml = require('toml');
const path = require('path');

let apiHost;

// --- 路径自动判断 ---
// 1. 优先尝试获取当前 JS 文件所在目录 (即 _internal) 下的 config.toml
let configPath = path.join(__dirname, 'config.toml');

// 2. 如果内部没有 (说明放在了 EXE 同级)，则尝试去上一级目录找
if (!fs.existsSync(configPath)) {
    configPath = path.join(__dirname, '..', 'config.toml');
}

try {
    const config = toml.parse(fs.readFileSync(configPath, 'utf-8'));
    apiHost = config.api.cf_host;
} catch (e) {
    console.error("错误: 无法读取或解析 config.toml 文件。请确保文件存在且格式正确。将回退到默认地址。");
    apiHost = "100.100.226.4"; // 如果配置文件读取失败，则使用默认值
}

// 接收一个 port 参数
async function bypass_cf_clearance(proxyConfig, port) {
    try {
        // 动态构建 API URL，使用从配置文件读取的地址
        const apiUrl = `http://${apiHost}:${port}/cf-clearance-scraper`;
        let payload;
        if (proxyConfig) {
            payload = {
                url: 'https://www.worten.pt/',
                mode: "waf-session",
                proxy: {
                    host: proxyConfig.host,
                    port: proxyConfig.port,
                    username: proxyConfig.auth ? proxyConfig.auth.username : undefined,
                    password: proxyConfig.auth ? proxyConfig.auth.password : undefined
                }
            };
        } else {
            payload = {
                url: 'https://www.worten.pt/',
                mode: "waf-session",
            };
        }
        // 使用动态构建的 URL
        const response = await axios.post(apiUrl, payload, {
            headers: {
                'Content-Type': 'application/json'
            },
        })
        return response.data;
    }
    catch (error) {
        let msg = error.response ? `${error.response.status} - ${error.response.statusText} : ${JSON.stringify(error.response.data)}` : error.message;
        // 在错误信息中加入端口号，方便调试
        console.error(`端口 ${port} 请求异常_bypass_cf_clearance: ${msg}`);
        throw new Error(`端口 ${port} 请求异常_bypass_cf_clearance: ${msg}`);
    }
}

// 接收一个 port 参数
async function bypass_cf_turnstile(proxyConfig, port) {
    try {
        const apiUrl = `http://${apiHost}:${port}/cf-clearance-scraper`;
        let payload;
        if (proxyConfig) {
            payload = {
                url: "https://klokapp.ai",
                siteKey: "0x4AAAAAABdQypM3HkDQTuaO",
                mode: "turnstile-min",
                proxy: {
                    host: proxyConfig.host,
                    port: proxyConfig.port,
                    username: proxyConfig.auth ? proxyConfig.auth.username : undefined,
                    password: proxyConfig.auth ? proxyConfig.auth.password : undefined
                }
            };
        } else {
            payload = {
                url: "https://klokapp.ai",
                siteKey: "0x4AAAAAABdQypM3HkDQTuaO",
                mode: "turnstile-min"
            };
        }
        const response = await axios.post(apiUrl, payload, {
            headers: {
                'Content-Type': 'application/json'
            },
        })
        return response.data.token;
    }
    catch (error) {
        let msg = error.response ? `${error.response.status} - ${error.response.statusText} : ${JSON.stringify(error.response.data)}` : error.message;
        console.error(`端口 ${port} 请求异常_bypass_cf_turnstile: ${msg}`);
        throw new Error(`端口 ${port} 请求异常_bypass_cf_turnstile: ${msg}`);
    }
}

// 接收一个 port 参数
async function get_page_source(proxyConfig, port) {
    try {
        const apiUrl = `http://${apiHost}:${port}/cf-clearance-scraper`;
        let payload;
        if (proxyConfig) {
            payload = {
                url: 'https://www.worten.pt/produtos/ventilador-e-purificador-de-ar-50-w-mrkean-8056420222975',
                mode: "source",
                proxy: {
                    host: proxyConfig.host,
                    port: proxyConfig.port,
                    username: proxyConfig.auth ? proxyConfig.auth.username : undefined,
                    password: proxyConfig.auth ? proxyConfig.auth.password : undefined
                }
            };
        } else {
            payload = {
                url: 'https://www.worten.pt/produtos/ventilador-e-purificador-de-ar-50-w-mrkean-8056420222975',
                mode: "source",
            };
        }
        const response = await axios.post(apiUrl, payload, {
            headers: {
                'Content-Type': 'application/json'
            },
        })
        return response.data;
    }
    catch (error) {
        let msg = error.response ? `${error.response.status} - ${error.response.statusText} : ${JSON.stringify(error.response.data)}` : error.message;
        console.error(`端口 ${port} 请求异常_get_page_source: ${msg}`);
        throw new Error(`端口 ${port} 请求异常_get_page_source: ${msg}`);
    }
}


// tls_bypass 函数不与该API交互，因此无需修改
const initCycleTLS = require('cycletls');
async function tls_bypass(proxyConfig, cf_clearance) {
    const cycleTLS = await initCycleTLS();
    const response = await cycleTLS('https://doi.org/10.1093/plcell/koaf210', {
        ja3: '772,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,23-27-65037-43-51-45-16-11-13-17513-5-18-65281-0-10-35,25497-29-23-24,0', // https://scrapfly.io/web-scraping-tools/ja3-fingerprint
        userAgent: cf_clearance.headers["user-agent"],
        proxy: proxyConfig ? proxyConfig.url : undefined,
        headers: {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'zh-CN,zh;q=0.9',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            'sec-ch-ua-arch': '"x86"',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-full-version': '"138.0.7204.169"',
            'sec-ch-ua-full-version-list': '"Not)A;Brand";v="8.0.0.0", "Chromium";v="138.0.7204.169", "Google Chrome";v="138.0.7204.169"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '""',
            'sec-ch-ua-platform': '"Windows"',
            'sec-ch-ua-platform-version': '"19.0.0"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'cookie': cf_clearance.cookies.map(cookie => `${cookie.name}=${cookie.value}`).join('; '),
            ...cf_clearance.headers
        }
    }, 'get');
    cycleTLS.exit().catch(err => { });

    const htmlContent = response.body;
    return htmlContent;
}


module.exports = {
    bypass_cf_clearance,
    bypass_cf_turnstile,
    tls_bypass,
    get_page_source
};