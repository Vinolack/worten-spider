// index.js
const { HttpsProxyAgent } = require('https-proxy-agent');
const axios = require('axios');

function parseProxyUrl(proxyString) {
    if (!proxyString || typeof proxyString !== 'string' || !proxyString.includes(':')) {
        return null; // 如果代理字符串无效，静默返回null
    }
    const parts = proxyString.split(':');
    if (parts.length !== 4) {
        console.error(`错误: 代理字符串格式不正确，应为 'ip:port:账号:密码'。收到的内容: ${proxyString}`);
        return null;
    }
    const [host, port, username, password] = parts;
    return {
        protocol: 'http',
        host: host,
        port: parseInt(port, 10),
        auth: { username: username, password: password }
    };
}

(async () => {
    // 参数1: 代理字符串 (可选)
    const proxyArg = process.argv[2]; 
    // 参数2: 端口号 (必需)
    const portArg = process.argv[3];

    // 验证端口号参数
    if (!portArg || isNaN(parseInt(portArg, 10))) {
        console.error("错误: 未提供有效的端口号。请在代理参数后提供端口号。");
        process.exit(1);
    }
    const port = parseInt(portArg, 10);
    
    let proxyConfig = null;
    if (proxyArg && proxyArg !== 'null' && proxyArg !== 'undefined') { // 增加检查，以防Python传来'null'字符串
        proxyConfig = parseProxyUrl(proxyArg);
        if (!proxyConfig) {
            console.error("错误: 提供的代理URL无效，程序退出。");
            process.exit(1);
        }
    }

    try {
        const request = require('./request');
        // 将解析后的 proxyConfig 和 port 传递给函数
        const cookie = await request.bypass_cf_clearance(proxyConfig, port);
        
        // 成功后，将结果以JSON格式输出到标准输出
        console.log(JSON.stringify(cookie));
    } catch (err) {
        // 将错误信息输出到标准错误流
        // err.message 已经包含了来自 request.js 的详细信息
        console.error(`获取Cookie失败: ${err.message}`);
        process.exit(1);
    }
})();
