const http = require('http');
const { WebSocketServer } = require('ws');

// إنشاء خادم HTTP عادي ليتوافق مع متطلبات منصة Render ويمنع توقفه
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('UDP-Proxy Tunnel Server is Running Successfully!\n');
});

// إنشاء خادم WebSocket فوق خادم الـ HTTP لاستقبال الحزم المغلفة من هاتفك
const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
    console.log('[+] Client connected from:', req.socket.remoteAddress);

    ws.on('message', (message) => {
        // هنا يتم استقبال البيانات المغلفة من تطبيق العميل على هاتفك
        // وإعادة توجيهها أو معالجتها حسب مسار التوجيه المطلوب
        console.log(`[Data Received] Size: ${message.length} bytes`);
        
        // صدى أو إعادة إرسال للتحقق من استقرار الاتصال
        ws.send(message);
    });

    ws.on('close', () => {
        console.log('[-] Client disconnected');
    });

    ws.on('error', (err) => {
        console.error('[Error] WebSocket error:', err);
    });
});

// استخدام المنفذ الذي تحدده منصة Render تلقائياً
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server is listening on port ${PORT}`);
});
