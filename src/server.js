import { startServer } from './app.js';

const app = startServer();

// 优雅关停：关闭 HTTP 监听并关闭数据库（WAL 检查点）
for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, () => {
    console.log(`收到 ${sig}，正在关停…`);
    app.server.close(() => {
      app.store.close();
      process.exit(0);
    });
    // 兜底：5 秒内未退出则强制
    setTimeout(() => process.exit(1), 5000).unref();
  });
}
