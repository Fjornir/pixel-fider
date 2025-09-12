module.exports = {
  apps: [
    {
      name: 'pixel-fider',
      script: './server.js',
      exec_mode: 'cluster',
      instances: 'max', // количество воркеров: число или 'max' (по числу vCPU)
      time: true,       // добавлять timestamp в логи
      // watch: false,
      env: {
        NODE_ENV: 'development',
        HOST: '0.0.0.0',
        PORT: 3000,
        BASE_PATH: '/pixel',
        // Пул соединений и глобальный лимит параллелизма (сетевые запросы)
        MAX_CONCURRENCY: 30,
        KEEPALIVE_CONNECTIONS: 30,
        // Redis и дефолты
        REDIS_URL: 'redis://127.0.0.1:6379/0',
        // Не задаём DEFAULT_ROUTE_URL здесь в dev, чтобы .env мог его подставить
        // TTL и дебаунс обновлений задач
        JOB_TTL_SECONDS: 60 * 60 * 24 * 3, // 3 дня
        CANCEL_TTL_SECONDS: 60 * 60 * 24,  // 1 день
        JOB_UPDATE_DEBOUNCE_MS: 300,
        JOB_UPDATE_MAX_MS: 2000,
        // Логировать каждую успешную отправку в dev
        SUCCESS_LOG_EVERY: 1,
      },
      env_production: {
        NODE_ENV: 'production',
        // При продовом запуске обычно поднимаем параллелизм
        MAX_CONCURRENCY: 50,
        KEEPALIVE_CONNECTIONS: 50,
        // ВАЖНО: выставляем значения из вашего .env, чтобы PM2 не перетирал их пустыми
        DEFAULT_ROUTE_URL: 'http://91.210.164.25:3001/api/send',
        COUNTRIES_DIR: '/var/www/pixel-fider/countries',
        BASE_PATH: '/pixel',
        PORT: 3000,
        HOST: '0.0.0.0',
        // Redis и TTL/дебаунс обновлений задач
        REDIS_URL: 'redis://127.0.0.1:6379/0',
        JOB_TTL_SECONDS: 60 * 60 * 24 * 3, // 3 дня хранить задачи
        CANCEL_TTL_SECONDS: 60 * 60 * 24,  // 1 день хранить флаг отмены
        JOB_UPDATE_DEBOUNCE_MS: 300,
        JOB_UPDATE_MAX_MS: 2000,
        // Логировать каждую успешную отправку в проде
        SUCCESS_LOG_EVERY: 1,
        MONGO_URL: 'mongodb://127.0.0.1:27017/pixel-fider',
      },
    },
  ],
};
