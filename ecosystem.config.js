module.exports = {
  apps: [
    {
      name: 'pixel-fider',
      script: './server.js',
      exec_mode: 'cluster',
      instances: 'max', // кол-во воркеров: число или 'max' (по числу vCPU)
      time: true,       // добавлять timestamp в логи
      // watch: false,  // при желании можно включить слежение за файлами
      env: {
        NODE_ENV: 'development',
        HOST: '0.0.0.0',
        PORT: 3000,
        BASE_PATH: '/pixel',
        // Пул соединений и глобальный лимит параллелизма (сетевые запросы)
        MAX_CONCURRENCY: 30,
        KEEPALIVE_CONNECTIONS: 30,
        // Количество одновременных задач на процесс
        MAX_CONCURRENT_JOBS: 5,
        MAX_QUEUE_SIZE: 50,
        // MongoDB для валидации пикселей
        MONGO_URL: 'mongodb://127.0.0.1:27017/pixel-db',
        // Redis и дефолты
        REDIS_URL: 'redis://127.0.0.1:6379/0',
        DEFAULT_ROUTE_URL: '',
        // TTL и дебаунс обновлений задач
        JOB_TTL_SECONDS: 60 * 60 * 24 * 3, // 3 дня
        CANCEL_TTL_SECONDS: 60 * 60 * 24,  // 1 день
        JOB_UPDATE_DEBOUNCE_MS: 300,
        JOB_UPDATE_MAX_MS: 2000,
      },
      env_production: {
        NODE_ENV: 'production',
        // При продовом запуске обычно поднимаем параллелизм
        MAX_CONCURRENCY: 50,
        KEEPALIVE_CONNECTIONS: 50,
        MAX_CONCURRENT_JOBS: 5,
        MAX_QUEUE_SIZE: 50,
      },
    },
  ],
};
