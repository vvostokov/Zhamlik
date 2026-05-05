# Zhamlik Crypto

Веб-приложение для учета и аналитики криптовалютного портфеля на основе API Zhamlik.

## Возможности

- 📊 **Портфель** - обзор всех крипто-активов с общей стоимостью в USD и RUB
- 📈 **Аналитика** - распределение портфеля и динамика стоимости
- 📝 **Операции** - история всех транзакций с фильтрами
- 💰 **Курсы** - текущие цены криптовалют с изменением за 24ч

## Технологии

- Backend: Flask + Python
- Frontend: Bootstrap 5 + Chart.js
- API: интеграция с Zhamlik Investment API
- Deployment: Gunicorn + Nginx

## Структура

```
zhamlik-crypto/
├── app.py                    # Flask приложение
├── requirements.txt          # Зависимости Python
├── static/
│   ├── js/
│   │   └── app.js           # JavaScript логика
│   ├── css/
│   └── images/
└── templates/
    └── index.html           # Главный HTML шаблон
```

## API endpoints

### Внутренние endpoints (этого приложения):

- `GET /api/crypto/overview` - Главный экран портфеля
- `GET /api/crypto/assets` - Список активов
- `GET /api/crypto/transactions` - Транзакции (с пагинацией)
- `GET /api/crypto/analytics` - Аналитика
- `GET /api/crypto/prices` - Текущие цены

### Внешние API (Zhamlik):

Приложение проксирует запросы к основному приложению Zhamlik:

- `GET /api/investment/crypto_overview`
- `GET /api/investment/crypto_assets`
- `GET /api/investment/crypto_transactions`
- `GET /api/investment/crypto_analytics`
- `GET /api/investment/crypto_prices`

## Установка

### Локально:

```bash
# Создание виртуального окружения
python3 -m venv venv
source venv/bin/activate

# Установка зависимостей
pip install -r requirements.txt

# Запуск
python app.py
```

Приложение будет доступно на http://localhost:5003

### На сервере:

```bash
# Запуск деплоймент скрипта
bash /tmp/deploy-crypto.sh
```

## Конфигурация

Переменные окружения:

- `SECRET_KEY` - секретный ключ Flask
- `CRYPTO_DB_URL` - URL базы данных (по умолчанию SQLite)
- `ZHAMLIK_API_URL` - URL основного API Zhamlik (по умолчанию http://localhost:5000)

## Дизайн

Приложение использует фиолетовую цветовую схему, отличную от основного Zhamlik (синий) и мобильного (голубой):

- Основной цвет: `#7952b3` (фиолетовый)
- Фон: градиент от `#f5f3ff` до `#e9e8f8`
- Карточки с белым фоном и фиолетовыми тенями

## Навигация

4 основные секции в нижнем меню:

1. **Портфель** - главный экран с балансом и активами
2. **Аналитика** - графики и распределение
3. **Операции** - история транзакций
4. **Цены** - курсы криптовалют
