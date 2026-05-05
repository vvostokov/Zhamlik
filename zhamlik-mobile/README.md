# Zhamlik Mobile PWA

Progressive Web App для учета финансов на мобильных устройствах.

## Возможности

- 📊 **Главная страница**: баланс, доходы/расходы, последние операции
- 📈 **Аналитика**: расходы по категориям, динамика по дням
- 💳 **Счета**: управление всеми банковскими счетами
- 💸 **Долги**: учет долгов и займов
- 🔔 **Уведомления**: push-уведомления (в разработке)
- 📱 **PWA**: работает оффлайн, устанавливается как нативное приложение

## Установка на сервере

### 1. Скопируйте файлы на сервер

```bash
rsync -avz --exclude 'venv/' --exclude '.git' \
  /home/onor/projects/zhamlik-mobile/ \
  root@193.29.224.20:/opt/zhamlik-mobile/
```

### 2. Настройте окружение на сервере

```bash
ssh root@193.29.224.20

# Установите зависимости
cd /opt/zhamlik-mobile
apt-get install -y python3-venv
python3 -m venv venv
venv/bin/pip install -r requirements.txt

# Скопируйте необходимые файлы из основного проекта
cp /opt/zhamlik/models.py /opt/zhamlik-mobile/
cp /opt/zhamlik/extensions.py /opt/zhamlik-mobile/

# Настройте .env
cat > .env << 'EOF'
SECRET_KEY=ваш-secret-key
DATABASE_URL=postgresql://zhamlik:пароль@localhost/zhamlik_db
EOF
```

### 3. Создайте systemd сервис

```bash
cat > /etc/systemd/system/zhamlik-mobile.service << 'EOF'
[Unit]
Description=Zhamlik Mobile PWA
After=network.target postgresql.service

[Service]
User=root
WorkingDirectory=/opt/zhamlik-mobile
Environment='PATH=/opt/zhamlik-mobile/venv/bin:/usr/local/bin:/usr/bin:/bin'
EnvironmentFile=-/opt/zhamlik-mobile/.env
ExecStart=/opt/zhamlik-mobile/venv/bin/gunicorn --workers 2 --bind unix:zhamlik-mobile.sock --timeout 120 app:app
ExecReload=/bin/kill -s HUP $MAINPID
KillMode=mixed
TimeoutStopSec=125
PrivateTmp=true
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable zhamlik-mobile
systemctl start zhamlik-mobile
```

### 4. Настройте Nginx

```bash
cat > /etc/nginx/sites-available/zhamlik-mobile << 'EOF'
server {
    listen 8080;
    server_name 193.29.224.20;

    proxy_connect_timeout 120;
    proxy_send_timeout 120;
    proxy_read_timeout 120;

    location / {
        proxy_pass http://unix:/opt/zhamlik-mobile/zhamlik-mobile.sock;
        proxy_redirect off;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /static {
        alias /opt/zhamlik-mobile/static;
    }
}
EOF

ln -sf /etc/nginx/sites-available/zhamlik-mobile /etc/nginx/sites-enabled/
nginx -t
systemctl reload nginx
```

## Доступ

После развертывания приложение будет доступно по адресу:
- **Web**: http://193.29.224.20:8080
- **PWA**: откройте в Chrome на Android и нажмите "Добавить на главный экран"

## API Endpoints

### GET /api/mobile/overview
Главный экран с балансами и последними транзакциями

### GET /api/mobile/accounts
Список всех счетов

### GET /api/mobile/transactions?page=1&type=expense
Список транзакций с пагинацией

### GET /api/mobile/analytics?days=30
Аналитика расходов по категориям

### GET /api/mobile/debts
Список долгов

### GET /api/mobile/recurring_payments
Ближайшие регулярные платежи

## Структура проекта

```
zhamlik-mobile/
├── app.py                    # Flask приложение
├── requirements.txt          # Зависимости
├── README.md                 # Этот файл
├── static/
│   ├── manifest.json        # PWA манифест
│   ├── sw.js                # Service Worker для оффлайн работы
│   ├── css/
│   ├── js/
│   │   └── app.js           # Главный JavaScript приложения
│   └── images/              # Иконки для PWA
└── templates/
    └── index.html           # Главный HTML шаблон
```

## Разработка

### Локальный запуск

```bash
cd /home/onor/projects/zhamlik-mobile
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Скопируйте модели и расширения из основного проекта
cp ../zhamlik/models.py .
cp ../zhamlik/extensions.py .

# Запустите
python app.py
```

Приложение будет доступно по адресу http://localhost:5002

### Добавление новых функций

1. Добавьте endpoint в `app.py`
2. Обновите `fetchAPI()` в `static/js/app.js`
3. Добавьте рендеринг новой страницы в `app.js`
4. Обновите навигацию в `templates/index.html`

## TODO

- [ ] Добавить формы создания операций
- [ ] Редактирование счетов и долгов
- [ ] Push-уведомления
- [ ] Графики аналитики
- [ ] Экспорт данных
- [ ] Биометрическая авторизация
- [ ] Темная тема
