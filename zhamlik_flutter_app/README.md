# Zhamlik Flutter Mobile App

Мобильное приложение для учета финансов Zhamlik на базе Flutter.

## Возможности

- 🔐 **Авторизация**: Вход в аккаунт
- 📊 **Главная страница**: Обзор баланса, доходов и расходов
- 💳 **Счета**: Просмотр всех банковских счетов и карт
- 📝 **Операции**: История всех транзакций с фильтрацией
- 📸 **QR-сканер**: Сканирование чеков и автоматическое создание операций

## Требования

- Flutter SDK 3.0+
- Android SDK 21+
- Dart 3.0+

## Установка

### 1. Клонирование репозитория

```bash
cd /home/onor/projects/zhamlik_flutter_app
```

### 2. Установка зависимостей

```bash
flutter pub get
```

### 3. Настройка API URL

Отредактируйте файл `lib/services/api_service.dart`:

```dart
static const String baseUrl = 'http://ВАШ_IP_АДРЕС:5001';
```

### 4. Запуск приложения

#### На устройстве/эмуляторе:

```bash
flutter run
```

#### Сборка APK:

```bash
flutter build apk --release
```

APK файл будет создан по пути:
`build/app/outputs/flutter-apk/app-release.apk`

### 5. Установка APK на Android устройство

#### Через USB:

```bash
flutter install
```

#### Передача файла:

Скопируйте файл `build/app/outputs/flutter-apk/app-release.apk` на устройство и установите.

## Структура проекта

```
lib/
├── main.dart                 # Точка входа приложения
├── models/                   # Модели данных
│   ├── account.dart
│   ├── transaction.dart
│   └── overview.dart
├── screens/                  # Экраны приложения
│   ├── splash_screen.dart
│   ├── login_screen.dart
│   ├── home_screen.dart
│   ├── transactions_screen.dart
│   ├── accounts_screen.dart
│   └── qr_scanner_screen.dart
└── services/                 # Сервисы
    ├── api_service.dart      # API клиент
    └── auth_service.dart     # Аутентификация
```

## API Endpoints

Приложение использует следующие эндпоинты бэкенда:

- `POST /api/mobile/auth/login` - Авторизация
- `GET /api/mobile/overview` - Обзор
- `GET /api/mobile/accounts` - Список счетов
- `GET /api/mobile/transactions` - Список операций
- `POST /api/mobile/parse-qr` - Парсинг QR-кода
- `POST /api/mobile/receipt-to-transaction` - Создание операции из чека

## Разработка

### Запуск с hot reload:

```bash
flutter run
```

### Проверка кода:

```bash
flutter analyze
```

### Запуск тестов:

```bash
flutter test
```

## Troubleshooting

### Ошибка "Flutter SDK not found"

Убедитесь, что Flutter установлен и добавлен в PATH:

```bash
export PATH="$PATH:/path/to/flutter/bin"
```

### Ошибка лицензий Android:

```bash
flutter doctor --android-licenses
```

### Проблемы с сетью

Убедитесь, что устройство может подключаться к серверу API. Для тестирования можно использовать эмулятор.

## Скриншоты

TODO: Добавить скриншоты приложения

## Лицензия

MIT License
