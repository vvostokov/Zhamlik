# Zhamlik Crypto - Crypto Portfolio Tracker

Мобильное приложение для отслеживания криптоинвестиций на основе Flutter.

## Возможности

- 💰 **Портфель криптовалют**: Общая стоимость портфеля в USD, RUB и BTC
- 📊 **Изменение стоимости**: Изменение за 24ч, 7д, 30д
- 🏦 **Крипто-биржи**: Подключение бирж через API ключи
- 🔄 **Синхронизация**: Автоматическая синхронизация балансов
- 📈 **Топ активы**: Самые крупные активы портфеля
- 🎨 **Material Design 3**: Современный интерфейс

## Требования

- Flutter SDK 3.0+
- Android SDK 21+
- Dart 3.0+

## Установка

### 1. Клонирование и установка зависимостей

```bash
cd /home/onor/projects/zhamlik_crypto_app

# Установка зависимостей Flutter
flutter pub get
```

### 2. Настройка API URL

Отредактируйте файл `lib/services/crypto_api_service.dart`:

```dart
static const String baseUrl = 'http://ВАШ_IP_АДРЕС:5001';
```

### 3. Запуск приложения

#### На устройстве/эмуляторе:

```bash
flutter run
```

#### Сборка APK:

```bash
./build.sh

# Или вручную:
flutter build apk --release
```

APK файл будет создан по пути:
`build/app/outputs/flutter-apk/app-release.apk`

### 4. Установка APK на Android устройство

#### Через USB:

```bash
flutter install
```

#### Передача файла:

Скопируйте файл `build/app/outputs/flutter-apk/app-release.apk` на устройство и установите.

## Структура проекта

```
lib/
├── main.dart                      # Точка входа приложения
├── models/                        # Модели данных
│   ├── crypto_asset.dart          # Крипто-актив
│   ├── crypto_platform.dart       # Крипто-биржа
│   └── crypto_overview.dart       # Обзор портфеля
├── screens/                       # Экраны приложения
│   ├── splash_screen.dart         # Загрузочный экран
│   ├── home_screen.dart           # Главная страница
│   ├── platforms_screen.dart      # Список бирж
│   └── assets_screen.dart         # Список активов
└── services/
    └── crypto_api_service.dart    # API клиент
```

## API Endpoints

Приложение использует следующие эндпоинты бэкенда:

- `GET /api/mobile/crypto/overview` - Обзор портфеля
- `GET /api/mobile/crypto/platforms` - Список бирж
- `POST /api/mobile/crypto/platforms` - Добавление биржи
- `POST /api/mobile/crypto/platforms/<id>/sync` - Синхронизация биржи
- `GET /api/mobile/crypto/platforms/<id>` - Детали биржи
- `GET /api/mobile/crypto/assets` - Список активов
- `POST /api/mobile/crypto/assets` - Добавление актива

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

## Скриншоты

TODO: Добавить скриншоты приложения

## Поддерживаемые биржи

- Binance (через API)
- Любые другие через ручное добавление активов

## Лицензия

MIT License
