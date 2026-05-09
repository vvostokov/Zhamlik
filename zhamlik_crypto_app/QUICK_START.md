# Zhamlik Crypto - Быстрый старт

## Что создано

✅ **Flutter приложение** для учета криптоинвестиций
- Обзор портфеля в реальном времени
- Управление крипто-биржами
- Список крипто-активов
- Material Design 3

✅ **API интеграция** с существующим бэкендом Zhamlik

## Как запустить

### 1. Запуск Flask API

```bash
cd /home/onor/projects/zhamlik
# API уже должно быть работающим на порту 5001
```

### 2. Установка Flutter (если еще не установлен)

```bash
# Flutter SDK уже скачан
export PATH="$PATH:/home/onor/projects/flutter/bin"
```

### 3. Сборка APK

```bash
cd /home/onor/projects/zhamlik_crypto_app

# Автоматическая сборка
./build.sh

# Или вручную
flutter pub get
flutter build apk --release
```

Готовый APK: `build/app/outputs/flutter-apk/app-release.apk`

## Установка на телефон

### Через USB:

```bash
adb install build/app/outputs/flutter-apk/app-release.apk
```

### Передача файла:

1. Скопируйте APK на телефон
2. Откройте файл и установите

## Настройка подключения к серверу

Адрес сервера: `lib/services/crypto_api_service.dart`

```dart
static const String baseUrl = 'http://193.29.224.20:5001';
```

## Функционал приложения

### 🏠 Главная страница
- Стоимость портфеля (USD, RUB, BTC)
- Изменение за 24ч, 7д, 30д
- Количество активов и бирж
- Топ активы портфеля

### 🏦 Биржи
- Список подключенных бирж
- Синхронизация балансов
- Добавление новых бирж с API ключами

### 💰 Активы
- Все крипто-активы
- Сортировка по стоимости/названию/изменению
- Текущая цена и изменение за 24ч

## API Эндпоинты

| Метод | Эндпоинт | Описание |
|-------|----------|----------|
| GET | `/api/mobile/crypto/overview` | Обзор портфеля |
| GET | `/api/mobile/crypto/platforms` | Список бирж |
| POST | `/api/mobile/crypto/platforms` | Добавить биржу |
| POST | `/api/mobile/crypto/platforms/<id>/sync` | Синхронизация |
| GET | `/api/mobile/crypto/assets` | Список активов |
| POST | `/api/mobile/crypto/assets` | Добавить актив |

## Поддерживаемые Android версии

- Минимум: Android 5.0 (API 21)
- Рекомендуется: Android 8.0+ (API 26+)

## Размер APK

Ожидаемый размер: **15-25 MB**

## Разрешения

- `INTERNET` - для связи с API

---

**Создано на основе Zhamlik** 🚀
