# Zhamlik Mobile App - Быстрый старт

## Что уже создано

✅ **Flask API** (`/home/onor/projects/zhamlik/mobile_api.py`)
- Полноценное RESTful API для мобильного приложения
- Эндпоинты для авторизации, счетов, транзакций, QR-сканирования
- Интегрировано с основным приложением Zhamlik

✅ **Flutter приложение** (`/home/onor/projects/zhamlik_flutter_app/`)
- Полностью функциональное мобильное приложение
- Экраны: вход, главная, операции, счета, QR-сканер
- Материал дизайн 3, темная тема

## Как запустить

### 1. Запуск Flask API

```bash
cd /home/onor/projects/zhamlik
# Убедитесь, что сервер запущен на порту 5001
python app.py
```

Или используйте ваш существующий сервер на `193.29.224.20:5001`

### 2. Установка Flutter (если еще не установлен)

```bash
# Клонирование Flutter (однократно)
cd ~
git clone https://github.com/flutter/flutter.git -b stable --depth 1

# Добавление в PATH
export PATH="$PATH:$HOME/flutter/bin"
echo 'export PATH="$PATH:$HOME/flutter/bin"' >> ~/.bashrc
```

### 3. Установка зависимостей Android

```bash
sudo apt-get update
sudo apt-get install -y unzip curl git xz-utils zip libglu1-mesa openjdk-11-jdk
```

### 4. Сборка APK

```bash
cd /home/onor/projects/zhamlik_flutter_app

# Автоматическая сборка
./build.sh

# Или вручную
flutter pub get
flutter build apk --release
```

Готовый APK будет в: `build/app/outputs/flutter-apk/app-release.apk`

## Установка на телефон

### Вариант 1: Через USB

```bash
adb install build/app/outputs/flutter-apk/app-release.apk
```

### Вариант 2: Передача файла

1. Скопируйте `build/app/outputs/flutter-apk/app-release.apk` на телефон
2. Откройте файл и установите

## Настройка подключения к серверу

По умолчанию приложение подключается к: `http://193.29.224.20:5001`

Чтобы изменить:

1. Откройте `lib/services/api_service.dart`
2. Измените строку:
   ```dart
   static const String baseUrl = 'http://ВАШ_IP:5001';
   ```
3. Пересоберите APK

## Функционал приложения

### 🏠 Главная страница
- Общий баланс по всем счетам
- Доходы и расходы за месяц
- Последние операции
- Быстрые действия

### 💳 Счета
- Список всех банковских счетов и карт
- Балансы в разных валютах
- Общая сумма

### 📝 Операции
- История всех транзакций
- Фильтрация по типу (доходы/расходы/переводы)
- Детали операций с товарами

### 📸 QR-сканер
- Сканирование QR-кодов с чеков
- Автоматическое распознавание
- Создание операции из чека

## API Эндпоинты

| Метод | Эндпоинт | Описание |
|-------|----------|----------|
| POST | `/api/mobile/auth/login` | Авторизация |
| GET | `/api/mobile/overview` | Обзор |
| GET | `/api/mobile/accounts` | Список счетов |
| GET | `/api/mobile/transactions` | Операции |
| POST | `/api/mobile/parse-qr` | Парсинг QR |
| POST | `/api/mobile/receipt-to-transaction` | Чек в операцию |

## Структура проекта

```
zhamlik_flutter_app/
├── lib/
│   ├── main.dart              # Точка входа
│   ├── models/                # Модели данных
│   │   ├── account.dart
│   │   ├── transaction.dart
│   │   └── overview.dart
│   ├── screens/               # Экраны
│   │   ├── splash_screen.dart
│   │   ├── login_screen.dart
│   │   ├── home_screen.dart
│   │   ├── transactions_screen.dart
│   │   ├── accounts_screen.dart
│   │   └── qr_scanner_screen.dart
│   └── services/              # API сервисы
│       ├── api_service.dart
│       └── auth_service.dart
├── android/                   # Android конфигурация
├── pubspec.yaml              # Зависимости
├── build.sh                  # Скрипт сборки
├── README.md                 # Документация
└── INSTALL.md                # Инструкция по установке
```

## Поддерживаемые Android версии

- Минимум: Android 5.0 (API 21)
- Рекомендуется: Android 8.0+ (API 26+)

## Возможные проблемы

### "Flutter command not found"
```bash
export PATH="$PATH:$HOME/flutter/bin"
```

### Ошибки лицензий
```bash
flutter doctor --android-licenses
```

### Очистка кэша
```bash
flutter clean
flutter pub get
```

## Следующие шаги

1. ✅ API готово
2. ✅ Flutter приложение создано
3. ⏳ Установите Flutter SDK
4. ⏳ Соберите APK
5. ⏳ Установите на устройство
6. ✅ Наслаждайтесь!

## Помощь

Полная документация: `INSTALL.md`
Вопросы по API: `mobile_api.py`

---

**Создано с помощью Claude Code** 🤖
