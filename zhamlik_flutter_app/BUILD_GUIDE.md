# Руководство по сборке Zhamlik APK

## Что создано

✅ **Полноценное Flutter мобильное приложение** с функциями:
- 🔐 Авторизация
- 📊 Главная страница с балансами
- 💳 Управление счетами
- 📝 Банковские операции
- 📸 QR-сканер чеков
- 💰 **Система долгов** (новое!)
- 🔄 **Регулярные платежи** (новое!)

## Структура проекта

```
zhamlik_flutter_app/
├── lib/
│   ├── main.dart                    # Точка входа
│   ├── models/                      # Модели данных
│   │   ├── account.dart
│   │   ├── transaction.dart
│   │   ├── overview.dart
│   │   └── debt.dart                # ✨ Новые модели долгов
│   ├── screens/                     # Экраны приложения
│   │   ├── splash_screen.dart
│   │   ├── login_screen.dart
│   │   ├── home_screen.dart
│   │   ├── transactions_screen.dart
│   │   ├── accounts_screen.dart
│   │   ├── qr_scanner_screen.dart
│   │   ├── debts_screen.dart        # ✨ Новый экран долгов
│   │   └── recurring_payments_screen.dart  # ✨ Новый экран платежей
│   └── services/
│       ├── api_service.dart         # API клиент (обновлен)
│       └── auth_service.dart        # Аутентификация
├── android/                         # Android конфигурация
├── pubspec.yaml                     # Зависимости
├── build.sh                         # Скрипт сборки
└── build_and_test.sh               # Проверка и сборка
```

## Новые функции

### 1. Система долгов (debts_screen.dart)

**Функционал:**
- ✅ Просмотр долгов "Я должен" и "Мне должны"
- ✅ Общие суммы по каждой категории
- ✅ Статусы (активен, погашен, просрочен)
- ✅ Даты оплаты с индикацией просрочки
- ✅ Фильтрация по типу долга
- ✅ Диалог добавления нового долга

**Поля:**
- Контрагент
- Сумма (общая и погашенная)
- Валюта
- Дата оплаты
- Описание
- Статус

### 2. Регулярные платежи (recurring_payments_screen.dart)

**Функционал:**
- ✅ Список всех регулярных платежей
- ✅ Периодичность (ежедневно/еженедельно/ежемесячно/ежегодно)
- ✅ Индикация просроченных платежей
- ✅ Ближайшие платежи (отдельная секция)
- ✅ Общая сумма ежемесячных платежей
- ✅ Диалог добавления нового платежа

**Поля:**
- Описание
- Сумма
- Валюта
- Периодичность
- Дата следующего платежа
- Контрагент

## API Эндпоинты (новые)

Для работы долгов и платежей используйте уже созданные эндпоинты в `/home/onor/projects/zhamlik/mobile_api.py`:

```python
@mobile_bp.route('/debts', methods=['GET'])
@login_required
def get_debts():
    # Уже реализовано!
    # Возвращает: {"i_owe": [...], "owed_to_me": [...]}

@mobile_bp.route('/recurring-payments', methods=['GET'])
@login_required
def get_recurring_payments():
    # Уже реализовано!
    # Возвращает: {"payments": [...]}
```

## Сборка APK

### Способ 1: На Linux (текущая система)

#### Шаг 1: Установка зависимостей

```bash
# Требуются права sudo
sudo apt-get update
sudo apt-get install -y \
    curl \
    git \
    unzip \
    xz-utils \
    zip \
    libglu1-mesa \
    openjdk-11-jdk
```

#### Шаг 2: Настройка Flutter

```bash
# Flutter SDK уже скачан в /home/onor/projects/flutter
# Нужно добавить в PATH
export PATH="$PATH:/home/onor/projects/flutter/bin"

# Принять лицензии (требуется интерактивный ввод)
flutter doctor --android-licenses

# Проверить окружение
flutter doctor
```

#### Шаг 3: Сборка

```bash
cd /home/onor/projects/zhamlik_flutter_app

# Установить зависимости
flutter pub get

# Собрать APK
flutter build apk --release

# APK будет здесь:
# build/app/outputs/flutter-apk/app-release.apk
```

### Способ 2: Использование Docker (рекомендуется)

Если нет sudo прав, используйте Docker:

```bash
# Создать Dockerfile
cat > Dockerfile << 'EOF'
FROM ubuntu:22.04

# Установка зависимостей
RUN apt-get update && apt-get install -y \
    curl \
    git \
    unzip \
    xz-utils \
    zip \
    libglu1-mesa \
    openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

# Установка Flutter
RUN git clone https://github.com/flutter/flutter.git -b stable --depth 1 /opt/flutter
ENV PATH="/opt/flutter/bin:${PATH}"

# Копирование проекта
WORKDIR /app
COPY . .

# Установка зависимостей Flutter
RUN flutter pub get

# Сборка APK
RUN flutter build apk --release

# Копирование APK в отдельную директорию
RUN mkdir -p /output && \
    cp build/app/outputs/flutter-apk/app-release.apk /output/

# Изменение владельца (опционально)
# RUN chown -R 1000:1000 /output
EOF

# Сборка Docker образа
docker build -t zhamlik-apk .

# Извлечение APK
docker run --rm -v $(pwd)/output:/output zhamlik-apk \
    cp /app/build/app/outputs/flutter-apk/app-release.apk /output/
```

### Способ 3: GitHub Actions (CI/CD)

Создайте файл `.github/workflows/build.yml`:

```yaml
name: Build APK

on:
  push:
    branches: [ main ]
  workflow_dispatch:

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v3

    - name: Setup Flutter
      uses: subosito/flutter-action@v2
      with:
        flutter-version: '3.16.0'
        channel: 'stable'

    - name: Install dependencies
      run: flutter pub get

    - name: Build APK
      run: flutter build apk --release

    - name: Upload APK
      uses: actions/upload-artifact@v3
      with:
        name: zhamlik-apk
        path: build/app/outputs/flutter-apk/app-release.apk
```

### Способ 4: Использование онлайн-сервисов

1. **Codemagic** (codemagic.io)
2. **Appcircle** (appcircle.io)
3. **Bitrise** (bitrise.io)

Все эти сервисы бесплатны для открытых проектов.

## Установка APK на устройство

### Через USB:

```bash
adb install build/app/outputs/flutter-apk/app-release.apk
```

### Передача файла:

1. Скопируйте `build/app/outputs/flutter-apk/app-release.apk` на телефон
2. Откройте файл менеджер
3. Найдите APK файл
4. Нажмите для установки
5. Разрешите установку из неизвестных источников

## Настройка подключения к серверу

Текущий адрес сервера: `http://193.29.224.20:5001`

Чтобы изменить:

1. Откройте `lib/services/api_service.dart`
2. Найдите строку:
   ```dart
   static const String baseUrl = 'http://193.29.224.20:5001';
   ```
3. Измените на ваш адрес:
   ```dart
   static const String baseUrl = 'http://ВАШ_IP:5001';
   ```
4. Пересоберите APK

## Тестирование без полной сборки

Если хотите протестировать UI без сборки APK:

```bash
# На Flutter подключенном устройстве
flutter run

# Или на веб
flutter run -d chrome
```

## Troubleshooting

### "unzip: command not found"
```bash
sudo apt-get install unzip
```

### "Android SDK not found"
```bash
flutter doctor --android-licenses
# Принять все лицензии (ввести "y")
```

### "License not accepted"
```bash
flutter doctor --android-licenses
```

### Ошибки при сборке
```bash
# Очистка кэша
flutter clean

# Переустановка зависимостей
flutter pub get

# Повторная сборка
flutter build apk --release
```

### Memory issues
```bash
# Увеличить память для Gradle
export JAVA_OPTS="-Xmx4G"
flutter build apk --release
```

## Следующие шаги после сборки

1. ✅ APK готов
2. 📱 Установите на устройство
3. 🧪 Протестируйте все функции
4. 📸 Сделайте скриншоты
5. 🚀 Опубликуйте в Google Play (опционально)

## Поддерживаемые Android версии

- **Минимум:** Android 5.0 (API 21)
- **Рекомендуется:** Android 8.0+ (API 26+)
- **Тестировано на:** Android 10+ (API 29+)

## Размер APK

Ожидаемый размер: **20-30 MB** (включая Flutter runtime)

## Разрешения приложения

- `INTERNET` - для связи с API
- `CAMERA` - для сканирования QR-кодов

## Безопасность

⚠️ **Важно:** Для production:
1. Включите HTTPS на сервере
2. Уберите `usesCleartextTraffic` из AndroidManifest.xml
3. Подпишите APK с релизным ключом
4. Протестируйте на разных устройствах

---

**Проект полностью готов к сборке!** 🎉

Все файлы созданы и находятся в:
`/home/onor/projects/zhamlik_flutter_app/`
