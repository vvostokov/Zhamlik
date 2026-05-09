# Инструкция по установке и сборке Zhamlik Mobile App

## Автоматическая сборка (рекомендуется)

### Шаг 1: Установка зависимостей системы

Вам нужно установить следующие пакеты (требуются права sudo):

```bash
sudo apt-get update
sudo apt-get install -y \
    curl \
    git \
    unzip \
    xz-utils \
    zip \
    libglu1-mesa \
    openjdk-11-jdk \
    android-sdk
```

### Шаг 2: Установка Flutter SDK

```bash
# Клонируем Flutter
cd ~
git clone https://github.com/flutter/flutter.git -b stable --depth 1

# Добавляем в PATH (добавьте эту строку в ~/.bashrc)
export PATH="$PATH:$HOME/flutter/bin"
```

### Шаг 3: Настройка Android SDK

```bash
# Принимаем лицензии
flutter doctor --android-licenses

# Проверяем установку
flutter doctor
```

### Шаг 4: Сборка приложения

```bash
cd /home/onor/projects/zhamlik_flutter_app

# Делаем скрипт сборки исполняемым
chmod +x build.sh

# Запускаем сборку
./build.sh
```

APK файл будет создан в: `build/app/outputs/flutter-apk/app-release.apk`

## Ручная сборка

Если автоматический скрипт не работает, выполните следующие шаги:

### 1. Установка Flutter

```bash
cd ~
git clone https://github.com/flutter/flutter.git -b stable --depth 1
export PATH="$PATH:$HOME/flutter/bin"
```

### 2. Установка зависимостей проекта

```bash
cd /home/onor/projects/zhamlik_flutter_app
flutter pub get
```

### 3. Проверка окружения

```bash
flutter doctor
```

Исправьте все ошибки, указанные в выводе команды.

### 4. Сборка APK

```bash
# Release версия (для продакшена)
flutter build apk --release

# Или Debug версия (для тестирования)
flutter build apk --debug
```

## Установка на Android устройство

### Вариант 1: Через USB (ADB)

```bash
# Включите режим разработчика и USB-отладку на устройстве

# Проверьте подключение
adb devices

# Установите APK
adb install build/app/outputs/flutter-apk/app-release.apk
```

### Вариант 2: Копирование файла

1. Скопируйте файл `build/app/outputs/flutter-apk/app-release.apk` на устройство
2. Откройте файл менеджер на устройстве
3. Найдите скопированный APK файл
4. Нажмите на него для установки
5. Разрешите установку из неизвестных источников, если потребуется

## Настройка сервера API

Приложение должно подключаться к вашему Flask серверу. Отредактируйте файл:

`lib/services/api_service.dart`

Измените строку:

```dart
static const String baseUrl = 'http://193.29.224.20:5001';
```

на ваш IP адрес или домен.

## Тестирование

### Запуск на эмуляторе

```bash
# Запустите эмулятор
flutter emulators --launch <emulator_id>

# Запустите приложение
flutter run
```

### Запуск на подключенном устройстве

```bash
# Включите USB-отладку на устройстве
flutter devices

# Запустите приложение
flutter run
```

## Troubleshooting

### Ошибка "Flutter command not found"

Добавьте Flutter в PATH:

```bash
export PATH="$PATH:$HOME/flutter/bin"

# Для постоянного добавления:
echo 'export PATH="$PATH:$HOME/flutter/bin"' >> ~/.bashrc
source ~/.bashrc
```

### Ошибка лицензий Android

```bash
flutter doctor --android-licenses
# Нажимайте "y" для принятия всех лицензий
```

### Ошибка "SDK not found"

Создайте файл `local.properties`:

```bash
echo "sdk.dir=$HOME/Android/Sdk" > android/local.properties
```

### Ошибка при сборке APK

1. Проверьте версию Java:
```bash
java -version
```
Должна быть версия 11 или выше.

2. Очистите кэш Flutter:
```bash
flutter clean
flutter pub get
```

3. Попробуйте снова собрать:
```bash
flutter build apk --release
```

## Структура APK

После сборки вы получите файлы:

- `app-release.apk` - Основной APK файл (универсальный для всех архитектур)
- `app-armeabi-v7a-release.apk` - Для 32-битных ARM устройств
- `app-arm64-v8a-release.apk` - Для 64-битных ARM устройств
- `app-x86_64-release.apk` - For x86_64 devices (эмуляторы)

Для установки на реальное устройство используйте `app-release.apk`.

## Поддерживаемые версии Android

Минимальная версия: Android 5.0 (API 21)
Рекомендуемая версия: Android 8.0 (API 26) и выше

## Безопасность

⚠️ **Важно**: В текущей конфигурации приложение использует `usesCleartextTraffic="true"`, что позволяет подключаться к HTTP серверам.

Для продакшена:
1. Настройте HTTPS на сервере
2. Уберите `usesCleartextTraffic` из AndroidManifest.xml
3. Используйте certificate pinning для дополнительной безопасности

## Контакты

По вопросам и проблемам создавайте issues в репозитории проекта.
