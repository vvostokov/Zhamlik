#!/bin/bash

# Скрипт для проверки и сборки Zhamlik Flutter приложения

set -e

echo "=========================================="
echo "Zhamlik Flutter App - Build & Check"
echo "=========================================="
echo ""

# Проверка Flutter
FLUTTER_PATH="$PWD/../flutter"
if [ ! -d "$FLUTTER_PATH" ]; then
    FLUTTER_PATH="$HOME/flutter"
fi

if [ ! -d "$FLUTTER_PATH" ]; then
    echo "❌ Flutter SDK не найден!"
    echo ""
    echo "Для сборки APK необходимо установить Flutter:"
    echo ""
    echo "1. Скачать Flutter:"
    echo "   git clone https://github.com/flutter/flutter.git -b stable --depth 1 ~/flutter"
    echo ""
    echo "2. Добавить в PATH:"
    echo "   export PATH=\"\$PATH:\$HOME/flutter/bin\""
    echo ""
    echo "3. Установить зависимости:"
    echo "   sudo apt-get install -y unzip curl git xz-utils zip libglu1-mesa openjdk-11-jdk"
    echo ""
    echo "4. Принять лицензии:"
    echo "   flutter doctor --android-licenses"
    echo ""
    echo "Затем запустите этот скрипт снова."
    exit 1
fi

export PATH="$FLUTTER_PATH/bin:$PATH"

echo "✅ Flutter SDK найден: $FLUTTER_PATH"
echo ""
echo "📱 Flutter версия:"
flutter --version 2>&1 | head -3
echo ""

# Проверка проекта
echo "📂 Проверка проекта..."
cd "$(dirname "$0")"

if [ ! -f "pubspec.yaml" ]; then
    echo "❌ pubspec.yaml не найден! Это не Flutter проект."
    exit 1
fi

echo "✅ Проект найден"
echo ""

# Подсчет файлов
DART_FILES=$(find lib -name "*.dart" 2>/dev/null | wc -l)
echo "📝 Dart файлов: $DART_FILES"
echo ""

# Проверка синтаксиса
echo "🔍 Проверка синтаксиса Dart..."
if ! command -v dart &> /dev/null; then
    echo "⚠️  Dart analyzer не найден, пропускаем проверку синтаксиса"
else
    flutter analyze --no-pub || echo "⚠️  Есть предупреждения, но продолжаем"
fi
echo ""

# Установка зависимостей
echo "📦 Установка зависимостей..."
flutter pub get
echo ""

# Проверка структуры проекта
echo "📋 Структура проекта:"
echo "  Models: $(find lib/models -name "*.dart" 2>/dev/null | wc -l) файлов"
echo "  Screens: $(find lib/screens -name "*.dart" 2>/dev/null | wc -l) файлов"
echo "  Services: $(find lib/services -name "*.dart" 2>/dev/null | wc -l) файлов"
echo ""

# Проверка Android конфигурации
echo "🤖 Проверка Android конфигурации..."
if [ -f "android/app/build.gradle" ]; then
    echo "✅ build.gradle найден"
else
    echo "❌ build.gradle не найден"
    exit 1
fi

if [ -f "android/app/src/main/AndroidManifest.xml" ]; then
    echo "✅ AndroidManifest.xml найден"
else
    echo "❌ AndroidManifest.xml не найден"
    exit 1
fi
echo ""

# Попытка сборки
echo "🔨 Попытка сборки APK..."
echo ""

# Проверяем, установлены ли Android SDK
if ! flutter doctor --verbose 2>&1 | grep -q "Android SDK"; then
    echo "⚠️  Android SDK не настроен полностью"
    echo ""
    echo "Для сборки APK нужны:"
    echo "1. Android SDK"
    echo "2. Android licenses приняты (flutter doctor --android-licenses)"
    echo ""
    echo "Текущий статус:"
    flutter doctor
    echo ""
    echo "Но можно попробовать собрать в любом случае..."
fi

# Сборка APK (если возможно)
if flutter build apk --release 2>&1; then
    echo ""
    echo "=========================================="
    echo "✅ Сборка завершена успешно!"
    echo "=========================================="
    echo ""
    echo "📂 APK файл создан:"
    echo "   $PWD/build/app/outputs/flutter-apk/app-release.apk"
    echo ""

    # Показываем размер файла
    if [ -f "build/app/outputs/flutter-apk/app-release.apk" ]; then
        APK_SIZE=$(du -h build/app/outputs/flutter-apk/app-release.apk | cut -f1)
        echo "📊 Размер APK: $APK_SIZE"
        echo ""

        # Информация об APK
        echo "📱 Для установки на устройство:"
        echo "   adb install build/app/outputs/flutter-apk/app-release.apk"
        echo ""
        echo "   Или скопируйте файл на устройство и откройте его"
    fi

    # Список всех созданных APK
    echo ""
    echo "📦 Все APK файлы:"
    ls -lh build/app/outputs/flutter-apk/*.apk 2>/dev/null || echo "   APK файлы не найдены"

    exit 0
else
    echo ""
    echo "=========================================="
    echo "❌ Сборка не удалась"
    echo "=========================================="
    echo ""
    echo "Вероятные причины:"
    echo "1. Не установлен Android SDK"
    echo "2. Не приняты лицензии Android"
    echo "3. Не настроены переменные окружения"
    echo ""
    echo "Решение:"
    echo "1. Установите Android SDK:"
    echo "   sudo apt-get install -y openjdk-11-jdk"
    echo ""
    echo "2. Примите лицензии:"
    echo "   flutter doctor --android-licenses"
    echo ""
    echo "3. Проверьте окружение:"
    echo "   flutter doctor"
    echo "   flutter doctor --verbose"
    echo ""
    echo "4. Повторите сборку:"
    echo "   flutter build apk --release"
    echo ""
    echo "Или используйте готовый Docker контейнер/CI для сборки"
    exit 1
fi
