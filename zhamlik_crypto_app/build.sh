#!/bin/bash

# Скрипт для сборки APK Zhamlik Crypto приложения

set -e

echo "========================================="
echo "Zhamlik Crypto - APK Build Script"
echo "========================================="
echo ""

# Проверка наличия Flutter
FLUTTER_PATH="$PWD/../flutter"
if [ ! -d "$FLUTTER_PATH" ]; then
    FLUTTER_PATH="$HOME/flutter"
fi

if [ ! -d "$FLUTTER_PATH" ]; then
    echo "❌ Flutter SDK не найден!"
    echo "Пожалуйста, скачайте Flutter SDK:"
    echo "  git clone https://github.com/flutter/flutter.git -b stable --depth 1 ~/flutter"
    exit 1
fi

export PATH="$FLUTTER_PATH/bin:$PATH"

echo "📱 Flutter версия:"
flutter --version
echo ""

# Переход в директорию проекта
cd "$(dirname "$0")"

# Установка зависимостей
echo "📦 Установка зависимостей..."
flutter pub get
echo ""

# Проверка окружения
echo "🔍 Проверка окружения..."
flutter doctor
echo ""

# Сборка APK
echo "🔨 Сборка APK..."
echo ""

flutter build apk --release

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Сборка завершена успешно!"
    echo ""
    echo "📂 APK файл находится:"
    echo "   $PWD/build/app/outputs/flutter-apk/app-release.apk"
    echo ""
    echo "📱 Для установки на устройство:"
    echo "   adb install build/app/outputs/flutter-apk/app-release.apk"
    echo ""

    # Показываем размер файла
    APK_SIZE=$(du -h build/app/outputs/flutter-apk/app-release.apk | cut -f1)
    echo "📊 Размер APK: $APK_SIZE"
else
    echo ""
    echo "❌ Ошибка сборки!"
    exit 1
fi
