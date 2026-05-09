# Инструкция по сборке APK для обоих приложений

## Проблема

Не установлен пакет `unzip`, необходимый для извлечения Dart SDK из Flutter.

## Решение

### Вариант 1: Установить unzip (требуются sudo права)

```bash
sudo apt-get update
sudo apt-get install -y unzip curl git xz-utils zip libglu1-mesa openjdk-11-jdk

# Затем собрать оба приложения
```

### Вариант 2: Использовать Docker (без sudo)

Создать Dockerfile:

```dockerfile
FROM ubuntu:22.04

# Установка зависимостей
RUN apt-get update && apt-get install -y \
    curl git unzip xz-utils zip libglu1-mesa openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

# Клонирование Flutter
RUN git clone https://github.com/flutter/flutter.git -b stable --depth 1 /opt/flutter
ENV PATH="/opt/flutter/bin:${PATH}"

# Копирование проекта
WORKDIR /app

# Сборка Zhamlik Banking
COPY zhamlik_flutter_app/ .
RUN flutter pub get
RUN flutter build apk --release
RUN mkdir -p /output/banking
RUN cp build/app/outputs/flutter-apk/app-release.apk /output/banking/

# Сборка Zhamlik Crypto
WORKDIR /app2
COPY zhamlik_crypto_app/ .
RUN flutter pub get
RUN flutter build apk --release
RUN mkdir -p /output/crypto
RUN cp build/app/outputs/flutter-apk/app-release.apk /output/crypto/

CMD ["bash"]
```

Сборка:

```bash
cd /home/onor/projects
docker build -t zhamlik-build .
docker run --rm -v $(pwd)/apks:/output zhamlik-build
```

### Вариант 3: GitHub Actions

Создать репозиторий на GitHub и добавить файл `.github/workflows/build.yml`:

```yaml
name: Build APKs

on:
  push:
    branches: [ main ]
  workflow_dispatch:

jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        app: [banking, crypto]
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Setup Flutter
      uses: subosito/flutter-action@v2
      with:
        flutter-version: '3.16.0'
        channel: 'stable'
    
    - name: Build Banking App
      if: matrix.app == 'banking'
      run: |
        cd zhamlik_flutter_app
        flutter pub get
        flutter build apk --release
    
    - name: Build Crypto App
      if: matrix.app == 'crypto'
      run: |
        cd zhamlik_crypto_app
        flutter pub get
        flutter build apk --release
    
    - name: Upload APK
      uses: actions/upload-artifact@v3
      with:
        name: zhamlik-${{ matrix.app }}-apk
        path: |
          zhamlik_flutter_app/build/app/outputs/flutter-apk/app-release.apk
          zhamlik_crypto_app/build/app/outputs/flutter-apk/app-release.apk
```

### Вариант 4: Онлайн сервисы (бесплатно)

1. **Codemagic** - https://codemagic.io/
   - Бесплатно для open-source
   - Подключить GitHub репозиторий
   - Настроить сборку APK

2. **Appcircle** - https://appcircle.io/
   - Бесплатно для небольших проектов
   - Flutter поддерживается

3. **Bitrise** - https://bitrise.io/
   - Бесплатно для open-source

## Быстрая проверка

Проверьте, что у вас есть:

```bash
# Проверка Flutter
ls -la /home/onor/projects/flutter/bin/flutter

# Проверка проектов
ls -la /home/onor/projects/zhamlik_flutter_app/
ls -la /home/onor/projects/zhamlik_crypto_app/

# Проверка файла pubspec.yaml
cat /home/onor/projects/zhamlik_flutter_app/pubspec.yaml | head -5
cat /home/onor/projects/zhamlik_crypto_app/pubspec.yaml | head -5
```

## После установки зависимостей

Когда у вас будут необходимые пакеты:

### Сборка Zhamlik Banking:

```bash
cd /home/onor/projects/zhamlik_flutter_app
flutter pub get
flutter build apk --release
# APK будет здесь: build/app/outputs/flutter-apk/app-release.apk
```

### Сборка Zhamlik Crypto:

```bash
cd /home/onor/projects/zhamlik_crypto_app
flutter pub get
flutter build apk --release
# APK будет здесь: build/app/outputs/flutter-apk/app-release.apk
```

## Полученные APK файлы

После успешной сборки вы получите:

1. **Zhamlik Banking APK**
   - Файл: `zhamlik_flutter_app/build/app/outputs/flutter-apk/app-release.apk`
   - Размер: ~20-30 MB
   - Package: com.example.zhamlik

2. **Zhamlik Crypto APK**
   - Файл: `zhamlik_crypto_app/build/app/outputs/flutter-apk/app-release.apk`
   - Размер: ~15-25 MB
   - Package: com.example.zhamlik_crypto

## Установка на устройство

```bash
# Установка Zhamlik Banking
adb install /home/onor/projects/zhamlik_flutter_app/build/app/outputs/flutter-apk/app-release.apk

# Установка Zhamlik Crypto
adb install /home/onor/projects/zhamlik_crypto_app/build/app/outputs/flutter-apk/app-release.apk
```

## Текущий статус

✅ Flutter SDK скачан: `/home/onor/projects/flutter/`
✅ Zhamlik Banking проект готов: 27 файлов
✅ Zhamlik Crypto проект готов: 18 файлов
❌ Не установлен unzip для извлечения Dart SDK
❌ Нет sudo прав для установки пакетов

## Рекомендация

Используйте **GitHub Actions** или **Docker** для сборки без необходимости установки системных пакетов.
