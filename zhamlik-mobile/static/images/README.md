# Иконки PWA

## Требуемые размеры

Для полноценной работы PWA нужны иконки следующих размеров:

### Regular icons
- 72x72
- 96x96
- 128x128
- 144x144
- 152x152
- 192x192
- 384x384
- 512x512

### Maskable icons (для Android adaptive icons)
- 192x192
- 512x512

## Как создать иконки

### Вариант 1: Онлайн генератор

1. Перейдите на https://realfavicongenerator.net/
2. Загрузите логотип (минимум 512x512)
3. Скачайте готовый набор иконок
4. Распакуйте в эту папку

### Вариант 2: Figma/Sketch/Photoshop

1. Создайте квадратный логотип
2. Экспортируйте в нужных размерах как PNG
3. Сохраните с названиями:
   - `icon-72x72.png`
   - `icon-96x96.png`
   - `icon-128x128.png`
   - `icon-144x144.png`
   - `icon-152x152.png`
   - `icon-192x192.png`
   - `icon-384x384.png`
   - `icon-512x512.png`
   - `icon-maskable-192x192.png`
   - `icon-maskable-512x512.png`

### Вариант 3: Использовать logo favicon

Пока нет своих иконок, можно использовать favicon с основного сайта.

## Временное решение

Для быстрого тестирования PWA можно использовать SVG иконку или создать простую иконку с текстом "Z".

Рекомендуемые цвета:
- Primary: #0d6efd (синий)
- Background: #ffffff (белый)

## Splash Screen

Размеры для splash screen:
- 640x1136 (iPhone SE)
- 750x1334 (iPhone 8)
- 1242x2208 (iPhone 8 Plus)
- 1125x2436 (iPhone X/XS/11 Pro)
- 828x1792 (iPhone XR/11)
- 1242x2688 (iPhone XS Max/11 Pro Max)
