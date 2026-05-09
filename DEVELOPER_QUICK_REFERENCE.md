# Developer Quick Reference - Zhamlik Apps

Дата: 30 января 2026

---

## 🚀 Быстрый старт

### Как использовать новые функции V2

---

## 🌗 Dark Mode (Темная тема)

### Включить тему в приложении:

```dart
// 1. Импортировать ThemeService
import 'package:your_app/services/theme_service.dart';

// 2. Получить экземпляр
final themeService = Provider.of<ThemeService>(context);

// 3. Использовать
bool isDark = themeService.isDarkMode(context);
themeService.toggleTheme(context);
themeService.setThemeMode(ThemeMode.dark);
```

### Добавить кнопку переключения темы:

```dart
IconButton(
  icon: Icon(
    Provider.of<ThemeService>(context).isDarkMode(context)
        ? Icons.light_mode
        : Icons.dark_mode,
  ),
  onPressed: () {
    Provider.of<ThemeService>(context, listen: false)
        .toggleTheme(context);
  },
)
```

### Перейти к настройкам:

```dart
Navigator.of(context).pushNamed('/settings');
```

---

## 🔔 Notifications (Уведомления)

### Импортировать:

```dart
import 'package:your_app/widgets/notification_widgets.dart';
```

### Быстрые уведомления:

```dart
// Успех
NotificationManager.showSuccess(
  context,
  title: 'Сохранено!',
  message: 'Данные успешно сохранены',
);

// Ошибка
NotificationManager.showError(
  context,
  title: 'Ошибка',
  message: 'Что-то пошло не так',
);

// Предупреждение
NotificationManager.showWarning(
  context,
  title: 'Внимание',
  message: 'Проверьте данные',
);

// Информация
NotificationManager.showInfo(
  context,
  title: 'Информация',
  message: 'Новое сообщение',
);
```

### С расширенными опциями:

```dart
NotificationManager.show(
  context,
  title: 'Кастомное уведомление',
  message: 'Описание',
  type: NotificationType.success,
  duration: Duration(seconds: 5),
  onTap: () {
    print('Нажато!');
  },
);
```

### Enhanced SnackBar:

```dart
ScaffoldMessenger.of(context).showSnackBar(
  EnhancedSnackBar(
    message: 'Сообщение',
    actionLabel: 'Отменить',
    onActionPressed: () {},
    type: NotificationType.info,
  ),
);
```

### Loading Dialog:

```dart
// Показать
LoadingDialog.show(context, message: 'Загрузка...');

// Скрыть
LoadingDialog.hide(context);
```

### Полный пример с ошибкой:

```dart
Future<void> _saveData() async {
  LoadingDialog.show(context, message: 'Сохранение...');

  try {
    await apiService.saveData(data);
    LoadingDialog.hide(context);

    NotificationManager.showSuccess(
      context,
      title: 'Успешно!',
      message: 'Данные сохранены',
    );
  } catch (e) {
    LoadingDialog.hide(context);

    NotificationManager.showError(
      context,
      title: 'Ошибка',
      message: e.toString(),
      duration: Duration(seconds: 5),
    );
  }
}
```

---

## 🎭 Page Transitions (Анимации переходов)

### Импортировать:

```dart
import 'package:your_app/widgets/page_transitions.dart';
```

### Использовать кастомные маршруты:

```dart
// Shared Axis (горизонтальный переход)
Navigator.of(context).push(
  SharedAxisPageRoute(child: NextScreen()),
);

// Fade Through (затухание)
Navigator.of(context).push(
  FadeThroughPageRoute(child: NextScreen()),
);

// Fade Scale (масштабирование)
Navigator.of(context).push(
  FadeScalePageRoute(child: NextScreen()),
);

// Slide (выезд)
Navigator.of(context).push(
  SlidePageRoute(child: NextScreen()),
);

// Scale (увеличение)
Navigator.of(context).push(
  ScalePageRoute(child: NextScreen()),
);
```

### Использовать NavigationHelper:

```dart
NavigationHelper.navigateSharedAxis(context, '/route');
NavigationHelper.navigateFadeThrough(context, '/route');
NavigationHelper.navigateFadeScale(context, '/route');
NavigationHelper.navigateSlide(context, '/route');
NavigationHelper.navigateScale(context, '/route');
```

---

## 📊 Charts (Графики)

### Monthly Spending Chart:

```dart
import 'package:your_app/widgets/monthly_spending_chart.dart';

MonthlySpendingChart(
  monthlyData: {
    'Янв': 50000.0,
    'Фев': 65000.0,
    'Мар': 45000.0,
    'Апр': 70000.0,
    'Май': 55000.0,
    'Июн': 60000.0,
  },
  title: 'Расходы по месяцам',
)
```

### Analytics Chart (Pie chart):

```dart
import 'package:your_app/widgets/analytics_chart.dart';

AnalyticsChartWidget(
  categoryData: {
    'Еда': 15000.0,
    'Транспорт': 5000.0,
    'Развлечения': 10000.0,
  },
  title: 'Расходы по категориям',
  totalAmount: 30000.0,
)
```

### Crypto Portfolio Chart:

```dart
import 'package:your_app/widgets/crypto_portfolio_chart.dart';

CryptoPortfolioChartWidget(
  holdings: {
    'BTC': 25000.0,
    'ETH': 10000.0,
    'USDT': 5000.0,
  },
  totalValue: 40000.0,
)
```

### Crypto Price Chart:

```dart
import 'package:your_app/widgets/crypto_price_chart.dart';

CryptoPriceChartWidget(
  prices: [45000, 46000, 45500, 47000, 46500],
  labels: ['1ч', '2ч', '3ч', '4ч', '5ч'],
  title: 'Цена BTC',
)
```

---

## 🎨 Стандартные UI компоненты

### Animated Stat Card:

```dart
import 'package:your_app/widgets/animated_stat_card.dart';

AnimatedStatCard(
  title: 'Баланс',
  value: '100 000 ₽',
  icon: Icons.account_balance_wallet,
  color: Colors.blue,
  change: '+5.2%',
  isPositive: true,
  onTap: () {},
)
```

### Quick Action Grid:

```dart
import 'package:your_app/widgets/quick_action_grid.dart';

QuickActionGrid(
  actions: [
    QuickAction(
      icon: Icons.send,
      label: 'Перевод',
      onTap: () {},
    ),
    // ... еще 5 действий
  ],
)
```

### Crypto Asset Card:

```dart
import 'package:your_app/widgets/crypto_asset_card.dart';

CryptoAssetCard(
  ticker: 'BTC',
  name: 'Bitcoin',
  quantity: 0.5,
  valueUsd: 22500.0,
  priceUsd: 45000.0,
  change24h: 5.2,
  onTap: () {},
)
```

---

## 📱 Экраны

### Навигация к экранам:

```dart
// Zhamlik Banking
Navigator.of(context).pushNamed('/home');
Navigator.of(context).pushNamed('/transactions');
Navigator.of(context).pushNamed('/accounts');
Navigator.of(context).pushNamed('/qr-scanner');
Navigator.of(context).pushNamed('/debts');
Navigator.of(context).pushNamed('/recurring-payments');
Navigator.of(context).pushNamed('/settings');

// Zhamlik Crypto
Navigator.of(context).pushNamed('/home');
Navigator.of(context).pushNamed('/platforms');
Navigator.of(context).pushNamed('/assets');
Navigator.of(context).pushNamed('/settings');
```

### С анимацией:

```dart
// Вместо pushNamed использовать
Navigator.of(context).push(
  SharedAxisPageRoute(
    child: SettingsScreen(),
  ),
);
```

---

## 🔧 Services

### ApiService (Banking):

```dart
final apiService = Provider.of<ApiService>(context);

// Получить данные
final overview = await apiService.getOverview();
final accounts = await apiService.getAccounts();
final transactions = await apiService.getTransactions();

// Создать
await apiService.createTransaction(transaction);

// QR
final receipt = await apiService.parseQR(imageData);

// Другое
final debts = await apiService.getDebts();
final recurring = await apiService.getRecurringPayments();
```

### CryptoApiService (Crypto):

```dart
final apiService = Provider.of<CryptoApiService>(context);

// Обзор
final overview = await apiService.getOverview();

// Платформы
final platforms = await apiService.getPlatforms();
await apiService.createPlatform(platform);
await apiService.syncPlatform(platformId);

// Активы
final assets = await apiService.getAssets();
await apiService.createAsset(asset);
```

### AuthService:

```dart
final authService = Provider.of<AuthService>(context);

// Вход
await authService.login(username, password);

// Выход
await authService.logout();

// Проверка
bool isAuthenticated = authService.isAuthenticated;
User? user = authService.user;
```

### ThemeService:

```dart
final themeService = Provider.of<ThemeService>(context);

// Текущая тема
ThemeMode mode = themeService.themeMode;
bool isDark = themeService.isDarkMode(context);

// Изменить
themeService.toggleTheme(context);
themeService.setThemeMode(ThemeMode.dark);
themeService.setThemeMode(ThemeMode.light);
themeService.setThemeMode(ThemeMode.system);
```

---

## 🎨 Темы и цвета

### Zhamlik Banking:

```dart
// Светлая тема
primary: Color(0xFF2196F3)    // Blue
secondary: Color(0xFF03DAC6)  // Teal
background: Color(0xFFF5F5F5) // Light Gray
surface: Colors.white

// Темная тема
primary: Color(0xFF64B5F6)    // Light Blue
secondary: Color(0xFF03DAC6)  // Teal
background: Color(0xFF121212) // Very Dark
surface: Color(0xFF2C2C2C)    // Dark Gray
```

### Zhamlik Crypto:

```dart
// Светлая тема
primary: Color(0xFFFF9800)    // Orange
secondary: Color(0xFFFF6F00)  // Dark Orange
background: Color(0xFFF5F5F5) // Light Gray
surface: Colors.white

// Темная тема
primary: Color(0xFFFFB74D)    // Light Orange
secondary: Color(0xFFFF6F00)  // Dark Orange
background: Color(0xFF121212) // Very Dark
surface: Color(0xFF2C2C2C)    // Dark Gray
```

### Использовать цвета темы:

```dart
// Получить colorScheme
final colorScheme = Theme.of(context).colorScheme;

// Использовать
Container(
  color: colorScheme.primary,
)

// Или
color: Theme.of(context).colorScheme.primary,
```

---

## 📦 Полные импорты для типового файла

```dart
import 'package:flutter/material.dart';
import 'package:provider/provider.dart';

// Services
import 'package:your_app/services/api_service.dart';
import 'package:your_app/services/auth_service.dart';
import 'package:your_app/services/theme_service.dart';

// Screens
import 'package:your_app/screens/home_screen.dart';
import 'package:your_app/screens/settings_screen.dart';

// Widgets
import 'package:your_app/widgets/notification_widgets.dart';
import 'package:your_app/widgets/page_transitions.dart';
import 'package:your_app/widgets/monthly_spending_chart.dart';
```

---

## 🎯 Типовой StatefulWidget с уведомлениями

```dart
class MyScreen extends StatefulWidget {
  const MyScreen({super.key});

  @override
  State<MyScreen> createState() => _MyScreenState();
}

class _MyScreenState extends State<MyScreen> {
  bool _isLoading = false;

  Future<void> _doSomething() async {
    setState(() {
      _isLoading = true;
    });

    LoadingDialog.show(context, message: 'Выполняем...');

    try {
      final apiService = Provider.of<ApiService>(context, listen: false);
      await apiService.someOperation();

      LoadingDialog.hide(context);

      if (mounted) {
        NotificationManager.showSuccess(
          context,
          title: 'Успех!',
          message: 'Операция выполнена',
        );
      }
    } catch (e) {
      LoadingDialog.hide(context);

      if (mounted) {
        NotificationManager.showError(
          context,
          title: 'Ошибка',
          message: e.toString(),
        );
      }
    } finally {
      if (mounted) {
        setState(() {
          _isLoading = false;
        });
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Мой экран'),
        actions: [
          IconButton(
            icon: const Icon(Icons.settings),
            onPressed: () {
              Navigator.of(context).push(
                SharedAxisPageRoute(
                  child: SettingsScreen(),
                ),
              );
            },
          ),
        ],
      ),
      body: _isLoading
          ? const Center(child: CircularProgressIndicator())
          : Center(
              child: ElevatedButton(
                onPressed: _doSomething,
                child: const Text('Выполнить'),
              ),
            ),
    );
  }
}
```

---

## 🔗 Полезные ресурсы

### Документация:

- `UI_IMPROVEMENTS.md` - Базовые улучшения UI
- `UI_ENHANCEMENTS_V2.md` - Продвинутые функции V2
- `DEVELOPER_QUICK_REFERENCE.md` - Этот файл

### Flutter Documentation:

- Material Design 3: https://m3.material.io/
- Animations package: https://pub.dev/packages/animations
- fl_chart: https://pub.dev/packages/fl_chart

---

## 💡 Советы

1. **Всегда используйте Provider** для доступа к сервисам
2. **Показывайте loading индикаторы** при операциях
3. **Используйте уведомления** для обратной связи
4. **Обрабатывайте ошибки** с try-catch
5. **Проверяйте mounted** перед setState и Navigator
6. **Используйте анимации** для улучшения UX
7. **Следуйте Material Design 3** гайдлайнам
8. **Тестируйте в обеих темах** (светлой и темной)

---

**Удачи в разработке!** 🚀
