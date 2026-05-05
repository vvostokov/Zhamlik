import 'package:flutter/material.dart';
import 'package:provider/provider.dart';

import 'services/crypto_api_service.dart';
import 'services/theme_service.dart';
import 'services/auto_refresh_service.dart';
import 'services/version_service.dart';
import 'screens/splash_screen.dart';
import 'screens/home_screen.dart';
import 'screens/platforms_screen.dart';
import 'screens/assets_screen.dart';
import 'screens/settings_screen.dart';
import 'screens/analytics_screen.dart';
import 'screens/transactions_screen.dart';
import 'screens/futures_screen.dart';
import 'screens/login_screen.dart';

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(const ZhamlikCryptoApp());
}

class ZhamlikCryptoApp extends StatefulWidget {
  const ZhamlikCryptoApp({super.key});

  @override
  State<ZhamlikCryptoApp> createState() => _ZhamlikCryptoAppState();
}

class _ZhamlikCryptoAppState extends State<ZhamlikCryptoApp> {
  late final CryptoApiService _cryptoApiService;
  late final AutoRefreshService _autoRefreshService;

  @override
  void initState() {
    super.initState();
    _cryptoApiService = CryptoApiService();
    _cryptoApiService.loadSession();
    _autoRefreshService = AutoRefreshService(_cryptoApiService);
  }

  @override
  void dispose() {
    _autoRefreshService.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return MultiProvider(
      providers: [
        ChangeNotifierProvider.value(value: _cryptoApiService),
        ChangeNotifierProvider(create: (_) => ThemeService()),
        ChangeNotifierProvider.value(value: _autoRefreshService),
        ChangeNotifierProvider(create: (_) => VersionService()),
      ],
      child: Consumer<ThemeService>(
        builder: (context, themeService, _) {
          return MaterialApp(
            title: 'Zhamlik Crypto',
            debugShowCheckedModeBanner: false,
            theme: _buildLightTheme(),
            darkTheme: _buildDarkTheme(),
            themeMode: themeService.themeMode,
            initialRoute: '/',
            routes: {
              '/': (context) => const SplashScreen(),
              '/login': (context) => const CryptoLoginScreen(),
              '/home': (context) => const HomeScreen(),
              '/platforms': (context) => const PlatformsScreen(),
              '/assets': (context) => const AssetsScreen(),
              '/settings': (context) => const CryptoSettingsScreen(),
              '/analytics': (context) => const CryptoAnalyticsScreen(),
              '/transactions': (context) => const CryptoTransactionsScreen(),
              '/futures': (context) => const FuturesScreen(),
            },
          );
        },
      ),
    );
  }

  static ThemeData _buildLightTheme() {
    return ThemeData(
      primarySwatch: Colors.orange,
      useMaterial3: true,
      colorScheme: ColorScheme.fromSeed(
        seedColor: const Color(0xFFFF9800),
        brightness: Brightness.light,
        primary: const Color(0xFFFF9800),
        secondary: const Color(0xFFFF6F00),
        surface: Colors.white,
        error: const Color(0xFFB00020),
      ),
      appBarTheme: const AppBarTheme(
        centerTitle: true,
        elevation: 0,
        backgroundColor: Color(0xFFFF9800),
        foregroundColor: Colors.white,
      ),
      cardTheme: CardThemeData(
        elevation: 2,
        shape: RoundedRectangleBorder(
          borderRadius: BorderRadius.circular(16),
        ),
        color: Colors.white,
      ),
      scaffoldBackgroundColor: const Color(0xFFF5F5F5),
      dividerTheme: const DividerThemeData(
        color: Color(0xFFE0E0E0),
        thickness: 1,
      ),
    );
  }

  static ThemeData _buildDarkTheme() {
    return ThemeData(
      primarySwatch: Colors.orange,
      useMaterial3: true,
      colorScheme: ColorScheme.fromSeed(
        seedColor: const Color(0xFFFF9800),
        brightness: Brightness.dark,
        primary: const Color(0xFFFFB74D),
        secondary: const Color(0xFFFF6F00),
        surface: const Color(0xFF1E1E1E),
        error: const Color(0xFFCF6679),
      ),
      appBarTheme: const AppBarTheme(
        centerTitle: true,
        elevation: 0,
        backgroundColor: Color(0xFF1E1E1E),
        foregroundColor: Color(0xFFFFB74D),
      ),
      cardTheme: CardThemeData(
        elevation: 2,
        shape: RoundedRectangleBorder(
          borderRadius: BorderRadius.circular(16),
        ),
        color: const Color(0xFF2C2C2C),
      ),
      scaffoldBackgroundColor: const Color(0xFF121212),
      dividerTheme: const DividerThemeData(
        color: Color(0xFF2C2C2C),
        thickness: 1,
      ),
    );
  }
}
