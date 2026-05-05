import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:shared_preferences/shared_preferences.dart';

import 'services/api_service.dart';
import 'services/auth_service.dart';
import 'services/theme_service.dart';
import 'services/version_service.dart';
import 'screens/splash_screen.dart';
import 'screens/home_screen.dart';
import 'screens/login_screen.dart';
import 'screens/transactions_screen.dart';
import 'screens/qr_scanner_screen.dart';
import 'screens/accounts_screen.dart';
import 'screens/debts_screen.dart';
import 'screens/recurring_payments_screen.dart';
import 'screens/settings_screen.dart';
import 'screens/analytics_screen.dart';

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(const ZhamlikApp());
}

class ZhamlikApp extends StatefulWidget {
  const ZhamlikApp({super.key});

  @override
  State<ZhamlikApp> createState() => _ZhamlikAppState();
}

class _ZhamlikAppState extends State<ZhamlikApp> {
  late final ApiService _apiService;
  late final AuthService _authService;

  @override
  void initState() {
    super.initState();
    _apiService = ApiService();
    _authService = AuthService();
    _authService.setApiService(_apiService);
    // Load saved session on startup
    _authService.loadAuthData();
    _apiService.loadSession();
  }

  @override
  Widget build(BuildContext context) {
    return MultiProvider(
      providers: [
        ChangeNotifierProvider.value(value: _apiService),
        ChangeNotifierProvider.value(value: _authService),
        ChangeNotifierProvider(create: (_) => ThemeService()),
        ChangeNotifierProvider(create: (_) => VersionService()),
      ],
      child: Consumer<ApiService>(
        builder: (context, apiService, _) {
          return Consumer<AuthService>(
            builder: (context, authService, _) {

              return Consumer<ThemeService>(
                builder: (context, themeService, _) {
                  return MaterialApp(
                    title: 'Zhamlik Finance',
                    debugShowCheckedModeBanner: false,
                    theme: _buildLightTheme(),
                    darkTheme: _buildDarkTheme(),
                    themeMode: themeService.themeMode,
                    initialRoute: '/',
                    routes: {
                      '/': (context) => const SplashScreen(),
                      '/login': (context) => const LoginScreen(),
                      '/home': (context) => const HomeScreen(),
                      '/transactions': (context) => const TransactionsScreen(),
                      '/qr-scanner': (context) => const QRScannerScreen(),
                      '/accounts': (context) => const AccountsScreen(),
                      '/debts': (context) => const DebtsScreen(),
                      '/recurring-payments': (context) => const RecurringPaymentsScreen(),
                      '/analytics': (context) => const AnalyticsScreen(),
                      '/settings': (context) => const SettingsScreen(),
                    },
                  );
                },
              );
            },
          );
        },
      ),
    );
  }

  static ThemeData _buildLightTheme() {
    return ThemeData(
      primarySwatch: Colors.blue,
      useMaterial3: true,
      colorScheme: ColorScheme.fromSeed(
        seedColor: const Color(0xFF2196F3),
        brightness: Brightness.light,
        primary: const Color(0xFF2196F3),
        secondary: const Color(0xFF03DAC6),
        surface: Colors.white,
        error: const Color(0xFFB00020),
      ),
      appBarTheme: const AppBarTheme(
        centerTitle: true,
        elevation: 0,
        backgroundColor: Color(0xFF2196F3),
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
      primarySwatch: Colors.blue,
      useMaterial3: true,
      colorScheme: ColorScheme.fromSeed(
        seedColor: const Color(0xFF2196F3),
        brightness: Brightness.dark,
        primary: const Color(0xFF64B5F6),
        secondary: const Color(0xFF03DAC6),
        surface: const Color(0xFF1E1E1E),
        error: const Color(0xFFCF6679),
      ),
      appBarTheme: const AppBarTheme(
        centerTitle: true,
        elevation: 0,
        backgroundColor: Color(0xFF1E1E1E),
        foregroundColor: Color(0xFF64B5F6),
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
