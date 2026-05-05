import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:animations/animations.dart';

import '../services/theme_service.dart';
import '../services/auth_service.dart';

class SettingsScreen extends StatelessWidget {
  const SettingsScreen({super.key});

  @override
  Widget build(BuildContext context) {
    final themeService = Provider.of<ThemeService>(context);
    final authService = Provider.of<AuthService>(context);

    return Scaffold(
      appBar: AppBar(
        title: const Text('Настройки'),
        actions: [
          IconButton(
            icon: const Icon(Icons.info_outline),
            onPressed: () {
              _showAboutDialog(context);
            },
          ),
        ],
      ),
      body: ListView(
        children: [
          _buildSectionHeader('Внешний вид'),
          _buildThemeSelector(context, themeService),
          const Divider(height: 32),
          _buildSectionHeader('Аккаунт'),
          _buildAccountTile(context, authService),
          const Divider(height: 32),
          _buildSectionHeader('Приложение'),
          _buildAppInfoTile(context),
          const SizedBox(height: 16),
        ],
      ),
    );
  }

  Widget _buildSectionHeader(String title) {
    return Padding(
      padding: const EdgeInsets.fromLTRB(16, 16, 16, 8),
      child: Text(
        title.toUpperCase(),
        style: const TextStyle(
          fontSize: 12,
          fontWeight: FontWeight.bold,
          letterSpacing: 1.2,
        ),
      ),
    );
  }

  Widget _buildThemeSelector(BuildContext context, ThemeService themeService) {
    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      decoration: BoxDecoration(
        color: Theme.of(context).cardColor,
        borderRadius: BorderRadius.circular(16),
        border: Border.all(
          color: Theme.of(context).dividerTheme.color!,
        ),
      ),
      child: Column(
        children: [
          _buildThemeOption(
            context: context,
            title: 'Светлая тема',
            subtitle: 'Всегда использовать светлую тему',
            icon: Icons.light_mode,
            themeMode: ThemeMode.light,
            selectedMode: themeService.themeMode,
          ),
          const Divider(height: 1),
          _buildThemeOption(
            context: context,
            title: 'Темная тема',
            subtitle: 'Всегда использовать темную тему',
            icon: Icons.dark_mode,
            themeMode: ThemeMode.dark,
            selectedMode: themeService.themeMode,
          ),
          const Divider(height: 1),
          _buildThemeOption(
            context: context,
            title: 'Системная тема',
            subtitle: 'Следовать настройкам системы',
            icon: Icons.brightness_auto,
            themeMode: ThemeMode.system,
            selectedMode: themeService.themeMode,
          ),
        ],
      ),
    );
  }

  Widget _buildThemeOption({
    required BuildContext context,
    required String title,
    required String subtitle,
    required IconData icon,
    required ThemeMode themeMode,
    required ThemeMode selectedMode,
  }) {
    final isSelected = themeMode == selectedMode;

    return OpenContainer(
      closedElevation: 0,
      closedColor: Colors.transparent,
      openColor: Theme.of(context).cardColor,
      transitionDuration: const Duration(milliseconds: 300),
      openBuilder: (context, action) {
        return const SizedBox();
      },
      closedBuilder: (context, action) {
        return ListTile(
          contentPadding: const EdgeInsets.symmetric(horizontal: 20, vertical: 8),
          leading: Container(
            padding: const EdgeInsets.all(8),
            decoration: BoxDecoration(
              color: isSelected
                  ? Theme.of(context).colorScheme.primary.withOpacity(0.1)
                  : Colors.transparent,
              borderRadius: BorderRadius.circular(12),
            ),
            child: Icon(
              icon,
              color: isSelected
                  ? Theme.of(context).colorScheme.primary
                  : Theme.of(context).iconTheme.color,
            ),
          ),
          title: Text(
            title,
            style: TextStyle(
              fontWeight: isSelected ? FontWeight.bold : FontWeight.normal,
              color: isSelected
                  ? Theme.of(context).colorScheme.primary
                  : Theme.of(context).textTheme.titleMedium?.color,
            ),
          ),
          subtitle: Text(
            subtitle,
            style: TextStyle(
              fontSize: 12,
              color: Theme.of(context).textTheme.bodySmall?.color,
            ),
          ),
          trailing: isSelected
              ? Icon(
                  Icons.check_circle,
                  color: Theme.of(context).colorScheme.primary,
                )
              : null,
        );
      },
      tappable: true,
    );
  }

  Widget _buildAccountTile(BuildContext context, AuthService authService) {
    final user = authService.user;

    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      decoration: BoxDecoration(
        color: Theme.of(context).cardColor,
        borderRadius: BorderRadius.circular(16),
        border: Border.all(
          color: Theme.of(context).dividerTheme.color!,
        ),
      ),
      child: ListTile(
        contentPadding: const EdgeInsets.all(20),
        leading: CircleAvatar(
          backgroundColor: Theme.of(context).colorScheme.primary,
          child: Text(
            (user?['username'] as String?)?.substring(0, 1).toUpperCase() ?? 'U',
            style: const TextStyle(
              color: Colors.white,
              fontWeight: FontWeight.bold,
            ),
          ),
        ),
        title: Text(
          user?['username'] as String? ?? 'Гость',
          style: const TextStyle(fontWeight: FontWeight.bold),
        ),
        subtitle: Text(user?['email'] as String? ?? ''),
        trailing: IconButton(
          icon: const Icon(Icons.logout),
          onPressed: () async {
            final confirmed = await _showLogoutDialog(context);
            if (confirmed == true) {
              await authService.logout();
              if (context.mounted) {
                Navigator.of(context).pushNamedAndRemoveUntil(
                  '/login',
                  (route) => false,
                );
              }
            }
          },
        ),
      ),
    );
  }

  Widget _buildAppInfoTile(BuildContext context) {
    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      decoration: BoxDecoration(
        color: Theme.of(context).cardColor,
        borderRadius: BorderRadius.circular(16),
        border: Border.all(
          color: Theme.of(context).dividerTheme.color!,
        ),
      ),
      child: Column(
        children: [
          ListTile(
            contentPadding: const EdgeInsets.all(20),
            leading: const Icon(Icons.info),
            title: const Text(
              'Zhamlik Finance',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            subtitle: const Text('Версия 1.0.0'),
          ),
          ListTile(
            contentPadding: const EdgeInsets.fromLTRB(20, 0, 20, 20),
            leading: const Icon(Icons.favorite, color: Colors.red),
            title: const Text('Создано с любовью'),
            subtitle: const Text('© 2026 Zhamlik'),
          ),
        ],
      ),
    );
  }

  Future<bool?> _showLogoutDialog(BuildContext context) {
    return showDialog<bool>(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Выйти из аккаунта?'),
        content: const Text('Вы уверены, что хотите выйти?'),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context, false),
            child: const Text('Отмена'),
          ),
          TextButton(
            onPressed: () => Navigator.pop(context, true),
            child: const Text('Выйти'),
          ),
        ],
      ),
    );
  }

  void _showAboutDialog(BuildContext context) {
    showAboutDialog(
      context: context,
      applicationName: 'Zhamlik Finance',
      applicationVersion: '1.0.0',
      applicationLegalese: '© 2026 Zhamlik. Все права защищены.',
      children: [
        const SizedBox(height: 16),
        const Text(
          'Умное управление личными финансами и криптоинвестициями.',
        ),
      ],
    );
  }
}
