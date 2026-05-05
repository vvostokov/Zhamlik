import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:intl/intl.dart';

import '../services/api_service.dart';
import '../services/auth_service.dart';
import '../services/version_service.dart';
import '../models/overview.dart';
import 'transactions_screen.dart';
import 'accounts_screen.dart';
import 'qr_scanner_screen.dart';
import 'debts_screen.dart';
import 'recurring_payments_screen.dart';

class HomeScreen extends StatefulWidget {
  const HomeScreen({super.key});

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  final DateFormat dateFormatter = DateFormat('dd MMM yyyy, HH:mm');
  Overview? _overview;
  bool _isLoading = true;
  bool _hasError = false;
  String? _errorMessage;

  @override
  void initState() {
    super.initState();
    _loadData();
    _checkForUpdates();
  }

  Future<void> _loadData() async {
    setState(() {
      _isLoading = true;
      _hasError = false;
      _errorMessage = null;
    });

    try {
      final apiService = Provider.of<ApiService>(context, listen: false);
      final overview = await apiService.getOverview();

      if (mounted) {
        setState(() {
          _overview = overview;
          _isLoading = false;
          if (overview == null) {
            _hasError = true;
            _errorMessage = 'Не удалось загрузить данные';
          }
        });
      }
    } catch (e) {
      if (mounted) {
        setState(() {
          _isLoading = false;
          _hasError = true;
          _errorMessage = 'Ошибка: ${e.toString()}';
          _overview = null;
        });
      }
    }
  }

  Future<void> _checkForUpdates() async {
    final versionService = Provider.of<VersionService>(context, listen: false);
    await versionService.checkForUpdates();

    if (mounted && versionService.hasUpdate) {
      await VersionService.showUpdateDialogIfNeeded(context, versionService);
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Zhamlik'),
        actions: [
          IconButton(
            icon: const Icon(Icons.refresh),
            onPressed: _loadData,
          ),
          PopupMenuButton<String>(
            onSelected: (value) {
              if (value == 'logout') {
                _handleLogout();
              }
            },
            itemBuilder: (context) => [
              const PopupMenuItem(
                value: 'logout',
                child: Row(
                  children: [
                    Icon(Icons.logout),
                    SizedBox(width: 8),
                    Text('Выйти'),
                  ],
                ),
              ),
            ],
          ),
        ],
      ),
      body: _isLoading
          ? const Center(child: CircularProgressIndicator())
          : _hasError
              ? Center(
                  child: Column(
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: [
                      Icon(
                        Icons.error_outline,
                        size: 64,
                        color: Theme.of(context).colorScheme.error,
                      ),
                      const SizedBox(height: 16),
                      Text(
                        _errorMessage ?? 'Ошибка загрузки данных',
                        style: Theme.of(context).textTheme.titleLarge,
                        textAlign: TextAlign.center,
                      ),
                      const SizedBox(height: 16),
                      ElevatedButton.icon(
                        onPressed: _loadData,
                        icon: const Icon(Icons.refresh),
                        label: const Text('Повторить'),
                      ),
                    ],
                  ),
                )
              : RefreshIndicator(
                  onRefresh: _loadData,
                  child: SingleChildScrollView(
                    physics: const AlwaysScrollableScrollPhysics(),
                    padding: const EdgeInsets.all(16),
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    _buildBalanceCard(context),
                    const SizedBox(height: 24),
                    _buildQuickActions(context),
                    const SizedBox(height: 24),
                    _buildRecentTransactions(context),
                  ],
                ),
              ),
            ),
          );
  }

  Widget _buildBalanceCard(BuildContext context) {
    final balance = _overview?.totalBalance ?? 0.0;
    final income = _overview?.monthlyIncome ?? 0.0;
    final expense = _overview?.monthlyExpense ?? 0.0;

    final formatCurrency = NumberFormat.currency(
      symbol: '₽',
      decimalDigits: 2,
    );

    return Card(
      child: Padding(
        padding: const EdgeInsets.all(20),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Общий баланс',
              style: Theme.of(context).textTheme.titleMedium?.copyWith(
                    color: Colors.grey[600],
                  ),
            ),
            const SizedBox(height: 8),
            Text(
              formatCurrency.format(balance),
              style: Theme.of(context).textTheme.headlineMedium?.copyWith(
                    fontWeight: FontWeight.bold,
                    color: Theme.of(context).colorScheme.primary,
                  ),
            ),
            const SizedBox(height: 20),
            Row(
              children: [
                Expanded(
                  child: _buildStatItem(
                    context,
                    'Доход за месяц',
                    income,
                    Colors.green,
                    Icons.arrow_downward,
                    formatCurrency,
                  ),
                ),
                const SizedBox(width: 16),
                Expanded(
                  child: _buildStatItem(
                    context,
                    'Расход за месяц',
                    expense,
                    Colors.red,
                    Icons.arrow_upward,
                    formatCurrency,
                  ),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildStatItem(
    BuildContext context,
    String label,
    double amount,
    Color color,
    IconData icon,
    NumberFormat format,
  ) {
    return Container(
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: color.withOpacity(0.1),
        borderRadius: BorderRadius.circular(12),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Icon(icon, size: 16, color: color),
              const SizedBox(width: 4),
              Text(
                label,
                style: Theme.of(context).textTheme.bodySmall?.copyWith(
                      color: color,
                    ),
              ),
            ],
          ),
          const SizedBox(height: 8),
          Text(
            format.format(amount),
            style: Theme.of(context).textTheme.titleLarge?.copyWith(
                  color: color,
                  fontWeight: FontWeight.bold,
                ),
          ),
        ],
      ),
    );
  }

  Widget _buildQuickActions(BuildContext context) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          'Быстрые действия',
          style: Theme.of(context).textTheme.titleLarge?.copyWith(
                fontWeight: FontWeight.bold,
              ),
        ),
        const SizedBox(height: 16),
        Row(
          children: [
            Expanded(
              child: _buildActionButton(
                context,
                'Сканировать QR',
                Icons.qr_code_scanner,
                () {
                  Navigator.of(context).pushNamed('/qr-scanner');
                },
                Colors.blue,
              ),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: _buildActionButton(
                context,
                'Операции',
                Icons.list_alt,
                () {
                  Navigator.of(context).pushNamed('/transactions');
                },
                Colors.red,
              ),
            ),
          ],
        ),
        const SizedBox(height: 12),
        Row(
          children: [
            Expanded(
              child: _buildActionButton(
                context,
                'Счета',
                Icons.account_balance,
                () {
                  Navigator.of(context).pushNamed('/accounts');
                },
                Colors.green,
              ),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: _buildActionButton(
                context,
                'Долги',
                Icons.money,
                () {
                  Navigator.of(context).push(MaterialPageRoute(
                    builder: (context) => const DebtsScreen(),
                  ));
                },
                Colors.indigo,
              ),
            ),
          ],
        ),
        const SizedBox(height: 12),
        Row(
          children: [
            Expanded(
              child: _buildActionButton(
                context,
                'Платежи',
                Icons.event_repeat,
                () {
                  Navigator.of(context).push(MaterialPageRoute(
                    builder: (context) => const RecurringPaymentsScreen(),
                  ));
                },
                Colors.purple,
              ),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: _buildActionButton(
                context,
                'Аналитика',
                Icons.analytics,
                () {
                  Navigator.of(context).pushNamed('/analytics');
                },
                Colors.teal,
              ),
            ),
          ],
        ),
      ],
    );
  }

  Widget _buildActionButton(
    BuildContext context,
    String label,
    IconData icon,
    VoidCallback onTap,
    Color color,
  ) {
    return InkWell(
      onTap: onTap,
      borderRadius: BorderRadius.circular(12),
      child: Container(
        padding: const EdgeInsets.symmetric(vertical: 20),
        decoration: BoxDecoration(
          color: color.withOpacity(0.1),
          borderRadius: BorderRadius.circular(12),
          border: Border.all(color: color.withOpacity(0.3)),
        ),
        child: Column(
          children: [
            Icon(icon, color: color, size: 32),
            const SizedBox(height: 8),
            Text(
              label,
              style: Theme.of(context).textTheme.bodyMedium?.copyWith(
                    color: color,
                    fontWeight: FontWeight.w600,
                  ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildRecentTransactions(BuildContext context) {
    final transactions = _overview?.recentTransactions ?? [];

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            Text(
              'Последние операции',
              style: Theme.of(context).textTheme.titleLarge?.copyWith(
                    fontWeight: FontWeight.bold,
                  ),
            ),
            TextButton(
              onPressed: () {
                Navigator.of(context).pushNamed('/transactions');
              },
              child: const Text('Все'),
            ),
          ],
        ),
        const SizedBox(height: 12),
        if (transactions.isEmpty)
          const Center(
            child: Padding(
              padding: EdgeInsets.all(32),
              child: Text('Нет операций'),
            ),
          )
        else
          Card(
            child: Column(
              children: transactions.take(5).map((tx) {
                return _buildTransactionItem(context, tx);
              }).toList(),
            ),
              ),
      ],
    );
  }

  Widget _buildTransactionItem(BuildContext context, TransactionSummary tx) {
    // Determine transaction type - if not specified, assume expense
    final isIncome = tx.type == 'income';
    final color = isIncome ? Colors.green : Colors.red;
    final icon = isIncome ? Icons.arrow_downward : Icons.arrow_upward;

    return ListTile(
      leading: Container(
        width: 40,
        height: 40,
        decoration: BoxDecoration(
          color: color.withOpacity(0.1),
          borderRadius: BorderRadius.circular(8),
        ),
        child: Icon(icon, color: color),
      ),
      title: Text(
        tx.description.isNotEmpty ? tx.description : (tx.accountName ?? 'Без названия'),
        style: const TextStyle(fontWeight: FontWeight.w500),
      ),
      subtitle: Text(
        dateFormatter.format(tx.date),
        style: TextStyle(color: Colors.grey[600], fontSize: 12),
      ),
      trailing: Text(
        '${isIncome ? '+' : '-'}${tx.amount.toStringAsFixed(2)} ₽',
        style: TextStyle(
          color: color,
          fontWeight: FontWeight.bold,
          fontSize: 16,
        ),
      ),
    );
  }

  void _handleLogout() {
    Provider.of<AuthService>(context, listen: false).logout();
    Navigator.of(context).pushNamedAndRemoveUntil('/', (route) => false);
  }
}
