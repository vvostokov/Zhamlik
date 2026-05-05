import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:intl/intl.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:animations/animations.dart';
import 'package:percent_indicator/percent_indicator.dart';

import '../services/api_service.dart';
import '../services/auth_service.dart';
import '../models/overview.dart';
import '../widgets/animated_stat_card.dart';
import '../widgets/quick_action_grid.dart';
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

class _HomeScreenState extends State<HomeScreen>
    with SingleTickerProviderStateMixin {
  late AnimationController _animationController;
  late Animation<double> _fadeAnimation;
  final DateFormat dateFormatter = DateFormat('dd MMM yyyy, HH:mm');
  Overview? _overview;
  bool _isLoading = true;

  @override
  void initState() {
    super.initState();
    _animationController = AnimationController(
      duration: const Duration(milliseconds: 1000),
      vsync: this,
    );
    _fadeAnimation = Tween<double>(begin: 0.0, end: 1.0).animate(
      CurvedAnimation(
        parent: _animationController,
        curve: Curves.easeInOut,
      ),
    );
    _animationController.forward();
    _loadData();
  }

  @override
  void dispose() {
    _animationController.dispose();
    super.dispose();
  }

  Future<void> _loadData() async {
    setState(() {
      _isLoading = true;
    });

    final apiService = Provider.of<ApiService>(context, listen: false);
    final overview = await apiService.getOverview();

    if (mounted) {
      setState(() {
        _overview = overview;
        _isLoading = false;
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: CustomScrollView(
        slivers: [
          _buildAppBar(context),
          SliverToBoxAdapter(
            child: _isLoading
                ? _buildLoadingShimmer()
                : FadeTransition(
                    opacity: _fadeAnimation,
                    child: RefreshIndicator(
                      onRefresh: _loadData,
                      child: SingleChildScrollView(
                        physics: const AlwaysScrollableScrollPhysics(),
                        padding: const EdgeInsets.all(16),
                        child: Column(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            _buildBalanceCards(context),
                            const SizedBox(height: 24),
                            _buildQuickActionsGrid(context),
                            const SizedBox(height: 24),
                            _buildRecentTransactions(context),
                          ],
                        ),
                      ),
                    ),
                  ),
          ),
        ],
      ),
    );
  }

  Widget _buildAppBar(BuildContext context) {
    return SliverAppBar(
      expandedHeight: 120,
      floating: false,
      pinned: true,
      backgroundColor: Theme.of(context).colorScheme.primary,
      flexibleSpace: FlexibleSpaceBar(
        title: Text(
          'Zhamlik',
          style: GoogleFonts.poppins(
            fontWeight: FontWeight.bold,
          ),
        ),
        background: DecoratedBox(
          position: DecorationPosition.foreground,
          child: Container(
            decoration: BoxDecoration(
              gradient: LinearGradient(
                begin: Alignment.topLeft,
                end: Alignment.bottomRight,
                colors: [
                  Theme.of(context).colorScheme.primary,
                  Theme.of(context).colorScheme.primary.withOpacity(0.8),
                ],
              ),
            ),
          ),
        ),
      ),
      actions: [
        IconButton(
          icon: const Icon(Icons.notifications_outlined),
          onPressed: () {
            ScaffoldMessenger.of(context).showSnackBar(
              const SnackBar(content: Text('Уведомления в разработке')),
            );
          },
        ),
        PopupMenuButton<String>(
          onSelected: (value) {
            if (value == 'logout') {
              _handleLogout();
            }
          },
          icon: const Icon(Icons.more_vert),
          itemBuilder: (context) => [
            const PopupMenuItem(
              value: 'settings',
              child: Row(
                children: [
                  Icon(Icons.settings),
                  SizedBox(width: 8),
                  Text('Настройки'),
                ],
              ),
            ),
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
    );
  }

  Widget _buildLoadingShimmer() {
    return Column(
      children: [
        Container(
          height: 150,
          margin: const EdgeInsets.all(16),
          decoration: BoxDecoration(
            color: Colors.grey[200],
            borderRadius: BorderRadius.circular(12),
          ),
        ),
        const SizedBox(height: 24),
        Row(
          children: [
            Expanded(
              child: Container(
                height: 100,
                margin: const EdgeInsets.only(left: 16),
                decoration: BoxDecoration(
                  color: Colors.grey[200],
                  borderRadius: BorderRadius.circular(12),
                ),
              ),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: Container(
                height: 100,
                margin: const EdgeInsets.only(right: 16),
                decoration: BoxDecoration(
                  color: Colors.grey[200],
                  borderRadius: BorderRadius.circular(12),
                ),
              ),
            ),
          ],
        ),
      ],
    );
  }

  Widget _buildBalanceCards(BuildContext context) {
    if (_overview == null) return const SizedBox();

    final balance = _overview!.totalBalance;
    final income = _overview!.monthlyIncome;
    final expense = _overview!.monthlyExpense;

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          'Общий баланс',
          style: Theme.of(context).textTheme.titleLarge?.copyWith(
                fontWeight: FontWeight.bold,
              ),
        ),
        const SizedBox(height: 16),
        Container(
          padding: const EdgeInsets.all(24),
          decoration: BoxDecoration(
            gradient: LinearGradient(
              begin: Alignment.topLeft,
              end: Alignment.bottomRight,
              colors: [
                Theme.of(context).colorScheme.primary,
                Theme.of(context).colorScheme.primary.withOpacity(0.7),
              ],
            ),
            borderRadius: BorderRadius.circular(20),
            boxShadow: [
              BoxShadow(
                color: Theme.of(context).colorScheme.primary.withOpacity(0.3),
                blurRadius: 20,
                offset: const Offset(0, 10),
              ),
            ],
          ),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                NumberFormat.currency(symbol: '₽', decimalDigits: 2).format(balance),
                style: Theme.of(context).textTheme.headlineLarge?.copyWith(
                      color: Colors.white,
                      fontWeight: FontWeight.bold,
                    ),
              ),
              const SizedBox(height: 8),
              Row(
                children: [
                  const Icon(Icons.account_balance_wallet, color: Colors.white70, size: 20),
                  const SizedBox(width: 8),
                  Text(
                    'Все счета',
                    style: TextStyle(color: Colors.white70),
                  ),
                ],
              ),
            ],
          ),
        ),
        const SizedBox(height: 20),
        Row(
          children: [
            Expanded(
              child: AnimatedStatCard(
                title: 'Доход за месяц',
                value: income,
                subtitle: 'За этот месяц',
                icon: Icons.arrow_downward,
                color: Colors.green,
                onTap: () {
                  Navigator.of(context).pushNamed('/transactions');
                },
              ),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: AnimatedStatCard(
                title: 'Расход за месяц',
                value: expense,
                subtitle: 'За этот месяц',
                icon: Icons.arrow_upward,
                color: Colors.red,
                onTap: () {
                  Navigator.of(context).pushNamed('/transactions');
                },
              ),
            ),
          ],
        ),
      ],
    );
  }

  Widget _buildQuickActionsGrid(BuildContext context) {
    final actions = [
      QuickAction(
        title: 'Сканировать',
        icon: Icons.qr_code_scanner,
        color: Colors.blue,
        onTap: () => Navigator.of(context).pushNamed('/qr-scanner'),
      ),
      QuickAction(
        title: 'Операции',
        icon: Icons.list_alt,
        color: Colors.orange,
        onTap: () => Navigator.of(context).pushNamed('/transactions'),
      ),
      QuickAction(
        title: 'Счета',
        icon: Icons.account_balance,
        color: Colors.green,
        onTap: () => Navigator.of(context).pushNamed('/accounts'),
      ),
      QuickAction(
        title: 'Долги',
        icon: Icons.money,
        color: Colors.deepOrange,
        onTap: () {
          Navigator.of(context).push(MaterialPageRoute(
            builder: (context) => const DebtsScreen(),
          ));
        },
      ),
      QuickAction(
        title: 'Платежи',
        icon: Icons.event_repeat,
        color: Colors.purple,
        onTap: () {
          Navigator.of(context).push(MaterialPageRoute(
            builder: (context) => const RecurringPaymentsScreen(),
          ));
        },
      ),
      QuickAction(
        title: 'Аналитика',
        icon: Icons.analytics,
        color: Colors.teal,
        onTap: () {
          _showAnalyticsDialog(context);
        },
      ),
    ];

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
        QuickActionGrid(actions: actions),
      ],
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
        transactions.isEmpty
            ? Center(
                child: Padding(
                  padding: const EdgeInsets.all(32),
                  child: Column(
                    children: [
                      Icon(
                        Icons.receipt_long,
                        size: 64,
                        color: Colors.grey[400],
                      ),
                      const SizedBox(height: 16),
                      Text(
                        'Нет операций',
                        style: Theme.of(context).textTheme.titleLarge?.copyWith(
                              color: Colors.grey[600],
                            ),
                      ),
                    ],
                  ),
                ),
              )
            : Card(
                elevation: 2,
                child: Column(
                  children: [
                    ...List.generate(
                      transactions.take(5).length,
                      (index) => _buildTransactionItem(
                        context,
                        transactions[index],
                        index < transactions.take(5).length - 1,
                      ),
                    ),
                  ],
                ),
              ),
      ],
    );
  }

  Widget _buildTransactionItem(BuildContext context, TransactionSummary tx, bool isLast) {
    final isIncome = tx.type == 'income';
    final color = isIncome ? Colors.green : Colors.red;

    return Column(
      children: [
        ListTile(
          contentPadding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
          leading: Container(
            width: 48,
            height: 48,
            decoration: BoxDecoration(
              color: color.withOpacity(0.1),
              borderRadius: BorderRadius.circular(12),
            ),
            child: Icon(
              isIncome ? Icons.arrow_downward : Icons.arrow_upward,
              color: color,
            ),
          ),
          title: Text(
            tx.description.isNotEmpty ? tx.description : tx.accountName,
            style: const TextStyle(fontWeight: FontWeight.w500),
          ),
          subtitle: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              const SizedBox(height: 4),
              Text(
                dateFormatter.format(tx.date),
                style: TextStyle(color: Colors.grey[600], fontSize: 12),
              ),
            ],
          ),
          trailing: Text(
            '${isIncome ? '+' : '-'}${tx.amount.toStringAsFixed(2)} ₽',
            style: TextStyle(
              color: color,
              fontWeight: FontWeight.bold,
              fontSize: 16,
            ),
          ),
        ),
        if (!isLast) const Divider(height: 1),
      ],
    );
  }

  void _showAnalyticsDialog(BuildContext context) {
    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Аналитика'),
        content: const Text('Функция аналитики скоро будет доступна!'),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(context).pop(),
            child: const Text('Закрыть'),
          ),
        ],
      ),
    );
  }

  void _handleLogout() {
    Provider.of<AuthService>(context, listen: false).logout();
    Navigator.of(context).pushNamedAndRemoveUntil('/', (route) => false);
  }
}
