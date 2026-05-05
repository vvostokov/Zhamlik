import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:intl/intl.dart';
import 'package:fl_chart/fl_chart.dart';

import '../services/api_service.dart';
import '../widgets/analytics_chart.dart';
import '../widgets/monthly_spending_chart.dart';

class AnalyticsScreen extends StatefulWidget {
  const AnalyticsScreen({super.key});

  @override
  State<AnalyticsScreen> createState() => _AnalyticsScreenState();
}

class _AnalyticsScreenState extends State<AnalyticsScreen> {
  final List<int> periodOptions = [7, 30, 90, 365];
  int selectedPeriod = 30;
  Map<String, dynamic>? _analyticsData;
  bool _isLoading = true;

  final NumberFormat currencyFormat = NumberFormat.currency(
    symbol: '₽',
    decimalDigits: 0,
  );

  @override
  void initState() {
    super.initState();
    _loadAnalytics();
  }

  Future<void> _loadAnalytics() async {
    setState(() {
      _isLoading = true;
    });

    final apiService = Provider.of<ApiService>(context, listen: false);
    final data = await apiService.getAnalytics(days: selectedPeriod);

    if (mounted) {
      setState(() {
        _analyticsData = data;
        _isLoading = false;
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Аналитика'),
        actions: [
          _buildPeriodSelector(),
        ],
      ),
      body: _isLoading
          ? const Center(child: CircularProgressIndicator())
          : _analyticsData == null
              ? _buildEmptyState()
              : RefreshIndicator(
                  onRefresh: _loadAnalytics,
                  child: SingleChildScrollView(
                    physics: const AlwaysScrollableScrollPhysics(),
                    padding: const EdgeInsets.all(16),
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        _buildSummaryCards(context),
                        const SizedBox(height: 24),
                        _buildCategoryChart(context),
                        const SizedBox(height: 24),
                        _buildTopCategories(context),
                        const SizedBox(height: 24),
                        _buildInsights(context),
                      ],
                    ),
                  ),
                ),
    );
  }

  Widget _buildPeriodSelector() {
    return PopupMenuButton<int>(
      initialValue: selectedPeriod,
      icon: const Icon(Icons.filter_list),
      onSelected: (value) {
        if (value != null) {
          setState(() {
            selectedPeriod = value;
          });
          _loadAnalytics();
        }
      },
      itemBuilder: (context) => [
        const PopupMenuItem(value: 7, child: Text('7 дней')),
        const PopupMenuItem(value: 30, child: Text('30 дней')),
        const PopupMenuItem(value: 90, child: Text('90 дней')),
        const PopupMenuItem(value: 365, child: Text('365 дней')),
      ],
    );
  }

  Widget _buildEmptyState() {
    return Center(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Icon(
            Icons.analytics_outlined,
            size: 80,
            color: Colors.grey[400],
          ),
          const SizedBox(height: 16),
          Text(
            'Нет данных для анализа',
            style: Theme.of(context).textTheme.titleLarge?.copyWith(
                  color: Colors.grey[600],
                ),
          ),
          const SizedBox(height: 8),
          Text(
            'Добавьте операции для просмотра аналитики',
            style: Theme.of(context).textTheme.bodyMedium?.copyWith(
                  color: Colors.grey[500],
                ),
          ),
        ],
      ),
    );
  }

  Widget _buildSummaryCards(BuildContext context) {
    final totalExpense = _analyticsData?['total_expense'] as double? ?? 0.0;
    final periodDays = _analyticsData?['period_days'] as int? ?? 30;
    final averagePerDay = periodDays > 0 ? totalExpense / periodDays : 0.0;

    return Row(
      children: [
        Expanded(
          child: _buildStatCard(
            context,
            'Расходы за период',
            currencyFormat.format(totalExpense),
            Icons.trending_down,
            Colors.red,
          ),
        ),
        const SizedBox(width: 12),
        Expanded(
          child: _buildStatCard(
            context,
            'Средний в день',
            currencyFormat.format(averagePerDay),
            Icons.calendar_today,
            Colors.blue,
          ),
        ),
      ],
    );
  }

  Widget _buildStatCard(
    BuildContext context,
    String title,
    String value,
    IconData icon,
    Color color,
  ) {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Container(
                  padding: const EdgeInsets.all(8),
                  decoration: BoxDecoration(
                    color: color.withOpacity(0.1),
                    borderRadius: BorderRadius.circular(8),
                  ),
                  child: Icon(icon, color: color, size: 20),
                ),
                const SizedBox(width: 8),
                Expanded(
                  child: Text(
                    title,
                    style: Theme.of(context).textTheme.bodyMedium?.copyWith(
                          color: Colors.grey[600],
                        ),
                  ),
                ),
              ],
            ),
            const SizedBox(height: 12),
            Text(
              value,
              style: Theme.of(context).textTheme.headlineSmall?.copyWith(
                    color: color,
                    fontWeight: FontWeight.bold,
                  ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildCategoryChart(BuildContext context) {
    final byCategory = _analyticsData?['by_category'] as List?;
    if (byCategory == null || byCategory.isEmpty) {
      return const SizedBox.shrink();
    }

    // Преобразуем данные для графика
    final categoryData = <String, double>{};
    double total = 0;

    for (var item in byCategory) {
      final categoryName = item['category_name'] as String;
      final amount = item['total'] as double;
      categoryData[categoryName] = amount;
      total += amount;
    }

    return AnalyticsChartWidget(
      categoryData: categoryData,
      title: 'Расходы по категориям',
      totalAmount: total,
    );
  }

  Widget _buildTopCategories(BuildContext context) {
    final byCategory = _analyticsData?['by_category'] as List?;
    if (byCategory == null || byCategory.isEmpty) {
      return const SizedBox.shrink();
    }

    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Топ категории',
              style: Theme.of(context).textTheme.titleLarge?.copyWith(
                    fontWeight: FontWeight.bold,
                  ),
            ),
            const SizedBox(height: 16),
            ...byCategory.take(5).map((item) {
              final categoryName = item['category_name'] as String;
              final amount = item['total'] as double;

              return Padding(
                padding: const EdgeInsets.symmetric(vertical: 8),
                child: Row(
                  children: [
                    Expanded(
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Text(
                            categoryName,
                            style: Theme.of(context).textTheme.bodyLarge?.copyWith(
                                  fontWeight: FontWeight.w500,
                                ),
                          ),
                          const SizedBox(height: 4),
                          Text(
                            currencyFormat.format(amount),
                            style: Theme.of(context).textTheme.bodyMedium?.copyWith(
                                  color: Colors.grey[600],
                                ),
                          ),
                        ],
                      ),
                    ),
                    Container(
                      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
                      decoration: BoxDecoration(
                        color: Theme.of(context).colorScheme.primary.withOpacity(0.1),
                        borderRadius: BorderRadius.circular(8),
                      ),
                      child: Text(
                        '${byCategory.indexOf(item) + 1}',
                        style: TextStyle(
                          color: Theme.of(context).colorScheme.primary,
                          fontWeight: FontWeight.bold,
                        ),
                      ),
                    ),
                  ],
                ),
              );
            }).toList(),
          ],
        ),
      ),
    );
  }

  Widget _buildInsights(BuildContext context) {
    final byCategory = _analyticsData?['by_category'] as List?;
    if (byCategory == null || byCategory.isEmpty) {
      return const SizedBox.shrink();
    }

    final totalExpense = _analyticsData?['total_expense'] as double? ?? 0.0;
    final topCategory = byCategory.first;
    final topCategoryName = topCategory['category_name'] as String;
    final topCategoryAmount = topCategory['total'] as double;
    final topCategoryPercent = totalExpense > 0
        ? (topCategoryAmount / totalExpense * 100)
        : 0.0;

    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Icon(
                  Icons.lightbulb_outline,
                  color: Colors.amber[700],
                ),
                const SizedBox(width: 8),
                Text(
                  'Инсайты',
                  style: Theme.of(context).textTheme.titleLarge?.copyWith(
                        fontWeight: FontWeight.bold,
                      ),
                ),
              ],
            ),
            const SizedBox(height: 16),
            _buildInsightItem(
              context,
              'Крупнейшая категория',
              topCategoryName,
              '${currencyFormat.format(topCategoryAmount)} (${topCategoryPercent.toStringAsFixed(1)}%)',
              Icons.category,
            ),
            const SizedBox(height: 12),
            _buildInsightItem(
              context,
              'Всего категорий',
              '${byCategory.length}',
              null,
              Icons.pie_chart,
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildInsightItem(
    BuildContext context,
    String label,
    String value,
    String? subtitle,
    IconData icon,
  ) {
    return Row(
      children: [
        Container(
          padding: const EdgeInsets.all(10),
          decoration: BoxDecoration(
            color: Theme.of(context).colorScheme.primary.withOpacity(0.1),
            borderRadius: BorderRadius.circular(10),
          ),
          child: Icon(
            icon,
            color: Theme.of(context).colorScheme.primary,
            size: 20,
          ),
        ),
        const SizedBox(width: 12),
        Expanded(
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                label,
                style: Theme.of(context).textTheme.bodySmall?.copyWith(
                      color: Colors.grey[600],
                    ),
              ),
              const SizedBox(height: 2),
              Text(
                value,
                style: Theme.of(context).textTheme.bodyLarge?.copyWith(
                      fontWeight: FontWeight.w600,
                    ),
              ),
              if (subtitle != null) ...[
                const SizedBox(height: 2),
                Text(
                  subtitle!,
                  style: Theme.of(context).textTheme.bodySmall?.copyWith(
                        color: Colors.grey[500],
                      ),
                ),
              ],
            ],
          ),
        ),
      ],
    );
  }
}
