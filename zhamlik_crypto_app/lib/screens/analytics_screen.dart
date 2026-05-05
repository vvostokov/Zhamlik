import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:intl/intl.dart';
import 'package:fl_chart/fl_chart.dart';

import '../services/crypto_api_service.dart';
import '../models/crypto_analytics.dart';

class CryptoAnalyticsScreen extends StatefulWidget {
  const CryptoAnalyticsScreen({super.key});

  @override
  State<CryptoAnalyticsScreen> createState() => _CryptoAnalyticsScreenState();
}

class _CryptoAnalyticsScreenState extends State<CryptoAnalyticsScreen> {
  final NumberFormat currencyFormat = NumberFormat.currency(
    symbol: '\$',
    decimalDigits: 2,
  );

  CryptoAnalytics? _analytics;
  bool _isLoading = true;
  int _selectedDays = 30;

  @override
  void initState() {
    super.initState();
    _loadAnalytics();
  }

  Future<void> _loadAnalytics() async {
    setState(() {
      _isLoading = true;
    });

    final apiService = Provider.of<CryptoApiService>(context, listen: false);
    final data = await apiService.getAnalytics(days: _selectedDays);

    if (mounted) {
      setState(() {
        if (data != null) {
          _analytics = CryptoAnalytics.fromJson(data);
        }
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
          DropdownButton<int>(
            value: _selectedDays,
            dropdownColor: Theme.of(context).cardColor,
            icon: Icon(Icons.calendar_today, color: Theme.of(context).colorScheme.onPrimary),
            style: TextStyle(color: Theme.of(context).colorScheme.onPrimary),
            items: const [
              DropdownMenuItem(value: 7, child: Text('7 дней')),
              DropdownMenuItem(value: 30, child: Text('30 дней')),
              DropdownMenuItem(value: 90, child: Text('90 дней')),
            ],
            onChanged: (value) {
              if (value != null) {
                setState(() {
                  _selectedDays = value;
                });
                _loadAnalytics();
              }
            },
          ),
        ],
      ),
      body: _isLoading
          ? const Center(child: CircularProgressIndicator())
          : _analytics == null
              ? const Center(child: Text('Нет данных'))
              : RefreshIndicator(
                  onRefresh: _loadAnalytics,
                  child: SingleChildScrollView(
                    padding: const EdgeInsets.all(16),
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        _buildPortfolioDistributionCard(),
                        const SizedBox(height: 16),
                        _buildPnLCard(),
                        const SizedBox(height: 16),
                        _buildPerformanceChartCard(),
                      ],
                    ),
                  ),
                ),
    );
  }

  Widget _buildPnLCard() {
    if (_analytics == null || _analytics!.performanceChart.isEmpty) {
      return const SizedBox.shrink();
    }

    // Расчитываем PnL
    final firstValue = _analytics!.performanceChart.first.totalValueUsd;
    final lastValue = _analytics!.performanceChart.last.totalValueUsd;
    final pnl = lastValue - firstValue;
    final pnlPercent = firstValue > 0 ? (pnl / firstValue * 100) : 0.0;

    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Icon(
                  Icons.trending_up,
                  color: pnl >= 0 ? Colors.green : Colors.red,
                ),
                const SizedBox(width: 12),
                Text(
                  'PnL (Прибыль/Убыток)',
                  style: Theme.of(context).textTheme.titleLarge?.copyWith(
                        fontWeight: FontWeight.bold,
                      ),
                ),
              ],
            ),
            const SizedBox(height: 16),
            Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'Абсолютный PnL',
                      style: TextStyle(
                        color: Colors.grey[600],
                        fontSize: 13,
                      ),
                    ),
                    const SizedBox(height: 4),
                    Text(
                      currencyFormat.format(pnl),
                      style: Theme.of(context).textTheme.headlineMedium?.copyWith(
                            color: pnl >= 0 ? Colors.green : Colors.red,
                            fontWeight: FontWeight.bold,
                          ),
                    ),
                  ],
                ),
                Column(
                  crossAxisAlignment: CrossAxisAlignment.end,
                  children: [
                    Text(
                      'Процент',
                      style: TextStyle(
                        color: Colors.grey[600],
                        fontSize: 13,
                      ),
                    ),
                    const SizedBox(height: 4),
                    Text(
                      '${pnlPercent.toStringAsFixed(2)}%',
                      style: Theme.of(context).textTheme.headlineMedium?.copyWith(
                            color: pnl >= 0 ? Colors.green : Colors.red,
                            fontWeight: FontWeight.bold,
                          ),
                    ),
                  ],
                ),
              ],
            ),
            const SizedBox(height: 16),
            Container(
              padding: const EdgeInsets.all(12),
              decoration: BoxDecoration(
                color: (pnl >= 0 ? Colors.green : Colors.red).withOpacity(0.1),
                borderRadius: BorderRadius.circular(8),
              ),
              child: Row(
                children: [
                  Icon(
                    pnl >= 0 ? Icons.arrow_upward : Icons.arrow_downward,
                    color: pnl >= 0 ? Colors.green : Colors.red,
                  ),
                  const SizedBox(width: 8),
                  Text(
                    pnl >= 0 ? 'Прибыль за период' : 'Убыток за период',
                    style: TextStyle(
                      color: pnl >= 0 ? Colors.green : Colors.red,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildPortfolioDistributionCard() {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Распределение портфеля',
              style: Theme.of(context).textTheme.titleLarge?.copyWith(
                    fontWeight: FontWeight.bold,
                  ),
            ),
            const SizedBox(height: 16),
            SizedBox(
              height: 250,
              child: _analytics!.portfolioDistribution.isEmpty
                  ? const Center(child: Text('Нет данных'))
                  : PieChart(
                      PieChartData(
                        sectionsSpace: 2,
                        centerSpaceRadius: 40,
                        sections: _buildPieChartSections(),
                      ),
                    ),
            ),
            const SizedBox(height: 16),
            ..._analytics!.portfolioDistribution.map((item) {
              return _buildDistributionItem(item);
            }).toList(),
          ],
        ),
      ),
    );
  }

  List<PieChartSectionData> _buildPieChartSections() {
    final colors = [
      const Color(0xFFFF9800),
      const Color(0xFF2196F3),
      const Color(0xFF4CAF50),
      const Color(0xFFFFEB3B),
      const Color(0xFF9C27B0),
      const Color(0xFFF44336),
      const Color(0xFF00BCD4),
    ];

    return _analytics!.portfolioDistribution.asMap().entries.map((entry) {
      final index = entry.key;
      final item = entry.value;
      final color = colors[index % colors.length];

      return PieChartSectionData(
        value: item.percentage,
        title: '${item.percentage.toStringAsFixed(1)}%',
        color: color,
        radius: 80,
        titleStyle: const TextStyle(
          fontSize: 14,
          fontWeight: FontWeight.bold,
          color: Colors.white,
        ),
      );
    }).toList();
  }

  Widget _buildDistributionItem(PortfolioDistribution item) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 4),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Row(
            children: [
              Container(
                width: 12,
                height: 12,
                decoration: BoxDecoration(
                  color: _getColorForTicker(item.ticker),
                  shape: BoxShape.circle,
                ),
              ),
              const SizedBox(width: 8),
              Text(
                item.ticker.toUpperCase(),
                style: const TextStyle(fontWeight: FontWeight.w600),
              ),
            ],
          ),
          Column(
            crossAxisAlignment: CrossAxisAlignment.end,
            children: [
              Text(
                currencyFormat.format(item.valueUsd),
                style: const TextStyle(fontWeight: FontWeight.bold),
              ),
              Text(
                '${item.percentage.toStringAsFixed(2)}%',
                style: TextStyle(
                  color: Colors.grey[600],
                  fontSize: 12,
                ),
              ),
            ],
          ),
        ],
      ),
    );
  }

  Widget _buildPerformanceChartCard() {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'История портфеля',
              style: Theme.of(context).textTheme.titleLarge?.copyWith(
                    fontWeight: FontWeight.bold,
                  ),
            ),
            const SizedBox(height: 16),
            SizedBox(
              height: 250,
              child: _analytics!.performanceChart.isEmpty
                  ? const Center(child: Text('Нет данных'))
                  : LineChart(
                      LineChartData(
                        gridData: FlGridData(
                          show: true,
                          drawVerticalLine: false,
                          getDrawingHorizontalLine: (value) {
                            return FlLine(
                              color: Colors.grey[300]!,
                              strokeWidth: 1,
                            );
                          },
                        ),
                        titlesData: FlTitlesData(
                          show: true,
                          bottomTitles: AxisTitles(
                            sideTitles: SideTitles(
                              showTitles: true,
                              getTitlesWidget: (value, meta) {
                                if (value.toInt() < 0 ||
                                    value.toInt() >= _analytics!.performanceChart.length) {
                                  return const SizedBox();
                                }
                                final dateStr = _analytics!.performanceChart[value.toInt()].date;
                                final date = DateTime.parse(dateStr);
                                return Text(
                                  DateFormat('dd/MM').format(date),
                                  style: const TextStyle(fontSize: 10),
                                );
                              },
                              reservedSize: 40,
                            ),
                          ),
                          leftTitles: AxisTitles(
                            sideTitles: SideTitles(
                              showTitles: true,
                              reservedSize: 60,
                              getTitlesWidget: (value, meta) {
                                return Text(
                                  '\$${value.toInt()}',
                                  style: const TextStyle(fontSize: 10),
                                );
                              },
                            ),
                          ),
                          topTitles: const AxisTitles(
                            sideTitles: SideTitles(showTitles: false),
                          ),
                          rightTitles: const AxisTitles(
                            sideTitles: SideTitles(showTitles: false),
                          ),
                        ),
                        borderData: FlBorderData(
                          show: true,
                          border: Border.all(color: Colors.grey[300]!),
                        ),
                        minX: 0,
                        maxX: (_analytics!.performanceChart.length - 1).toDouble(),
                        minY: _getMinValue(),
                        maxY: _getMaxValue(),
                        lineBarsData: [
                          LineChartBarData(
                            spots: _buildChartDataPoints(),
                            isCurved: true,
                            color: Theme.of(context).colorScheme.primary,
                            barWidth: 3,
                            dotData: const FlDotData(show: false),
                            belowBarData: BarAreaData(
                              show: true,
                              color: Theme.of(context).colorScheme.primary.withOpacity(0.3),
                            ),
                          ),
                        ],
                      ),
                    ),
            ),
          ],
        ),
      ),
    );
  }

  List<FlSpot> _buildChartDataPoints() {
    return _analytics!.performanceChart.asMap().entries.map((entry) {
      return FlSpot(
        entry.key.toDouble(),
        entry.value.totalValueUsd,
      );
    }).toList();
  }

  double _getMinValue() {
    if (_analytics!.performanceChart.isEmpty) return 0;
    final values = _analytics!.performanceChart.map((e) => e.totalValueUsd).toList();
    return (values.reduce((a, b) => a < b ? a : b) * 0.95);
  }

  double _getMaxValue() {
    if (_analytics!.performanceChart.isEmpty) return 100;
    final values = _analytics!.performanceChart.map((e) => e.totalValueUsd).toList();
    return (values.reduce((a, b) => a > b ? a : b) * 1.05);
  }

  Color _getColorForTicker(String ticker) {
    final colors = [
      const Color(0xFFFF9800),
      const Color(0xFF2196F3),
      const Color(0xFF4CAF50),
      const Color(0xFFFFEB3B),
      const Color(0xFF9C27B0),
      const Color(0xFFF44336),
      const Color(0xFF00BCD4),
    ];
    final index = _analytics!.portfolioDistribution.indexWhere(
      (item) => item.ticker.toLowerCase() == ticker.toLowerCase(),
    );
    return colors[index % colors.length];
  }
}
