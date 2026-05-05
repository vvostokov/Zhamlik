class CryptoAnalytics {
  final List<PortfolioDistribution> portfolioDistribution;
  final List<PerformanceChartDataPoint> performanceChart;

  CryptoAnalytics({
    required this.portfolioDistribution,
    required this.performanceChart,
  });

  factory CryptoAnalytics.fromJson(Map<String, dynamic> json) {
    final distributionList = json['portfolio_distribution'] as List? ?? [];
    final chartList = json['performance_chart'] as List? ?? [];

    return CryptoAnalytics(
      portfolioDistribution: distributionList
          .map((item) => PortfolioDistribution.fromJson(item as Map<String, dynamic>))
          .toList(),
      performanceChart: chartList
          .map((item) => PerformanceChartDataPoint.fromJson(item as Map<String, dynamic>))
          .toList(),
    );
  }
}

class PortfolioDistribution {
  final String ticker;
  final double percentage;
  final double valueUsd;

  PortfolioDistribution({
    required this.ticker,
    required this.percentage,
    required this.valueUsd,
  });

  factory PortfolioDistribution.fromJson(Map<String, dynamic> json) {
    return PortfolioDistribution(
      ticker: json['ticker'] as String,
      percentage: (json['percentage'] as num).toDouble(),
      valueUsd: (json['value_usd'] as num).toDouble(),
    );
  }
}

class PerformanceChartDataPoint {
  final String date;
  final double totalValueUsd;

  PerformanceChartDataPoint({
    required this.date,
    required this.totalValueUsd,
  });

  factory PerformanceChartDataPoint.fromJson(Map<String, dynamic> json) {
    return PerformanceChartDataPoint(
      date: json['date'] as String,
      totalValueUsd: (json['total_value_usd'] as num?)?.toDouble() ?? 0.0,
    );
  }
}
