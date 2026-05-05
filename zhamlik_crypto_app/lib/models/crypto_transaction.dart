class CryptoTransaction {
  final int id;
  final String type;
  final String? asset1Ticker;
  final double amount1;
  final String? asset2Ticker;
  final double amount2;
  final DateTime? timestamp;
  final String? platformName;

  CryptoTransaction({
    required this.id,
    required this.type,
    this.asset1Ticker,
    required this.amount1,
    this.asset2Ticker,
    required this.amount2,
    this.timestamp,
    this.platformName,
  });

  factory CryptoTransaction.fromJson(Map<String, dynamic> json) {
    return CryptoTransaction(
      id: json['id'] as int,
      type: json['type'] as String,
      asset1Ticker: json['asset1_ticker'] as String?,
      amount1: (json['amount1'] as num).toDouble(),
      asset2Ticker: json['asset2_ticker'] as String?,
      amount2: (json['amount2'] as num).toDouble(),
      timestamp: json['timestamp'] != null
          ? DateTime.parse(json['timestamp'] as String)
          : null,
      platformName: json['platform_name'] as String?,
    );
  }

  String get typeDisplay {
    switch (type.toLowerCase()) {
      case 'deposit':
        return 'Пополнение';
      case 'withdrawal':
        return 'Вывод';
      case 'trade':
        return 'Обмен';
      case 'transfer':
        return 'Перевод';
      default:
        return type;
    }
  }
}
