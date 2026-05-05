class Overview {
  final double totalBalance;
  final double monthlyIncome;
  final double monthlyExpense;
  final List<AccountSummary> accounts;
  final List<TransactionSummary> recentTransactions;

  Overview({
    required this.totalBalance,
    required this.monthlyIncome,
    required this.monthlyExpense,
    required this.accounts,
    required this.recentTransactions,
  });

  factory Overview.fromJson(Map<String, dynamic> json) {
    return Overview(
      totalBalance: json['total_balance'] != null
          ? (json['total_balance'] as num).toDouble()
          : 0.0,
      monthlyIncome: json['monthly_income'] != null
          ? (json['monthly_income'] as num).toDouble()
          : 0.0,
      monthlyExpense: json['monthly_expense'] != null
          ? (json['monthly_expense'] as num).toDouble()
          : 0.0,
      accounts: (json['accounts'] as List?)
          ?.map((item) => AccountSummary.fromJson(item))
          .toList() ?? [],
      recentTransactions: (json['recent_transactions'] as List?)
          ?.map((item) => TransactionSummary.fromJson(item))
          .toList() ?? [],
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'total_balance': totalBalance,
      'monthly_income': monthlyIncome,
      'monthly_expense': monthlyExpense,
      'accounts': accounts.map((a) => a.toJson()).toList(),
      'recent_transactions':
          recentTransactions.map((t) => t.toJson()).toList(),
    };
  }
}

class AccountSummary {
  final int id;
  final String name;
  final String? type;
  final String currency;
  final double balance;
  final double? balanceRub;

  AccountSummary({
    required this.id,
    required this.name,
    this.type,
    required this.currency,
    required this.balance,
    this.balanceRub,
  });

  factory AccountSummary.fromJson(Map<String, dynamic> json) {
    return AccountSummary(
      id: json['id'] as int,
      name: json['name'] as String,
      type: json['type'] as String?,
      currency: json['currency'] as String? ?? 'RUB',
      balance: (json['balance'] as num).toDouble(),
      balanceRub: json['balance_rub'] != null
          ? (json['balance_rub'] as num).toDouble()
          : null,
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'name': name,
      'type': type,
      'currency': currency,
      'balance': balance,
      'balance_rub': balanceRub,
    };
  }
}

class TransactionSummary {
  final int id;
  final double amount;
  final String? type;
  final DateTime date;
  final String description;
  final String? accountName;

  TransactionSummary({
    required this.id,
    required this.amount,
    this.type,
    required this.date,
    required this.description,
    this.accountName,
  });

  factory TransactionSummary.fromJson(Map<String, dynamic> json) {
    return TransactionSummary(
      id: json['id'] as int,
      amount: (json['amount'] as num).toDouble(),
      type: json['type'] as String?,
      date: DateTime.parse(json['date'] as String),
      description: json['description'] as String? ?? '',
      accountName: json['account_name'] as String? ??
                   json['account'] as String?, // Fallback to 'account' field
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'amount': amount,
      'type': type,
      'date': date.toIso8601String(),
      'description': description,
      'account_name': accountName,
    };
  }
}
