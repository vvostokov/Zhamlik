class Account {
  final int id;
  final String name;
  final String type;
  final String currency;
  final double balance;
  final bool isActive;
  final String? notes;
  final double? balanceRub;

  Account({
    required this.id,
    required this.name,
    required this.type,
    required this.currency,
    required this.balance,
    required this.isActive,
    this.notes,
    this.balanceRub,
  });

  factory Account.fromJson(Map<String, dynamic> json) {
    return Account(
      id: json['id'] as int,
      name: json['name'] as String,
      type: json['type'] as String,
      currency: json['currency'] as String,
      balance: (json['balance'] as num).toDouble(),
      isActive: json['is_active'] as bool? ?? true,
      notes: json['notes'] as String?,
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
      'is_active': isActive,
      'notes': notes,
      'balance_rub': balanceRub,
    };
  }

  String get typeDisplay {
    switch (type) {
      case 'bank_account':
        return 'Банковский счет';
      case 'bank_card':
        return 'Банковская карта';
      case 'credit':
        return 'Кредитная карта';
      case 'deposit':
        return 'Вклад';
      case 'cash':
        return 'Наличные';
      case 'ewallet':
        return 'Электронный кошелек';
      default:
        return type;
    }
  }
}
