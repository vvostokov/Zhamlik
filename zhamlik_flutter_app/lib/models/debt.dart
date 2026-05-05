class Debt {
  final int id;
  final String debtType; // 'i_owe' or 'owed_to_me'
  final String counterparty;
  final double initialAmount;
  final double repaidAmount;
  final double remaining;
  final String currency;
  final String status; // 'active', 'repaid', 'cancelled'
  final String? dueDate;
  final String? description;

  Debt({
    required this.id,
    required this.debtType,
    required this.counterparty,
    required this.initialAmount,
    required this.repaidAmount,
    required this.remaining,
    required this.currency,
    required this.status,
    this.dueDate,
    this.description,
  });

  factory Debt.fromJson(Map<String, dynamic> json) {
    // Handle simplified API response (from GET /api/mobile/debts)
    final amount = json['amount'] != null
        ? (json['amount'] as num).toDouble()
        : (json['initial_amount'] as num?)?.toDouble() ?? 0.0;

    return Debt(
      id: json['id'] as int,
      debtType: json['debt_type'] as String? ?? 'i_owe',
      counterparty: json['counterparty'] as String,
      initialAmount: amount,
      repaidAmount: (json['repaid_amount'] as num?)?.toDouble() ?? 0.0,
      remaining: (json['remaining'] as num?)?.toDouble() ?? amount,
      currency: json['currency'] as String? ?? 'RUB',
      status: json['status'] as String? ?? 'active',
      dueDate: json['due_date'] as String?,
      description: json['description'] as String?,
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'debt_type': debtType,
      'counterparty': counterparty,
      'initial_amount': initialAmount,
      'repaid_amount': repaidAmount,
      'remaining': remaining,
      'currency': currency,
      'status': status,
      'due_date': dueDate,
      'description': description,
    };
  }

  bool get isIOwe => debtType == 'i_owe';
  bool get isActive => status == 'active';
  bool get isOverdue {
    if (dueDate == null) return false;
    return DateTime.now().isAfter(DateTime.parse(dueDate!));
  }

  String get typeDisplay => isIOwe ? 'Я должен' : 'Мне должны';
  String get statusDisplay {
    switch (status) {
      case 'active':
        return 'Активен';
      case 'repaid':
        return 'Погашен';
      case 'cancelled':
        return 'Отменен';
      default:
        return status;
    }
  }
}

class RecurringPayment {
  final int id;
  final String description;
  final double amount;
  final String currency;
  final String frequency; // 'monthly', 'yearly', etc.
  final String nextDueDate;
  final String? counterparty;

  RecurringPayment({
    required this.id,
    required this.description,
    required this.amount,
    required this.currency,
    required this.frequency,
    required this.nextDueDate,
    this.counterparty,
  });

  factory RecurringPayment.fromJson(Map<String, dynamic> json) {
    return RecurringPayment(
      id: json['id'] as int,
      description: json['description'] as String,
      amount: (json['amount'] as num).toDouble(),
      currency: json['currency'] as String,
      frequency: json['frequency'] as String,
      nextDueDate: json['next_due_date'] as String,
      counterparty: json['counterparty'] as String?,
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'description': description,
      'amount': amount,
      'currency': currency,
      'frequency': frequency,
      'next_due_date': nextDueDate,
      'counterparty': counterparty,
    };
  }

  String get frequencyDisplay {
    switch (frequency) {
      case 'daily':
        return 'Ежедневно';
      case 'weekly':
        return 'Еженедельно';
      case 'monthly':
        return 'Ежемесячно';
      case 'yearly':
        return 'Ежегодно';
      default:
        return frequency;
    }
  }

  bool get isDueSoon {
    final dueDate = DateTime.parse(nextDueDate);
    final now = DateTime.now();
    final difference = dueDate.difference(now).inDays;
    return difference >= 0 && difference <= 7;
  }

  bool get isOverdue {
    final dueDate = DateTime.parse(nextDueDate);
    return DateTime.now().isAfter(dueDate);
  }
}
