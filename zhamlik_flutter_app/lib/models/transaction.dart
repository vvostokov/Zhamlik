class Transaction {
  final int id;
  final double amount;
  final double? toAmount;
  final String type;
  final DateTime date;
  final String? description;
  final String? merchant;
  final String? counterparty;
  final int? accountId;
  final String? accountName;
  final int? toAccountId;
  final int? categoryId;
  final int? debtId;
  final List<TransactionItem>? items;

  Transaction({
    required this.id,
    required this.amount,
    this.toAmount,
    required this.type,
    required this.date,
    this.description,
    this.merchant,
    this.counterparty,
    this.accountId,
    this.accountName,
    this.toAccountId,
    this.categoryId,
    this.debtId,
    this.items,
  });

  factory Transaction.fromJson(Map<String, dynamic> json) {
    return Transaction(
      id: json['id'] as int,
      amount: (json['amount'] as num).toDouble(),
      toAmount: json['to_amount'] != null
          ? (json['to_amount'] as num).toDouble()
          : null,
      type: json['type'] as String,
      date: json['date'] != null
          ? DateTime.parse(json['date'] as String)
          : DateTime.now(), // Fallback to current time if not provided
      description: json['description'] as String?,
      merchant: json['merchant'] as String?,
      counterparty: json['counterparty'] as String?,
      accountId: json['account_id'] as int?,
      accountName: json['account'] as String?,
      toAccountId: json['to_account_id'] as int?,
      categoryId: json['category_id'] as int?,
      debtId: json['debt_id'] as int?,
      items: json['items'] != null
          ? (json['items'] as List)
              .map((item) => TransactionItem.fromJson(item))
              .toList()
          : null,
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'amount': amount,
      'to_amount': toAmount,
      'type': type,
      'date': date.toIso8601String(),
      'description': description,
      'merchant': merchant,
      'counterparty': counterparty,
      'account_id': accountId,
      'account': accountName,
      'to_account_id': toAccountId,
      'category_id': categoryId,
      'debt_id': debtId,
      'items': items?.map((item) => item.toJson()).toList(),
    };
  }
}

class TransactionItem {
  final int id;
  final String name;
  final double quantity;
  final double price;
  final double total;
  final int? categoryId;

  TransactionItem({
    required this.id,
    required this.name,
    required this.quantity,
    required this.price,
    required this.total,
    this.categoryId,
  });

  factory TransactionItem.fromJson(Map<String, dynamic> json) {
    return TransactionItem(
      id: json['id'] as int,
      name: json['name'] as String,
      quantity: (json['quantity'] as num).toDouble(),
      price: (json['price'] as num).toDouble(),
      total: (json['total'] as num).toDouble(),
      categoryId: json['category_id'] as int?,
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'name': name,
      'quantity': quantity,
      'price': price,
      'total': total,
      'category_id': categoryId,
    };
  }
}
