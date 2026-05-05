import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:intl/intl.dart';

import '../services/api_service.dart';
import '../models/account.dart';

class AccountsScreen extends StatefulWidget {
  const AccountsScreen({super.key});

  @override
  State<AccountsScreen> createState() => _AccountsScreenState();
}

class _AccountsScreenState extends State<AccountsScreen> {
  final NumberFormat currencyFormat = NumberFormat.currency(
    symbol: '₽',
    decimalDigits: 2,
  );
  List<Account> _accounts = [];
  bool _isLoading = true;

  @override
  void initState() {
    super.initState();
    _loadAccounts();
  }

  Future<void> _loadAccounts() async {
    setState(() {
      _isLoading = true;
    });

    final apiService = Provider.of<ApiService>(context, listen: false);
    final accounts = await apiService.getAccounts();

    if (mounted) {
      setState(() {
        _accounts = accounts;
        _isLoading = false;
      });
    }
  }

  double _getTotalBalance() {
    return _accounts.fold(0.0, (sum, account) => sum + (account.balanceRub ?? account.balance));
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Счета'),
      ),
      body: _isLoading
          ? const Center(child: CircularProgressIndicator())
          : RefreshIndicator(
              onRefresh: _loadAccounts,
              child: CustomScrollView(
                slivers: [
                  SliverToBoxAdapter(
                    child: Padding(
                      padding: const EdgeInsets.all(16),
                      child: Card(
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
                                currencyFormat.format(_getTotalBalance()),
                                style: Theme.of(context).textTheme.headlineMedium?.copyWith(
                                      color: Theme.of(context).colorScheme.primary,
                                      fontWeight: FontWeight.bold,
                                    ),
                              ),
                            ],
                          ),
                        ),
                      ),
                    ),
                  ),
                  _accounts.isEmpty
                      ? SliverFillRemaining(
                          child: Center(
                            child: Column(
                              mainAxisAlignment: MainAxisAlignment.center,
                              children: [
                                Icon(
                                  Icons.account_balance_wallet,
                                  size: 64,
                                  color: Colors.grey[400],
                                ),
                                const SizedBox(height: 16),
                                Text(
                                  'Нет счетов',
                                  style: Theme.of(context).textTheme.titleLarge?.copyWith(
                                        color: Colors.grey[600],
                                      ),
                                ),
                                const SizedBox(height: 16),
                                ElevatedButton.icon(
                                  onPressed: () => _showAddAccountDialog(context),
                                  icon: const Icon(Icons.add),
                                  label: const Text('Создать счет'),
                                ),
                              ],
                            ),
                          ),
                        )
                      : SliverPadding(
                          padding: const EdgeInsets.symmetric(horizontal: 16),
                          sliver: SliverList(
                            delegate: SliverChildBuilderDelegate(
                              (context, index) {
                                return _buildAccountCard(_accounts[index]);
                              },
                              childCount: _accounts.length,
                            ),
                          ),
                        ),
                ],
              ),
            ),
      floatingActionButton: FloatingActionButton(
        onPressed: () => _showAddAccountDialog(context),
        child: const Icon(Icons.add),
      ),
    );
  }

  Widget _buildAccountCard(Account account) {
    return Card(
      margin: const EdgeInsets.only(bottom: 12),
      child: ListTile(
        leading: Container(
          width: 48,
          height: 48,
          decoration: BoxDecoration(
            color: _getAccountColor(account.type).withOpacity(0.1),
            borderRadius: BorderRadius.circular(12),
          ),
          child: Icon(
            _getAccountIcon(account.type),
            color: _getAccountColor(account.type),
          ),
        ),
        title: Text(
          account.name,
          style: const TextStyle(fontWeight: FontWeight.w600),
        ),
        subtitle: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(account.typeDisplay),
            if (!account.isActive)
              Text(
                'Неактивен',
                style: TextStyle(
                  color: Colors.grey[600],
                  fontSize: 12,
                ),
              ),
          ],
        ),
        trailing: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          crossAxisAlignment: CrossAxisAlignment.end,
          children: [
            Text(
              currencyFormat.format(account.balanceRub ?? account.balance),
              style: TextStyle(
                fontSize: 16,
                fontWeight: FontWeight.bold,
                color: account.balance >= 0 ? Colors.green : Colors.red,
              ),
            ),
            Text(
              account.currency,
              style: TextStyle(
                fontSize: 12,
                color: Colors.grey[600],
              ),
            ),
          ],
        ),
      ),
    );
  }

  Color _getAccountColor(String type) {
    switch (type) {
      case 'bank_account':
        return Colors.blue;
      case 'bank_card':
        return Colors.indigo;
      case 'credit':
        return Colors.orange;
      case 'deposit':
        return Colors.green;
      case 'cash':
        return Colors.brown;
      case 'ewallet':
        return Colors.purple;
      default:
        return Colors.grey;
    }
  }

  IconData _getAccountIcon(String type) {
    switch (type) {
      case 'bank_account':
        return Icons.account_balance;
      case 'bank_card':
        return Icons.credit_card;
      case 'credit':
        return Icons.credit_score;
      case 'deposit':
        return Icons.savings;
      case 'cash':
        return Icons.money;
      case 'ewallet':
        return Icons.account_balance_wallet;
      default:
        return Icons.account_balance;
    }
  }

  void _showAddAccountDialog(BuildContext context) {
    final nameController = TextEditingController();
    final bankController = TextEditingController();
    final notesController = TextEditingController();
    String _selectedType = 'cash';
    String _selectedCurrency = 'RUB';

    final List<Map<String, dynamic>> accountTypes = [
      {'value': 'cash', 'label': 'Наличные', 'icon': Icons.money},
      {'value': 'bank_card', 'label': 'Банковская карта', 'icon': Icons.credit_card},
      {'value': 'bank_account', 'label': 'Банковский счет', 'icon': Icons.account_balance},
      {'value': 'credit', 'label': 'Кредитная карта', 'icon': Icons.credit_score},
      {'value': 'deposit', 'label': 'Вклад', 'icon': Icons.savings},
      {'value': 'ewallet', 'label': 'Электронный кошелек', 'icon': Icons.account_balance_wallet},
    ];

    final List<Map<String, dynamic>> currencies = [
      {'value': 'RUB', 'label': '₽ Рубль'},
      {'value': 'USD', 'label': '\$ Доллар'},
      {'value': 'EUR', 'label': '€ Евро'},
      {'value': 'GBP', 'label': '£ Фунт'},
      {'value': 'KZT', 'label': '₸ Тенге'},
    ];

    showDialog(
      context: context,
      builder: (context) => StatefulBuilder(
        builder: (context, setDialogState) => AlertDialog(
          title: const Text('Создать счет'),
          content: SingleChildScrollView(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                TextField(
                  controller: nameController,
                  decoration: const InputDecoration(
                    labelText: 'Название',
                    prefixIcon: Icon(Icons.account_balance_wallet),
                    border: OutlineInputBorder(),
                  ),
                ),
                const SizedBox(height: 16),
                DropdownButtonFormField<String>(
                  value: _selectedType,
                  decoration: const InputDecoration(
                    labelText: 'Тип счета',
                    prefixIcon: Icon(Icons.category),
                    border: OutlineInputBorder(),
                  ),
                  items: accountTypes.map((type) {
                    return DropdownMenuItem(
                      value: type['value'] as String,
                      child: Row(
                        children: [
                          Icon(type['icon'] as IconData, size: 20),
                          const SizedBox(width: 12),
                          Text(type['label'] as String),
                        ],
                      ),
                    );
                  }).toList(),
                  onChanged: (value) {
                    if (value != null) {
                      setDialogState(() {
                        _selectedType = value;
                      });
                    }
                  },
                ),
                const SizedBox(height: 16),
                DropdownButtonFormField<String>(
                  value: _selectedCurrency,
                  decoration: const InputDecoration(
                    labelText: 'Валюта',
                    prefixIcon: Icon(Icons.money),
                    border: OutlineInputBorder(),
                  ),
                  items: currencies.map((currency) {
                    return DropdownMenuItem(
                      value: currency['value'] as String,
                      child: Text(currency['label'] as String),
                    );
                  }).toList(),
                  onChanged: (value) {
                    if (value != null) {
                      setDialogState(() {
                        _selectedCurrency = value;
                      });
                    }
                  },
                ),
                if (_selectedType == 'bank_card' || _selectedType == 'bank_account') ...[
                  const SizedBox(height: 16),
                  TextField(
                    controller: bankController,
                    decoration: const InputDecoration(
                      labelText: 'Банк',
                      prefixIcon: Icon(Icons.business),
                      border: OutlineInputBorder(),
                    ),
                  ),
                ],
                const SizedBox(height: 16),
                TextField(
                  controller: notesController,
                  decoration: const InputDecoration(
                    labelText: 'Заметки (необязательно)',
                    prefixIcon: Icon(Icons.note),
                    border: OutlineInputBorder(),
                  ),
                  maxLines: 2,
                ),
              ],
            ),
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(context).pop(),
              child: const Text('Отмена'),
            ),
            ElevatedButton(
              onPressed: () async {
                final name = nameController.text.trim();
                if (name.isEmpty) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(content: Text('Введите название счета')),
                  );
                  return;
                }

                Navigator.of(context).pop();

                final apiService = Provider.of<ApiService>(context, listen: false);
                final result = await apiService.createAccount(
                  name: name,
                  type: _selectedType,
                  currency: _selectedCurrency,
                  bank: bankController.text.trim().isEmpty ? null : bankController.text.trim(),
                  notes: notesController.text.trim().isEmpty ? null : notesController.text.trim(),
                );

                if (result != null) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(
                      content: Text('Счет создан'),
                      backgroundColor: Colors.green,
                    ),
                  );
                  _loadAccounts();
                } else {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(
                      content: Text('Ошибка при создании счета'),
                      backgroundColor: Colors.red,
                    ),
                  );
                }
              },
              child: const Text('Создать'),
            ),
          ],
        ),
      ),
    );
  }
}
