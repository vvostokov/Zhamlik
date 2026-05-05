import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:intl/intl.dart';

import '../services/api_service.dart';
import '../models/debt.dart';

class DebtsScreen extends StatefulWidget {
  const DebtsScreen({super.key});

  @override
  State<DebtsScreen> createState() => _DebtsScreenState();
}

class _DebtsScreenState extends State<DebtsScreen> {
  final NumberFormat currencyFormat = NumberFormat.currency(
    symbol: '₽',
    decimalDigits: 2,
  );

  List<Debt> _iOwe = [];
  List<Debt> _owedToMe = [];
  bool _isLoading = true;

  @override
  void initState() {
    super.initState();
    _loadDebts();
  }

  Future<void> _loadDebts() async {
    setState(() {
      _isLoading = true;
    });

    final apiService = Provider.of<ApiService>(context, listen: false);
    final debtsData = await apiService.getDebts();

    if (mounted) {
      setState(() {
        // debtsData returns Map<String, dynamic> with List<Debt> as values
        _iOwe = (debtsData['i_owe'] as List?)?.cast<Debt>() ?? [];
        _owedToMe = (debtsData['owed_to_me'] as List?)?.cast<Debt>() ?? [];
        _isLoading = false;
      });
    }
  }

  double _getTotalIOwe() {
    return _iOwe.fold(0.0, (sum, debt) => sum + debt.remaining);
  }

  double _getTotalOwedToMe() {
    return _owedToMe.fold(0.0, (sum, debt) => sum + debt.remaining);
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Долги'),
      ),
      body: _isLoading
          ? const Center(child: CircularProgressIndicator())
          : RefreshIndicator(
              onRefresh: _loadDebts,
              child: SingleChildScrollView(
                physics: const AlwaysScrollableScrollPhysics(),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    _buildSummaryCards(),
                    _buildDebtsSection(
                      title: 'Я должен',
                      debts: _iOwe,
                      isIOwe: true,
                      totalAmount: _getTotalIOwe(),
                      icon: Icons.arrow_upward,
                      color: Colors.red,
                    ),
                    _buildDebtsSection(
                      title: 'Мне должны',
                      debts: _owedToMe,
                      isIOwe: false,
                      totalAmount: _getTotalOwedToMe(),
                      icon: Icons.arrow_downward,
                      color: Colors.green,
                    ),
                  ],
                ),
              ),
            ),
      floatingActionButton: FloatingActionButton(
        onPressed: () => _showAddDebtDialog(context),
        child: const Icon(Icons.add),
      ),
    );
  }

  Widget _buildSummaryCards() {
    return Padding(
      padding: const EdgeInsets.all(16),
      child: Row(
        children: [
          Expanded(
            child: Card(
              elevation: 4,
              color: Colors.red.shade50,
              child: Padding(
                padding: const EdgeInsets.all(16),
                child: Column(
                  children: [
                    Row(
                      mainAxisAlignment: MainAxisAlignment.center,
                      children: [
                        Icon(Icons.arrow_upward, color: Colors.red.shade700, size: 20),
                        const SizedBox(width: 8),
                        Text(
                          'Я должен',
                          style: Theme.of(context).textTheme.titleMedium?.copyWith(
                                color: Colors.red.shade700,
                              ),
                        ),
                      ],
                    ),
                    const SizedBox(height: 12),
                    Text(
                      currencyFormat.format(_getTotalIOwe()),
                      style: Theme.of(context).textTheme.headlineSmall?.copyWith(
                            color: Colors.red.shade700,
                            fontWeight: FontWeight.bold,
                          ),
                    ),
                  ],
                ),
              ),
            ),
          ),
          const SizedBox(width: 16),
          Expanded(
            child: Card(
              elevation: 4,
              color: Colors.green.shade50,
              child: Padding(
                padding: const EdgeInsets.all(16),
                child: Column(
                  children: [
                    Row(
                      mainAxisAlignment: MainAxisAlignment.center,
                      children: [
                        Icon(Icons.arrow_downward, color: Colors.green.shade700, size: 20),
                        const SizedBox(width: 8),
                        Text(
                          'Мне должны',
                          style: Theme.of(context).textTheme.titleMedium?.copyWith(
                                color: Colors.green.shade700,
                              ),
                        ),
                      ],
                    ),
                    const SizedBox(height: 12),
                    Text(
                      currencyFormat.format(_getTotalOwedToMe()),
                      style: Theme.of(context).textTheme.headlineSmall?.copyWith(
                            color: Colors.green.shade700,
                            fontWeight: FontWeight.bold,
                          ),
                    ),
                  ],
                ),
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildDebtsSection({
    required String title,
    required List<Debt> debts,
    required bool isIOwe,
    required double totalAmount,
    required IconData icon,
    required MaterialColor color,
  }) {
    if (debts.isEmpty) {
      return const SizedBox.shrink();
    }

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Padding(
          padding: const EdgeInsets.fromLTRB(16, 16, 16, 8),
          child: Row(
            children: [
              Icon(icon, color: color, size: 24),
              const SizedBox(width: 12),
              Text(
                title,
                style: Theme.of(context).textTheme.titleLarge?.copyWith(
                      color: color,
                      fontWeight: FontWeight.bold,
                    ),
              ),
              const SizedBox(width: 8),
              Container(
                padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
                decoration: BoxDecoration(
                  color: color.withOpacity(0.1),
                  borderRadius: BorderRadius.circular(12),
                ),
                child: Text(
                  '${debts.length}',
                  style: TextStyle(
                    color: color,
                    fontWeight: FontWeight.bold,
                  ),
                ),
              ),
            ],
          ),
        ),
        ListView.builder(
          shrinkWrap: true,
          physics: const NeverScrollableScrollPhysics(),
          padding: const EdgeInsets.symmetric(horizontal: 16),
          itemCount: debts.length,
          itemBuilder: (context, index) {
            return _buildDebtCard(debts[index], isIOwe, color);
          },
        ),
        const Divider(height: 32),
      ],
    );
  }

  Widget _buildDebtCard(Debt debt, bool isIOwe, MaterialColor color) {
    final isOverdue = debt.isOverdue;

    return Card(
      margin: const EdgeInsets.only(bottom: 12),
      elevation: 2,
      child: ListTile(
        leading: Container(
          width: 48,
          height: 48,
          decoration: BoxDecoration(
            color: color.withOpacity(0.1),
            borderRadius: BorderRadius.circular(24),
          ),
          child: Icon(
            isIOwe ? Icons.arrow_upward : Icons.arrow_downward,
            color: color,
          ),
        ),
        title: Text(
          debt.counterparty,
          style: const TextStyle(fontWeight: FontWeight.w600),
        ),
        subtitle: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            if (debt.description != null && debt.description!.isNotEmpty)
              Text(debt.description!),
            if (debt.dueDate != null) ...[
              const SizedBox(height: 4),
              Row(
                children: [
                  Icon(
                    Icons.calendar_today,
                    size: 12,
                    color: isOverdue ? Colors.red : Colors.grey[600],
                  ),
                  const SizedBox(width: 4),
                  Text(
                    DateFormat('dd MMM yyyy').format(DateTime.parse(debt.dueDate!)),
                    style: TextStyle(
                      color: isOverdue ? Colors.red : Colors.grey[600],
                      fontSize: 12,
                    ),
                  ),
                  if (isOverdue) ...[
                    const SizedBox(width: 8),
                    Container(
                      padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
                      decoration: BoxDecoration(
                        color: Colors.red,
                        borderRadius: BorderRadius.circular(4),
                      ),
                      child: const Text(
                        'Просрочен',
                        style: TextStyle(
                          color: Colors.white,
                          fontSize: 10,
                          fontWeight: FontWeight.bold,
                        ),
                      ),
                    ),
                  ],
                ],
              ),
            ],
            const SizedBox(height: 4),
            Text(
              'Статус: ${debt.statusDisplay}',
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
              currencyFormat.format(debt.remaining),
              style: TextStyle(
                color: color,
                fontWeight: FontWeight.bold,
                fontSize: 16,
              ),
            ),
            if (debt.repaidAmount > 0) ...[
              const SizedBox(height: 4),
              Text(
                'Оплачено: ${currencyFormat.format(debt.repaidAmount)}',
                style: TextStyle(
                  color: Colors.grey[600],
                  fontSize: 11,
                ),
              ),
            ],
          ],
        ),
      ),
    );
  }

  void _showAddDebtDialog(BuildContext context) {
    final counterpartyController = TextEditingController();
    final amountController = TextEditingController();
    final descriptionController = TextEditingController();
    // Default to "Я должен"
    bool isIOwe = true;

    showDialog(
      context: context,
      builder: (context) => StatefulBuilder(
        builder: (context, setDialogState) => AlertDialog(
          title: const Text('Добавить долг'),
          content: SingleChildScrollView(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                SegmentedButton<bool>(
                  segments: const [
                    ButtonSegment(
                      value: true,
                      label: Text('Я должен'),
                      icon: Icon(Icons.arrow_upward),
                    ),
                    ButtonSegment(
                      value: false,
                      label: Text('Мне должны'),
                      icon: Icon(Icons.arrow_downward),
                    ),
                  ],
                  selected: {isIOwe},
                  onSelectionChanged: (Set<bool> selected) {
                    setDialogState(() {
                      isIOwe = selected.first;
                    });
                  },
                ),
                const SizedBox(height: 16),
                TextField(
                  controller: counterpartyController,
                  decoration: const InputDecoration(
                    labelText: 'Контрагент',
                    prefixIcon: Icon(Icons.person),
                    border: OutlineInputBorder(),
                  ),
                ),
                const SizedBox(height: 16),
                TextField(
                  controller: amountController,
                  decoration: const InputDecoration(
                    labelText: 'Сумма',
                    prefixIcon: Icon(Icons.money),
                    border: OutlineInputBorder(),
                  ),
                  keyboardType: TextInputType.number,
                ),
                const SizedBox(height: 16),
                TextField(
                  controller: descriptionController,
                  decoration: const InputDecoration(
                    labelText: 'Описание (необязательно)',
                    prefixIcon: Icon(Icons.description),
                    border: OutlineInputBorder(),
                  ),
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
                final counterparty = counterpartyController.text.trim();
                final amount = double.tryParse(amountController.text);

                if (counterparty.isEmpty) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(content: Text('Введите контрагента')),
                  );
                  return;
                }

                if (amount == null || amount <= 0) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(content: Text('Введите корректную сумму')),
                  );
                  return;
                }

                Navigator.of(context).pop();

                final apiService = Provider.of<ApiService>(context, listen: false);
                final result = await apiService.createDebt(
                  counterparty: counterparty,
                  amount: amount,
                  isIOwe: isIOwe,
                  description: descriptionController.text.trim().isEmpty
                      ? null
                      : descriptionController.text.trim(),
                );

                if (result != null) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(
                      content: Text('Долг добавлен'),
                      backgroundColor: Colors.green,
                    ),
                  );
                  _loadDebts();
                } else {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(
                      content: Text('Ошибка при добавлении долга'),
                      backgroundColor: Colors.red,
                    ),
                  );
                }
              },
              child: const Text('Добавить'),
            ),
          ],
        ),
      ),
    );
  }
}
