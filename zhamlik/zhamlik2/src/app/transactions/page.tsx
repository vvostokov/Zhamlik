"use client";

import { useState } from "react";
import { motion } from "framer-motion";
import { Navigation } from "@/components/Navigation";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Plus,
  Search,
  Filter,
  ArrowUpDown,
  Calendar,
  Wallet,
  TrendingUp,
  TrendingDown,
  Edit,
  Trash2,
} from "lucide-react";

// Моковые данные
const transactions = [
  {
    id: 1,
    name: "Магнит Косметик",
    category: "Продукты",
    amount: -3450.5,
    date: "2024-01-27",
    account: "Тинькофф",
    type: "expense",
  },
  {
    id: 2,
    name: "Зарплата",
    category: "Доход",
    amount: 150000,
    date: "2024-01-26",
    account: "Сбербанк",
    type: "income",
  },
  {
    id: 3,
    name: "Netflix",
    category: "Развлечения",
    amount: -890,
    date: "2024-01-26",
    account: "Тинькофф",
    type: "expense",
  },
  {
    id: 4,
    name: "АЗС Лукойл",
    category: "Транспорт",
    amount: -5200,
    date: "2024-01-25",
    account: "Тинькофф",
    type: "expense",
  },
  {
    id: 5,
    name: "Подработка дизайн",
    category: "Фриланс",
    amount: 25000,
    date: "2024-01-25",
    account: "Тинькофф",
    type: "income",
  },
  {
    id: 6,
    name: "Пятерочка",
    category: "Продукты",
    amount: -2150.3,
    date: "2024-01-24",
    account: "Сбербанк",
    type: "expense",
  },
  {
    id: 7,
    name: "Аптека",
    category: "Здоровье",
    amount: -3200,
    date: "2024-01-24",
    account: "Сбербанк",
    type: "expense",
  },
  {
    id: 8,
    name: "Spotify",
    category: "Развлечения",
    amount: -699,
    date: "2024-01-23",
    account: "Тинькофф",
    type: "expense",
  },
];

export default function TransactionsPage() {
  const [searchTerm, setSearchTerm] = useState("");
  const [typeFilter, setTypeFilter] = useState("all");
  const [categoryFilter, setCategoryFilter] = useState("all");
  const [sortBy, setSortBy] = useState("date");

  const filteredTransactions = transactions
    .filter((tx) => {
      const matchesSearch =
        tx.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
        tx.category.toLowerCase().includes(searchTerm.toLowerCase());
      const matchesType = typeFilter === "all" || tx.type === typeFilter;
      const matchesCategory =
        categoryFilter === "all" || tx.category === categoryFilter;
      return matchesSearch && matchesType && matchesCategory;
    })
    .sort((a, b) => {
      if (sortBy === "date") return new Date(b.date).getTime() - new Date(a.date).getTime();
      if (sortBy === "amount") return Math.abs(b.amount) - Math.abs(a.amount);
      return 0;
    });

  const total = filteredTransactions.reduce(
    (acc, tx) => acc + tx.amount,
    0
  );
  const income = filteredTransactions
    .filter((tx) => tx.amount > 0)
    .reduce((acc, tx) => acc + tx.amount, 0);
  const expenses = filteredTransactions
    .filter((tx) => tx.amount < 0)
    .reduce((acc, tx) => acc + tx.amount, 0);

  return (
    <div className="flex">
      <Navigation />

      <main className="flex-1 lg:ml-64 pt-16 lg:pt-0">
        <div className="p-4 lg:p-8 max-w-7xl mx-auto">
          {/* Header */}
          <motion.div
            initial={{ opacity: 0, y: -20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5 }}
            className="mb-8"
          >
            <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
              <div>
                <h1 className="text-3xl lg:text-4xl font-bold bg-gradient-to-r from-slate-900 to-slate-700 dark:from-slate-100 dark:to-slate-300 bg-clip-text text-transparent">
                  Транзакции
                </h1>
                <p className="text-slate-600 dark:text-slate-400 mt-2">
                  Управление вашими доходами и расходами
                </p>
              </div>
              <Button className="bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700 shadow-lg shadow-blue-500/30">
                <Plus className="w-4 h-4 mr-2" />
                Добавить
              </Button>
            </div>
          </motion.div>

          {/* Stats Cards */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-6">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.1 }}
              className="bg-gradient-to-br from-green-500 to-emerald-600 rounded-2xl p-6 text-white shadow-lg shadow-green-500/30"
            >
              <div className="flex items-center justify-between mb-2">
                <span className="text-green-100">Доходы</span>
                <TrendingUp className="w-5 h-5" />
              </div>
              <p className="text-2xl font-bold">
                {income.toLocaleString()} ₽
              </p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2 }}
              className="bg-gradient-to-br from-red-500 to-rose-600 rounded-2xl p-6 text-white shadow-lg shadow-red-500/30"
            >
              <div className="flex items-center justify-between mb-2">
                <span className="text-red-100">Расходы</span>
                <TrendingDown className="w-5 h-5" />
              </div>
              <p className="text-2xl font-bold">
                {Math.abs(expenses).toLocaleString()} ₽
              </p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.3 }}
              className="bg-gradient-to-br from-blue-500 to-indigo-600 rounded-2xl p-6 text-white shadow-lg shadow-blue-500/30"
            >
              <div className="flex items-center justify-between mb-2">
                <span className="text-blue-100">Баланс</span>
                <Wallet className="w-5 h-5" />
              </div>
              <p className="text-2xl font-bold">
                {total.toLocaleString()} ₽
              </p>
            </motion.div>
          </div>

          {/* Filters */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.4 }}
            className="bg-white/80 dark:bg-slate-900/80 backdrop-blur-xl rounded-2xl p-6 border border-slate-200/50 dark:border-slate-800/50 shadow-lg mb-6"
          >
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                <Input
                  placeholder="Поиск..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10"
                />
              </div>

              <Select value={typeFilter} onValueChange={setTypeFilter}>
                <SelectTrigger>
                  <SelectValue placeholder="Тип" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Все типы</SelectItem>
                  <SelectItem value="income">Доходы</SelectItem>
                  <SelectItem value="expense">Расходы</SelectItem>
                </SelectContent>
              </Select>

              <Select value={categoryFilter} onValueChange={setCategoryFilter}>
                <SelectTrigger>
                  <SelectValue placeholder="Категория" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Все категории</SelectItem>
                  <SelectItem value="Продукты">Продукты</SelectItem>
                  <SelectItem value="Транспорт">Транспорт</SelectItem>
                  <SelectItem value="Развлечения">Развлечения</SelectItem>
                  <SelectItem value="Здоровье">Здоровье</SelectItem>
                </SelectContent>
              </Select>

              <Select value={sortBy} onValueChange={setSortBy}>
                <SelectTrigger>
                  <SelectValue placeholder="Сортировка" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="date">По дате</SelectItem>
                  <SelectItem value="amount">По сумме</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </motion.div>

          {/* Transactions Table */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.5 }}
            className="bg-white/80 dark:bg-slate-900/80 backdrop-blur-xl rounded-2xl border border-slate-200/50 dark:border-slate-800/50 shadow-lg overflow-hidden"
          >
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-slate-50/50 dark:bg-slate-800/50">
                  <tr>
                    <th className="px-6 py-4 text-left text-xs font-semibold text-slate-600 dark:text-slate-400 uppercase tracking-wider">
                      Транзакция
                    </th>
                    <th className="px-6 py-4 text-left text-xs font-semibold text-slate-600 dark:text-slate-400 uppercase tracking-wider">
                      Категория
                    </th>
                    <th className="px-6 py-4 text-left text-xs font-semibold text-slate-600 dark:text-slate-400 uppercase tracking-wider">
                      Счёт
                    </th>
                    <th className="px-6 py-4 text-left text-xs font-semibold text-slate-600 dark:text-slate-400 uppercase tracking-wider">
                      Дата
                    </th>
                    <th className="px-6 py-4 text-right text-xs font-semibold text-slate-600 dark:text-slate-400 uppercase tracking-wider">
                      Сумма
                    </th>
                    <th className="px-6 py-4 text-right text-xs font-semibold text-slate-600 dark:text-slate-400 uppercase tracking-wider">
                      Действия
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-200/50 dark:divide-slate-800/50">
                  {filteredTransactions.map((tx, index) => (
                    <motion.tr
                      key={tx.id}
                      initial={{ opacity: 0, x: -20 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: 0.6 + index * 0.05 }}
                      className="hover:bg-slate-50/50 dark:hover:bg-slate-800/50 transition-colors"
                    >
                      <td className="px-6 py-4">
                        <div className="font-medium text-slate-900 dark:text-slate-100">
                          {tx.name}
                        </div>
                      </td>
                      <td className="px-6 py-4">
                        <Badge
                          variant="outline"
                          className="border-slate-300 dark:border-slate-700"
                        >
                          {tx.category}
                        </Badge>
                      </td>
                      <td className="px-6 py-4 text-slate-600 dark:text-slate-400">
                        {tx.account}
                      </td>
                      <td className="px-6 py-4 text-slate-600 dark:text-slate-400">
                        {new Date(tx.date).toLocaleDateString("ru-RU")}
                      </td>
                      <td className="px-6 py-4 text-right">
                        <span
                          className={`font-semibold ${
                            tx.amount > 0
                              ? "text-green-600 dark:text-green-400"
                              : "text-slate-900 dark:text-slate-100"
                          }`}
                        >
                          {tx.amount > 0 ? "+" : ""}
                          {tx.amount.toLocaleString()} ₽
                        </span>
                      </td>
                      <td className="px-6 py-4 text-right">
                        <div className="flex items-center justify-end gap-2">
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-8 w-8 p-0 hover:bg-blue-100 dark:hover:bg-blue-900/30"
                          >
                            <Edit className="w-4 h-4" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-8 w-8 p-0 hover:bg-red-100 dark:hover:bg-red-900/30"
                          >
                            <Trash2 className="w-4 h-4 text-red-600 dark:text-red-400" />
                          </Button>
                        </div>
                      </td>
                    </motion.tr>
                  ))}
                </tbody>
              </table>
            </div>

            {filteredTransactions.length === 0 && (
              <div className="text-center py-12">
                <Wallet className="w-16 h-16 mx-auto text-slate-300 dark:text-slate-700 mb-4" />
                <p className="text-slate-600 dark:text-slate-400">
                  Транзакции не найдены
                </p>
              </div>
            )}
          </motion.div>
        </div>
      </main>
    </div>
  );
}
