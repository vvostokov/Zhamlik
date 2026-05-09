"use client";

import { useState } from "react";
import { motion } from "framer-motion";
import { Navigation } from "@/components/Navigation";
import { Card } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { BarChart3, TrendingUp, TrendingDown, PieChart, Calendar } from "lucide-react";

const mockData = {
  monthlyIncome: 234500,
  monthlyExpenses: 145200,
  byCategory: [
    { category: "Продукты", amount: 45000, color: "from-red-500 to-rose-500" },
    { category: "Транспорт", amount: 25000, color: "from-blue-500 to-cyan-500" },
    { category: "Развлечения", amount: 18000, color: "from-purple-500 to-pink-500" },
    { category: "Жильё", amount: 35000, color: "from-green-500 to-emerald-500" },
    { category: "Здоровье", amount: 12000, color: "from-orange-500 to-amber-500" },
  ],
  cashFlow: [
    { month: "Окт", income: 210000, expense: 138000 },
    { month: "Ноя", income: 225000, expense: 142000 },
    { month: "Дек", income: 234500, expense: 145200 },
  ],
};

export default function AnalyticsPage() {
  const [period, setPeriod] = useState("30d");

  return (
    <div className="flex">
      <Navigation />

      <main className="flex-1 lg:ml-64 pt-16 lg:pt-0">
        <div className="p-4 lg:p-8 max-w-7xl mx-auto">
          {/* Header */}
          <motion.div
            initial={{ opacity: 0, y: -20 }}
            animate={{ opacity: 1, y: 0 }}
            className="mb-8"
          >
            <h1 className="text-3xl lg:text-4xl font-bold bg-gradient-to-r from-slate-900 to-slate-700 dark:from-slate-100 dark:to-slate-300 bg-clip-text text-transparent mb-2">
              Аналитика
            </h1>
            <p className="text-slate-600 dark:text-slate-400">
              Детальный анализ ваших финансов
            </p>
          </motion.div>

          {/* Period Selector */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.1 }}
            className="mb-6"
          >
            <Select value={period} onValueChange={setPeriod}>
              <SelectTrigger className="w-full sm:w-64">
                <SelectValue placeholder="Период" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="7d">7 дней</SelectItem>
                <SelectItem value="30d">30 дней</SelectItem>
                <SelectItem value="3m">3 месяца</SelectItem>
                <SelectItem value="1y">1 год</SelectItem>
                <SelectItem value="all">Всё время</SelectItem>
              </SelectContent>
            </Select>
          </motion.div>

          {/* Summary Cards */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-8">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2 }}
              className="bg-gradient-to-br from-blue-500 to-indigo-600 rounded-2xl p-6 text-white shadow-xl shadow-blue-500/30"
            >
              <div className="flex items-center justify-between mb-2">
                <TrendingUp className="w-5 h-5" />
                <span className="text-blue-100">Доходы</span>
              </div>
              <p className="text-3xl font-bold">
                {mockData.monthlyIncome.toLocaleString()} ₽
              </p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.3 }}
              className="bg-gradient-to-br from-red-500 to-rose-600 rounded-2xl p-6 text-white shadow-xl shadow-red-500/30"
            >
              <div className="flex items-center justify-between mb-2">
                <TrendingDown className="w-5 h-5" />
                <span className="text-red-100">Расходы</span>
              </div>
              <p className="text-3xl font-bold">
                {mockData.monthlyExpenses.toLocaleString()} ₽
              </p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.4 }}
              className="bg-gradient-to-br from-green-500 to-emerald-600 rounded-2xl p-6 text-white shadow-xl shadow-green-500/30"
            >
              <div className="flex items-center justify-between mb-2">
                <BarChart3 className="w-5 h-5" />
                <span className="text-green-100">Баланс</span>
              </div>
              <p className="text-3xl font-bold">
                {(mockData.monthlyIncome - mockData.monthlyExpenses).toLocaleString()} ₽
              </p>
            </motion.div>
          </div>

          {/* Charts Grid */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Expenses by Category */}
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.5 }}
            >
              <Card className="p-6">
                <div className="flex items-center justify-between mb-6">
                  <div>
                    <h3 className="text-xl font-bold text-slate-900 dark:text-slate-100">
                      Расходы по категориям
                    </h3>
                    <p className="text-sm text-slate-600 dark:text-slate-400">
                      За выбранный период
                    </p>
                  </div>
                  <PieChart className="w-5 h-5 text-slate-400" />
                </div>

                <div className="space-y-4">
                  {mockData.byCategory.map((item, index) => {
                    const percentage = (
                      (item.amount / mockData.monthlyExpenses) *
                      100
                    ).toFixed(1);

                    return (
                      <div key={index}>
                        <div className="flex items-center justify-between mb-2">
                          <span className="text-sm font-medium text-slate-700 dark:text-slate-300">
                            {item.category}
                          </span>
                          <span className="text-sm text-slate-600 dark:text-slate-400">
                            {item.amount.toLocaleString()} ₽ ({percentage}%)
                          </span>
                        </div>
                        <div className="w-full bg-slate-200 dark:bg-slate-700 rounded-full h-2 overflow-hidden">
                          <motion.div
                            initial={{ width: 0 }}
                            animate={{ width: `${percentage}%` }}
                            transition={{ delay: 0.6 + index * 0.1, duration: 0.8 }}
                            className={`h-full bg-gradient-to-r ${item.color}`}
                          />
                        </div>
                      </div>
                    );
                  })}
                </div>
              </Card>
            </motion.div>

            {/* Cash Flow */}
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.6 }}
            >
              <Card className="p-6">
                <div className="flex items-center justify-between mb-6">
                  <div>
                    <h3 className="text-xl font-bold text-slate-900 dark:text-slate-100">
                      Денежный поток
                    </h3>
                    <p className="text-sm text-slate-600 dark:text-slate-400">
                      По месяцам
                    </p>
                  </div>
                  <Calendar className="w-5 h-5 text-slate-400" />
                </div>

                <div className="space-y-4">
                  {mockData.cashFlow.map((item, index) => (
                    <div
                      key={index}
                      className="flex items-center justify-between p-4 rounded-xl bg-slate-50 dark:bg-slate-800"
                    >
                      <span className="font-medium text-slate-900 dark:text-slate-100">
                        {item.month}
                      </span>
                      <div className="flex items-center gap-6">
                        <div className="text-center">
                          <p className="text-xs text-slate-600 dark:text-slate-400 mb-1">
                            Доход
                          </p>
                          <p className="text-sm font-semibold text-green-600 dark:text-green-400">
                            {item.income.toLocaleString()} ₽
                          </p>
                        </div>
                        <div className="text-center">
                          <p className="text-xs text-slate-600 dark:text-slate-400 mb-1">
                            Расход
                          </p>
                          <p className="text-sm font-semibold text-red-600 dark:text-red-400">
                            {item.expense.toLocaleString()} ₽
                          </p>
                        </div>
                        <div className="text-center">
                          <p className="text-xs text-slate-600 dark:text-slate-400 mb-1">
                            Баланс
                          </p>
                          <p
                            className={`text-sm font-bold ${
                              item.income - item.expense >= 0
                                ? "text-green-600 dark:text-green-400"
                                : "text-red-600 dark:text-red-400"
                            }`}
                          >
                            {(item.income - item.expense).toLocaleString()} ₽
                          </p>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </Card>
            </motion.div>
          </div>

          {/* Insights */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.7 }}
            className="mt-6"
          >
            <Card className="p-6 bg-gradient-to-r from-blue-50 to-indigo-50 dark:from-slate-800 dark:to-slate-900 border-2 border-blue-200 dark:border-slate-700">
              <div className="flex items-start gap-4">
                <div className="p-3 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-600 text-white">
                  <TrendingUp className="w-6 h-6" />
                </div>
                <div className="flex-1">
                  <h3 className="text-lg font-bold text-slate-900 dark:text-slate-100 mb-2">
                    Инсайты за месяц
                  </h3>
                  <ul className="space-y-2 text-sm text-slate-700 dark:text-slate-300">
                    <li className="flex items-start gap-2">
                      <span className="text-green-600 dark:text-green-400">✓</span>
                      <span>
                        Доходы выросли на <strong>12.5%</strong> по сравнению с прошлым месяцем
                      </span>
                    </li>
                    <li className="flex items-start gap-2">
                      <span className="text-red-600 dark:text-red-400">!</span>
                      <span>
                        Расходы на транспорт выросли на <strong>8%</strong>
                      </span>
                    </li>
                    <li className="flex items-start gap-2">
                      <span className="text-blue-600 dark:text-blue-400">→</span>
                      <span>
                        Наибольшие расходы: <strong>Продукты</strong> и <strong>Жильё</strong>
                      </span>
                    </li>
                  </ul>
                </div>
              </div>
            </Card>
          </motion.div>
        </div>
      </main>
    </div>
  );
}
