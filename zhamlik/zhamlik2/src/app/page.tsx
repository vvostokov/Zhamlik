"use client";

import { motion } from "framer-motion";
import { Navigation } from "@/components/Navigation";
import {
  TrendingUp,
  TrendingDown,
  Wallet,
  CreditCard,
  DollarSign,
  Activity,
  ArrowUpRight,
  ArrowDownRight,
} from "lucide-react";

const stats = [
  {
    title: "Общий баланс",
    value: "1 234 567 ₽",
    change: "+12.5%",
    trend: "up",
    icon: Wallet,
    color: "from-blue-500 to-cyan-500",
  },
  {
    title: "Доходы за месяц",
    value: "234 500 ₽",
    change: "+8.2%",
    trend: "up",
    icon: TrendingUp,
    color: "from-green-500 to-emerald-500",
  },
  {
    title: "Расходы за месяц",
    value: "145 200 ₽",
    change: "-3.1%",
    trend: "down",
    icon: TrendingDown,
    color: "from-red-500 to-rose-500",
  },
  {
    title: "Инвестиции",
    value: "890 300 ₽",
    change: "+15.7%",
    trend: "up",
    icon: DollarSign,
    color: "from-purple-500 to-indigo-500",
  },
];

const recentTransactions = [
  {
    id: 1,
    name: "Магнит",
    category: "Продукты",
    amount: -3450.5,
    date: "Сегодня",
  },
  {
    id: 2,
    name: "Зарплата",
    category: "Доход",
    amount: 150000,
    date: "Вчера",
  },
  {
    id: 3,
    name: "Netflix",
    category: "Развлечения",
    amount: -890,
    date: "Вчера",
  },
  {
    id: 4,
    name: "АЗС",
    category: "Транспорт",
    amount: -5200,
    date: "2 дня назад",
  },
];

export default function Home() {
  return (
    <div className="flex">
      <Navigation />

      <main className="flex-1 lg:ml-64 pt-16 lg:pt-0">
        <div className="p-4 lg:p-8">
          {/* Header */}
          <motion.div
            initial={{ opacity: 0, y: -20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5 }}
            className="mb-8"
          >
            <h1 className="text-3xl lg:text-4xl font-bold bg-gradient-to-r from-slate-900 to-slate-700 dark:from-slate-100 dark:to-slate-300 bg-clip-text text-transparent mb-2">
              Добро пожаловать!
            </h1>
            <p className="text-slate-600 dark:text-slate-400">
              Вот обзор ваших финансов на сегодня
            </p>
          </motion.div>

          {/* Stats Grid */}
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 lg:gap-6 mb-8">
            {stats.map((stat, index) => {
              const Icon = stat.icon;
              const isPositive = stat.trend === "up";

              return (
                <motion.div
                  key={stat.title}
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: index * 0.1, duration: 0.4 }}
                  className="relative group"
                >
                  <div className="absolute inset-0 bg-gradient-to-br opacity-0 group-hover:opacity-100 transition-opacity duration-300 rounded-2xl blur-xl"
                    style={{ backgroundImage: `linear-gradient(to bottom right, var(--tw-gradient-stops))` }}
                  />
                  <div className="relative bg-white/80 dark:bg-slate-900/80 backdrop-blur-xl rounded-2xl p-6 border border-slate-200/50 dark:border-slate-800/50 shadow-lg hover:shadow-xl transition-all duration-300">
                    <div className="flex items-start justify-between mb-4">
                      <div className={`p-3 rounded-xl bg-gradient-to-br ${stat.color} shadow-lg`}>
                        <Icon className="w-6 h-6 text-white" />
                      </div>
                      <div className={`flex items-center gap-1 text-sm font-medium ${
                        isPositive ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"
                      }`}>
                        {stat.change}
                        {isPositive ? (
                          <ArrowUpRight className="w-4 h-4" />
                        ) : (
                          <ArrowDownRight className="w-4 h-4" />
                        )}
                      </div>
                    </div>
                    <p className="text-slate-600 dark:text-slate-400 text-sm mb-1">
                      {stat.title}
                    </p>
                    <p className="text-2xl font-bold text-slate-900 dark:text-slate-100">
                      {stat.value}
                    </p>
                  </div>
                </motion.div>
              );
            })}
          </div>

          {/* Recent Transactions */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.4, duration: 0.5 }}
            className="bg-white/80 dark:bg-slate-900/80 backdrop-blur-xl rounded-2xl border border-slate-200/50 dark:border-slate-800/50 shadow-lg"
          >
            <div className="p-6 border-b border-slate-200/50 dark:border-slate-800/50">
              <div className="flex items-center justify-between">
                <div>
                  <h2 className="text-xl font-semibold text-slate-900 dark:text-slate-100">
                    Последние транзакции
                  </h2>
                  <p className="text-sm text-slate-600 dark:text-slate-400 mt-1">
                    Ваши последние операции
                  </p>
                </div>
                <button className="text-blue-600 hover:text-blue-700 text-sm font-medium">
                  Все →
                </button>
              </div>
            </div>

            <div className="divide-y divide-slate-200/50 dark:divide-slate-800/50">
              {recentTransactions.map((tx, index) => (
                <motion.div
                  key={tx.id}
                  initial={{ opacity: 0, x: -20 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: 0.5 + index * 0.1, duration: 0.3 }}
                  className="p-6 hover:bg-slate-50/50 dark:hover:bg-slate-800/50 transition-colors cursor-pointer"
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-4">
                      <div className={`p-3 rounded-xl ${
                        tx.amount > 0
                          ? "bg-green-100 dark:bg-green-900/30"
                          : "bg-red-100 dark:bg-red-900/30"
                      }`}>
                        <Activity className={`w-5 h-5 ${
                          tx.amount > 0
                            ? "text-green-600 dark:text-green-400"
                            : "text-red-600 dark:text-red-400"
                        }`} />
                      </div>
                      <div>
                        <p className="font-medium text-slate-900 dark:text-slate-100">
                          {tx.name}
                        </p>
                        <p className="text-sm text-slate-600 dark:text-slate-400">
                          {tx.category} · {tx.date}
                        </p>
                      </div>
                    </div>
                    <p className={`text-lg font-semibold ${
                      tx.amount > 0
                        ? "text-green-600 dark:text-green-400"
                        : "text-slate-900 dark:text-slate-100"
                    }`}>
                      {tx.amount > 0 ? "+" : ""}{tx.amount.toLocaleString()} ₽
                    </p>
                  </div>
                </motion.div>
              ))}
            </div>
          </motion.div>
        </div>
      </main>
    </div>
  );
}
