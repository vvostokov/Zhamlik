"use client";

import { useState } from "react";
import { motion } from "framer-motion";
import { Navigation } from "@/components/Navigation";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Label } from "@/components/ui/label";
import {
  Plus,
  ArrowDown,
  ArrowUp,
  Calendar,
  User,
  History,
  Exchange,
  Search,
  Filter,
} from "lucide-react";

// Моковые данные
const debts = [
  {
    id: 1,
    counterparty: "Иван Иванов",
    type: "i_owe",
    initialAmount: 50000,
    repaidAmount: 20000,
    currency: "RUB",
    dueDate: "2024-02-15",
    status: "active",
  },
  {
    id: 2,
    counterparty: "Петр Петров",
    type: "owed_to_me",
    initialAmount: 30000,
    repaidAmount: 5000,
    currency: "RUB",
    dueDate: "2024-03-01",
    status: "active",
  },
  {
    id: 3,
    counterparty: "Сидор Сидоров",
    type: "i_owe",
    initialAmount: 15000,
    repaidAmount: 15000,
    currency: "RUB",
    dueDate: "2024-01-20",
    status: "repaid",
  },
];

const recurringPayments = [
  {
    id: 1,
    name: "Аренда квартиры",
    amount: 35000,
    currency: "RUB",
    frequency: "monthly",
    nextDueDate: "2024-02-01",
    category: "Жильё",
  },
  {
    id: 2,
    name: "Интернет",
    amount: 890,
    currency: "RUB",
    frequency: "monthly",
    nextDueDate: "2024-02-05",
    category: "Связь",
  },
  {
    id: 3,
    name: "Netflix",
    amount: 699,
    currency: "RUB",
    frequency: "monthly",
    nextDueDate: "2024-02-10",
    category: "Развлечения",
  },
];

export default function DebtsPage() {
  const [activeTab, setActiveTab] = useState("debts");
  const [searchTerm, setSearchTerm] = useState("");
  const [filterType, setFilterType] = useState("all");

  const filteredDebts = debts.filter((debt) => {
    const matchesSearch = debt.counterparty
      .toLowerCase()
      .includes(searchTerm.toLowerCase());
    const matchesType =
      filterType === "all" || debt.type === filterType;
    return matchesSearch && matchesType;
  });

  const totalOwed = filteredDebts
    .filter((d) => d.type === "i_owe" && d.status === "active")
    .reduce((acc, d) => acc + (d.initialAmount - d.repaidAmount), 0);
  const totalOwedToMe = filteredDebts
    .filter((d) => d.type === "owed_to_me" && d.status === "active")
    .reduce((acc, d) => acc + (d.initialAmount - d.repaidAmount), 0);

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
              Обязательства
            </h1>
            <p className="text-slate-600 dark:text-slate-400">
              Управление долгами и регулярными платежами
            </p>
          </motion.div>

          {/* Stats */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-6">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.1 }}
              className="bg-gradient-to-br from-red-500 to-rose-600 rounded-2xl p-6 text-white shadow-lg shadow-red-500/30"
            >
              <div className="flex items-center justify-between mb-2">
                <span className="text-red-100">Я должен</span>
                <ArrowDown className="w-5 h-5" />
              </div>
              <p className="text-3xl font-bold">
                {totalOwed.toLocaleString()} ₽
              </p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2 }}
              className="bg-gradient-to-br from-green-500 to-emerald-600 rounded-2xl p-6 text-white shadow-lg shadow-green-500/30"
            >
              <div className="flex items-center justify-between mb-2">
                <span className="text-green-100">Мне должны</span>
                <ArrowUp className="w-5 h-5" />
              </div>
              <p className="text-3xl font-bold">
                {totalOwedToMe.toLocaleString()} ₽
              </p>
            </motion.div>
          </div>

          {/* Tabs */}
          <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
            <TabsList className="grid w-full max-w-lg grid-cols-3">
              <TabsTrigger value="debts">Долги</TabsTrigger>
              <TabsTrigger value="recurring">Регулярные</TabsTrigger>
              <TabsTrigger value="counterparties">Контрагенты</TabsTrigger>
            </TabsList>

            {/* Debts Tab */}
            <TabsContent value="debts">
              {/* Filters */}
              <Card className="p-4 mb-6">
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                  <div className="relative">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                    <Input
                      placeholder="Поиск по контрагенту..."
                      value={searchTerm}
                      onChange={(e) => setSearchTerm(e.target.value)}
                      className="pl-10"
                    />
                  </div>
                  <div className="flex gap-2">
                    <Button
                      variant={filterType === "all" ? "default" : "outline"}
                      onClick={() => setFilterType("all")}
                      className="flex-1"
                    >
                      Все
                    </Button>
                    <Button
                      variant={filterType === "i_owe" ? "default" : "outline"}
                      onClick={() => setFilterType("i_owe")}
                      className="flex-1"
                    >
                      Я должен
                    </Button>
                    <Button
                      variant={filterType === "owed_to_me" ? "default" : "outline"}
                      onClick={() => setFilterType("owed_to_me")}
                      className="flex-1"
                    >
                      Мне должны
                    </Button>
                  </div>
                </div>
              </Card>

              {/* Debts List */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {filteredDebts.map((debt, index) => {
                  const remaining = debt.initialAmount - debt.repaidAmount;
                  const isOwed = debt.type === "i_owe";

                  return (
                    <motion.div
                      key={debt.id}
                      initial={{ opacity: 0, y: 20 }}
                      animate={{ opacity: 1, y: 0 }}
                      transition={{ delay: index * 0.1 }}
                    >
                      <Card className="overflow-hidden border-2 hover:shadow-xl transition-all duration-300">
                        <div
                          className={`p-4 bg-gradient-to-r ${
                            isOwed
                              ? "from-red-500 to-rose-500"
                              : "from-green-500 to-emerald-500"
                          } bg-opacity-10`}
                        >
                          <div className="flex items-center justify-between mb-3">
                            <div className="flex items-center gap-3">
                              <div
                                className={`p-2 rounded-lg bg-gradient-to-br ${
                                  isOwed
                                    ? "from-red-500 to-rose-500"
                                    : "from-green-500 to-emerald-500"
                                } text-white`}
                              >
                                <User className="w-5 h-5" />
                              </div>
                              <div>
                                <h3 className="font-semibold text-slate-900 dark:text-slate-100">
                                  {debt.counterparty}
                                </h3>
                                <p className="text-xs text-slate-600 dark:text-slate-400">
                                  {debt.status === "active" ? "Активен" : "Погашен"}
                                </p>
                              </div>
                            </div>
                          </div>

                          <div className="space-y-2">
                            <div className="flex justify-between text-sm">
                              <span className="text-slate-600 dark:text-slate-400">
                                Остаток:
                              </span>
                              <span
                                className={`font-semibold ${
                                  isOwed
                                    ? "text-red-600 dark:text-red-400"
                                    : "text-green-600 dark:text-green-400"
                                }`}
                              >
                                {remaining.toLocaleString()} {debt.currency}
                              </span>
                            </div>
                            <div className="flex justify-between text-sm">
                              <span className="text-slate-600 dark:text-slate-400">
                                Погашено:
                              </span>
                              <span className="font-medium text-slate-900 dark:text-slate-100">
                                {debt.repaidAmount.toLocaleString()} {debt.currency}
                              </span>
                            </div>
                            <div className="flex justify-between text-sm">
                              <span className="text-slate-600 dark:text-slate-400">
                                Срок:
                              </span>
                              <span className="font-medium text-slate-900 dark:text-slate-100 flex items-center gap-1">
                                <Calendar className="w-3 h-3" />
                                {new Date(debt.dueDate).toLocaleDateString("ru-RU")}
                              </span>
                            </div>
                          </div>

                          {debt.status === "active" && (
                            <div className="flex gap-2 mt-4">
                              <Button className="flex-1" size="sm">
                                Погасить
                              </Button>
                              <Button variant="outline" size="sm" className="flex-1">
                                История
                              </Button>
                            </div>
                          )}
                        </div>
                      </Card>
                    </motion.div>
                  );
                })}
              </div>
            </TabsContent>

            {/* Recurring Payments Tab */}
            <TabsContent value="recurring">
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {recurringPayments.map((payment, index) => (
                  <motion.div
                    key={payment.id}
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: index * 0.1 }}
                  >
                    <Card className="p-6 hover:shadow-xl transition-all duration-300">
                      <div className="flex items-start justify-between mb-4">
                        <div className="p-3 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-600 text-white">
                          <Exchange className="w-6 h-6" />
                        </div>
                        <Badge variant="outline">{payment.frequency}</Badge>
                      </div>

                      <h3 className="font-semibold text-slate-900 dark:text-slate-100 mb-2">
                        {payment.name}
                      </h3>

                      <div className="space-y-2 mb-4">
                        <div className="flex justify-between text-sm">
                          <span className="text-slate-600 dark:text-slate-400">
                            Сумма:
                          </span>
                          <span className="font-semibold text-slate-900 dark:text-slate-100">
                            {payment.amount.toLocaleString()} {payment.currency}
                          </span>
                        </div>
                        <div className="flex justify-between text-sm">
                          <span className="text-slate-600 dark:text-slate-400">
                            Следующий:
                          </span>
                          <span className="font-medium text-slate-900 dark:text-slate-100">
                            {new Date(payment.nextDueDate).toLocaleDateString("ru-RU")}
                          </span>
                        </div>
                      </div>

                      <Badge variant="secondary" className="w-fit">
                        {payment.category}
                      </Badge>
                    </Card>
                  </motion.div>
                ))}
              </div>
            </TabsContent>

            {/* Counterparties Tab */}
            <TabsContent value="counterparties">
              <Card className="p-12 text-center">
                <User className="w-16 h-16 mx-auto text-slate-300 dark:text-slate-700 mb-4" />
                <h3 className="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-2">
                  Контрагенты
                </h3>
                <p className="text-slate-600 dark:text-slate-400">
                  Баланс по каждому контрагенту с возможностью фильтрации
                </p>
              </Card>
            </TabsContent>
          </Tabs>
        </div>
      </main>
    </div>
  );
}
