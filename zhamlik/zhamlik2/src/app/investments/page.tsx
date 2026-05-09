"use client";

import { useState } from "react";
import { motion } from "framer-motion";
import { Navigation } from "@/components/Navigation";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  TrendingUp,
  TrendingDown,
  DollarSign,
  Bitcoin,
  BarChart3,
  Plus,
  RefreshCw,
} from "lucide-react";

// Моковые данные
const cryptoAssets = [
  {
    id: 1,
    symbol: "BTC",
    name: "Bitcoin",
    amount: 0.05,
    currentPrice: 4500000,
    change24h: 2.5,
    value: 225000,
  },
  {
    id: 2,
    symbol: "ETH",
    name: "Ethereum",
    amount: 1.5,
    currentPrice: 185000,
    change24h: -1.2,
    value: 277500,
  },
  {
    id: 3,
    symbol: "USDT",
    name: "Tether",
    amount: 1000,
    currentPrice: 92.5,
    change24h: 0.01,
    value: 92500,
  },
];

const stockAssets = [
  {
    id: 1,
    symbol: "SBER",
    name: "Сбербанк",
    amount: 100,
    currentPrice: 267.5,
    change24h: 1.8,
    value: 26750,
  },
  {
    id: 2,
    symbol: "GAZP",
    name: "Газпром",
    amount: 50,
    currentPrice: 162.3,
    change24h: -0.5,
    value: 8115,
  },
  {
    id: 3,
    symbol: "YNDX",
    name: "Яндекс",
    amount: 20,
    currentPrice: 3840,
    change24h: 3.2,
    value: 76800,
  },
];

const platforms = [
  { id: 1, name: "Bybit", type: "crypto", balance: 250000 },
  { id: 2, name: "Тинькофф", type: "stocks", balance: 111665 },
];

export default function InvestmentsPage() {
  const [activeTab, setActiveTab] = useState("overview");
  const [isRefreshing, setIsRefreshing] = useState(false);

  const totalCrypto = cryptoAssets.reduce((acc, asset) => acc + asset.value, 0);
  const totalStocks = stockAssets.reduce((acc, asset) => acc + asset.value, 0);
  const totalValue = totalCrypto + totalStocks;

  const handleRefresh = () => {
    setIsRefreshing(true);
    setTimeout(() => setIsRefreshing(false), 2000);
  };

  return (
    <div className="flex">
      <Navigation />

      <main className="flex-1 lg:ml-64 pt-16 lg:pt-0">
        <div className="p-4 lg:p-8 max-w-7xl mx-auto">
          {/* Header */}
          <motion.div
            initial={{ opacity: 0, y: -20 }}
            animate={{ opacity: 1, y: 0 }}
            className="mb-8 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4"
          >
            <div>
              <h1 className="text-3xl lg:text-4xl font-bold bg-gradient-to-r from-slate-900 to-slate-700 dark:from-slate-100 dark:to-slate-300 bg-clip-text text-transparent">
                Инвестиции
              </h1>
              <p className="text-slate-600 dark:text-slate-400 mt-2">
                Криптовалюты и акции
              </p>
            </div>
            <div className="flex gap-2">
              <Button
                variant="outline"
                onClick={handleRefresh}
                disabled={isRefreshing}
              >
                <RefreshCw className={`w-4 h-4 mr-2 ${isRefreshing ? "animate-spin" : ""}`} />
                Обновить
              </Button>
              <Button className="bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700 shadow-lg shadow-blue-500/30">
                <Plus className="w-4 h-4 mr-2" />
                Добавить
              </Button>
            </div>
          </motion.div>

          {/* Total Value Card */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.1 }}
            className="bg-gradient-to-r from-purple-500 via-pink-500 to-indigo-600 rounded-2xl p-8 text-white shadow-xl shadow-purple-500/30 mb-8"
          >
            <div className="flex items-center justify-between">
              <div>
                <p className="text-purple-100 mb-2">Общий портфель</p>
                <p className="text-4xl lg:text-5xl font-bold">
                  {totalValue.toLocaleString()} ₽
                </p>
                <p className="text-purple-100 mt-2 flex items-center gap-2">
                  <TrendingUp className="w-5 h-5" />
                  <span className="font-semibold">+5.2% за месяц</span>
                </p>
              </div>
              <div className="hidden sm:block">
                <BarChart3 className="w-24 h-24 opacity-50" />
              </div>
            </div>
          </motion.div>

          {/* Platform Cards */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8"
          >
            {platforms.map((platform, index) => (
              <Card
                key={platform.id}
                className="p-6 hover:shadow-xl transition-all duration-300 border-2"
              >
                <div className="flex items-center justify-between mb-4">
                  <div
                    className={`p-3 rounded-xl bg-gradient-to-br ${
                      platform.type === "crypto"
                        ? "from-orange-500 to-amber-600"
                        : "from-blue-500 to-indigo-600"
                    } text-white`}
                  >
                    {platform.type === "crypto" ? (
                      <Bitcoin className="w-6 h-6" />
                    ) : (
                      <BarChart3 className="w-6 h-6" />
                    )}
                  </div>
                  <Badge variant="outline">{platform.type}</Badge>
                </div>
                <h3 className="font-semibold text-slate-900 dark:text-slate-100 mb-1">
                  {platform.name}
                </h3>
                <p className="text-2xl font-bold text-slate-900 dark:text-slate-100">
                  {platform.balance.toLocaleString()} ₽
                </p>
              </Card>
            ))}
          </motion.div>

          {/* Tabs */}
          <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
            <TabsList className="grid w-full max-w-md grid-cols-3">
              <TabsTrigger value="overview">Обзор</TabsTrigger>
              <TabsTrigger value="crypto">Крипто</TabsTrigger>
              <TabsTrigger value="stocks">Акции</TabsTrigger>
            </TabsList>

            {/* Overview Tab */}
            <TabsContent value="overview">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                {/* Crypto */}
                <motion.div
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.3 }}
                >
                  <Card className="p-6">
                    <div className="flex items-center justify-between mb-6">
                      <div className="flex items-center gap-3">
                        <div className="p-3 rounded-xl bg-gradient-to-br from-orange-500 to-amber-600 text-white">
                          <Bitcoin className="w-6 h-6" />
                        </div>
                        <div>
                          <h3 className="text-xl font-bold text-slate-900 dark:text-slate-100">
                            Криптовалюты
                          </h3>
                        </div>
                      </div>
                    </div>

                    <div className="space-y-4">
                      {cryptoAssets.map((asset, index) => (
                        <div
                          key={asset.id}
                          className="flex items-center justify-between p-4 rounded-xl bg-slate-50 dark:bg-slate-800 hover:bg-slate-100 dark:hover:bg-slate-700 transition-colors"
                        >
                          <div className="flex items-center gap-3">
                            <div className="w-10 h-10 rounded-full bg-gradient-to-br from-orange-500 to-amber-600 flex items-center justify-center text-white font-bold">
                              {asset.symbol[0]}
                            </div>
                            <div>
                              <p className="font-semibold text-slate-900 dark:text-slate-100">
                                {asset.name}
                              </p>
                              <p className="text-sm text-slate-600 dark:text-slate-400">
                                {asset.amount} {asset.symbol}
                              </p>
                            </div>
                          </div>
                          <div className="text-right">
                            <p className="font-semibold text-slate-900 dark:text-slate-100">
                              {asset.value.toLocaleString()} ₽
                            </p>
                            <p
                              className={`text-sm flex items-center gap-1 ${
                                asset.change24h >= 0
                                  ? "text-green-600 dark:text-green-400"
                                  : "text-red-600 dark:text-red-400"
                              }`}
                            >
                              {asset.change24h >= 0 ? (
                                <TrendingUp className="w-3 h-3" />
                              ) : (
                                <TrendingDown className="w-3 h-3" />
                              )}
                              {Math.abs(asset.change24h)}%
                            </p>
                          </div>
                        </div>
                      ))}
                    </div>

                    <div className="mt-6 pt-6 border-t border-slate-200 dark:border-slate-800">
                      <div className="flex justify-between items-center">
                        <span className="text-slate-600 dark:text-slate-400">
                          Всего:
                        </span>
                        <span className="text-xl font-bold text-slate-900 dark:text-slate-100">
                          {totalCrypto.toLocaleString()} ₽
                        </span>
                      </div>
                    </div>
                  </Card>
                </motion.div>

                {/* Stocks */}
                <motion.div
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.4 }}
                >
                  <Card className="p-6">
                    <div className="flex items-center justify-between mb-6">
                      <div className="flex items-center gap-3">
                        <div className="p-3 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-600 text-white">
                          <BarChart3 className="w-6 h-6" />
                        </div>
                        <div>
                          <h3 className="text-xl font-bold text-slate-900 dark:text-slate-100">
                            Акции
                          </h3>
                        </div>
                      </div>
                    </div>

                    <div className="space-y-4">
                      {stockAssets.map((asset, index) => (
                        <div
                          key={asset.id}
                          className="flex items-center justify-between p-4 rounded-xl bg-slate-50 dark:bg-slate-800 hover:bg-slate-100 dark:hover:bg-slate-700 transition-colors"
                        >
                          <div className="flex items-center gap-3">
                            <div className="w-10 h-10 rounded-full bg-gradient-to-br from-blue-500 to-indigo-600 flex items-center justify-center text-white font-bold">
                              {asset.symbol[0]}
                            </div>
                            <div>
                              <p className="font-semibold text-slate-900 dark:text-slate-100">
                                {asset.name}
                              </p>
                              <p className="text-sm text-slate-600 dark:text-slate-400">
                                {asset.amount} акций
                              </p>
                            </div>
                          </div>
                          <div className="text-right">
                            <p className="font-semibold text-slate-900 dark:text-slate-100">
                              {asset.value.toLocaleString()} ₽
                            </p>
                            <p
                              className={`text-sm flex items-center gap-1 ${
                                asset.change24h >= 0
                                  ? "text-green-600 dark:text-green-400"
                                  : "text-red-600 dark:text-red-400"
                              }`}
                            >
                              {asset.change24h >= 0 ? (
                                <TrendingUp className="w-3 h-3" />
                              ) : (
                                <TrendingDown className="w-3 h-3" />
                              )}
                              {Math.abs(asset.change24h)}%
                            </p>
                          </div>
                        </div>
                      ))}
                    </div>

                    <div className="mt-6 pt-6 border-t border-slate-200 dark:border-slate-800">
                      <div className="flex justify-between items-center">
                        <span className="text-slate-600 dark:text-slate-400">
                          Всего:
                        </span>
                        <span className="text-xl font-bold text-slate-900 dark:text-slate-100">
                          {totalStocks.toLocaleString()} ₽
                        </span>
                      </div>
                    </div>
                  </Card>
                </motion.div>
              </div>
            </TabsContent>

            {/* Crypto Tab */}
            <TabsContent value="crypto">
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {cryptoAssets.map((asset, index) => (
                  <motion.div
                    key={asset.id}
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: index * 0.1 }}
                  >
                    <Card className="p-6 hover:shadow-xl transition-all duration-300">
                      <div className="flex items-start justify-between mb-4">
                        <div className="p-3 rounded-xl bg-gradient-to-br from-orange-500 to-amber-600 text-white">
                          <Bitcoin className="w-6 h-6" />
                        </div>
                        <Badge
                          variant={asset.change24h >= 0 ? "default" : "destructive"}
                          className="bg-green-600"
                        >
                          {asset.change24h > 0 ? "+" : ""}
                          {asset.change24h}%
                        </Badge>
                      </div>

                      <h3 className="font-bold text-slate-900 dark:text-slate-100 mb-1">
                        {asset.name}
                      </h3>
                      <p className="text-sm text-slate-600 dark:text-slate-400 mb-4">
                        {asset.symbol}
                      </p>

                      <div className="space-y-2">
                        <div className="flex justify-between text-sm">
                          <span className="text-slate-600 dark:text-slate-400">
                            Количество:
                          </span>
                          <span className="font-medium text-slate-900 dark:text-slate-100">
                            {asset.amount}
                          </span>
                        </div>
                        <div className="flex justify-between text-sm">
                          <span className="text-slate-600 dark:text-slate-400">
                            Цена:
                          </span>
                          <span className="font-medium text-slate-900 dark:text-slate-100">
                            {asset.currentPrice.toLocaleString()} $
                          </span>
                        </div>
                      </div>

                      <div className="mt-4 pt-4 border-t border-slate-200 dark:border-slate-800">
                        <p className="text-sm text-slate-600 dark:text-slate-400 mb-1">
                          Стоимость:
                        </p>
                        <p className="text-xl font-bold text-slate-900 dark:text-slate-100">
                          {asset.value.toLocaleString()} ₽
                        </p>
                      </div>
                    </Card>
                  </motion.div>
                ))}
              </div>
            </TabsContent>

            {/* Stocks Tab */}
            <TabsContent value="stocks">
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {stockAssets.map((asset, index) => (
                  <motion.div
                    key={asset.id}
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: index * 0.1 }}
                  >
                    <Card className="p-6 hover:shadow-xl transition-all duration-300">
                      <div className="flex items-start justify-between mb-4">
                        <div className="p-3 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-600 text-white">
                          <BarChart3 className="w-6 h-6" />
                        </div>
                        <Badge
                          variant={asset.change24h >= 0 ? "default" : "destructive"}
                          className="bg-green-600"
                        >
                          {asset.change24h > 0 ? "+" : ""}
                          {asset.change24h}%
                        </Badge>
                      </div>

                      <h3 className="font-bold text-slate-900 dark:text-slate-100 mb-1">
                        {asset.name}
                      </h3>
                      <p className="text-sm text-slate-600 dark:text-slate-400 mb-4">
                        {asset.symbol}
                      </p>

                      <div className="space-y-2">
                        <div className="flex justify-between text-sm">
                          <span className="text-slate-600 dark:text-slate-400">
                            Количество:
                          </span>
                          <span className="font-medium text-slate-900 dark:text-slate-100">
                            {asset.amount} шт
                          </span>
                        </div>
                        <div className="flex justify-between text-sm">
                          <span className="text-slate-600 dark:text-slate-400">
                            Цена:
                          </span>
                          <span className="font-medium text-slate-900 dark:text-slate-100">
                            {asset.currentPrice.toLocaleString()} ₽
                          </span>
                        </div>
                      </div>

                      <div className="mt-4 pt-4 border-t border-slate-200 dark:border-slate-800">
                        <p className="text-sm text-slate-600 dark:text-slate-400 mb-1">
                          Стоимость:
                        </p>
                        <p className="text-xl font-bold text-slate-900 dark:text-slate-100">
                          {asset.value.toLocaleString()} ₽
                        </p>
                      </div>
                    </Card>
                  </motion.div>
                ))}
              </div>
            </TabsContent>
          </Tabs>
        </div>
      </main>
    </div>
  );
}
