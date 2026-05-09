"use client";

import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Navigation } from "@/components/Navigation";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Plus,
  ChevronDown,
  ChevronRight,
  Edit,
  Trash2,
  MoreVertical,
  FolderOpen,
  Tag,
} from "lucide-react";

// Данные категорий
const expenseCategories = [
  {
    id: 1,
    name: "Продукты",
    subcategories: [
      { id: 11, name: "Продукты питания" },
      { id: 12, name: "Напитки" },
      { id: 13, name: "Снеки и сладости" },
    ],
  },
  {
    id: 2,
    name: "Транспорт",
    subcategories: [
      { id: 21, name: "Топливо" },
      { id: 22, name: "Общественный транспорт" },
      { id: 23, name: "Такси" },
      { id: 24, name: "Парковка" },
    ],
  },
  {
    id: 3,
    name: "Жильё и коммунальные",
    subcategories: [
      { id: 31, name: "Аренда" },
      { id: 32, name: "Коммунальные услуги" },
      { id: 33, name: "Ремонт" },
    ],
  },
  {
    id: 4,
    name: "Здоровье",
    subcategories: [
      { id: 41, name: "Лекарства" },
      { id: 42, name: "Врачи" },
      { id: 43, name: "Спорт" },
    ],
  },
];

const incomeCategories = [
  {
    id: 101,
    name: "Зарплата",
    subcategories: [
      { id: 111, name: "Основная" },
      { id: 112, name: "Премия" },
      { id: 113, name: "Надбавки" },
    ],
  },
  {
    id: 102,
    name: "Подработка",
    subcategories: [
      { id: 121, name: "Фриланс" },
      { id: 122, name: "Консалтинг" },
    ],
  },
  {
    id: 103,
    name: "Инвестиции",
    subcategories: [
      { id: 131, name: "Дивиденды" },
      { id: 132, name: "Проценты" },
    ],
  },
];

interface Category {
  id: number;
  name: string;
  subcategories: Subcategory[];
}

interface Subcategory {
  id: number;
  name: string;
}

interface CategoryCardProps {
  category: Category;
  type: "expense" | "income";
}

function CategoryCard({ category, type }: CategoryCardProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const colorClass = type === "expense" ? "from-red-500 to-rose-500" : "from-green-500 to-emerald-500";
  const borderClass = type === "expense" ? "border-red-500/30" : "border-green-500/30";

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className="group"
    >
      <Card className="overflow-hidden border-2 hover:border-opacity-100 transition-all duration-300 hover:shadow-xl">
        <div className={`p-6 bg-gradient-to-r ${colorClass} bg-opacity-10`}>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className={`p-2 rounded-lg bg-gradient-to-br ${colorClass} text-white`}>
                <FolderOpen className="w-5 h-5" />
              </div>
              <div>
                <h3 className="text-lg font-semibold">{category.name}</h3>
                <p className="text-sm opacity-75">
                  {category.subcategories.length} подкатегорий
                </p>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant="ghost"
                size="sm"
                className="h-8 w-8 p-0 hover:bg-white/20"
                onClick={() => setIsExpanded(!isExpanded)}
              >
                <motion.div
                  animate={{ rotate: isExpanded ? 180 : 0 }}
                  transition={{ duration: 0.2 }}
                >
                  <ChevronDown className="w-4 h-4" />
                </motion.div>
              </Button>
              <Button
                variant="ghost"
                size="sm"
                className="h-8 w-8 p-0 hover:bg-white/20"
              >
                <MoreVertical className="w-4 h-4" />
              </Button>
            </div>
          </div>
        </div>

        <AnimatePresence>
          {isExpanded && (
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: "auto", opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={{ duration: 0.3 }}
              className="overflow-hidden"
            >
              <div className="p-4 space-y-2">
                {category.subcategories.map((sub) => (
                  <motion.div
                    key={sub.id}
                    initial={{ opacity: 0, x: -20 }}
                    animate={{ opacity: 1, x: 0 }}
                    className="flex items-center justify-between p-3 rounded-lg hover:bg-slate-50 dark:hover:bg-slate-800 transition-colors group/item"
                  >
                    <div className="flex items-center gap-3">
                      <ChevronRight className="w-4 h-4 text-slate-400" />
                      <span className="text-slate-700 dark:text-slate-300">
                        {sub.name}
                      </span>
                    </div>
                    <div className="flex items-center gap-2 opacity-0 group-hover/item:opacity-100 transition-opacity">
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 w-7 p-0 hover:bg-blue-100 dark:hover:bg-blue-900/30"
                      >
                        <Edit className="w-3 h-3" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 w-7 p-0 hover:bg-red-100 dark:hover:bg-red-900/30"
                      >
                        <Trash2 className="w-3 h-3 text-red-600 dark:text-red-400" />
                      </Button>
                    </div>
                  </motion.div>
                ))}
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {!isExpanded && category.subcategories.length === 0 && (
          <div className="p-4">
            <Button
              variant="outline"
              size="sm"
              className="w-full border-dashed"
            >
              <Plus className="w-4 h-4 mr-2" />
              Добавить подкатегорию
            </Button>
          </div>
        )}
      </Card>
    </motion.div>
  );
}

export default function CategoriesPage() {
  const [activeTab, setActiveTab] = useState("expense");

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
                  Категории
                </h1>
                <p className="text-slate-600 dark:text-slate-400 mt-2">
                  Двухуровневая система категорий для доходов и расходов
                </p>
              </div>
              <Button className="bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700 shadow-lg shadow-blue-500/30">
                <Plus className="w-4 h-4 mr-2" />
                Добавить категорию
              </Button>
            </div>
          </motion.div>

          {/* Tabs */}
          <Tabs value={activeTab} onValueChange={setActiveTab} className="mb-6">
            <TabsList className="grid w-full max-w-md grid-cols-2">
              <TabsTrigger value="expense" className="gap-2">
                <Tag className="w-4 h-4" />
                Расходы
                <Badge variant="secondary">{expenseCategories.length}</Badge>
              </TabsTrigger>
              <TabsTrigger value="income" className="gap-2">
                <Wallet className="w-4 h-4" />
                Доходы
                <Badge variant="secondary">{incomeCategories.length}</Badge>
              </TabsTrigger>
            </TabsList>

            <TabsContent value="expense" className="mt-6">
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {expenseCategories.map((category, index) => (
                  <CategoryCard
                    key={category.id}
                    category={category}
                    type="expense"
                  />
                ))}
              </div>
            </TabsContent>

            <TabsContent value="income" className="mt-6">
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {incomeCategories.map((category, index) => (
                  <CategoryCard
                    key={category.id}
                    category={category}
                    type="income"
                  />
                ))}
              </div>
            </TabsContent>
          </Tabs>
        </div>
      </main>
    </div>
  );
}
