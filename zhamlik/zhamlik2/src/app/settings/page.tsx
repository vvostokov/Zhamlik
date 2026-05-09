"use client";

import { useState } from "react";
import { motion } from "framer-motion";
import { Navigation } from "@/components/Navigation";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Switch } from "@/components/ui/switch";
import {
  User,
  Bell,
  Shield,
  Palette,
  CreditCard,
  Database,
  Save,
} from "lucide-react";

export default function SettingsPage() {
  const [activeTab, setActiveTab] = useState("profile");
  const [notifications, setNotifications] = useState(true);
  const [darkMode, setDarkMode] = useState(false);

  const handleSave = () => {
    // Toast notification will be added here
    alert("Настройки сохранены!");
  };

  return (
    <div className="flex">
      <Navigation />

      <main className="flex-1 lg:ml-64 pt-16 lg:pt-0">
        <div className="p-4 lg:p-8 max-w-4xl mx-auto">
          {/* Header */}
          <motion.div
            initial={{ opacity: 0, y: -20 }}
            animate={{ opacity: 1, y: 0 }}
            className="mb-8"
          >
            <h1 className="text-3xl lg:text-4xl font-bold bg-gradient-to-r from-slate-900 to-slate-700 dark:from-slate-100 dark:to-slate-300 bg-clip-text text-transparent">
              Настройки
            </h1>
            <p className="text-slate-600 dark:text-slate-400">
              Управление аккаунтом и предпочтениями
            </p>
          </motion.div>

          {/* Tabs */}
          <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
            <TabsList className="grid w-full grid-cols-2 lg:grid-cols-5">
              <TabsTrigger value="profile" className="gap-2">
                <User className="w-4 h-4" />
                Профиль
              </TabsTrigger>
              <TabsTrigger value="notifications" className="gap-2">
                <Bell className="w-4 h-4" />
                Уведомления
              </TabsTrigger>
              <TabsTrigger value="security" className="gap-2">
                <Shield className="w-4 h-4" />
                Безопасность
              </TabsTrigger>
              <TabsTrigger value="appearance" className="gap-2">
                <Palette className="w-4 h-4" />
                Оформление
              </TabsTrigger>
              <TabsTrigger value="accounts" className="gap-2">
                <CreditCard className="w-4 h-4" />
                Счёта
              </TabsTrigger>
            </TabsList>

            {/* Profile Tab */}
            <TabsContent value="profile">
              <Card className="p-6">
                <div className="space-y-6">
                  <div>
                    <h3 className="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-4">
                      Личная информация
                    </h3>
                    <div className="space-y-4">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div className="space-y-2">
                          <Label htmlFor="firstName">Имя</Label>
                          <Input id="firstName" placeholder="Иван" />
                        </div>
                        <div className="space-y-2">
                          <Label htmlFor="lastName">Фамилия</Label>
                          <Input id="lastName" placeholder="Иванов" />
                        </div>
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="email">Email</Label>
                        <Input id="email" type="email" placeholder="ivan@example.com" />
                      </div>
                    </div>
                  </div>

                  <div className="flex justify-end">
                    <Button
                      onClick={handleSave}
                      className="bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700"
                    >
                      <Save className="w-4 h-4 mr-2" />
                      Сохранить
                    </Button>
                  </div>
                </div>
              </Card>
            </TabsContent>

            {/* Notifications Tab */}
            <TabsContent value="notifications">
              <Card className="p-6">
                <div className="space-y-6">
                  <div>
                    <h3 className="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-4">
                      Push-уведомления
                    </h3>
                    <div className="space-y-4">
                      <div className="flex items-center justify-between p-4 rounded-xl bg-slate-50 dark:bg-slate-800">
                        <div>
                          <p className="font-medium text-slate-900 dark:text-slate-100">
                            Уведомления о транзакциях
                          </p>
                          <p className="text-sm text-slate-600 dark:text-slate-400">
                            Получать уведомления о новых транзакциях
                          </p>
                        </div>
                        <Switch checked={notifications} onCheckedChange={setNotifications} />
                      </div>

                      <div className="flex items-center justify-between p-4 rounded-xl bg-slate-50 dark:bg-slate-800">
                        <div>
                          <p className="font-medium text-slate-900 dark:text-slate-100">
                            Напоминания о платежах
                          </p>
                          <p className="text-sm text-slate-600 dark:text-slate-400">
                            Напоминать о предстоящих платежах по долгам
                          </p>
                        </div>
                        <Switch defaultChecked />
                      </div>

                      <div className="flex items-center justify-between p-4 rounded-xl bg-slate-50 dark:bg-slate-800">
                        <div>
                          <p className="font-medium text-slate-900 dark:text-slate-100">
                            Еженедельный отчёт
                          </p>
                          <p className="text-sm text-slate-600 dark:text-slate-400">
                            Получать еженедельный итог по расходам
                          </p>
                        </div>
                        <Switch defaultChecked />
                      </div>
                    </div>
                  </div>

                  <div className="flex justify-end">
                    <Button
                      onClick={handleSave}
                      className="bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700"
                    >
                      <Save className="w-4 h-4 mr-2" />
                      Сохранить
                    </Button>
                  </div>
                </div>
              </Card>
            </TabsContent>

            {/* Security Tab */}
            <TabsContent value="security">
              <Card className="p-6">
                <div className="space-y-6">
                  <div>
                    <h3 className="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-4">
                      Изменить пароль
                    </h3>
                    <div className="space-y-4 max-w-md">
                      <div className="space-y-2">
                        <Label htmlFor="currentPassword">Текущий пароль</Label>
                        <Input id="currentPassword" type="password" />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="newPassword">Новый пароль</Label>
                        <Input id="newPassword" type="password" />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="confirmPassword">Подтвердите пароль</Label>
                        <Input id="confirmPassword" type="password" />
                      </div>
                    </div>
                  </div>

                  <div className="flex justify-end">
                    <Button className="bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700">
                      <Shield className="w-4 h-4 mr-2" />
                      Изменить пароль
                    </Button>
                  </div>
                </div>
              </Card>
            </TabsContent>

            {/* Appearance Tab */}
            <TabsContent value="appearance">
              <Card className="p-6">
                <div className="space-y-6">
                  <div>
                    <h3 className="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-4">
                      Тема оформления
                    </h3>
                    <div className="space-y-4">
                      <div className="flex items-center justify-between p-4 rounded-xl bg-slate-50 dark:bg-slate-800">
                        <div>
                          <p className="font-medium text-slate-900 dark:text-slate-100">
                            Тёмная тема
                          </p>
                          <p className="text-sm text-slate-600 dark:text-slate-400">
                            Переключить на тёмный режим
                          </p>
                        </div>
                        <Switch checked={darkMode} onCheckedChange={setDarkMode} />
                      </div>
                    </div>
                  </div>

                  <div>
                    <h3 className="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-4">
                      Валюта по умолчанию
                    </h3>
                    <div className="space-y-2 max-w-md">
                      <select className="w-full p-3 rounded-xl border border-slate-300 dark:border-slate-700 bg-transparent">
                        <option value="RUB">Рубль (₽)</option>
                        <option value="USD">Доллар ($)</option>
                        <option value="EUR">Евро (€)</option>
                      </select>
                    </div>
                  </div>

                  <div className="flex justify-end">
                    <Button
                      onClick={handleSave}
                      className="bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700"
                    >
                      <Save className="w-4 h-4 mr-2" />
                      Сохранить
                    </Button>
                  </div>
                </div>
              </Card>
            </TabsContent>

            {/* Accounts Tab */}
            <TabsContent value="accounts">
              <Card className="p-12 text-center">
                <Database className="w-16 h-16 mx-auto text-slate-300 dark:text-slate-700 mb-4" />
                <h3 className="text-lg font-semibold text-slate-900 dark:text-slate-100 mb-2">
                  Управление счетами
                </h3>
                <p className="text-slate-600 dark:text-slate-400 mb-6">
                  Подключение банковских счетов и кошельков для криптовалют
                </p>
                <Button className="bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700">
                  <CreditCard className="w-4 h-4 mr-2" />
                  Подключить счёт
                </Button>
              </Card>
            </TabsContent>
          </Tabs>
        </div>
      </main>
    </div>
  );
}
