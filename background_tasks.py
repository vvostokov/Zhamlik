from flask import current_app
import time
import json

from logic.news_analysis import get_news_trends_for_portfolio
from news_logic import get_crypto_news, get_securities_news
from logic.platform_sync_logic import sync_platform_balances, sync_platform_transactions
from models import InvestmentPlatform, JsonCache
from api_clients import fetch_usdt_rub_rate
from routes.debts import _create_debt_from_recurring_payment
from extensions import db
from notification_logic import check_debts_and_create_notifications


def update_all_news_in_background():
    """
    Фоновая задача для обновления и кэширования всех новостей.
    Эта функция будет запускаться планировщиком периодически.
    """
    # flask-apscheduler автоматически предоставляет контекст приложения для фоновых задач.
    # Явное создание приложения через create_app() здесь не требуется и вызывает ошибку.
    current_app.logger.info("--- [BG_TASK] Запуск фонового обновления новостей ---")
    try:
        # 1. Определяем топ-10 тикеров, как это делает страница новостей.
        # Нам не нужны сами тренды, только список тикеров для обновления кэша.
        _, top_10_tickers = get_news_trends_for_portfolio()

        # 2. Обновляем кэш для каждого тикера из топа.
        if top_10_tickers:
            for ticker in top_10_tickers:
                current_app.logger.info(f"--- [BG_TASK] Обновление кэша новостей для: {ticker} ---")
                get_crypto_news(categories=ticker, limit=30)

        # 3. Обновляем кэш для общих крипто-новостей (используется на главной и странице новостей).
        current_app.logger.info("--- [BG_TASK] Обновление кэша общих крипто-новостей ---")
        get_crypto_news(limit=30)  # Для /crypto-news
        get_crypto_news(limit=5)   # Для /crypto-assets (главная)

        # 4. Обновляем кэш для новостей фондового рынка.
        current_app.logger.info("--- [BG_TASK] Обновление кэша новостей фондового рынка ---")
        get_securities_news(limit=50)

        current_app.logger.info("--- [BG_TASK] Фоновое обновление новостей завершено успешно. ---")

    except Exception as e:
        current_app.logger.error(f"--- [BG_TASK] Ошибка во время фонового обновления новостей: {e}", exc_info=True)


def sync_all_platforms_in_background():
    """
    Фоновая задача для обновления балансов и транзакций по всем активным крипто-платформам.
    """
    with current_app.app_context():
        current_app.logger.info("--- [BG_TASK] Запуск фонового обновления платформ ---")
        try:
            active_platforms = InvestmentPlatform.query.filter_by(platform_type='crypto_exchange', is_active=True).all()

            if not active_platforms:
                current_app.logger.info("--- [BG_TASK] Нет активных крипто-платформ для синхронизации.")
                return

            for platform in active_platforms:
                current_app.logger.info(f"--- [BG_TASK] Синхронизация балансов для: {platform.name} ---")
                sync_platform_balances(platform)
                time.sleep(5)  # Пауза между разными типами синхронизации

                current_app.logger.info(f"--- [BG_TASK] Синхронизация транзакций для: {platform.name} ---")
                sync_platform_transactions(platform)
                time.sleep(10)  # Пауза между платформами, чтобы не превышать лимиты API

            current_app.logger.info("--- [BG_TASK] Фоновое обновление платформ завершено успешно. ---")
        except Exception as e:
            current_app.logger.error(f"--- [BG_TASK] Ошибка во время фонового обновления платформ: {e}", exc_info=True)


def update_usdt_rub_rate_in_background():
    """Фоновая задача для обновления курса USDT/RUB в кэше."""
    with current_app.app_context():
        try:        
            rate = fetch_usdt_rub_rate()
            if rate is not None:
                cache_key = 'currency_rates'
                cache_entry = JsonCache.query.filter_by(cache_key=cache_key).first()
                if not cache_entry:
                    cache_entry = JsonCache(cache_key=cache_key)
                    db.session.add(cache_entry)

                try:                
                    rates_data = json.loads(cache_entry.json_data) if cache_entry.json_data else {}
                except (json.JSONDecodeError, TypeError):
                    rates_data = {}

                rates_data['USDT'] = str(rate)
                rates_data['USD'] = str(rate)

                cache_entry.json_data = json.dumps(rates_data)
                db.session.commit()
                current_app.logger.info(f"--- [BG_TASK] Курс USDT/RUB успешно обновлен в кэше: {rate}")
        except Exception as e:        
            db.session.rollback()
            current_app.logger.error(f"--- [BG_TASK] Ошибка во время фонового обновления курса USDT/RUB: {e}", exc_info=True)

def create_debts_from_recurring_payments_in_background():
    """
    Фоновая задача для создания долгов из регулярных платежей.
    Создаёт долги для платежей с включённым флагом auto_create_debt,
    у которых наступила дата платежа и транзакция ещё не проведена.
    """
    with current_app.app_context():
        current_app.logger.info("--- [BG_TASK] Запуск проверки регулярных платежей для создания долгов ---")
        from models import RecurringPayment

        recurring_payments = RecurringPayment.query.filter_by(auto_create_debt=True).all()

        if not recurring_payments:
            current_app.logger.info("--- [BG_TASK] Нет регулярных платежей с включённым авто-созданием долгов")
            return

        current_app.logger.info(f"--- [BG_TASK] Найдено {len(recurring_payments)} регулярных платежей для проверки")

        for payment in recurring_payments:
            try:
                _create_debt_from_recurring_payment(payment)
            except Exception as e:
                current_app.logger.error(f"--- [BG_TASK] Ошибка при обработке платежа {payment.description}: {e}", exc_info=True)
                db.session.rollback()

        db.session.commit()
        current_app.logger.info("--- [BG_TASK] Проверка регулярных платежей завершена")


def check_notifications_in_background():
    """
    Фоновая задача для проверки долгов и создания уведомлений.
    Запускается каждый час.
    """
    with current_app.app_context():
        current_app.logger.info("--- [BG_TASK] Запуск проверки долгов для создания уведомлений")
        try:
            check_debts_and_create_notifications()
            current_app.logger.info("--- [BG_TASK] Проверка долгов и создание уведомлений завершено")
        except Exception as e:
            current_app.logger.error(f"--- [BG_TASK] Ошибка при проверке долгов: {e}", exc_info=True)


def sync_platform_transactions_background(platform_id: int, user_id: int):
    """
    Фоновая задача для синхронизации транзакций одной платформы с уведомлением.
    """
    with current_app.app_context():
        from models import InvestmentPlatform
        from notification_logic import create_notification

        try:
            platform = InvestmentPlatform.query.filter_by(id=platform_id, user_id=user_id).first()

            if not platform:
                current_app.logger.error(f"--- [BG_SYNC] Платформа ID={platform_id} не найдена")
                return

            current_app.logger.info(f"--- [BG_SYNC] Запуск фоновой синхронизации транзакций для: {platform.name}")

            # Выполняем синхронизацию
            success, message = sync_platform_transactions(platform)

            if success:
                # Разбираем сообщение чтобы получить количество новых транзакций
                # Формат сообщения: "Success: X new transactions found."
                tx_count = "0"
                if "new transactions found" in message:
                    try:
                        tx_count = message.split(":")[1].split("new")[0].strip()
                    except:
                        pass

                title = f"✅ Синхронизация {platform.name} завершена"
                full_message = f"Синхронизация транзакций для платформы '{platform.name}' успешно завершена.\n\nНовых транзакций: {tx_count}"

                current_app.logger.info(f"--- [BG_SYNC] Синхронизация успешна: {message}")

            else:
                title = f"❌ Ошибка синхронизации {platform.name}"
                full_message = f"При синхронизации транзакций для платформы '{platform.name}' произошла ошибка:\n\n{message}"
                current_app.logger.error(f"--- [BG_SYNC] Ошибка синхронизации: {message}")

            # Создаём уведомление
            create_notification(
                user_id=user_id,
                notification_type='sync_complete',
                title=title,
                message=full_message,
                link=f"/platforms/{platform_id}",
                send_email=True
            )

            current_app.logger.info(f"--- [BG_SYNC] Фоновая синхронизация завершена для: {platform.name}")

        except Exception as e:
            current_app.logger.error(f"--- [BG_SYNC] Критическая ошибка при фоновой синхронизации: {e}", exc_info=True)

            # Создаём уведомление об ошибке
            try:
                create_notification(
                    user_id=user_id,
                    notification_type='sync_error',
                    title=f"🔥 Критическая ошибка синхронизации",
                    message=f"При синхронизации платформы ID={platform_id} произошла критическая ошибка:\n\n{str(e)}",
                    link=f"/platforms/{platform_id}",
                    send_email=True
                )
            except:
                pass