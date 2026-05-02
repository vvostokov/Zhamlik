from flask import current_app
from datetime import date, datetime, timedelta, timezone
from models import Debt, Notification, User
from extensions import db
from flask_mail import Message


def create_notification(user_id: int, notification_type: str, title: str, message: str, link: str = None, send_email: bool = True):
    """Создаёт уведомление для пользователя и отправляет email."""
    notification = Notification(
        user_id=user_id,
        type=notification_type,
        title=title,
        message=message,
        link=link
    )
    db.session.add(notification)
    db.session.commit()
    current_app.logger.info(f"--- [Notification] Создано уведомление для user {user_id}: {title}")

    # Отправляем email, если настроено и включено
    if send_email:
        send_notification_email(user_id, title, message, link)

    return notification


def send_notification_email(user_id: int, title: str, message: str, link: str = None):
    """Отправляет email-уведомление пользователю."""
    try:
        from extensions import mail

        user = User.query.filter_by(id=user_id).first()
        if not user or not user.email:
            current_app.logger.warning(f"--- [Notification] Нет email для user {user_id}")
            return

        # Проверяем, настроен ли MAIL_USERNAME
        if not current_app.config.get('MAIL_USERNAME'):
            current_app.logger.warning(f"--- [Notification] MAIL_USERNAME не настроен, email не отправлен")
            return

        # Создаём письмо
        msg = Message(
            subject=f'Уведомление Zhamlik: {title}',
            recipients=[user.email]
        )

        # Формируем HTML тело письма
        base_url = current_app.config.get('APPLICATION_URL', 'http://172.25.50.61:5001')
        full_link = f'{base_url}{link}' if link and not link.startswith('http') else link
        link_html = f'<p><a href="{full_link}">Перейти к странице</a></p>' if link else ''
        msg.html = f'''
        <html>
        <body style="font-family: Arial, sans-serif;">
            <h2>{title}</h2>
            <p>{message}</p>
            {link_html}
            <hr>
            <p style="color: #666; font-size: 12px;">
                Это автоматическое уведомление от системы Zhamlik.<br>
                Вы получили это письмо, потому что зарегистрированы в системе.
            </p>
        </body>
        </html>
        '''

        mail.send(msg)
        current_app.logger.info(f"--- [Notification] Email отправлен на {user.email}: {title}")

    except Exception as e:
        current_app.logger.error(f"--- [Notification] Ошибка отправки email: {e}", exc_info=True)


def check_debts_and_create_notifications():
    """
    Проверяет все долги и создаёт уведомления для:
    - Долгов, которые нужно погасить сегодня
    - Просроченных долгов
    """
    today = date.today()
    
    # Находим долги с датой погашения сегодня
    due_today = Debt.query.filter(
        Debt.due_date == today,
        Debt.status == 'active'
    ).all()
    
    for debt in due_today:
        # Проверяем, есть ли уже уведомление на сегодня
        existing_notif = Notification.query.filter(
            Notification.user_id == debt.user_id,
            Notification.type == 'debt_due',
            Notification.created_at >= datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        ).first()
        
        if not existing_notif:
            debt_type = "долг" if debt.debt_type == 'i_owe' else "вам должны"
            create_notification(
                user_id=debt.user_id,
                notification_type='debt_due',
                title=f'🔔 Погашение сегодня: {debt.counterparty}',
                message=f'Сегодня {debt.due_date.strftime("%d.%m.%Y")} нужно погасить {debt_type} на сумму {debt.initial_amount} {debt.currency} контрагенту "{debt.counterparty}".',
                link='/debts'
            )
    
    # Находим просроченные долги
    overdue = Debt.query.filter(
        Debt.due_date < today,
        Debt.status == 'active'
    ).all()
    
    for debt in overdue:
        # Проверяем, было ли уже уведомление об этом долге за последние 7 дней
        week_ago = datetime.now(timezone.utc) - timedelta(days=7)
        existing_notif = Notification.query.filter(
            Notification.user_id == debt.user_id,
            Notification.type == 'debt_overdue',
            Notification.message.like(f'%{debt.counterparty}%'),
            Notification.created_at >= week_ago
        ).first()
        
        if not existing_notif:
            days_overdue = (today - debt.due_date).days
            debt_type = "долг" if debt.debt_type == 'i_owe' else "вам должны"
            create_notification(
                user_id=debt.user_id,
                notification_type='debt_overdue',
                title=f'⚠️ Просрочено: {debt.counterparty}',
                message=f'Просрочено {days_overdue} дн. Погасите {debt_type} на сумму {debt.initial_amount} {debt.currency} контрагенту "{debt.counterparty}". Дата была: {debt.due_date.strftime("%d.%m.%Y")}.',
                link='/debts'
            )
    
    current_app.logger.info(f"--- [Notification] Проверено {len(due_today)} долгов на сегодня, {len(overdue)} просроченных")


def get_unread_notifications(user_id: int):
    """Получает непрочитанные уведомления пользователя."""
    return Notification.query.filter_by(
        user_id=user_id,
        is_read=False
    ).order_by(Notification.created_at.desc()).all()


def mark_notification_as_read(notification_id: int, user_id: int):
    """Помечает уведомление как прочитанное."""
    notification = Notification.query.filter_by(id=notification_id, user_id=user_id).first()
    if notification:
        notification.is_read = True
        db.session.commit()
    return notification


def mark_all_as_read(user_id: int):
    """Помечает все уведомления пользователя как прочитанные."""
    Notification.query.filter_by(user_id=user_id, is_read=False).update(
        {'is_read': True}
    )
    db.session.commit()
