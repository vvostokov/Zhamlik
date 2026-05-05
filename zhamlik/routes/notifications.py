from flask import Blueprint, request, jsonify, render_template
from flask_login import login_required, current_user
from notification_logic import get_unread_notifications, mark_notification_as_read, mark_all_as_read

notifications_bp = Blueprint('notifications', __name__)

@notifications_bp.route('/api/notifications')
@login_required
def api_all_notifications():
    """API для получения всех уведомлений."""
    from models import Notification
    notifications = Notification.query.filter_by(user_id=current_user.id).order_by(Notification.created_at.desc()).limit(50).all()
    notifications_data = []
    for notif in notifications:
        notifications_data.append({
            'id': notif.id,
            'type': notif.type,
            'title': notif.title,
            'message': notif.message,
            'link': notif.link,
            'is_read': notif.is_read,
            'created_at': notif.created_at.strftime('%Y-%m-%d %H:%M')
        })
    return jsonify({'notifications': notifications_data})

@notifications_bp.route('/api/notifications/unread')
@login_required
def api_unread_notifications():
    """API для получения непрочитанных уведомлений."""
    notifications = get_unread_notifications(current_user.id)
    notifications_data = []
    for notif in notifications:
        notifications_data.append({
            'id': notif.id,
            'type': notif.type,
            'title': notif.title,
            'message': notif.message,
            'link': notif.link,
            'created_at': notif.created_at.strftime('%Y-%m-%d %H:%M')
        })
    return jsonify({'notifications': notifications_data, 'count': len(notifications_data)})

@notifications_bp.route('/api/notifications/<int:notification_id>/read', methods=['POST'])
@login_required
def api_mark_as_read(notification_id):
    """API для пометки уведомления как прочитанного."""
    notification = mark_notification_as_read(notification_id, current_user.id)
    if notification:
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Notification not found'}), 404

@notifications_bp.route('/api/notifications/read-all', methods=['POST'])
@login_required
def api_mark_all_as_read():
    """API для пометки всех уведомлений как прочитанных."""
    mark_all_as_read(current_user.id)
    return jsonify({'success': True})

@notifications_bp.route('/notifications')
@login_required
def ui_notifications():
    """Страница всех уведомлений."""
    from models import Notification
    notifications = Notification.query.filter_by(user_id=current_user.id).order_by(Notification.created_at.desc()).limit(50).all()
    return render_template('notifications.html', notifications=notifications)
