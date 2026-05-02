"""
Mobile App Versions API
Serves update information for mobile apps
"""
from flask import Blueprint, jsonify
from datetime import datetime

versions_bp = Blueprint('versions', __name__, url_prefix='/api/mobile')

@versions_bp.route('/versions')
def get_versions():
    """Return available versions for all apps"""
    return jsonify({
        'success': True,
        'versions': {
            'banking': {
                'version': '1.2.8',
                'version_code': 8,
                'apk_url': 'http://193.29.224.20/apks/zhamlik.apk',
                'release_date': datetime.now().strftime('%Y-%m-%d'),
                'changes': [
                    'Добавлены уведомления',
                    'Исправлены ошибки'
                ],
                'force_update': False
            },
            'crypto': {
                'version': '1.3.0',
                'version_code': 5,
                'apk_url': 'http://193.29.224.20/apks/zhamlik_crypto.apk',
                'release_date': datetime.now().strftime('%Y-%m-%d'),
                'changes': [
                    'Добавлена поддержка фьючерсов',
                    'Исправлены ошибки API Bitget',
                    'Добавлены уведомления'
                ],
                'force_update': False
            }
        }
    })
