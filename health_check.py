"""
Health Check Endpoint for Zhamlik
Add this to Flask app to provide /health endpoint
"""

from datetime import datetime
from flask import jsonify
import os
import psutil


def register_health_endpoint(app, db):
    """Register health check endpoint"""

    @app.route('/health')
    def health_check():
        """Basic health check endpoint"""
        checks = {
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat(),
            'checks': {}
        }

        # Check database connection
        try:
            db.session.execute(db.text('SELECT 1'))
            checks['checks']['database'] = {
                'status': 'healthy',
                'message': 'Database connection OK'
            }
        except Exception as e:
            checks['status'] = 'unhealthy'
            checks['checks']['database'] = {
                'status': 'unhealthy',
                'message': str(e)
            }

        # Check disk space
        try:
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent
            if disk_percent < 80:
                checks['checks']['disk'] = {
                    'status': 'healthy',
                    'usage_percent': disk_percent,
                    'free_gb': round(disk.free / (1024**3), 2)
                }
            elif disk_percent < 90:
                checks['checks']['disk'] = {
                    'status': 'warning',
                    'usage_percent': disk_percent,
                    'free_gb': round(disk.free / (1024**3), 2)
                }
            else:
                checks['status'] = 'unhealthy'
                checks['checks']['disk'] = {
                    'status': 'unhealthy',
                    'usage_percent': disk_percent,
                    'free_gb': round(disk.free / (1024**3), 2)
                }
        except Exception as e:
            checks['checks']['disk'] = {
                'status': 'unknown',
                'message': str(e)
            }

        # Check memory
        try:
            memory = psutil.virtual_memory()
            checks['checks']['memory'] = {
                'status': 'healthy',
                'usage_percent': memory.percent,
                'available_gb': round(memory.available / (1024**3), 2)
            }
        except Exception as e:
            checks['checks']['memory'] = {
                'status': 'unknown',
                'message': str(e)
            }

        status_code = 200 if checks['status'] == 'healthy' else 503
        return jsonify(checks), status_code

    @app.route('/health/ready')
    def readiness_check():
        """Readiness check - is the app ready to serve requests?"""
        try:
            db.session.execute(db.text('SELECT 1'))
            return jsonify({
                'status': 'ready',
                'timestamp': datetime.utcnow().isoformat()
            }), 200
        except Exception as e:
            return jsonify({
                'status': 'not_ready',
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e)
            }), 503

    @app.route('/health/live')
    def liveness_check():
        """Liveness check - is the app alive?"""
        return jsonify({
            'status': 'alive',
            'timestamp': datetime.utcnow().isoformat()
        }), 200


def register_health_monitoring(app, db):
    """
    Register health monitoring endpoints
    Add this to app.py after db initialization
    """
    register_health_endpoint(app, db)
