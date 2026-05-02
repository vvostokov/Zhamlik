from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app
from flask_login import login_user, logout_user, login_required, current_user
from urllib.parse import urlsplit
from models import User
from extensions import db, login_manager

auth_bp = Blueprint('auth', __name__)

@login_manager.user_loader
def load_user(user_id):
    try:
        return User.query.get(int(user_id))
    except Exception as e:
        current_app.logger.error(f"Error loading user {user_id}: {e}", exc_info=True)
        return None

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('main.index'))
    
    if request.method == 'POST':
        try:
            username = request.form.get('username')
            password = request.form.get('password')
            remember = True if request.form.get('remember') else False

            current_app.logger.info(f"Attempting to login user: {username}")

            user = User.query.filter_by(username=username).first()

            if not user or not user.check_password(password):
                current_app.logger.warning(f"Failed login attempt for user: {username}")
                flash('Неверное имя пользователя или пароль.', 'danger')
                return redirect(url_for('auth.login'))

            login_user(user, remember=remember)
            current_app.logger.info(f"User {username} logged in successfully")
            next_page = request.args.get('next')
            if not next_page or urlsplit(next_page).netloc != '':
                next_page = url_for('main.index')
            return redirect(next_page)
        except Exception as e:
            current_app.logger.error(f"Error during login for user {username}: {e}", exc_info=True)
            flash('Произошла ошибка при входе. Попробуйте снова.', 'danger')
            return redirect(url_for('auth.login'))

    return render_template('login.html', title='Вход')

@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('main.index'))

@auth_bp.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    """Страница восстановления пароля"""
    if request.method == 'POST':
        username = request.form.get('username')
        user = User.query.filter_by(username=username).first()
        
        if not user:
            user = User.query.filter_by(email=username).first()
        
        if user:
            # Генерируем токен для сброса
            import secrets
            token = secrets.token_urlsafe(32)
            user.reset_token = token
            user.reset_token_expires = datetime.now(timezone.utc) + timedelta(hours=24)
            db.session.commit()
            
            flash(f'Ссылка для сброса пароля отправлена. Токен: {token}', 'info')
            current_app.logger.info(f"Password reset token generated for user: {user.username}")
        else:
            flash('Пользователь не найден.', 'warning')
        
        return redirect(url_for('auth.login'))
    
    return render_template('forgot_password.html', title='Восстановление пароля')

@auth_bp.route('/reset-password', methods=['GET', 'POST'])
def reset_password():
    """Страница установки нового пароля"""
    token = request.args.get('token', '')
    
    if not token:
        flash('Токен не предоставлен.', 'danger')
        return redirect(url_for('auth.login'))
    
    user = User.query.filter_by(reset_token=token).first()
    
    if not user or not user.reset_token_expires or user.reset_token_expires < datetime.now(timezone.utc):
        flash('Токен истёк или недействителен.', 'danger')
        return redirect(url_for('auth.login'))
    
    if request.method == 'POST':
        new_password = request.form.get('password')
        confirm_password = request.form.get('confirm_password')
        
        if new_password != confirm_password:
            flash('Пароли не совпадают.', 'danger')
            return redirect(url_for('auth.reset_password', token=token))
        
        if len(new_password) < 6:
            flash('Пароль должен быть не менее 6 символов.', 'danger')
            return redirect(url_for('auth.reset_password', token=token))
        
        user.set_password(new_password)
        user.reset_token = None
        user.reset_token_expires = None
        db.session.commit()
        
        flash('Пароль успешно изменён. Теперь вы можете войти.', 'success')
        current_app.logger.info(f"Password reset completed for user: {user.username}")
        return redirect(url_for('auth.login'))
    
    return render_template('reset_password.html', title='Новый пароль', token=token)

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('main.index'))
    
    if request.method == 'POST':
        try:
            username = request.form.get('username')
            password = request.form.get('password')
            email = request.form.get('email')

            current_app.logger.info(f"Attempting to register user: {username}")

            if User.query.filter_by(username=username).first():
                flash('Пользователь с таким именем уже существует.', 'danger')
                return redirect(url_for('auth.register'))

            if email and User.query.filter_by(email=email).first():
                flash('Пользователь с таким email уже существует.', 'danger')
                return redirect(url_for('auth.register'))

            user = User(username=username, email=email)
            user.set_password(password)
            db.session.add(user)
            db.session.commit()
            current_app.logger.info(f"User {username} registered successfully")
            flash('Регистрация прошла успешно! Теперь вы можете войти.', 'success')
            return redirect(url_for('auth.login'))
        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"Error registering user {username}: {e}", exc_info=True)
            flash('Произошла ошибка при регистрации. Попробуйте снова.', 'danger')
            return redirect(url_for('auth.register'))

    return render_template('register.html', title='Регистрация')
