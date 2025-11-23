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
