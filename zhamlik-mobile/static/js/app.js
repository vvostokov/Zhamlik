// API Base URL
const API_BASE = '/api/mobile';

// App State
let currentPage = 'home';
let cache = {};
let accounts = [];
let categories = [];

// Utility Functions
function formatMoney(amount) {
    return new Intl.NumberFormat('ru-RU', {
        style: 'currency',
        currency: 'RUB',
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    }).format(amount);
}

// Pull-to-refresh functionality
let startY = 0;
let currentY = 0;
let isPulling = false;
const PULL_THRESHOLD = 80;

function initPullToRefresh() {
    const app = document.getElementById('app');

    app.addEventListener('touchstart', (e) => {
        if (window.scrollY === 0) {
            startY = e.touches[0].clientY;
            isPulling = true;
        }
    });

    app.addEventListener('touchmove', (e) => {
        if (!isPulling) return;

        currentY = e.touches[0].clientY;
        const diff = currentY - startY;

        if (diff > 0 && window.scrollY === 0) {
            e.preventDefault();

            const pullProgress = Math.min(diff / PULL_THRESHOLD, 1);

            if (diff >= PULL_THRESHOLD) {
                showPullToRefreshIndicator('Отпустите для обновления');
            } else {
                showPullToRefreshIndicator('Потяните для обновления');
            }
        }
    });

    app.addEventListener('touchend', () => {
        if (!isPulling) return;

        const diff = currentY - startY;

        if (diff >= PULL_THRESHOLD) {
            refreshCurrentPage();
        }

        hidePullToRefreshIndicator();
        isPulling = false;
        startY = 0;
        currentY = 0;
    });
}

function showPullToRefreshIndicator(text) {
    let indicator = document.getElementById('pullToRefreshIndicator');

    if (!indicator) {
        indicator = document.createElement('div');
        indicator.id = 'pullToRefreshIndicator';
        indicator.style.cssText = `
            position: fixed;
            top: 60px;
            left: 50%;
            transform: translateX(-50%);
            background: white;
            padding: 12px 24px;
            border-radius: 24px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            z-index: 1000;
            font-size: 0.875rem;
            font-weight: 500;
            color: #0d6efd;
            display: flex;
            align-items: center;
            gap: 8px;
        `;
        document.body.appendChild(indicator);
    }

    indicator.innerHTML = `<i class="bi bi-arrow-clockwise"></i> ${text}`;
    indicator.style.display = 'flex';
}

function hidePullToRefreshIndicator() {
    const indicator = document.getElementById('pullToRefreshIndicator');
    if (indicator) {
        indicator.style.display = 'none';
    }
}

async function refreshCurrentPage() {
    const indicator = document.getElementById('pullToRefreshIndicator');
    if (indicator) {
        indicator.innerHTML = '<div class="spinner-border spinner-border-sm" role="status"></div> Обновление...';
    }

    try {
        await navigateTo(currentPage);
    } catch (error) {
        console.error('Refresh failed:', error);
    }

    setTimeout(() => {
        hidePullToRefreshIndicator();
    }, 500);
}

function formatDate(dateStr) {
    const date = new Date(dateStr);
    const today = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);

    if (date.toDateString() === today.toDateString()) {
        return 'Сегодня';
    } else if (date.toDateString() === yesterday.toDateString()) {
        return 'Вчера';
    } else {
        return date.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' });
    }
}

async function fetchAPI(endpoint, options = {}) {
    try {
        const response = await fetch(`${API_BASE}${endpoint}`, options);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error('API Error:', error);
        throw error;
    }
}

// Page Templates
function renderHomePage(data) {
    return `
        <div class="balance-card">
            <div class="text-white-50">Общий баланс</div>
            <div class="balance-amount">${formatMoney(data.total_balance)}</div>
            <div class="row mt-3">
                <div class="col-6">
                    <small class="text-white-50">Доход</small>
                    <div class="fw-bold text-success">+${formatMoney(data.monthly_income)}</div>
                </div>
                <div class="col-6">
                    <small class="text-white-50">Расход</small>
                    <div class="fw-bold text-danger">-${formatMoney(data.monthly_expense)}</div>
                </div>
            </div>
        </div>

        <div class="px-3 mb-2">
            <div class="d-flex justify-content-between align-items-center">
                <h6 class="mb-0 fw-bold">Последние операции</h6>
                <a href="#transactions" onclick="navigateTo('transactions')" class="text-primary text-decoration-none small">Все</a>
            </div>
        </div>

        <div class="mx-3" style="background: white; border-radius: 16px; overflow: hidden; box-shadow: var(--card-shadow);">
            ${data.recent_transactions.length > 0 ? data.recent_transactions.map(tx => `
                <div class="transaction-item">
                    <div class="transaction-icon ${tx.amount >= 0 ? 'income' : 'expense'}">
                        <i class="bi bi-${tx.amount >= 0 ? 'arrow-down-left' : 'arrow-up-right'}"></i>
                    </div>
                    <div class="transaction-details">
                        <div class="transaction-title">${tx.description || 'Без названия'}</div>
                        <div class="transaction-date">${formatDate(tx.date)}</div>
                    </div>
                    <div class="transaction-amount ${tx.amount >= 0 ? 'income' : 'expense'}">
                        ${tx.amount >= 0 ? '+' : ''}${formatMoney(tx.amount)}
                    </div>
                </div>
            `).join('') : '<div class="empty-state"><i class="bi bi-inbox"></i><p>Нет операций</p></div>'}
        </div>
    `;
}

function renderAnalyticsPage(data) {
    if (data.categories.length === 0) {
        return `
            <div class="empty-state">
                <i class="bi bi-bar-chart"></i>
                <p>Нет данных о расходах<br><small>за выбранный период</small></p>
            </div>
        `;
    }

    return `
        <div class="stat-card">
            <div class="text-muted small">Общие расходы</div>
            <div class="fs-3 fw-bold text-danger">${formatMoney(data.total_expense)}</div>
        </div>

        <div class="chart-container">
            <h6 class="fw-bold mb-3">Расходы по категориям</h6>
            <canvas id="categoryChart"></canvas>
        </div>

        <div class="chart-container">
            <h6 class="fw-bold mb-3">Динамика по дням</h6>
            <canvas id="dailyChart"></canvas>
        </div>

        <div class="px-3 mb-3">
            <h6 class="fw-bold mb-3">Детализация по категориям</h6>
            ${data.categories.map(cat => `
                <div class="stat-card">
                    <div class="d-flex justify-content-between">
                        <span class="fw-bold">${cat.name}</span>
                        <span class="fw-bold text-danger">${formatMoney(cat.amount)}</span>
                    </div>
                </div>
            `).join('')}
        </div>
    `;
}

function initAnalyticsCharts(data) {
    // Destroy existing charts if they exist
    if (window.categoryChart) {
        window.categoryChart.destroy();
    }
    if (window.dailyChart) {
        window.dailyChart.destroy();
    }

    // Category pie chart
    const categoryCtx = document.getElementById('categoryChart');
    if (categoryCtx) {
        const colors = [
            '#0d6efd', '#0dcaf0', '#20c997', '#198754', '#ffc107',
            '#fd7e14', '#dc3545', '#d63384', '#6f42c1', '#6610f2'
        ];

        window.categoryChart = new Chart(categoryCtx, {
            type: 'doughnut',
            data: {
                labels: data.categories.map(c => c.name),
                datasets: [{
                    data: data.categories.map(c => c.amount),
                    backgroundColor: colors.slice(0, data.categories.length),
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            padding: 15,
                            usePointStyle: true
                        }
                    }
                }
            }
        });
    }

    // Daily line chart
    const dailyCtx = document.getElementById('dailyChart');
    if (dailyCtx && data.daily && data.daily.length > 0) {
        window.dailyChart = new Chart(dailyCtx, {
            type: 'line',
            data: {
                labels: data.daily.map(d => d.date),
                datasets: [{
                    label: 'Расходы',
                    data: data.daily.map(d => d.amount),
                    borderColor: '#dc3545',
                    backgroundColor: 'rgba(220, 53, 69, 0.1)',
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
    }
}

function renderAccountsPage(data) {
    return `
        <div class="px-3 mb-3">
            <div class="d-flex justify-content-between align-items-center mb-3">
                <h6 class="fw-bold mb-0">Мои счета</h6>
                <button class="btn btn-primary btn-sm" onclick="showAddAccount()">
                    <i class="bi bi-plus-lg"></i> Добавить
                </button>
            </div>

            ${data.accounts.map(acc => `
                <div class="stat-card">
                    <div class="d-flex justify-content-between align-items-center">
                        <div>
                            <div class="fw-bold">${acc.name}</div>
                            <div class="text-muted small">${acc.type || 'Счет'} • ${acc.bank || ''}</div>
                        </div>
                        <div class="text-end">
                            <div class="fw-bold ${acc.balance >= 0 ? '' : 'text-danger'}">${formatMoney(acc.balance)}</div>
                            <div class="text-muted small">${acc.currency}</div>
                        </div>
                    </div>
                </div>
            `).join('')}

            ${data.accounts.length === 0 ? `
                <div class="empty-state">
                    <i class="bi bi-wallet2"></i>
                    <p>Нет счетов</p>
                    <button class="btn btn-primary btn-sm" onclick="showAddAccount()">Создать первый счет</button>
                </div>
            ` : ''}
        </div>
    `;
}

function renderTransactionsPage() {
    return `
        <div class="px-3 mb-3">
            <h6 class="fw-bold mb-3">Все операции</h6>
            <div id="transactionsList" class="bg-white" style="border-radius: 16px; overflow: hidden; box-shadow: var(--card-shadow);">
                <div class="loading">
                    <div class="spinner-border text-primary" role="status"></div>
                </div>
            </div>
            <div class="text-center mt-3">
                <button class="btn btn-outline-primary btn-sm" onclick="loadMoreTransactions()">Загрузить еще</button>
            </div>
        </div>
    `;
}

function renderDebtsPage(data) {
    return `
        <div class="px-3 mb-3">
            <div class="d-flex justify-content-between align-items-center mb-3">
                <h6 class="fw-bold mb-0">Долги</h6>
                <button class="btn btn-primary btn-sm" onclick="showAddDebt()">
                    <i class="bi bi-plus-lg"></i> Добавить
                </button>
            </div>

            <h6 class="text-muted mb-2">Я должен</h6>
            ${data.i_owe.length > 0 ? data.i_owe.map(debt => `
                <div class="stat-card border-start border-4 border-danger">
                    <div class="fw-bold">${debt.counterparty}</div>
                    <div class="text-danger fw-bold">${formatMoney(debt.amount)}</div>
                    ${debt.due_date ? `<div class="text-muted small">До ${formatDate(debt.due_date)}</div>` : ''}
                    ${debt.description ? `<div class="text-muted small mt-1">${debt.description}</div>` : ''}
                </div>
            `).join('') : '<div class="text-muted text-center py-3 small">Нет долгов</div>'}

            <h6 class="text-muted mb-2 mt-4">Мне должны</h6>
            ${data.owed_to_me.length > 0 ? data.owed_to_me.map(debt => `
                <div class="stat-card border-start border-4 border-success">
                    <div class="fw-bold">${debt.counterparty}</div>
                    <div class="text-success fw-bold">${formatMoney(debt.amount)}</div>
                    ${debt.due_date ? `<div class="text-muted small">До ${formatDate(debt.due_date)}</div>` : ''}
                    ${debt.description ? `<div class="text-muted small mt-1">${debt.description}</div>` : ''}
                </div>
            `).join('') : '<div class="text-muted text-center py-3 small">Нет долгов</div>'}

            ${data.i_owe.length === 0 && data.owed_to_me.length === 0 ? `
                <div class="empty-state">
                    <i class="bi bi-arrow-left-right"></i>
                    <p>Нет долгов<br><small>Добавьте свой первый долг</small></p>
                </div>
            ` : ''}
        </div>
    `;
}

function renderObligationsPage(debtsData, recurringData) {
    return `
        <div class="px-3 mb-3">
            <ul class="nav nav-tabs nav-fill mb-3" id="obligationsTabs" role="tablist">
                <li class="nav-item" role="presentation">
                    <button class="nav-link active" id="debts-tab" data-bs-toggle="tab" data-bs-target="#debts-tab-pane" type="button" role="tab">
                        <i class="bi bi-arrow-left-right"></i> Долги
                    </button>
                </li>
                <li class="nav-item" role="presentation">
                    <button class="nav-link" id="recurring-tab" data-bs-toggle="tab" data-bs-target="#recurring-tab-pane" type="button" role="tab">
                        <i class="bi bi-calendar-check"></i> Платежи
                    </button>
                </li>
            </ul>

            <div class="tab-content" id="obligationsTabContent">
                <!-- Debts Tab -->
                <div class="tab-pane fade show active" id="debts-tab-pane" role="tabpanel">
                    <div class="d-flex justify-content-between align-items-center mb-3">
                        <h6 class="fw-bold mb-0">Долги и займы</h6>
                        <div class="btn-group">
                            <button class="btn btn-danger btn-sm" onclick="showAddDebt('i_owe')">
                                <i class="bi bi-arrow-up-right"></i> Дать
                            </button>
                            <button class="btn btn-success btn-sm" onclick="showAddDebt('owed_to_me')">
                                <i class="bi bi-arrow-down-left"></i> Взять
                            </button>
                        </div>
                    </div>

                    <h6 class="text-muted mb-2">Я должен</h6>
                    ${debtsData.i_owe.length > 0 ? debtsData.i_owe.map(debt => `
                        <div class="stat-card border-start border-4 border-danger mb-2">
                            <div class="fw-bold">${debt.counterparty}</div>
                            <div class="text-danger fw-bold">${formatMoney(debt.amount)}</div>
                            ${debt.due_date ? `<div class="text-muted small">До ${formatDate(debt.due_date)}</div>` : ''}
                            ${debt.description ? `<div class="text-muted small mt-1">${debt.description}</div>` : ''}
                        </div>
                    `).join('') : '<div class="text-muted text-center py-3 small">Нет долгов</div>'}

                    <h6 class="text-muted mb-2 mt-3">Мне должны</h6>
                    ${debtsData.owed_to_me.length > 0 ? debtsData.owed_to_me.map(debt => `
                        <div class="stat-card border-start border-4 border-success mb-2">
                            <div class="fw-bold">${debt.counterparty}</div>
                            <div class="text-success fw-bold">${formatMoney(debt.amount)}</div>
                            ${debt.due_date ? `<div class="text-muted small">До ${formatDate(debt.due_date)}</div>` : ''}
                            ${debt.description ? `<div class="text-muted small mt-1">${debt.description}</div>` : ''}
                        </div>
                    `).join('') : '<div class="text-muted text-center py-3 small">Нет долгов</div>'}

                    ${debtsData.i_owe.length === 0 && debtsData.owed_to_me.length === 0 ? `
                        <div class="empty-state">
                            <i class="bi bi-arrow-left-right"></i>
                            <p>Нет долгов<br><small>Добавьте свой первый долг</small></p>
                        </div>
                    ` : ''}
                </div>

                <!-- Recurring Payments Tab -->
                <div class="tab-pane fade" id="recurring-tab-pane" role="tabpanel">
                    <div class="d-flex justify-content-between align-items-center mb-3">
                        <h6 class="fw-bold mb-0">Ближайшие платежи</h6>
                        <button class="btn btn-primary btn-sm" onclick="alert('Скоро будет доступно')">
                            <i class="bi bi-plus-lg"></i> Добавить
                        </button>
                    </div>

                    ${recurringData.upcoming.length > 0 ? recurringData.upcoming.map(payment => `
                        <div class="stat-card border-start border-4 border-warning mb-2">
                            <div class="d-flex justify-content-between align-items-start">
                                <div>
                                    <div class="fw-bold">${payment.description}</div>
                                    <div class="text-muted small">${payment.counterparty || 'Регулярный платеж'}</div>
                                    <div class="text-warning small">
                                        <i class="bi bi-calendar3"></i> ${formatDate(payment.next_due_date)}
                                    </div>
                                </div>
                                <div class="text-end">
                                    <div class="fw-bold text-warning">${formatMoney(payment.amount)}</div>
                                </div>
                            </div>
                        </div>
                    `).join('') : `
                        <div class="empty-state">
                            <i class="bi bi-calendar-check"></i>
                            <p>Нет платежей<br><small>Добавьте регулярный платеж</small></p>
                        </div>
                    `}
                </div>
            </div>
        </div>
    `;
}

function renderMorePage() {
    return `
        <div class="px-3 mb-3">
            <h6 class="fw-bold mb-3">Быстрое добавление</h6>

            <div class="row g-2 mb-4">
                <div class="col-6">
                    <button class="btn btn-danger w-100" onclick="showAddDebt('i_owe')">
                        <i class="bi bi-arrow-up-right"></i><br>
                        <small>Я должен</small>
                    </button>
                </div>
                <div class="col-6">
                    <button class="btn btn-success w-100" onclick="showAddDebt('owed_to_me')">
                        <i class="bi bi-arrow-down-left"></i><br>
                        <small>Мне должны</small>
                    </button>
                </div>
            </div>

            <h6 class="fw-bold mb-3">Ещё функции</h6>

            <div class="stat-card" onclick="navigateTo('obligations')" style="cursor: pointer;">
                <div class="d-flex align-items-center">
                    <i class="bi bi-calendar-check fs-4 me-3 text-primary"></i>
                    <div>
                        <div class="fw-bold">Обязательства</div>
                        <div class="text-muted small">Долги и регулярные платежи</div>
                    </div>
                    <i class="bi bi-chevron-right ms-auto text-muted"></i>
                </div>
            </div>

            <div class="stat-card" onclick="navigateTo('accounts')" style="cursor: pointer;">
                <div class="d-flex align-items-center">
                    <i class="bi bi-wallet2 fs-4 me-3 text-primary"></i>
                    <div>
                        <div class="fw-bold">Счета</div>
                        <div class="text-muted small">Управление счетами</div>
                    </div>
                    <i class="bi bi-chevron-right ms-auto text-muted"></i>
                </div>
            </div>

            <div class="stat-card" onclick="navigateTo('transactions')" style="cursor: pointer;">
                <div class="d-flex align-items-center">
                    <i class="bi bi-list-ul fs-4 me-3 text-primary"></i>
                    <div>
                        <div class="fw-bold">Операции</div>
                        <div class="text-muted small">История транзакций</div>
                    </div>
                    <i class="bi bi-chevron-right ms-auto text-muted"></i>
                </div>
            </div>

            <div class="stat-card" onclick="alert('Версия 1.0')" style="cursor: pointer;">
                <div class="text-center text-muted small">
                    Zhamlik Mobile v1.0<br>
                    Сделано с ❤️
                </div>
            </div>
        </div>
    `;
}

// Loading State
function showLoading() {
    document.getElementById('app').innerHTML = '<div class="loading"><div class="spinner-border text-primary" role="status"></div></div>';
}

// Modal Functions
function showAddTransaction(type) {
    const modal = document.getElementById('addTransactionModal');
    const title = document.getElementById('modalTitle');
    const typeInput = document.getElementById('transactionType');
    const dateInput = document.getElementById('txDate');

    title.textContent = type === 'income' ? 'Добавить доход' : 'Добавить расход';
    typeInput.value = type;
    dateInput.value = new Date().toISOString().split('T')[0];

    loadAccountsAndCategories();
    modal.classList.add('active');
}

function showAddTransfer() {
    alert('Переводы в разработке');
}

function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

function showQuickActions() {
    const quickActions = document.getElementById('quickActions');
    quickActions.style.display = quickActions.style.display === 'none' ? 'flex' : 'none';
}

async function loadAccountsAndCategories() {
    try {
        const accountsData = await fetchAPI('/accounts');
        accounts = accountsData.accounts;

        const accountSelect = document.getElementById('txAccount');
        accountSelect.innerHTML = '<option value="">Выберите счет</option>';
        accounts.forEach(acc => {
            accountSelect.innerHTML += `<option value="${acc.id}">${acc.name} (${formatMoney(acc.balance)})</option>`;
        });

        // Load categories
        const categoriesData = await fetchAPI('/categories');
        categories = categoriesData.categories;

        const categorySelect = document.getElementById('txCategory');
        categorySelect.innerHTML = '<option value="">Без категории</option>';
        categories.forEach(cat => {
            categorySelect.innerHTML += `<option value="${cat.id}">${cat.name}</option>`;
        });
    } catch (error) {
        console.error('Error loading accounts and categories:', error);
    }
}

async function saveTransaction() {
    const type = document.getElementById('transactionType').value;
    const amount = document.getElementById('txAmount').value;
    const description = document.getElementById('txDescription').value;
    const accountId = document.getElementById('txAccount').value;
    const categoryId = document.getElementById('txCategory').value;
    const date = document.getElementById('txDate').value;

    if (!amount || !accountId || !date) {
        alert('Заполните обязательные поля');
        return;
    }

    try {
        const response = await fetchAPI('/transactions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                type,
                amount: parseFloat(amount),
                description,
                account_id: parseInt(accountId),
                category_id: categoryId || null,
                date
            })
        });

        closeModal('addTransactionModal');
        alert('Операция добавлена');
        navigateTo(currentPage); // Reload current page
    } catch (error) {
        alert('Ошибка при сохранении: ' + error.message);
    }
}

// Router
async function navigateTo(page) {
    currentPage = page;
    showLoading();

    // Update nav
    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.remove('active');
        if (item.dataset.page === page) {
            item.classList.add('active');
        }
    });

    // Update page title
    const titles = {
        'home': 'Zhamlik',
        'analytics': 'Аналитика',
        'accounts': 'Счета',
        'more': 'Ещё',
        'debts': 'Долги',
        'obligations': 'Обязательства',
        'transactions': 'Операции'
    };
    document.getElementById('pageTitle').textContent = titles[page] || 'Zhamlik';

    // Hide quick actions
    document.getElementById('quickActions').style.display = 'none';

    try {
        let data;
        let html;

        switch(page) {
            case 'home':
                data = await fetchAPI('/overview');
                html = renderHomePage(data);
                break;
            case 'analytics':
                data = await fetchAPI('/analytics?days=30');
                html = renderAnalyticsPage(data);
                // Initialize charts after render
                setTimeout(() => initAnalyticsCharts(data), 100);
                break;
            case 'accounts':
                data = await fetchAPI('/accounts');
                html = renderAccountsPage(data);
                break;
            case 'debts':
                data = await fetchAPI('/debts');
                html = renderDebtsPage(data);
                break;
            case 'obligations':
                const debtsData = await fetchAPI('/debts');
                const recurringData = await fetchAPI('/recurring_payments');
                html = renderObligationsPage(debtsData, recurringData);
                break;
            case 'more':
                html = renderMorePage();
                break;
            case 'transactions':
                html = renderTransactionsPage();
                // Load transactions after render
                setTimeout(loadTransactionsList, 100);
                break;
            default:
                html = '<div class="empty-state"><i class="bi bi-question-circle"></i><p>Страница в разработке</p></div>';
        }

        document.getElementById('app').innerHTML = html;
    } catch (error) {
        document.getElementById('app').innerHTML = `
            <div class="empty-state">
                <i class="bi bi-exclamation-triangle text-warning"></i>
                <p>Ошибка загрузки данных</p>
                <button class="btn btn-primary btn-sm" onclick="navigateTo('${page}')">Попробовать снова</button>
            </div>
        `;
    }
}

async function loadTransactionsList(page = 1) {
    try {
        const data = await fetchAPI(`/transactions?page=${page}`);
        const container = document.getElementById('transactionsList');

        if (data.transactions.length === 0) {
            container.innerHTML = '<div class="empty-state"><i class="bi bi-inbox"></i><p>Нет операций</p></div>';
            return;
        }

        container.innerHTML = data.transactions.map(tx => `
            <div class="transaction-item" onclick="showTransactionDetails(${tx.id})">
                <div class="transaction-icon ${tx.type === 'income' ? 'income' : 'expense'}">
                    <i class="bi bi-${tx.type === 'income' ? 'arrow-down-left' : 'arrow-up-right'}"></i>
                </div>
                <div class="transaction-details">
                    <div class="transaction-title">${tx.description || 'Без названия'}</div>
                    <div class="transaction-date">${formatDate(tx.date)} • ${tx.account}</div>
                </div>
                <div class="transaction-amount ${tx.type === 'income' ? 'income' : 'expense'}">
                    ${tx.type === 'income' ? '+' : '-'}${formatMoney(tx.amount)}
                </div>
                <div class="transaction-actions">
                    <button class="btn btn-sm btn-link text-danger p-0" onclick="event.stopPropagation(); deleteTransaction(${tx.id})">
                        <i class="bi bi-trash"></i>
                    </button>
                </div>
            </div>
        `).join('');

        // Store page info for load more
        container.dataset.page = page;
        container.dataset.hasNext = data.has_next;

        // Hide load more button if no more pages
        const loadMoreBtn = container.parentElement.querySelector('.btn');
        if (loadMoreBtn) {
            loadMoreBtn.style.display = data.has_next ? 'inline-block' : 'none';
        }
    } catch (error) {
        document.getElementById('transactionsList').innerHTML = `
            <div class="empty-state">
                <i class="bi bi-exclamation-triangle text-warning"></i>
                <p>Ошибка загрузки</p>
            </div>
        `;
    }
}

function loadMoreTransactions() {
    const container = document.getElementById('transactionsList');
    const currentPage = parseInt(container.dataset.page) || 1;
    loadTransactionsList(currentPage + 1);
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    // Initialize pull-to-refresh
    initPullToRefresh();

    // Handle navigation
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            const page = item.dataset.page;
            navigateTo(page);
        });
    });

    // Load initial page
    navigateTo('home');
});

// Show add menu from FAB
function showAddMenu() {
    const quickActions = document.getElementById('quickActions');
    quickActions.style.display = quickActions.style.display === 'none' ? 'flex' : 'none';
}

// Account Functions
function showAddAccount() {
    const modal = document.getElementById('addAccountModal');
    if (modal) {
        modal.classList.add('active');
    } else {
        alert('Модальное окно создания счета в разработке');
    }
}

async function saveAccount() {
    const name = document.getElementById('accName').value;
    const type = document.getElementById('accType').value;
    const balance = document.getElementById('accBalance').value;
    const currency = document.getElementById('accCurrency').value;

    if (!name || !balance) {
        alert('Заполните обязательные поля');
        return;
    }

    try {
        const response = await fetchAPI('/accounts', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                name,
                type,
                balance: parseFloat(balance),
                currency
            })
        });

        closeModal('addAccountModal');
        alert('Счет создан');
        navigateTo('accounts');
    } catch (error) {
        alert('Ошибка при сохранении: ' + error.message);
    }
}

// Transaction Delete Function
async function deleteTransaction(transactionId) {
    if (!confirm('Вы уверены, что хотите удалить эту транзакцию?')) {
        return;
    }

    try {
        await fetchAPI(`/transactions/${transactionId}`, {
            method: 'DELETE'
        });

        alert('Транзакция удалена');
        // Reload current page
        navigateTo(currentPage);
    } catch (error) {
        alert('Ошибка при удалении: ' + error.message);
    }
}

// Show transaction details
function showTransactionDetails(transactionId) {
    const modal = document.getElementById('transactionDetailsModal');
    if (modal) {
        modal.classList.add('active');
    } else {
        alert('Детали транзакции в разработке');
    }
}

// Debt Functions
function showAddDebt(debtType = 'i_owe') {
    const modal = document.getElementById('addDebtModal');
    const typeSelect = document.getElementById('debtType');

    typeSelect.value = debtType;
    updateDebtModalTitle();

    // Clear form
    document.getElementById('debtCounterparty').value = '';
    document.getElementById('debtAmount').value = '';
    document.getElementById('debtDueDate').value = '';
    document.getElementById('debtDescription').value = '';

    modal.classList.add('active');
}

function updateDebtModalTitle() {
    const typeSelect = document.getElementById('debtType');
    const title = document.getElementById('debtModalTitle');

    if (typeSelect.value === 'i_owe') {
        title.textContent = 'Я должен';
    } else {
        title.textContent = 'Мне должны';
    }
}

async function saveDebt() {
    const debtType = document.getElementById('debtType').value;
    const counterparty = document.getElementById('debtCounterparty').value;
    const amount = document.getElementById('debtAmount').value;
    const dueDate = document.getElementById('debtDueDate').value;
    const description = document.getElementById('debtDescription').value;

    if (!counterparty || !amount) {
        alert('Заполните обязательные поля');
        return;
    }

    try {
        const response = await fetchAPI('/debts', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                debt_type: debtType,
                counterparty,
                amount: parseFloat(amount),
                due_date: dueDate || null,
                description: description || ''
            })
        });

        closeModal('addDebtModal');
        alert('Долг добавлен');
        navigateTo('obligations');
    } catch (error) {
        alert('Ошибка при сохранении: ' + error.message);
    }
}

// Purchase/QR Code Functions
let purchaseData = {
    amount: null,
    description: '',
    account: null,
    category: null
};

function showPurchaseModal() {
    const modal = document.getElementById('purchaseModal');
    modal.classList.add('active');

    // Reset form
    showQRUpload();
    document.getElementById('qrCodeFile').value = '';
    document.getElementById('qrCodeCamera').value = '';
    document.getElementById('qrPreview').style.display = 'none';
    document.getElementById('purchaseAmount').value = '';
    document.getElementById('purchaseDescription').value = '';
    document.getElementById('purchaseAccount').value = '';
    document.getElementById('purchaseCategory').value = '';

    // Load accounts
    loadAccountsAndCategories();
}

function showQRUpload() {
    document.getElementById('qrUploadSection').style.display = 'block';
    document.getElementById('manualPurchaseSection').style.display = 'none';
    document.getElementById('qrProcessing').style.display = 'none';
}

function showManualPurchase() {
    document.getElementById('qrUploadSection').style.display = 'none';
    document.getElementById('manualPurchaseSection').style.display = 'block';
    document.getElementById('qrProcessing').style.display = 'none';
}

function handleQRUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    // Show preview
    const reader = new FileReader();
    reader.onload = function(e) {
        document.getElementById('qrPreviewImage').src = e.target.result;
        document.getElementById('qrPreview').style.display = 'block';
    };
    reader.readAsDataURL(file);

    // Process QR code
    processQRCode(file);
}

async function processQRCode(file) {
    document.getElementById('qrProcessing').style.display = 'block';
    document.getElementById('qrUploadSection').style.display = 'none';

    try {
        // Create form data
        const formData = new FormData();
        formData.append('qr_code', file);

        // Send to server for processing
        const response = await fetch(`${API_BASE}/purchase/qr`, {
            method: 'POST',
            body: formData
        });

        const data = await response.json();

        document.getElementById('qrProcessing').style.display = 'none';

        if (data.success) {
            // Fill form with extracted data
            showManualPurchase();

            if (data.amount) {
                document.getElementById('purchaseAmount').value = data.amount;
                purchaseData.amount = data.amount;
            }

            if (data.description) {
                document.getElementById('purchaseDescription').value = data.description;
                purchaseData.description = data.description;
            }

            if (data.category_id) {
                document.getElementById('purchaseCategory').value = data.category_id;
                purchaseData.category = data.category_id;
            }

            alert(`QR-код распознан!\nСумма: ${data.amount || 'Не указано'}\n${data.description ? 'Описание: ' + data.description : ''}`);
        } else {
            showManualPurchase();
            alert('Не удалось распознать QR-код. Введите данные вручную.');
        }
    } catch (error) {
        console.error('QR processing error:', error);
        document.getElementById('qrProcessing').style.display = 'none';
        showManualPurchase();
        alert('Ошибка при распознавании QR-кода. Введите данные вручную.');
    }
}

async function savePurchase() {
    const manualMode = document.getElementById('manualPurchaseSection').style.display === 'block';

    if (manualMode) {
        // Manual entry
        const amount = document.getElementById('purchaseAmount').value;
        const description = document.getElementById('purchaseDescription').value;
        const account = document.getElementById('purchaseAccount').value;
        const category = document.getElementById('purchaseCategory').value;

        if (!amount || !account) {
            alert('Заполните обязательные поля (сумма и счет)');
            return;
        }

        try {
            const response = await fetchAPI('/transactions', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    type: 'expense',
                    amount: parseFloat(amount),
                    description: description || 'Покупка',
                    account_id: parseInt(account),
                    category_id: category || null,
                    date: new Date().toISOString().split('T')[0]
                })
            });

            closeModal('purchaseModal');
            alert('Покупка сохранена');
            navigateTo('home');
        } catch (error) {
            alert('Ошибка при сохранении: ' + error.message);
        }
    } else {
        alert('Сначала загрузите QR-код или введите данные вручную');
    }
}

function showAddTransfer() {
    alert('Переводы в разработке. Используйте "Расход" и "Доход" для переводов между счетами.');
}

// QR Code handling
// Camera capture uses HTML5 file input with capture="environment" attribute
// Photos are processed server-side using pyzbar library
