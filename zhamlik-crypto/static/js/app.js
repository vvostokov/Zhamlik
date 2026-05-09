// API Base URL
const API_BASE = '/api/crypto';

// App State
let currentPage = 'home';
let cache = {};
let currentPlatformId = null;

// Utility Functions
function formatMoney(amount, currency = 'USD') {
    return new Intl.NumberFormat('ru-RU', {
        style: 'currency',
        currency: currency,
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    }).format(amount);
}

function formatCrypto(amount) {
    if (amount < 0.0001) {
        return amount.toFixed(6);
    } else if (amount < 1) {
        return amount.toFixed(4);
    }
    return amount.toFixed(2);
}

function formatDate(dateStr) {
    const date = new Date(dateStr);
    return date.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' });
}

async function fetchAPI(endpoint, options = {}) {
    try {
        const response = await fetch(`${API_BASE}${endpoint}`, options);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error(`API Error (${endpoint}):`, error);
        throw error;
    }
}

// Page Rendering Functions
function renderHomePage(data) {
    // Проверяем флаг auth_required
    if (data.auth_required) {
        return renderAuthRequiredPage();
    }

    const totalBalanceUSD = data.total_balance_usd || 0;
    const totalBalanceRUB = data.total_balance_rub || 0;
    const assets = data.assets || [];
    const recentTransactions = data.recent_transactions || [];

    return `
        <div class="header">
            <h5 class="mb-0">Zhamlik Crypto</h5>
        </div>

        <div class="crypto-balance-card">
            <div class="balance-subtitle">Общий баланс портфеля</div>
            <div class="balance-amount">${formatMoney(totalBalanceUSD, 'USD')}</div>
            <div class="balance-subtitle">≈ ${formatMoney(totalBalanceRUB, 'RUB')}</div>
        </div>

        <h6 class="section-title">Ваши активы</h6>
        ${assets.length > 0 ? `
            <div style="margin: 0 16px;">
                ${assets.map(asset => `
                    <div class="asset-item">
                        <div class="asset-icon">
                            ${asset.ticker.substring(0, 2).toUpperCase()}
                        </div>
                        <div style="flex: 1;">
                            <div class="fw-bold">${asset.name}</div>
                            <div class="text-muted small">${asset.ticker.toUpperCase()}</div>
                        </div>
                        <div class="text-end">
                            <div class="fw-bold">${formatCrypto(asset.quantity)}</div>
                            <div class="text-muted small">${formatMoney(asset.value_usd, 'USD')}</div>
                        </div>
                    </div>
                `).join('')}
            </div>
        ` : `
            <div class="empty-state">
                <i class="bi bi-wallet"></i>
                <p>Нет активов<br><small>Добавьте свой первый крипто-актив</small></p>
            </div>
        `}

        <h6 class="section-title">Последние операции</h6>
        ${recentTransactions.length > 0 ? `
            <div style="margin: 0 16px;">
                ${recentTransactions.slice(0, 5).map(tx => `
                    <div class="asset-item">
                        <div class="asset-icon" style="background: ${tx.type === 'buy' ? 'var(--success-color)' : 'var(--danger-color)'}">
                            <i class="bi bi-arrow-${tx.type === 'buy' ? 'down' : 'up'}"></i>
                        </div>
                        <div style="flex: 1;">
                            <div class="fw-bold">${tx.type === 'buy' ? 'Покупка' : 'Продажа'}</div>
                            <div class="text-muted small">${formatDate(tx.timestamp)}</div>
                        </div>
                        <div class="text-end">
                            <div class="fw-bold">${formatCrypto(tx.amount1)}</div>
                            <div class="text-muted small">${tx.asset1_ticker.toUpperCase()}</div>
                        </div>
                    </div>
                `).join('')}
            </div>
        ` : `
            <div class="empty-state">
                <i class="bi bi-clock-history"></i>
                <p>Нет операций<br><small>История транзакций пуста</small></p>
            </div>
        `}
    `;
}

function renderAnalyticsPage(data) {
    const distribution = data.portfolio_distribution || [];
    const performanceChart = data.performance_chart || [];

    return `
        <div class="header">
            <h5 class="mb-0">Аналитика</h5>
        </div>

        <div class="section-title">Распределение портфеля</div>
        ${distribution.length > 0 ? `
            <div style="margin: 0 16px;">
                ${distribution.map(item => `
                    <div class="stat-card">
                        <div class="d-flex justify-content-between align-items-center mb-2">
                            <span class="fw-bold">${item.ticker}</span>
                            <span class="text-muted">${item.percentage.toFixed(1)}%</span>
                        </div>
                        <div class="progress" style="height: 8px;">
                            <div class="progress-bar" style="width: ${item.percentage}%; background: var(--primary-color);"></div>
                        </div>
                        <div class="text-muted small mt-1">${formatMoney(item.value_usd, 'USD')}</div>
                    </div>
                `).join('')}
            </div>
        ` : `
            <div class="empty-state">
                <i class="bi bi-pie-chart"></i>
                <p>Нет данных<br><small>Добавьте активы для анализа</small></p>
            </div>
        `}

        <div class="section-title">Динамика портфеля</div>
        <div class="chart-container">
            <canvas id="performanceChart"></canvas>
        </div>
    `;
}

function renderTransactionsPage() {
    return `
        <div class="header">
            <h5 class="mb-0">Операции</h5>
        </div>

        <div class="section-title">Фильтры</div>
        <div class="stat-card">
            <div class="row g-2">
                <div class="col-6">
                    <select class="form-select form-select-sm" id="filterType" onchange="loadTransactions(1)">
                        <option value="all">Все типы</option>
                        <option value="buy">Покупка</option>
                        <option value="sell">Продажа</option>
                        <option value="transfer">Перевод</option>
                        <option value="earn">Earn</option>
                    </select>
                </div>
                <div class="col-6">
                    <select class="form-select form-select-sm" id="sortOrder" onchange="loadTransactions(1)">
                        <option value="desc">Сначала новые</option>
                        <option value="asc">Сначала старые</option>
                    </select>
                </div>
            </div>
        </div>

        <div id="transactionsList">
            <div class="loading" style="height: 200px;">
                <div class="spinner-border spinner-border-sm" role="status"></div>
            </div>
        </div>

        <div id="paginationButtons"></div>
    `;
}

function renderPricesPage() {
    return `
        <div class="header">
            <h5 class="mb-0">Курсы криптовалют</h5>
        </div>

        <div id="pricesList">
            <div class="loading" style="height: 200px;">
                <div class="spinner-border spinner-border-sm" role="status"></div>
            </div>
        </div>
    `;
}

function renderPlatformsPage() {
    return `
        <div class="header d-flex justify-content-between align-items-center">
            <h5 class="mb-0">Биржи</h5>
            <button class="btn btn-light btn-sm" onclick="showAddPlatformModal()">
                <i class="bi bi-plus-lg"></i>
            </button>
        </div>

        <div id="platformsList">
            <div class="loading" style="height: 200px;">
                <div class="spinner-border spinner-border-sm" role="status"></div>
            </div>
        </div>

        <div class="section-title">Быстрые действия</div>
        <div class="stat-card">
            <button class="btn btn-primary w-100" onclick="showAddAssetModal()">
                <i class="bi bi-plus-circle me-2"></i>Добавить актив вручную
            </button>
        </div>
    `;
}

// Navigation
async function navigateTo(page) {
    currentPage = page;

    // Update active nav item
    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.remove('active');
        if (item.getAttribute('data-page') === page) {
            item.classList.add('active');
        }
    });

    // Update page title
    const titles = {
        'home': 'Zhamlik Crypto',
        'analytics': 'Аналитика',
        'transactions': 'Операции',
        'prices': 'Курсы',
        'platforms': 'Биржи'
    };

    try {
        let html;
        switch(page) {
            case 'home':
                const data = await fetchAPI('/overview');
                html = renderHomePage(data);
                break;
            case 'analytics':
                const analyticsData = await fetchAPI('/analytics?days=30');
                html = renderAnalyticsPage(analyticsData);
                // Initialize chart after render
                setTimeout(() => initAnalyticsChart(analyticsData.performance_chart || []), 100);
                break;
            case 'transactions':
                html = renderTransactionsPage();
                break;
            case 'prices':
                html = renderPricesPage();
                break;
            case 'platforms':
                html = renderPlatformsPage();
                break;
            default:
                html = '<div class="empty-state"><i class="bi bi-question-circle"></i><p>Страница в разработке</p></div>';
        }

        document.getElementById('app').innerHTML = html;

        // Load data for pages that need it
        if (page === 'transactions') {
            loadTransactions(1);
        } else if (page === 'prices') {
            loadPrices();
        } else if (page === 'platforms') {
            loadPlatforms();
        }
    } catch (error) {
        document.getElementById('app').innerHTML = `
            <div class="empty-state">
                <i class="bi bi-exclamation-triangle text-warning"></i>
                <p>Ошибка загрузки данных</p>
                <small class="text-muted">${error.message}</small>
                <button class="btn btn-primary btn-sm mt-3" onclick="navigateTo('${page}')">Попробовать снова</button>
            </div>
        `;
    }
}

function renderAuthRequiredPage() {
    return `
        <div class="header">
            <h5 class="mb-0">Zhamlik Crypto</h5>
        </div>

        <div style="text-align: center; padding: 60px 20px;">
            <i class="bi bi-lock-fill" style="font-size: 4rem; color: var(--primary-color);"></i>
            <h4 class="mt-4">Требуется авторизация</h4>
            <p class="text-muted mt-3">
                Для просмотра крипто-портфеля необходимо авторизоваться в основном приложении Zhamlik.
            </p>
            <div class="card mt-4" style="max-width: 500px; margin: 0 auto;">
                <div class="card-body">
                    <h6 class="card-title">Как использовать:</h6>
                    <ol class="text-start" style="padding-left: 20px;">
                        <li>Откройте <a href="/" target="_blank" style="color: var(--primary-color);">основное приложение Zhamlik</a></li>
                        <li>Авторизуйтесь в системе</li>
                        <li>Вернитесь в это приложение</li>
                        <li>Данные загрузятся автоматически</li>
                    </ol>
                </div>
            </div>
            <p class="text-muted small mt-4">
                <i class="bi bi-info-circle"></i> Это приложение использует общую сессию авторизации с Zhamlik
            </p>
        </div>
    `;
}

async function loadTransactions(page = 1) {
    const filterType = document.getElementById('filterType')?.value || 'all';

    try {
        const data = await fetchAPI(`/transactions?page=${page}&filter_type=${filterType}`);
        const container = document.getElementById('transactionsList');

        if (data.transactions.length === 0) {
            container.innerHTML = '<div class="empty-state"><i class="bi bi-inbox"></i><p>Нет операций</p></div>';
            return;
        }

        container.innerHTML = `
            <div style="margin: 0 16px;">
                ${data.transactions.map(tx => `
                    <div class="asset-item">
                        <div class="asset-icon" style="background: ${getTypeColor(tx.type)}">
                            <i class="bi bi-${getTypeIcon(tx.type)}"></i>
                        </div>
                        <div style="flex: 1;">
                            <div class="fw-bold">${getTypeLabel(tx.type)}</div>
                            <div class="text-muted small">${formatDate(tx.timestamp)}</div>
                            ${tx.platform_name ? `<div class="text-muted small">${tx.platform_name}</div>` : ''}
                        </div>
                        <div class="text-end">
                            <div class="fw-bold">${formatCrypto(tx.amount1)}</div>
                            <div class="text-muted small">${tx.asset1_ticker.toUpperCase()}</div>
                        </div>
                    </div>
                `).join('')}
            </div>
        `;

        // Pagination
        if (data.has_next) {
            document.getElementById('paginationButtons').innerHTML = `
                <div style="text-align: center; padding: 16px;">
                    <button class="btn btn-outline-primary btn-sm" onclick="loadTransactions(${page + 1})">
                        Загрузить ещё
                    </button>
                </div>
            `;
        }
    } catch (error) {
        console.error('Error loading transactions:', error);
    }
}

async function loadPrices() {
    try {
        const data = await fetchAPI('/prices');
        const container = document.getElementById('pricesList');

        if (!data.prices || data.prices.length === 0) {
            container.innerHTML = '<div class="empty-state"><i class="bi bi-currency-bitcoin"></i><p>Нет данных о ценах</p></div>';
            return;
        }

        container.innerHTML = `
            <div style="margin: 0 16px;">
                ${data.prices.map(coin => `
                    <div class="asset-item">
                        <div class="asset-icon">
                            ${coin.symbol.substring(0, 2).toUpperCase()}
                        </div>
                        <div style="flex: 1;">
                            <div class="fw-bold">${coin.name}</div>
                            <div class="text-muted small">${coin.symbol.toUpperCase()}</div>
                        </div>
                        <div class="text-end">
                            <div class="fw-bold">$${parseFloat(coin.price_usd).toFixed(2)}</div>
                            ${coin.price_change_24h ? `
                                <div class="price-change ${parseFloat(coin.price_change_24h) >= 0 ? 'positive' : 'negative'}">
                                    ${parseFloat(coin.price_change_24h) >= 0 ? '+' : ''}${parseFloat(coin.price_change_24h).toFixed(2)}%
                                </div>
                            ` : ''}
                        </div>
                    </div>
                `).join('')}
            </div>
        `;
    } catch (error) {
        console.error('Error loading prices:', error);
    }
}

function getTypeColor(type) {
    const colors = {
        'buy': 'var(--success-color)',
        'sell': 'var(--danger-color)',
        'transfer': 'var(--info-color)',
        'earn': 'var(--warning-color)',
        'deposit': 'var(--success-color)',
        'withdrawal': 'var(--danger-color)'
    };
    return colors[type] || 'var(--primary-color)';
}

function getTypeIcon(type) {
    const icons = {
        'buy': 'arrow-down',
        'sell': 'arrow-up',
        'transfer': 'arrow-left-right',
        'earn': 'percent',
        'deposit': 'box-arrow-in-down',
        'withdrawal': 'box-arrow-up'
    };
    return icons[type] || 'circle';
}

function getTypeLabel(type) {
    const labels = {
        'buy': 'Покупка',
        'sell': 'Продажа',
        'transfer': 'Перевод',
        'earn': 'Earn',
        'deposit': 'Депозит',
        'withdrawal': 'Вывод'
    };
    return labels[type] || type;
}

function initAnalyticsChart(performanceData) {
    const ctx = document.getElementById('performanceChart');
    if (!ctx) return;

    new Chart(ctx, {
        type: 'line',
        data: {
            labels: performanceData.map(d => d.date),
            datasets: [{
                label: 'Стоимость портфеля (USD)',
                data: performanceData.map(d => d.value_usd),
                borderColor: '#7952b3',
                backgroundColor: 'rgba(121, 82, 179, 0.1)',
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: false,
                    ticks: {
                        callback: function(value) {
                            return '$' + value.toLocaleString();
                        }
                    }
                }
            }
        }
    });
}

// Platform Management Functions
async function loadPlatforms() {
    try {
        const data = await fetchAPI('/platforms');
        const container = document.getElementById('platformsList');

        if (!data.platforms || data.platforms.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <i class="bi bi-house-gear"></i>
                    <p>Нет бирж<br><small>Добавьте свою первую биржу</small></p>
                </div>
            `;
            return;
        }

        container.innerHTML = `
            <div style="margin: 0 16px;">
                ${data.platforms.map(platform => `
                    <div class="asset-item" onclick="showPlatformDetail(${platform.id})">
                        <div class="asset-icon">
                            <i class="bi bi-${platform.is_active ? 'check-circle' : 'x-circle'}"></i>
                        </div>
                        <div style="flex: 1;">
                            <div class="fw-bold">${platform.name}</div>
                            <div class="text-muted small">
                                ${platform.has_api_keys ? '<i class="bi bi-key"></i> API ключи' : 'Без API ключей'}
                                ${platform.assets_count > 0 ? ` • ${platform.assets_count} активов` : ''}
                            </div>
                            ${platform.notes ? `<div class="text-muted small">${platform.notes}</div>` : ''}
                        </div>
                        <div class="text-end">
                            <i class="bi bi-chevron-right text-muted"></i>
                        </div>
                    </div>
                `).join('')}
            </div>
        `;
    } catch (error) {
        console.error('Error loading platforms:', error);
        document.getElementById('platformsList').innerHTML = `
            <div class="empty-state">
                <i class="bi bi-exclamation-triangle text-warning"></i>
                <p>Ошибка загрузки</p>
                <small class="text-muted">${error.message}</small>
            </div>
        `;
    }
}

function showAddPlatformModal() {
    const modal = new bootstrap.Modal(document.getElementById('addPlatformModal'));
    modal.show();
}

async function submitAddPlatform() {
    const form = document.getElementById('addPlatformForm');
    const formData = new FormData(form);
    const data = {
        name: formData.get('name'),
        api_key: formData.get('api_key'),
        api_secret: formData.get('api_secret'),
        passphrase: formData.get('passphrase') || '',
        notes: formData.get('notes') || '',
        is_active: true
    };

    try {
        const response = await fetch(`${API_BASE}/platforms`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(data)
        });

        const result = await response.json();

        if (result.success) {
            // Close modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('addPlatformModal'));
            modal.hide();

            // Clear form
            form.reset();

            // Reload platforms
            loadPlatforms();

            // Show success message
            alert('Биржа успешно добавлена!');
        } else {
            alert(`Ошибка: ${result.message}`);
        }
    } catch (error) {
        alert(`Ошибка: ${error.message}`);
    }
}

async function showPlatformDetail(platformId) {
    currentPlatformId = platformId;

    try {
        const data = await fetchAPI(`/platforms/${platformId}`);

        document.getElementById('platformDetailTitle').textContent = data.name;

        let content = `
            <div class="stat-card mb-3">
                <div class="d-flex justify-content-between align-items-center">
                    <div>
                        <div class="text-muted small">Общая стоимость</div>
                        <div class="fw-bold fs-5">${formatMoney(data.total_value_usd, 'USD')}</div>
                    </div>
                    <button class="btn btn-primary btn-sm" onclick="syncPlatform(${platformId})">
                        <i class="bi bi-arrow-clockwise"></i> Синхронизировать
                    </button>
                </div>
            </div>

            <h6 class="mb-3">Активы на бирже</h6>
        `;

        if (data.assets && data.assets.length > 0) {
            content += `
                <div style="max-height: 300px; overflow-y: auto;">
                    ${data.assets.map(asset => `
                        <div class="asset-item">
                            <div class="asset-icon">${asset.ticker.substring(0, 2).toUpperCase()}</div>
                            <div style="flex: 1;">
                                <div class="fw-bold">${asset.name}</div>
                                <div class="text-muted small">${asset.ticker.toUpperCase()}</div>
                            </div>
                            <div class="text-end">
                                <div class="fw-bold">${formatCrypto(asset.quantity)}</div>
                                <div class="text-muted small">${formatMoney(asset.value_usd, 'USD')}</div>
                            </div>
                        </div>
                    `).join('')}
                </div>
            `;
        } else {
            content += `
                <div class="empty-state" style="padding: 40px 20px;">
                    <i class="bi bi-inbox"></i>
                    <p>Нет активов<br><small>Синхронизируйте биржу или добавьте актив вручную</small></p>
                </div>
            `;
        }

        document.getElementById('platformDetailContent').innerHTML = content;

        // Show modal
        const modal = new bootstrap.Modal(document.getElementById('platformDetailModal'));
        modal.show();
    } catch (error) {
        alert(`Ошибка загрузки деталей: ${error.message}`);
    }
}

async function syncPlatform(platformId) {
    const btn = event.target.closest('button');
    const originalHTML = btn.innerHTML;

    // Disable button and show loading
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Синхронизация...';
    btn.disabled = true;

    // Add timeout indicator
    const timeoutIndicator = setTimeout(() => {
        if (btn.disabled) {
            btn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Ещё немного...';
        }
    }, 30000); // Show "ещё немного" after 30 seconds

    try {
        const response = await fetch(`${API_BASE}/platforms/${platformId}/sync`, {
            method: 'POST'
        });

        clearTimeout(timeoutIndicator);

        const result = await response.json();

        if (response.ok && result.success) {
            // Success!
            alert('✅ Синхронизация завершена успешно!\n\n' + result.message);

            // Close modal and reload
            const modal = bootstrap.Modal.getInstance(document.getElementById('platformDetailModal'));
            if (modal) modal.hide();

            // Reload platforms list to show updated asset counts
            loadPlatforms();
        } else {
            // Error from server
            let errorMsg = 'Ошибка синхронизации';
            if (result.message) {
                errorMsg += `:\n${result.message}`;
            }

            if (response.status === 408) {
                errorMsg += '\n\n💡 Синхронизация занимает много времени. Это может быть связано с медленным ответом биржи. Попробуйте ещё раз.';
            }

            alert(errorMsg);
            btn.innerHTML = originalHTML;
            btn.disabled = false;
        }
    } catch (error) {
        clearTimeout(timeoutIndicator);

        // Network error
        alert(`❌ Ошибка соединения:\n${error.message}\n\nПроверьте подключение к интернету и попробуйте снова.`);
        btn.innerHTML = originalHTML;
        btn.disabled = false;
    }
}

async function deletePlatform() {
    if (!confirm('Вы уверены, что хотите удалить эту биржу? Все активы будут удалены.')) {
        return;
    }

    // For now, we'll need to add a delete endpoint
    alert('Функция удаления в разработке');
}

async function showAddAssetModal() {
    // Load platforms for the select
    try {
        const data = await fetchAPI('/platforms');
        const select = document.getElementById('assetPlatformSelect');
        select.innerHTML = '<option value="">Выберите биржу</option>';

        if (data.platforms && data.platforms.length > 0) {
            data.platforms.forEach(platform => {
                const option = document.createElement('option');
                option.value = platform.id;
                option.textContent = platform.name;
                select.appendChild(option);
            });

            const modal = new bootstrap.Modal(document.getElementById('addAssetModal'));
            modal.show();
        } else {
            alert('Сначала добавьте биржу');
        }
    } catch (error) {
        alert(`Ошибка: ${error.message}`);
    }
}

async function submitAddAsset() {
    const form = document.getElementById('addAssetForm');
    const formData = new FormData(form);
    const data = {
        platform_id: parseInt(formData.get('platform_id')),
        ticker: formData.get('ticker'),
        quantity: parseFloat(formData.get('quantity'))
    };

    if (!data.platform_id || !data.ticker || !data.quantity) {
        alert('Заполните все поля');
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/assets`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(data)
        });

        const result = await response.json();

        if (result.success) {
            // Close modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('addAssetModal'));
            modal.hide();

            // Clear form
            form.reset();

            // Reload platforms
            loadPlatforms();

            // Show success message
            alert('Актив успешно добавлен!');
        } else {
            alert(`Ошибка: ${result.message}`);
        }
    } catch (error) {
        alert(`Ошибка: ${error.message}`);
    }
}

// Initialize app
document.addEventListener('DOMContentLoaded', function() {
    // Handle navigation clicks
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', function(e) {
            e.preventDefault();
            const page = this.getAttribute('data-page');
            navigateTo(page);
        });
    });

    // Load initial page
    navigateTo('home');
});
