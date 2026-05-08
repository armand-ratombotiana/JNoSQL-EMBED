/* ==========================================================================
   JUNIFYDB WEB CONSOLE - UI/UX ENHANCEMENTS
   ========================================================================== */

// ============================================================================
// TOAST NOTIFICATION SYSTEM
// ============================================================================

let toastIdCounter = 0;
const toasts = new Map();

/**
 * Show a toast notification
 * @param {string} title - Toast title
 * @param {string} message - Toast message
 * @param {'success'|'error'|'warning'|'info'} type - Toast type
 * @param {number} duration - Auto-hide duration in ms (0 = persistent)
 */
function showToast(title, message, type = 'info', duration = 5000) {
    const container = document.getElementById('toastContainer');
    if (!container) return;

    const toastId = ++toastIdCounter;
    const icons = {
        success: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><path d="M22 4L12 14.01l-3-3"/></svg>',
        error: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M15 9l-6 6M9 9l6 6"/></svg>',
        warning: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><path d="M12 9v4M12 17h.01"/></svg>',
        info: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/></svg>'
    };

    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.id = `toast-${toastId}`;
    toast.innerHTML = `
        <div class="toast-icon">${icons[type]}</div>
        <div class="toast-content">
            <div class="toast-title">${escapeHtml(title)}</div>
            <div class="toast-message">${escapeHtml(message)}</div>
        </div>
        <button class="toast-close" onclick="hideToast(${toastId})">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M18 6L6 18M6 6l12 12"/>
            </svg>
        </button>
    `;

    toast.onclick = (e) => {
        if (!e.target.closest('.toast-close')) {
            hideToast(toastId);
        }
    };

    container.appendChild(toast);
    toasts.set(toastId, toast);

    if (duration > 0) {
        setTimeout(() => hideToast(toastId), duration);
    }

    return toastId;
}

/**
 * Hide a toast notification
 * @param {number} toastId - Toast ID to hide
 */
function hideToast(toastId) {
    const toast = toasts.get(toastId);
    if (!toast) return;

    toast.classList.add('toast-hiding');
    setTimeout(() => {
        toast.remove();
        toasts.delete(toastId);
    }, 300);
}

/**
 * Clear all toasts
 */
function clearAllToasts() {
    toasts.forEach((_, id) => hideToast(id));
}

// ============================================================================
// CONFIRMATION DIALOG SYSTEM
// ============================================================================

/**
 * Show a confirmation dialog
 * @param {string} title - Dialog title
 * @param {string} message - Dialog message
 * @param {Function} onConfirm - Callback when confirmed
 * @param {'warning'|'danger'|'info'} type - Dialog type
 * @param {string} confirmText - Confirm button text
 * @param {string} cancelText - Cancel button text
 */
function showConfirmDialog(title, message, onConfirm, type = 'warning', confirmText = 'Confirm', cancelText = 'Cancel') {
    const overlay = document.getElementById('confirmModal');
    if (!overlay) {
        // Create modal if it doesn't exist
        createConfirmModal();
    }

    const modalOverlay = document.getElementById('confirmModal');
    const icons = {
        warning: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><path d="M12 9v4M12 17h.01"/></svg>',
        danger: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><path d="M12 9v4M12 17h.01"/></svg>',
        info: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/></svg>'
    };

    document.getElementById('confirmModalIcon').innerHTML = icons[type];
    document.getElementById('confirmModalTitle').textContent = title;
    document.getElementById('confirmModalMessage').textContent = message;
    document.getElementById('confirmBtn').textContent = confirmText;
    document.getElementById('cancelBtn').textContent = cancelText;

    modalOverlay.classList.add('active');
    document.body.style.overflow = 'hidden';

    // Setup confirm handler
    window._confirmCallback = onConfirm;
}

/**
 * Hide confirmation dialog
 */
function hideConfirmDialog() {
    const modalOverlay = document.getElementById('confirmModal');
    if (!modalOverlay) return;

    modalOverlay.classList.remove('active');
    document.body.style.overflow = '';
    window._confirmCallback = null;
}

/**
 * Confirm action handler
 */
function confirmAction() {
    if (window._confirmCallback) {
        window._confirmCallback();
    }
    hideConfirmDialog();
}

/**
 * Create confirmation modal HTML
 */
function createConfirmModal() {
    const modal = document.createElement('div');
    modal.id = 'confirmModal';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
        <div class="modal">
            <div class="modal-header">
                <div class="modal-icon warning" id="confirmModalIcon"></div>
                <span class="modal-title" id="confirmModalTitle">Confirm</span>
            </div>
            <div class="modal-body" id="confirmModalMessage">
                Are you sure?
            </div>
            <div class="modal-actions">
                <button class="btn btn-ghost" id="cancelBtn" onclick="hideConfirmDialog()">Cancel</button>
                <button class="btn btn-danger" id="confirmBtn" onclick="confirmAction()">Confirm</button>
            </div>
        </div>
    `;
    document.body.appendChild(modal);
}

// ============================================================================
// LOADING OVERLAY
// ============================================================================

/**
 * Show loading overlay
 * @param {string} message - Loading message
 */
function showLoading(message = 'Loading...') {
    let overlay = document.getElementById('loadingOverlay');
    if (!overlay) {
        createLoadingOverlay();
        overlay = document.getElementById('loadingOverlay');
    }
    document.getElementById('loadingText').textContent = message;
    overlay.classList.add('active');
    document.body.style.overflow = 'hidden';
}

/**
 * Hide loading overlay
 */
function hideLoading() {
    const overlay = document.getElementById('loadingOverlay');
    if (!overlay) return;
    overlay.classList.remove('active');
    document.body.style.overflow = '';
}

/**
 * Create loading overlay HTML
 */
function createLoadingOverlay() {
    const overlay = document.createElement('div');
    overlay.id = 'loadingOverlay';
    overlay.className = 'loading-overlay';
    overlay.innerHTML = `
        <div class="loading-spinner-large"></div>
        <div class="loading-text" id="loadingText">Loading...</div>
    `;
    document.body.appendChild(overlay);
}

// ============================================================================
// KEYBOARD SHORTCUTS
// ============================================================================

/**
 * Initialize keyboard shortcuts
 */
function initKeyboardShortcuts() {
    document.addEventListener('keydown', (e) => {
        // Ctrl+S: Save (context-dependent)
        if (e.ctrlKey && e.key === 's') {
            e.preventDefault();
            handleSaveShortcut();
        }

        // Ctrl+F: Focus search/filter
        if (e.ctrlKey && e.key === 'f') {
            const activeTab = document.querySelector('.tab-content.active');
            if (activeTab) {
                const searchInput = activeTab.querySelector('input[type="text"], input[type="search"]');
                if (searchInput) {
                    e.preventDefault();
                    searchInput.focus();
                    searchInput.select();
                }
            }
        }

        // Ctrl+Enter: Execute query (in query tabs)
        if (e.ctrlKey && e.key === 'Enter') {
            const activeTab = document.querySelector('.tab-content.active');
            if (activeTab) {
                if (activeTab.id === 'tab-sql') {
                    e.preventDefault();
                    executeSql();
                } else if (activeTab.id === 'tab-hybrid') {
                    e.preventDefault();
                    executeHybridQuery();
                } else if (activeTab.id === 'tab-query') {
                    e.preventDefault();
                    executeQuery();
                }
            }
        }

        // Escape: Close modals/panels
        if (e.key === 'Escape') {
            hideConfirmDialog();
            hideLoading();
            closeQueryHistory();
        }

        // Ctrl+H: Toggle query history
        if (e.ctrlKey && e.key === 'h') {
            e.preventDefault();
            toggleQueryHistory();
        }

        // Ctrl+R: Refresh current view
        if (e.ctrlKey && e.key === 'r' && !e.shiftKey) {
            // Allow browser refresh with Ctrl+Shift+R
            const activeTab = document.querySelector('.tab-content.active');
            if (activeTab) {
                if (activeTab.id === 'tab-schema') {
                    e.preventDefault();
                    refreshSchema();
                    refreshTables();
                } else if (activeTab.id === 'tab-overview') {
                    e.preventDefault();
                    loadMetrics();
                    loadStats();
                }
            }
        }
    });
}

/**
 * Handle Ctrl+S save shortcut
 */
function handleSaveShortcut() {
    const activeTab = document.querySelector('.tab-content.active');
    if (!activeTab) return;

    // SQL tab - save query to history
    if (activeTab.id === 'tab-sql') {
        const sql = document.getElementById('sqlQuery')?.value.trim();
        if (sql) {
            addToSqlHistory(sql);
            showToast('Saved', 'Query saved to history', 'success');
        }
    }

    // Collections tab - export current collection
    if (activeTab.id === 'tab-collections') {
        exportCollectionData();
    }
}

// ============================================================================
// AUTO-REFRESH TOGGLE
// ============================================================================

let autoRefreshEnabled = false;
let autoRefreshInterval = null;

/**
 * Toggle auto-refresh for metrics
 */
function toggleAutoRefresh() {
    autoRefreshEnabled = !autoRefreshEnabled;
    const toggle = document.getElementById('autoRefreshToggle');
    const switchEl = document.getElementById('autoRefreshSwitch');

    if (autoRefreshEnabled) {
        toggle.classList.add('active');
        switchEl.classList.add('active');
        startAutoRefresh();
        showToast('Auto-refresh', 'Metrics will refresh every 5 seconds', 'info');
    } else {
        toggle.classList.remove('active');
        switchEl.classList.remove('active');
        stopAutoRefresh();
        showToast('Auto-refresh', 'Disabled', 'info');
    }
}

/**
 * Start auto-refresh interval
 */
function startAutoRefresh() {
    stopAutoRefresh();
    autoRefreshInterval = setInterval(() => {
        loadMetrics();
        loadStats();
    }, 5000);
}

/**
 * Stop auto-refresh interval
 */
function stopAutoRefresh() {
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
        autoRefreshInterval = null;
    }
}

// ============================================================================
// QUERY HISTORY (localStorage)
// ============================================================================

const SQL_HISTORY_KEY = 'junifydb_sql_history';
const HYBRID_HISTORY_KEY = 'junifydb_hybrid_history';
const MAX_HISTORY_ITEMS = 50;

/**
 * Add query to history
 * @param {string} query - Query text
 * @param {string} type - Query type (sql, hybrid, nosql, vector)
 * @param {number} rows - Number of rows returned
 * @param {number} executionTime - Execution time in ms
 */
function addToQueryHistory(query, type, rows = 0, executionTime = 0) {
    const historyKey = type === 'sql' ? SQL_HISTORY_KEY : HYBRID_HISTORY_KEY;
    let history = JSON.parse(localStorage.getItem(historyKey) || '[]');

    history.unshift({
        query,
        type,
        rows,
        executionTime,
        timestamp: new Date().toISOString()
    });

    // Limit history size
    if (history.length > MAX_HISTORY_ITEMS) {
        history = history.slice(0, MAX_HISTORY_ITEMS);
    }

    localStorage.setItem(historyKey, JSON.stringify(history));
    updateQueryHistoryPanel();
}

/**
 * Get query history
 * @param {string} type - Query type
 * @returns {Array} History items
 */
function getQueryHistory(type = 'sql') {
    const historyKey = type === 'sql' ? SQL_HISTORY_KEY : HYBRID_HISTORY_KEY;
    return JSON.parse(localStorage.getItem(historyKey) || '[]');
}

/**
 * Clear query history
 * @param {string} type - Query type
 */
function clearQueryHistory(type) {
    if (type) {
        const historyKey = type === 'sql' ? SQL_HISTORY_KEY : HYBRID_HISTORY_KEY;
        localStorage.removeItem(historyKey);
    } else {
        localStorage.removeItem(SQL_HISTORY_KEY);
        localStorage.removeItem(HYBRID_HISTORY_KEY);
    }
    updateQueryHistoryPanel();
    showToast('History Cleared', 'Query history has been cleared', 'success');
}

/**
 * Update query history panel UI
 */
function updateQueryHistoryPanel() {
    const listEl = document.getElementById('queryHistoryList');
    if (!listEl) return;

    const sqlHistory = getQueryHistory('sql');
    const hybridHistory = getQueryHistory('hybrid');
    const allHistory = [...sqlHistory, ...hybridHistory]
        .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
        .slice(0, 20);

    if (allHistory.length === 0) {
        listEl.innerHTML = `
            <div class="query-history-empty">
                <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"/>
                </svg>
                <p>No query history yet</p>
            </div>
        `;
        return;
    }

    listEl.innerHTML = allHistory.map(item => {
        const date = new Date(item.timestamp);
        const timeStr = date.toLocaleTimeString();
        const dateStr = date.toLocaleDateString();

        return `
            <div class="query-history-item" onclick="loadQueryFromHistory(${escapeHtml(JSON.stringify(item).replace(/'/g, "\\'"))})">
                <div class="time">${timeStr} - ${dateStr}</div>
                <div class="query">${escapeHtml(item.query)}</div>
                <div class="meta">
                    <span class="badge badge-info">${item.type.toUpperCase()}</span>
                    <span style="font-size: 0.75rem; color: var(--text-muted);">
                        ${item.rows} rows, ${item.executionTime}ms
                    </span>
                </div>
            </div>
        `;
    }).join('');
}

/**
 * Toggle query history panel
 */
function toggleQueryHistory() {
    const panel = document.getElementById('queryHistoryPanel');
    const overlay = document.getElementById('queryHistoryOverlay');
    if (!panel || !overlay) return;

    const isActive = panel.classList.contains('active');
    if (isActive) {
        closeQueryHistory();
    } else {
        updateQueryHistoryPanel();
        panel.classList.add('active');
        overlay.classList.add('active');
        document.body.style.overflow = 'hidden';
    }
}

/**
 * Close query history panel
 */
function closeQueryHistory() {
    const panel = document.getElementById('queryHistoryPanel');
    const overlay = document.getElementById('queryHistoryOverlay');
    if (!panel || !overlay) return;

    panel.classList.remove('active');
    overlay.classList.remove('active');
    document.body.style.overflow = '';
}

/**
 * Load query from history
 * @param {Object} item - History item
 */
function loadQueryFromHistory(item) {
    if (item.type === 'sql') {
        document.getElementById('sqlQuery').value = item.query;
        showTab('sql');
    } else {
        document.getElementById('hybridQuery').value = item.query;
        document.getElementById('hybridMode').value = item.type;
        showTab('hybrid');
    }
    closeQueryHistory();
}

/**
 * Legacy function for backward compatibility
 */
function loadSqlHistory() {
    toggleQueryHistory();
}

// ============================================================================
// COPY TO CLIPBOARD
// ============================================================================

/**
 * Copy text to clipboard
 * @param {string} text - Text to copy
 * @returns {Promise<boolean>} Success status
 */
async function copyToClipboard(text) {
    try {
        await navigator.clipboard.writeText(text);
        showToast('Copied', 'Content copied to clipboard', 'success');
        return true;
    } catch (err) {
        // Fallback for older browsers
        const textarea = document.createElement('textarea');
        textarea.value = text;
        textarea.style.position = 'fixed';
        textarea.style.opacity = '0';
        document.body.appendChild(textarea);
        textarea.select();
        try {
            document.execCommand('copy');
            showToast('Copied', 'Content copied to clipboard', 'success');
            return true;
        } catch (err2) {
            showToast('Error', 'Failed to copy to clipboard', 'error');
            return false;
        } finally {
            document.body.removeChild(textarea);
        }
    }
}

/**
 * Copy SQL results to clipboard
 */
function copySqlResults() {
    const resultsEl = document.getElementById('sqlResults');
    if (!resultsEl) return;

    const table = resultsEl.querySelector('table');
    if (!table) {
        showToast('Error', 'No results to copy', 'error');
        return;
    }

    // Extract table data as TSV
    let text = '';
    const rows = table.querySelectorAll('tr');
    rows.forEach((row, i) => {
        const cells = row.querySelectorAll('th, td');
        text += Array.from(cells).map(cell => cell.textContent.trim()).join('\t') + '\n';
    });

    copyToClipboard(text);
}

/**
 * Export SQL results as CSV
 */
function exportSqlAsCsv() {
    const resultsEl = document.getElementById('sqlResults');
    if (!resultsEl) return;

    const table = resultsEl.querySelector('table');
    if (!table) {
        showToast('Error', 'No results to export', 'error');
        return;
    }
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
    exportTableAsCsv(table, `sql_results_${timestamp}.csv`);
}

/**
 * Copy JSON to clipboard
 * @param {Object} data - JSON data to copy
 */
function copyJsonToClipboard(data) {
    copyToClipboard(JSON.stringify(data, null, 2));
}

// ============================================================================
// EXPORT FUNCTIONS
// ============================================================================

/**
 * Export data as JSON file
 * @param {Object} data - Data to export
 * @param {string} filename - Filename
 */
function exportAsJson(data, filename) {
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
    showToast('Exported', `Data exported to ${filename}`, 'success');
}

/**
 * Export table as CSV
 * @param {HTMLTableElement} table - Table element
 * @param {string} filename - Filename
 */
function exportTableAsCsv(table, filename) {
    if (!table) return;

    let csv = '';
    const rows = table.querySelectorAll('tr');
    rows.forEach(row => {
        const cells = row.querySelectorAll('th, td');
        csv += Array.from(cells).map(cell => {
            const text = cell.textContent.trim();
            // Escape quotes and wrap in quotes if contains comma
            if (text.includes(',') || text.includes('"') || text.includes('\n')) {
                return '"' + text.replace(/"/g, '""') + '"';
            }
            return text;
        }).join(',') + '\n';
    });

    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
    showToast('Exported', `Data exported to ${filename}`, 'success');
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

/**
 * Escape HTML special characters
 * @param {string} str - Input string
 * @returns {string} Escaped string
 */
function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

/**
 * Format execution time
 * @param {number} ms - Milliseconds
 * @returns {string} Formatted time
 */
function formatExecutionTime(ms) {
    if (ms < 1) return '<1ms';
    if (ms < 1000) return `${Math.round(ms)}ms`;
    return `${(ms / 1000).toFixed(2)}s`;
}

/**
 * Format number with commas
 * @param {number} num - Number to format
 * @returns {string} Formatted number
 */
function formatNumber(num) {
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

/**
 * Debounce function
 * @param {Function} func - Function to debounce
 * @param {number} wait - Wait time in ms
 * @returns {Function} Debounced function
 */
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

/**
 * Throttle function
 * @param {Function} func - Function to throttle
 * @param {number} limit - Time limit in ms
 * @returns {Function} Throttled function
 */
function throttle(func, limit) {
    let inThrottle;
    return function(...args) {
        if (!inThrottle) {
            func.apply(this, args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
        }
    };
}

// ============================================================================
// MOBILE NAVIGATION
// ============================================================================

/**
 * Toggle mobile navigation
 */
function toggleMobileNav() {
    const tabs = document.querySelector('.tabs');
    if (tabs) {
        tabs.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
}

// ============================================================================
// ENHANCED MESSAGE DISPLAY
// ============================================================================

/**
 * Show enhanced message with toast
 * @param {string} elementId - Message element ID
 * @param {string} message - Message text
 * @param {'success'|'error'|'warning'|'info'} type - Message type
 * @param {boolean} showToastAlso - Also show toast notification
 */
function showMessage(elementId, message, type = 'info', showToastAlso = true) {
    const el = document.getElementById(elementId);
    if (!el) return;

    const icons = {
        success: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><path d="M22 4L12 14.01l-3-3"/></svg>',
        error: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M15 9l-6 6M9 9l6 6"/></svg>',
        warning: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><path d="M12 9v4M12 17h.01"/></svg>',
        info: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/></svg>'
    };

    el.innerHTML = `<div class="msg msg-${type}">${icons[type]}<span>${escapeHtml(message)}</span></div>`;

    if (showToastAlso !== false) {
        const toastTitles = {
            success: 'Success',
            error: 'Error',
            warning: 'Warning',
            info: 'Info'
        };
        showToast(toastTitles[type], message, type);
    }
}

// ============================================================================
// INITIALIZATION
// ============================================================================

/**
 * Initialize all enhancements
 */
function initEnhancements() {
    initKeyboardShortcuts();
    createConfirmModal();
    createLoadingOverlay();
    updateQueryHistoryPanel();

    // Show keyboard shortcuts hint on first visit
    if (!localStorage.getItem('shortcutsHintShown')) {
        setTimeout(() => {
            showToast('Keyboard Shortcuts', 'Press Ctrl+H for query history, Ctrl+Enter to execute', 'info', 8000);
            localStorage.setItem('shortcutsHintShown', 'true');
        }, 2000);
    }

    console.log('JunifyDB Console Enhancements initialized');
}

// Auto-initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initEnhancements);
} else {
    initEnhancements();
}
