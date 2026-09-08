/* ==========================================================================
   JUNIFYDB WEB CONSOLE - UI/UX ENHANCEMENTS
   Version 2.0 — Fixed & Improved
   ========================================================================== */

'use strict';

// ============================================================================
// SAFE QUERY HISTORY ITEM CACHE
// Maps string keys → history item objects to avoid JSON-in-HTML-attribute bugs
// ============================================================================
window.__queryHistoryItems = Object.create(null);

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
    // Ensure a toast container exists (create one if index.html didn't supply it)
    let container = document.getElementById('toastContainer');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toastContainer';
        container.className = 'toast-container';
        document.body.appendChild(container);
    }

    const toastId = ++toastIdCounter;
    const icons = {
        success: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><path d="M22 4L12 14.01l-3-3"/></svg>',
        error:   '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M15 9l-6 6M9 9l6 6"/></svg>',
        warning: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><path d="M12 9v4M12 17h.01"/></svg>',
        info:    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/></svg>'
    };
    const safeType = icons[type] ? type : 'info';

    const toast = document.createElement('div');
    toast.className = `toast toast-${safeType}`;
    toast.id = `toast-${toastId}`;
    toast.setAttribute('role', 'alert');
    toast.setAttribute('aria-live', 'assertive');
    toast.innerHTML = `
        <div class="toast-icon">${icons[safeType]}</div>
        <div class="toast-content">
            <div class="toast-title">${escapeHtml(String(title))}</div>
            <div class="toast-message">${escapeHtml(String(message))}</div>
        </div>
        <button class="toast-close" aria-label="Dismiss notification">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M18 6L6 18M6 6l12 12"/>
            </svg>
        </button>
    `;

    // Use event delegation — no inline onclick with IDs in HTML
    const closeBtn = toast.querySelector('.toast-close');
    closeBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        hideToast(toastId);
    });
    toast.addEventListener('click', () => hideToast(toastId));

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
    // Lazily create the modal if it doesn't yet exist in the DOM
    if (!document.getElementById('confirmModal')) {
        createConfirmModal();
    }

    const modalOverlay = document.getElementById('confirmModal');
    const icons = {
        warning: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><path d="M12 9v4M12 17h.01"/></svg>',
        danger:  '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M15 9l-6 6M9 9l6 6"/></svg>',
        info:    '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/></svg>'
    };
    const safeType = icons[type] ? type : 'warning';

    const iconEl    = document.getElementById('confirmModalIcon');
    const titleEl   = document.getElementById('confirmModalTitle');
    const msgEl     = document.getElementById('confirmModalMessage');
    const confirmEl = document.getElementById('confirmBtn');
    const cancelEl  = document.getElementById('cancelBtn');

    wireConfirmModal(modalOverlay);

    if (iconEl)    iconEl.innerHTML        = icons[safeType];
    if (titleEl)   titleEl.textContent     = title;
    if (msgEl)     msgEl.textContent       = message;
    if (confirmEl) confirmEl.textContent   = confirmText;
    if (cancelEl)  cancelEl.textContent    = cancelText;

    // Update icon wrapper class for correct colour
    if (iconEl) {
        iconEl.className = `modal-icon ${safeType}`;
    }

    modalOverlay.classList.add('active');
    document.body.style.overflow = 'hidden';

    // Store callback; cleared on dismiss
    window._confirmCallback = typeof onConfirm === 'function' ? onConfirm : null;

    // Focus the confirm button for keyboard accessibility
    if (confirmEl) setTimeout(() => confirmEl.focus(), 50);
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
 * Wire confirmation dialog controls once, regardless of whether the modal
 * came from the HTML shell or was created lazily by this script.
 */
function wireConfirmModal(modalOverlay) {
    if (!modalOverlay || modalOverlay.dataset.wired === 'true') return;

    const confirmEl = document.getElementById('confirmBtn');
    const cancelEl = document.getElementById('cancelBtn');

    if (confirmEl) {
        confirmEl.addEventListener('click', confirmAction);
    }
    if (cancelEl) {
        cancelEl.addEventListener('click', hideConfirmDialog);
    }

    modalOverlay.addEventListener('click', (e) => {
        if (e.target === modalOverlay) hideConfirmDialog();
    });

    modalOverlay.dataset.wired = 'true';
}

/**
 * Create confirmation modal HTML — uses event listeners, no inline onclick
 */
function createConfirmModal() {
    const modal = document.createElement('div');
    modal.id = 'confirmModal';
    modal.className = 'modal-overlay';
    modal.setAttribute('role', 'dialog');
    modal.setAttribute('aria-modal', 'true');
    modal.setAttribute('aria-labelledby', 'confirmModalTitle');
    modal.innerHTML = `
        <div class="modal">
            <div class="modal-header">
                <div class="modal-icon warning" id="confirmModalIcon"></div>
                <span class="modal-title" id="confirmModalTitle">Confirm</span>
            </div>
            <div class="modal-body" id="confirmModalMessage">Are you sure?</div>
            <div class="modal-actions">
                <button class="btn btn-ghost" id="cancelBtn">Cancel</button>
                <button class="btn btn-danger" id="confirmBtn">Confirm</button>
            </div>
        </div>
    `;
    document.body.appendChild(modal);
    wireConfirmModal(modal);

    // Close on Escape is handled by the global keydown listener in initKeyboardShortcuts
}

// ============================================================================
// LOADING OVERLAY
// ============================================================================

/**
 * Show loading overlay
 * @param {string} message - Loading message
 */
function showLoading(message = 'Loading...') {
    if (!document.getElementById('loadingOverlay')) {
        createLoadingOverlay();
    }
    const overlay  = document.getElementById('loadingOverlay');
    const textEl   = document.getElementById('loadingText');
    if (textEl) textEl.textContent = message;
    if (overlay) {
        overlay.classList.add('active');
        document.body.style.overflow = 'hidden';
    }
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
                if (activeTab.id === 'tab-hybrid') {
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
    const toggle   = document.getElementById('autoRefreshToggle');
    const switchEl = document.getElementById('autoRefreshSwitch');

    if (toggle)   toggle.classList.toggle('active', autoRefreshEnabled);
    if (switchEl) switchEl.classList.toggle('active', autoRefreshEnabled);

    if (autoRefreshEnabled) {
        startAutoRefresh();
        showToast('Auto-refresh', 'Metrics will refresh every 5 seconds', 'info');
    } else {
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
 * @param {Object} metadata - Optional UI state needed to reload the query
 */
function addToQueryHistory(query, type, rows = 0, executionTime = 0, metadata = null) {
    if (!query || typeof query !== 'string') return;

    const historyKey = (type === 'sql' || type === 'query') ? SQL_HISTORY_KEY : HYBRID_HISTORY_KEY;
    let history;
    try {
        history = JSON.parse(localStorage.getItem(historyKey) || '[]');
        if (!Array.isArray(history)) history = [];
    } catch (_) {
        history = [];
    }

    // Deduplicate: remove identical previous entry if present
    history = history.filter(h => h.query !== query);

    history.unshift({
        query,
        type,
        rows:          Number(rows)         || 0,
        executionTime: Number(executionTime) || 0,
        metadata: metadata && typeof metadata === 'object' ? metadata : null,
        timestamp: new Date().toISOString()
    });

    if (history.length > MAX_HISTORY_ITEMS) {
        history = history.slice(0, MAX_HISTORY_ITEMS);
    }

    try {
        localStorage.setItem(historyKey, JSON.stringify(history));
    } catch (_) {
        // localStorage quota exceeded — silently skip persistence
    }
    updateQueryHistoryPanel();
}

/**
 * Get query history
 * @param {string} type - Query type
 * @returns {Array} History items
 */
function getQueryHistory(type = 'sql') {
    const historyKey = (type === 'sql' || type === 'query') ? SQL_HISTORY_KEY : HYBRID_HISTORY_KEY;
    try {
        const parsed = JSON.parse(localStorage.getItem(historyKey) || '[]');
        return Array.isArray(parsed) ? parsed : [];
    } catch (_) {
        return [];
    }
}

/**
 * Clear query history
 * @param {string} type - Query type
 */
function clearQueryHistory(type) {
    if (type === 'sql') {
        localStorage.removeItem(SQL_HISTORY_KEY);
    } else if (type === 'hybrid') {
        localStorage.removeItem(HYBRID_HISTORY_KEY);
    } else {
        // Clear both if no specific type given
        localStorage.removeItem(SQL_HISTORY_KEY);
        localStorage.removeItem(HYBRID_HISTORY_KEY);
    }
    // Reset the in-memory cache too
    window.__queryHistoryItems = Object.create(null);
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

    listEl.innerHTML = '';

    // Build items using DOM creation (no innerHTML with user data for item rows)
    const fragment = document.createDocumentFragment();
    allHistory.forEach((item, idx) => {
        const dataIdx = `qh-${idx}`;
        window.__queryHistoryItems[dataIdx] = item;

        const date    = new Date(item.timestamp);
        const timeStr = date.toLocaleTimeString();
        const dateStr = date.toLocaleDateString();

        const div = document.createElement('div');
        div.className = 'query-history-item';
        div.setAttribute('data-qh-key', dataIdx);
        div.setAttribute('role', 'button');
        div.setAttribute('tabindex', '0');

        const timeDiv = document.createElement('div');
        timeDiv.className = 'time';
        timeDiv.textContent = `${timeStr} - ${dateStr}`;

        const queryDiv = document.createElement('div');
        queryDiv.className = 'query';
        queryDiv.textContent = item.query; // textContent = safe, no XSS

        const metaDiv = document.createElement('div');
        metaDiv.className = 'meta';

        const badge = document.createElement('span');
        badge.className = 'badge badge-info';
        badge.textContent = (item.type || 'sql').toUpperCase();

        const info = document.createElement('span');
        info.style.cssText = 'font-size:0.75rem;color:var(--text-muted)';
        info.textContent = `${item.rows || 0} rows, ${item.executionTime || 0}ms`;

        metaDiv.appendChild(badge);
        metaDiv.appendChild(info);
        div.appendChild(timeDiv);
        div.appendChild(queryDiv);
        div.appendChild(metaDiv);

        // Click / keyboard handler
        const handler = () => loadQueryFromHistory(dataIdx);
        div.addEventListener('click', handler);
        div.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); handler(); }
        });

        fragment.appendChild(div);
    });
    listEl.appendChild(fragment);
}

/**
 * Toggle query history panel
 */
function toggleQueryHistory() {
    const panel   = document.getElementById('queryHistoryPanel');
    const overlay = document.getElementById('queryHistoryOverlay');
    if (!panel || !overlay) return;

    if (panel.classList.contains('active')) {
        closeQueryHistory();
    } else {
        // Reset item cache before rebuilding
        window.__queryHistoryItems = Object.create(null);
        updateQueryHistoryPanel();
        panel.classList.add('active');
        overlay.classList.add('active');
        document.body.style.overflow = 'hidden';
        // Focus the close button for keyboard users
        const closeBtn = panel.querySelector('.query-history-close');
        if (closeBtn) setTimeout(() => closeBtn.focus(), 50);
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
 * @param {string} key - Cache key (e.g. "qh-0")
 */
function loadQueryFromHistory(key) {
    const item = window.__queryHistoryItems[key];
    if (!item) return;

    const type = item.type || 'nosql';

    if (type === 'query' && item.metadata) {
        const state = item.metadata;
        const applyQueryState = () => {
            const setValue = (id, value) => {
                const el = document.getElementById(id);
                if (!el) return;
                if (id === 'queryCol' && value && !Array.from(el.options).some(option => option.value === value)) {
                    el.add(new Option(value, value));
                }
                el.value = value ?? '';
            };
            setValue('queryCol', state.collection);
            setValue('queryField', state.field);
            setValue('queryOp', state.op || 'eq');
            setValue('queryValue', state.value);
            setValue('querySort', state.sort);
            setValue('querySkip', state.skip ?? 0);
            setValue('queryLimit', state.limit ?? 100);
        };
        if (typeof showTab === 'function') showTab('query');
        if (typeof loadCollectionList === 'function') {
            Promise.resolve(loadCollectionList()).finally(applyQueryState);
        } else {
            applyQueryState();
        }
        closeQueryHistory();
        return;
    }

    const hq = document.getElementById('hybridQuery');
    const hm = document.getElementById('hybridMode');
    if (hq) {
        hq.value = item.query;
        hq.dispatchEvent(new Event('input'));
    }
    if (hm) hm.value = (type === 'nosql' || type === 'vector') ? type : 'nosql';
    if (typeof showTab === 'function') showTab('hybrid');
    closeQueryHistory();
}

/** @deprecated Legacy alias */
function loadSqlHistory() { toggleQueryHistory(); }

// ============================================================================
// COPY TO CLIPBOARD
// ============================================================================

/**
 * Copy text to clipboard
 * @param {string} text - Text to copy
 * @returns {Promise<boolean>} Success status
 */
async function copyToClipboard(text) {
    if (typeof text !== 'string') text = String(text);
    // Modern Clipboard API
    if (navigator.clipboard && window.isSecureContext) {
        try {
            await navigator.clipboard.writeText(text);
            showToast('Copied', 'Content copied to clipboard', 'success');
            return true;
        } catch (_) { /* fall through to legacy approach */ }
    }
    // Legacy execCommand fallback
    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.style.cssText = 'position:fixed;top:0;left:0;opacity:0;pointer-events:none;';
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();
    let success = false;
    try {
        success = document.execCommand('copy');
    } finally {
        document.body.removeChild(textarea);
    }
    if (success) {
        showToast('Copied', 'Content copied to clipboard', 'success');
    } else {
        showToast('Error', 'Failed to copy to clipboard', 'error');
    }
    return success;
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
    try {
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
        _triggerDownload(blob, filename || 'export.json');
        showToast('Exported', `Data exported to ${filename}`, 'success');
    } catch (e) {
        showToast('Export Failed', e.message || 'Could not export data', 'error');
    }
}

/**
 * Export table as CSV
 * @param {HTMLTableElement} table - Table element
 * @param {string} filename - Filename
 */
function exportTableAsCsv(table, filename) {
    if (!table) return;

    let csv = '\uFEFF'; // BOM for Excel UTF-8 compatibility
    const rows = table.querySelectorAll('tr');
    rows.forEach(row => {
        const cells = row.querySelectorAll('th, td');
        csv += Array.from(cells).map(cell => {
            const text = cell.textContent.trim();
            // RFC 4180: wrap in quotes if value contains comma, quote or newline
            if (text.includes(',') || text.includes('"') || text.includes('\n') || text.includes('\r')) {
                return '"' + text.replace(/"/g, '""') + '"';
            }
            return text;
        }).join(',') + '\r\n'; // CRLF per RFC 4180
    });

    try {
        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
        _triggerDownload(blob, filename || 'export.csv');
        showToast('Exported', `Data exported to ${filename}`, 'success');
    } catch (e) {
        showToast('Export Failed', e.message || 'Could not export CSV', 'error');
    }
}

/**
 * Internal helper — creates a temporary <a> and triggers a download
 * @param {Blob} blob
 * @param {string} filename
 */
function _triggerDownload(blob, filename) {
    const url = URL.createObjectURL(blob);
    const a   = document.createElement('a');
    a.href     = url;
    a.download = filename;
    a.style.display = 'none';
    document.body.appendChild(a);
    a.click();
    // Defer revoke so browser has time to initiate the download
    setTimeout(() => {
        URL.revokeObjectURL(url);
        document.body.removeChild(a);
    }, 150);
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
    if (str === null || str === undefined) return '';
    // Faster path using a regex replace instead of a DOM node
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
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
    const n = Number(num);
    if (isNaN(n)) return '0';
    // Use Intl if available for locale-aware formatting
    if (typeof Intl !== 'undefined' && Intl.NumberFormat) {
        return new Intl.NumberFormat().format(n);
    }
    return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
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
    let inThrottle = false;
    return function throttled(...args) {
        if (!inThrottle) {
            func.apply(this, args);
            inThrottle = true;
            setTimeout(() => { inThrottle = false; }, limit);
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
 * @param {boolean} autoDismiss - Whether the inline message should clear itself
 * @param {boolean} showToastAlso - Also show toast notification
 */
function showMessage(elementId, message, type = 'info', autoDismiss = true, showToastAlso = true) {
    const el = document.getElementById(elementId);
    if (!el) return;

    // Preserve legacy 4-arg calls: showMessage(id, msg, type, false)
    // should mean "do not auto-dismiss and do not toast".
    if (arguments.length === 4) {
        showToastAlso = autoDismiss !== false;
    }

    const validTypes = ['success', 'error', 'warning', 'info'];
    const safeType   = validTypes.includes(type) ? type : 'info';

    const icons = {
        success: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><path d="M22 4L12 14.01l-3-3"/></svg>',
        error:   '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M15 9l-6 6M9 9l6 6"/></svg>',
        warning: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><path d="M12 9v4M12 17h.01"/></svg>',
        info:    '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/></svg>'
    };

    // Preserve the legacy inline message contract: callers may pass trusted HTML.
    el.innerHTML = '';
    const wrapper = document.createElement('div');
    wrapper.className = `msg msg-${safeType}`;
    wrapper.setAttribute('role', safeType === 'error' ? 'alert' : 'status');
    wrapper.innerHTML = icons[safeType]; // SVG is trusted static content
    const span = document.createElement('span');
    span.innerHTML = message;
    wrapper.appendChild(span);
    el.appendChild(wrapper);

    if (autoDismiss) {
        setTimeout(() => {
            if (el.firstChild === wrapper) {
                el.innerHTML = '';
            }
        }, safeType === 'error' ? 12000 : 5000);
    }

    if (showToastAlso !== false) {
        const toastTitles = { success: 'Success', error: 'Error', warning: 'Warning', info: 'Info' };
        showToast(toastTitles[safeType], message, safeType);
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

    // Only create modal/overlay if they don't already exist in the HTML
    if (!document.getElementById('confirmModal'))   createConfirmModal();
    if (!document.getElementById('loadingOverlay')) createLoadingOverlay();

    // Ensure a toast container exists
    if (!document.getElementById('toastContainer')) {
        const tc = document.createElement('div');
        tc.id = 'toastContainer';
        tc.className = 'toast-container';
        tc.setAttribute('aria-live', 'polite');
        tc.setAttribute('aria-atomic', 'false');
        document.body.appendChild(tc);
    }

    // Show keyboard shortcuts hint on first visit (non-blocking)
    try {
        if (!localStorage.getItem('shortcutsHintShown')) {
            setTimeout(() => {
                showToast('Keyboard Shortcuts', 'Ctrl+H — history • Ctrl+Enter — execute • Ctrl+R — refresh', 'info', 8000);
                localStorage.setItem('shortcutsHintShown', 'true');
            }, 2500);
        }
    } catch (_) { /* localStorage might be blocked in private mode */ }

    // Populate history panel if it already exists in DOM
    updateQueryHistoryPanel();

    // Set up real-time JSON validation for console textareas
    setupJsonValidation();

    console.info('[JunifyDB] Console Enhancements v3.0 initialized');
}

/**
 * Setup Real-time JSON validation and formatting for textareas
 */
function setupJsonValidation() {
    const targets = [
        { id: 'docJson' }
    ];

    targets.forEach(target => {
        const textarea = document.getElementById(target.id);
        if (!textarea) return;

        // Ensure we wrap the textarea's label to place validation badge & format button next to it
        const parent = textarea.parentElement;
        const label = parent.querySelector('.form-label');
        if (label) {
            // Check if wrapper already exists to prevent double insertion
            if (parent.querySelector('.json-status-wrapper')) return;

            const wrapper = document.createElement('div');
            wrapper.className = 'json-status-wrapper';
            wrapper.style.cssText = 'display: flex; gap: 8px; align-items: center; margin-left: auto;';

            const badge = document.createElement('span');
            badge.className = 'badge badge-ghost json-badge';
            badge.id = `${target.id}-json-badge`;
            badge.style.cssText = 'font-size: 0.65rem; padding: 2px 8px; text-transform: uppercase; font-weight: 700; transition: all 0.2s;';
            badge.textContent = 'Empty';

            const formatBtn = document.createElement('button');
            formatBtn.type = 'button';
            formatBtn.className = 'btn-copy';
            formatBtn.style.cssText = 'padding: 2px 6px; font-size: 0.7rem; cursor: pointer; border: 1px solid var(--border); background: var(--bg-secondary); color: var(--text-secondary); border-radius: 4px; transition: all 0.2s; height: 20px; display: inline-flex; align-items: center;';
            formatBtn.innerHTML = '<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="margin-right:3px;"><path d="M21 16V8a2 2 0 0 0-2-2h-2m-9 0H4a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2m4-12v12m4-12v12"/></svg> Format';
            formatBtn.onclick = () => formatJsonTextarea(target.id);

            wrapper.appendChild(badge);
            wrapper.appendChild(formatBtn);

            // Turn label parent or header container flex
            const headerContainer = document.createElement('div');
            headerContainer.style.cssText = 'display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; width: 100%;';
            label.style.marginBottom = '0';

            parent.insertBefore(headerContainer, label);
            headerContainer.appendChild(label);
            headerContainer.appendChild(wrapper);
        }

        // Add real-time keyup validation
        const validate = () => {
            const val = textarea.value.trim();
            const badge = document.getElementById(`${target.id}-json-badge`);
            if (!badge) return;

            if (!val) {
                badge.textContent = 'Empty';
                badge.className = 'badge badge-ghost json-badge';
                textarea.style.borderColor = 'var(--border)';
                textarea.style.boxShadow = '';
                return;
            }

            try {
                JSON.parse(val);
                badge.textContent = 'Valid JSON';
                badge.className = 'badge badge-success json-badge';
                textarea.style.borderColor = 'var(--success)';
                textarea.style.boxShadow = '0 0 0 3px rgba(16, 185, 129, 0.15)';
            } catch (err) {
                badge.textContent = 'Invalid JSON';
                badge.className = 'badge badge-error json-badge';
                textarea.style.borderColor = 'var(--error)';
                textarea.style.boxShadow = '0 0 0 3px rgba(244, 63, 94, 0.15)';
            }
        };

        textarea.addEventListener('input', validate);
        // Initial run
        validate();
    });
}

/**
 * Format & pretty-print the content of a JSON textarea
 * @param {string} textareaId
 */
function formatJsonTextarea(textareaId) {
    const textarea = document.getElementById(textareaId);
    if (!textarea) return;
    const val = textarea.value.trim();
    if (!val) return;

    try {
        const obj = JSON.parse(val);
        textarea.value = JSON.stringify(obj, null, 2);
        // Trigger input event to re-validate badge
        textarea.dispatchEvent(new Event('input'));
        showToast('JSON Formatted', 'JSON structured & pretty-printed', 'success', 2000);
    } catch (err) {
        showToast('JSON Parse Error', err.message || 'Could not parse JSON for formatting', 'error', 4000);
    }
}

// Auto-initialize when DOM is ready — guard against double-init
let _enhancementsInitialized = false;
function _safeInit() {
    if (_enhancementsInitialized) return;
    _enhancementsInitialized = true;
    initEnhancements();
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _safeInit);
} else {
    _safeInit();
}
