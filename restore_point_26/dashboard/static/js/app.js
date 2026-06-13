let lastSystemScanCount = -1;
let idleTimeout = null;

function setHtml(el, html) {
    if (el.innerHTML !== html) el.innerHTML = html;
}

function patchRow(tr, rowHtml) {
    if (tr.innerHTML === rowHtml) return;
    const temp = document.createElement('tr');
    temp.innerHTML = rowHtml;
    const oldCells = Array.from(tr.children);
    const newCells = Array.from(temp.children);
    for (let i = 0; i < Math.max(oldCells.length, newCells.length); i++) {
        if (!oldCells[i]) {
            tr.appendChild(newCells[i].cloneNode(true));
        } else if (!newCells[i]) {
            tr.removeChild(oldCells[i]);
        } else if (oldCells[i].innerHTML !== newCells[i].innerHTML || oldCells[i].className !== newCells[i].className) {
            oldCells[i].innerHTML = newCells[i].innerHTML;
            oldCells[i].className = newCells[i].className;
        }
    }
}

const state = {
    currentInstrument: 'NIFTY',
    currentTimeframe: '5min',
    lastSimId: 0,
    latestData: {},
    chart: null,
    candleSeries: null,
    volumeSeries: null,
    tsLineSeries: null,
    pcrLineSeries: null,
    ofrLineSeries: null,
    oiDeltaSeries: null,
    supportWallSeries: null,
    resistanceWallSeries: null,
    signalSeries: null,
    chartResizeFrame: null,
    logFilter: 'all',
    activityView: 'logs',
    fullLog: [],
    lastRenderedLogData: '',
    lastRenderedFilter: 'all',
    restHydrationTimer: null,
    chartRefreshTimer: null,
    lastRestHydrationAt: 0,
    lastWsUpdateAt: 0,
    pendingChartRequest: null,
    chartRequestInFlight: false,
    chartStreamEnabled: localStorage.getItem('ut_chart_stream_enabled') !== 'false',
    lastEquityRenderKey: '',
    wsRequestSeq: 0,
    pendingCommands: new Map(),
    tradePagination: {
        key: '',
        page: 1,
        pageSize: 100,
        inFlight: false
    }
};

const INDEX_NAMES = ['NIFTY', 'BANKNIFTY', 'SENSEX', 'MIDCPNIFTY'];

const TICKER_MAP = {
    'NIFTY': 'NIFTY', 'NSEI': 'NIFTY', '^NSEI': 'NIFTY',
    'BANKNIFTY': 'BANKNIFTY', 'NSEBANK': 'BANKNIFTY', '^NSEBANK': 'BANKNIFTY',
    'SENSEX': 'SENSEX', 'BSESN': 'SENSEX', '^BSESN': 'SENSEX',
    'MIDCPNIFTY': 'MIDCPNIFTY', 'MIDCAPNIFTY': 'MIDCPNIFTY'
};

document.addEventListener('DOMContentLoaded', () => {
    setupIntelTabs();
    setupSettingsExperience();
    initChart();
    applyChartStreamState(state.chartStreamEnabled, { notifyBackend: false, requestSnapshot: false });
    connectWebSocket();
    setupEventListeners();
    completeFyersRedirectIfPresent();
    updateMarketTime();
    setInterval(updateMarketTime, 1000);
});

async function refreshDashboardSnapshot(reason = 'manual') {
    try {
        const now = Date.now();
        if (now - state.lastRestHydrationAt < 1500) return;
        state.lastRestHydrationAt = now;

        const response = await fetch('/api/state', { cache: 'no-store' });
        if (!response.ok) throw new Error(`State fetch failed: ${response.status}`);

        const snapshot = await response.json();
        if (!snapshot || snapshot.status === 'starting') return;

        const mergedData = mergeDashboardData(state.latestData, snapshot);
        state.latestData = mergedData;
        updateDashboard(mergedData);
    } catch (error) {
        console.warn(`Dashboard REST hydration failed (${reason})`, error);
    }
}

function initChart() {
    const chartElement = document.getElementById('mainChart');
    if (!chartElement) return;

    // ═══ PREMIUM CHART INITIALIZATION ═══
    const chart = LightweightCharts.createChart(chartElement, {
        width: chartElement.clientWidth,
        height: chartElement.clientHeight,
        layout: {
            background: { type: 'solid', color: '#05070b' },
            textColor: 'rgba(226, 232, 240, 0.9)',
            fontSize: 12,
            fontFamily: "'JetBrains Mono', monospace",
        },
        grid: {
            vertLines: { color: 'rgba(148, 163, 184, 0.035)' },
            horzLines: { color: 'rgba(148, 163, 184, 0.035)' },
        },
        crosshair: {
            mode: LightweightCharts.CrosshairMode.Normal,
            vertLine: { color: '#555', labelBackgroundColor: '#111', width: 1 },
            horzLine: { color: '#555', labelBackgroundColor: '#111', width: 1 },
        },
        priceScale: {
            borderColor: 'rgba(255, 255, 255, 0.1)',
            autoScale: true,
            scaleMargins: { top: 0.1, bottom: 0.2 }, // More room for indicators
        },
        localization: {
            timeFormatter: (time) => {
                return new Date(time * 1000).toLocaleString('en-IN', {
                    timeZone: 'Asia/Kolkata',
                    day: '2-digit',
                    month: 'short',
                    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
                });
            }
        },
        timeScale: {
            borderColor: 'rgba(255, 255, 255, 0.1)',
            timeVisible: true,
            secondsVisible: false,
            barSpacing: 15, // Wider candles
            tickMarkFormatter: (time, tickMarkType) => {
                const date = new Date(time * 1000);
                if (tickMarkType === 0 || tickMarkType === 1 || tickMarkType === 2) { // Year, Month, Day
                    return date.toLocaleDateString('en-IN', {
                        timeZone: 'Asia/Kolkata',
                        day: '2-digit',
                        month: 'short',
                    });
                }
                return date.toLocaleTimeString('en-IN', {
                    timeZone: 'Asia/Kolkata',
                    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
                });
            }
        },
        handleScroll: true,
        handleScale: true,
    });
    
    state.chart = chart;

    // 1. Candlestick Series (Vibrant & Sharp)
    state.candleSeries = chart.addCandlestickSeries({
        upColor: '#26a69a',
        downColor: '#ef5350',
        borderVisible: false,
        wickUpColor: '#26a69a',
        wickDownColor: '#ef5350',
        priceLineVisible: true,
        priceLineWidth: 2,
        priceLineColor: 'rgba(255, 255, 255, 0.8)',
    });
    
    // 2. Volume Series (Subtle)
    state.volumeSeries = chart.addHistogramSeries({
        priceFormat: { type: 'volume' },
        priceScaleId: '',
        scaleMargins: { top: 0.88, bottom: 0 }
    });

    // 3. Trailing Stop Line (Thick Professional)
    state.tsLineSeries = chart.addLineSeries({
        color: '#f97316', // Bold Orange
        lineWidth: 3, // Thicker for visibility
        lineStyle: LightweightCharts.LineStyle.Dashed,
        crosshairMarkerVisible: false,
        lastValueVisible: true,
        priceLineVisible: false
    });

    state.pcrLineSeries = chart.addLineSeries({
        color: '#38bdf8',
        lineWidth: 1,
        priceScaleId: 'intel',
        crosshairMarkerVisible: false,
        lastValueVisible: true,
        priceLineVisible: false,
    });
    state.ofrLineSeries = chart.addLineSeries({
        color: '#facc15',
        lineWidth: 1,
        priceScaleId: 'intel',
        crosshairMarkerVisible: false,
        lastValueVisible: true,
        priceLineVisible: false,
    });
    state.oiDeltaSeries = chart.addHistogramSeries({
        priceScaleId: 'intel',
        priceFormat: { type: 'volume' },
        scaleMargins: { top: 0.78, bottom: 0.02 },
    });
    state.supportWallSeries = chart.addLineSeries({
        color: 'rgba(34,197,94,0.75)',
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        crosshairMarkerVisible: false,
        lastValueVisible: false,
        priceLineVisible: false,
    });
    state.resistanceWallSeries = chart.addLineSeries({
        color: 'rgba(239,68,68,0.75)',
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        crosshairMarkerVisible: false,
        lastValueVisible: false,
        priceLineVisible: false,
    });
    
    const resizeChart = () => {
        if (state.chartResizeFrame) cancelAnimationFrame(state.chartResizeFrame);
        state.chartResizeFrame = requestAnimationFrame(() => {
            chart.applyOptions({ width: chartElement.clientWidth, height: chartElement.clientHeight });
            state.chartResizeFrame = null;
        });
    };
    if (window.ResizeObserver) {
        const observer = new ResizeObserver(resizeChart);
        observer.observe(chartElement);
        state.chartResizeObserver = observer;
    } else {
        window.addEventListener('resize', resizeChart);
    }
}

async function getDashboardToken() {
    if (window.dashboardAuthToken !== undefined) return window.dashboardAuthToken;
    try {
        const res = await fetch('/api/client_config', { cache: 'no-store' });
        const cfg = await res.json();
        window.dashboardAuthToken = cfg.dashboard_token || '';
    } catch (err) {
        window.dashboardAuthToken = '';
    }
    return window.dashboardAuthToken;
}

async function authFetch(url, options = {}) {
    const token = await getDashboardToken();
    const headers = { ...(options.headers || {}) };
    if (token) headers['X-Dashboard-Token'] = token;
    return fetch(url, { ...options, headers });
}

async function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const token = await getDashboardToken();
    const tokenParam = token ? `?token=${encodeURIComponent(token)}` : '';
    const wsUrl = `${protocol}//${window.location.host}/ws${tokenParam}`;
    const ws = new WebSocket(wsUrl);
    const statusDot = document.querySelector('.status-dot');
    const statusText = document.querySelector('.status-text');
    
    ws.onopen = () => {
        window.wsReconnectDelay = 1000;
        if (statusDot) statusDot.style.background = '#22c55e';
        if (statusText) statusText.textContent = 'Connected';
        ws.send(JSON.stringify({ cmd: 'get_state' }));
        ws.send(JSON.stringify({ cmd: 'set_chart_stream', enabled: state.chartStreamEnabled }));
        refreshDashboardSnapshot('websocket-open');
        startChartRefreshLoop();
        if (!state.restHydrationTimer) {
            state.restHydrationTimer = setInterval(() => {
                const wsIsStale = Date.now() - (state.lastWsUpdateAt || 0) > 10000;
                const hasState = state.latestData?.instruments && Object.keys(state.latestData.instruments).length > 0;
                if (wsIsStale || !hasState) {
                    refreshDashboardSnapshot('periodic-stale');
                }
            }, 30000);
        }
    };

    ws.onmessage = (event) => {
        try {
            const msg = JSON.parse(event.data);
            if (msg.request_id && state.pendingCommands.has(msg.request_id)) {
                const pending = state.pendingCommands.get(msg.request_id);
                state.pendingCommands.delete(msg.request_id);
                if (pending?.resolve) pending.resolve(msg);
            }
            if (msg.type === 'full_update' || msg.type === 'delta_update') {
                state.lastWsUpdateAt = Date.now();
                const mergedData = mergeDashboardData(state.latestData, msg.data);
                state.latestData = mergedData;
                if (msg.type === 'delta_update' && isLightweightDashboardDelta(msg.data)) {
                    updateDashboardHeartbeat(mergedData);
                } else {
                    updateDashboard(mergedData);
                }
            } else if (msg.type === 'chart_snapshot') {
                applyChartSnapshot(msg.data);
            } else if (msg.type === 'chart_stream_updated') {
                applyChartStreamState(msg.data?.enabled !== false, { notifyBackend: false, requestSnapshot: true });
            } else if (msg.type === 'config_ack') {
                if (msg.data && msg.data.status === 'blocked') {
                    showToast("Settings blocked by REAL-mode safety gate", "warning");
                } else {
                    showToast("Settings Applied Successfully", "success");
                }
                refreshDashboardSnapshot('config-ack');
            }
        } catch (e) {
            console.error("Data error:", e);
        }
    };
    
    ws.onclose = () => {
        if (statusDot) statusDot.style.background = '#ef4444';
        if (statusText) statusText.textContent = 'Disconnected - Retrying...';
        
        window.wsReconnectDelay = (window.wsReconnectDelay || 1000) * 1.5;
        if (window.wsReconnectDelay > 30000) window.wsReconnectDelay = 30000;
        
        setTimeout(connectWebSocket, window.wsReconnectDelay);
    };
    
    window.sendCommand = (data) => {
        if (ws.readyState === WebSocket.OPEN) {
            const requestId = data.request_id || `cmd_${Date.now()}_${++state.wsRequestSeq}`;
            const payload = { ...data, request_id: requestId };
            state.pendingCommands.set(requestId, {
                cmd: payload.cmd,
                sentAt: Date.now(),
                resolve: null,
            });
            ws.send(JSON.stringify(payload));
            setTimeout(() => {
                const pending = state.pendingCommands.get(requestId);
                if (pending && Date.now() - pending.sentAt > 15000) {
                    state.pendingCommands.delete(requestId);
                    console.warn(`Command timed out: ${pending.cmd}`, requestId);
                }
            }, 16000);
            return requestId;
        } else {
            if (window.showToast) window.showToast("Connection lost. Reconnecting...", "error");
            return null;
        }
    };
    // ═══ SYSTEM POWER CONTROL ═══
    const powerToggle = document.getElementById('powerToggle');
    if (powerToggle) {
        powerToggle.addEventListener('change', () => {
            const stateVal = powerToggle.checked ? 'ON' : 'OFF';
            window.sendCommand({ 
                cmd: 'set_power', 
                state: stateVal 
            });
            if (state.latestData) {
                state.latestData.system_power = stateVal;
            }
            showToast(`System Power: ${stateVal}`, stateVal === 'ON' ? 'info' : 'warning');
        });
    }
    
    // ═══ REGIME MODE SWITCH ═══
    const regimeModeSwitch = document.getElementById('regimeModeSwitch');
    if (regimeModeSwitch) {
        regimeModeSwitch.addEventListener('change', () => {
            const isChecked = regimeModeSwitch.checked;
            const stateVal = isChecked ? 'ON' : 'OFF';
            window.sendCommand({ 
                cmd: 'set_regime_adaptation', 
                state: stateVal 
            });
            if (state.latestData && state.latestData.config) {
                state.latestData.config.ut_regime_adaptation = isChecked;
            }
            showToast(`Regime Adaptation: ${stateVal}`, isChecked ? 'info' : 'warning');
        });
    }

    // ═══ SL MODE SWITCH ═══
    const slModeSwitch = document.getElementById('slModeSwitch');
    if (slModeSwitch) {
        slModeSwitch.addEventListener('change', () => {
            const isChecked = slModeSwitch.checked;
            const stateVal = isChecked ? 'NATURAL' : 'HARDCODED';
            window.sendCommand({ 
                cmd: 'set_sl_mode', 
                state: stateVal 
            });
            if (state.latestData && state.latestData.config) {
                state.latestData.config.sl_mode = stateVal;
            }
            showToast(`SL Mode: ${stateVal}`, isChecked ? 'info' : 'warning');
        });
    }

    // ═══ CONCURRENCY GUARD SWITCH ═══
    const concurGuardSwitch = document.getElementById('concurGuardSwitch');
    if (concurGuardSwitch) {
        concurGuardSwitch.addEventListener('change', () => {
            const isChecked = concurGuardSwitch.checked;
            const stateVal = isChecked ? 'ON' : 'OFF';
            window.sendCommand({ 
                cmd: 'set_concurrency_guard', 
                state: stateVal 
            });
            if (state.latestData && state.latestData.config) {
                state.latestData.config.ut_concurrency_guard = isChecked;
            }
            showToast(`Concurrency Guard: ${stateVal}`, isChecked ? 'info' : 'warning');
        });
    }

    // ═══ SYSTEM RECALIBRATE / REFRESH BUTTON ═══
    const systemRefreshBtn = document.getElementById('systemRefreshBtn');
    if (systemRefreshBtn) {
        systemRefreshBtn.addEventListener('click', () => {
            const icon = systemRefreshBtn.querySelector('.logo-icon');
            if (icon) {
                icon.classList.remove('recalibrating');
                void icon.offsetWidth; // Force reflow to restart animation
                icon.classList.add('recalibrating');
                setTimeout(() => {
                    icon.classList.remove('recalibrating');
                }, 1000);
            }
            window.sendCommand({
                cmd: 'system_recalibrate'
            });
            showToast("System Recalibration initiated...", "info");
            
            // Hard refresh the UI as requested by user for bottleneck recovery
            setTimeout(() => {
                window.location.reload();
            }, 600);
        });
    }
}

async function requestChartSnapshot(instrument, tf, reason = 'tab-change') {
    if (!state.chartStreamEnabled) return;
    if (state.chartRequestInFlight && reason === 'auto-refresh') return;
    state.pendingChartRequest = `${instrument}_${tf}_${Date.now()}`;
    if (window.sendCommand && reason !== 'auto-refresh') {
        window.sendCommand({ cmd: 'subscribe_chart', instrument, tf });
    }

    // REST fallback covers missed/stale WebSocket responses and makes tab
    // changes deterministic across all active indices x 3 timeframes.
    try {
        state.chartRequestInFlight = true;
        const response = await fetch(`/api/chart?instrument=${encodeURIComponent(instrument)}&tf=${encodeURIComponent(tf)}`, { cache: 'no-store' });
        if (!response.ok) throw new Error(`Chart fetch failed: ${response.status}`);
        applyChartSnapshot(await response.json());
    } catch (error) {
        console.warn(`Chart snapshot failed (${reason})`, error);
    } finally {
        state.chartRequestInFlight = false;
    }
}

function applyChartSnapshot(snapshot) {
    if (!snapshot || snapshot.status === 'starting') return;
    if (snapshot.chart_enabled === false) {
        applyChartStreamState(false, { notifyBackend: false, requestSnapshot: false });
        return;
    }
    if (!state.chartStreamEnabled) return;
    const instrument = TICKER_MAP[snapshot.instrument] || snapshot.instrument || state.currentInstrument;
    if (!instrument || instrument !== state.currentInstrument) return;

    const current = state.latestData || {};
    const instruments = { ...(current.instruments || {}) };
    const inst = { ...(instruments[instrument] || {}) };
    inst.chart = { ...(inst.chart || {}), ...(snapshot.chart || {}) };
    inst.simulation_id = snapshot.simulation_id || current.simulation_id || 0;
    instruments[instrument] = inst;

    state.latestData = {
        ...current,
        instruments,
        simulation_id: snapshot.simulation_id || current.simulation_id || 0,
    };
    state.lastChartId = null;
    state.lastBarTime = null;
    state.lastBarKey = null;
    updateChart(inst);
    updateInfoPanel(inst);
}

function startChartRefreshLoop() {
    if (state.chartRefreshTimer) return;
    state.chartRefreshTimer = setInterval(() => {
        if (!state.chartStreamEnabled) return;
        if (document.hidden) return;
        const hasActiveChart = Boolean(state.latestData?.instruments?.[state.currentInstrument]);
        if (hasActiveChart) {
            requestChartSnapshot(state.currentInstrument, state.currentTimeframe, 'auto-refresh');
        }
    }, 3000);
}

function applyChartStreamState(enabled, options = {}) {
    state.chartStreamEnabled = enabled !== false;
    if (state.latestData) {
        state.latestData.config = {
            ...(state.latestData.config || {}),
            chart_stream_enabled: state.chartStreamEnabled,
        };
    }
    localStorage.setItem('ut_chart_stream_enabled', String(state.chartStreamEnabled));

    const toggle = document.getElementById('chartStreamSwitch');
    if (toggle && toggle.checked !== state.chartStreamEnabled) {
        toggle.checked = state.chartStreamEnabled;
    }

    const panel = document.getElementById('chartPanel');
    const container = document.getElementById('mainChart');
    if (panel) panel.classList.toggle('chart-disabled', !state.chartStreamEnabled);
    if (container) {
        let placeholder = container.querySelector('.chart-disabled-placeholder');
        if (!state.chartStreamEnabled && !placeholder) {
            placeholder = document.createElement('div');
            placeholder.className = 'chart-disabled-placeholder';
            placeholder.textContent = 'Chart stream is paused. Trading, signals, intel, logs, and P&L are still running.';
            container.appendChild(placeholder);
        } else if (state.chartStreamEnabled && placeholder) {
            placeholder.remove();
        }
    }

    if (!state.chartStreamEnabled) {
        state.candleSeries?.setData([]);
        state.volumeSeries?.setData([]);
        state.tsLineSeries?.setData([]);
        clearIntelOverlay();
        state.candleSeries?.setMarkers([]);
        state.lastChartId = null;
        state.lastBarTime = null;
        state.lastBarKey = null;
        updateEquityReplacement(state.latestData?.trades || {});
    } else if (options.requestSnapshot !== false) {
        requestChartSnapshot(state.currentInstrument, state.currentTimeframe, 'chart-stream-enabled');
    }

    if (options.notifyBackend !== false && window.sendCommand) {
        window.sendCommand({ cmd: 'set_chart_stream', enabled: state.chartStreamEnabled });
    }
}

function mergeDashboardData(current, incoming) {
    if (!incoming) return current;
    if (!current) return incoming;

    const merged = {
        ...current,
        ...incoming,
        instruments: { ...(current.instruments || {}) }
    };
    if (incoming.trades) {
        const currentTrades = current.trades || {};
        const incomingTrades = incoming.trades || {};
        merged.trades = { ...currentTrades, ...incomingTrades };
        const currentWindow = currentTrades.window || {};
        const incomingWindow = incomingTrades.window || {};
        const currentOffset = Number(currentWindow.offset || 0);
        const incomingOffset = Number(incomingWindow.offset || 0);
        const keepUserPage =
            currentOffset > 0 &&
            incomingOffset === 0 &&
            tradePayloadContextKey(current, currentTrades) === tradePayloadContextKey({ ...current, ...incoming }, incomingTrades);
        if (keepUserPage) {
            merged.trades.closed = currentTrades.closed || [];
            merged.trades.signals = currentTrades.signals || [];
            merged.trades.open = currentTrades.open || incomingTrades.open || [];
            merged.trades.window = currentTrades.window || incomingTrades.window;
        }
    }

    if (!incoming.instruments) {
        return merged;
    }

    Object.entries(incoming.instruments).forEach(([inst, incomingInst]) => {
        const currentInst = current.instruments?.[inst] || {};
        const nextInst = { ...currentInst, ...incomingInst };

        if (incomingInst.chart) {
            nextInst.chart = { ...(currentInst.chart || {}) };
            Object.entries(incomingInst.chart).forEach(([tf, incomingTf]) => {
                nextInst.chart[tf] = mergeChartData(currentInst.chart?.[tf], incomingTf);
            });
        }

        merged.instruments[inst] = nextInst;
    });

    return merged;
}

function mergeChartData(currentChart, incomingChart) {
    if (!currentChart || !incomingChart) return incomingChart || currentChart;

    const incomingCandles = incomingChart.candles || [];
    const hasFullCurrent = (currentChart.candles || []).length > 1;
    const isIncremental = incomingCandles.length <= 1 && hasFullCurrent;
    if (!isIncremental) return incomingChart;

    return {
        ...currentChart,
        ...incomingChart,
        candles: upsertByTime(currentChart.candles || [], incomingCandles[0]),
        trailing_stop: upsertByTime(currentChart.trailing_stop || [], (incomingChart.trailing_stop || [])[0]),
        markers: incomingChart.markers && incomingChart.markers.length ? incomingChart.markers : (currentChart.markers || []),
    };
}

function upsertByTime(series, point) {
    if (!point || point.time === undefined || point.time === null) return series;

    const next = series.slice();
    const last = next[next.length - 1];
    if (last && last.time === point.time) {
        next[next.length - 1] = point;
    } else if (!last || point.time > last.time) {
        next.push(point);
    }
    return next;
}

function isLightweightDashboardDelta(delta) {
    if (!delta || typeof delta !== 'object') return false;
    const heavyKeys = ['instruments', 'trades', 'activity_log', 'config', 'diagnostics', 'gateway_status'];
    return !heavyKeys.some(key => Object.prototype.hasOwnProperty.call(delta, key));
}

function updateDashboardHeartbeat(data) {
    if (!data) return;
    if (data.scan_count !== undefined) setText('scanCount', data.scan_count || 0);
    if (data.latency !== undefined) setText('scanLatency', (data.latency || 0).toFixed(0) + 'ms');
    const heartbeat = data.dashboard_heartbeat || {};
    const heartbeatTime = heartbeat.timestamp ? new Date(heartbeat.timestamp).getTime() : NaN;
    const heartbeatAgeMs = Number.isFinite(heartbeatTime) ? Math.max(0, Date.now() - heartbeatTime) : null;
    const scanAgeMs = Number.isFinite(Number(heartbeat.scan_age_ms)) ? Number(heartbeat.scan_age_ms) : null;
    const wsAgeMs = state.lastWsUpdateAt ? Math.max(0, Date.now() - state.lastWsUpdateAt) : null;
    const liveAgeMs = heartbeatAgeMs !== null ? heartbeatAgeMs : wsAgeMs;
    const liveSuffix = heartbeatAgeMs !== null
        ? (scanAgeMs !== null ? ` / scan ${(scanAgeMs / 1000).toFixed(1)}s` : '')
        : ' / ws';
    const liveText = liveAgeMs === null ? '--' : `${(liveAgeMs / 1000).toFixed(1)}s${liveSuffix}`;
    setText('liveRefreshAge', liveText);
}

function tradeHistoryTotals(trades) {
    const closedTotal = Number(trades.closed_total ?? (trades.closed || []).length) || 0;
    const signalsTotal = Number(trades.signals_total ?? (trades.signals || []).length) || 0;
    const closedVisible = (trades.closed || []).length;
    const signalsVisible = (trades.signals || []).length;
    const summaryTotal = Number((trades.summary || {}).total_trades || 0) || 0;
    const windowTotal = Number((trades.window || {}).total || 0) || 0;
    const duplicateLedger = summaryTotal > 0 && closedTotal > 0 && signalsTotal > 0 && Math.max(closedTotal, signalsTotal) === summaryTotal;
    const visible = duplicateLedger ? Math.max(closedVisible, signalsVisible) : closedVisible + signalsVisible;
    const total = windowTotal || (duplicateLedger
        ? summaryTotal
        : Math.max(summaryTotal, closedTotal + signalsTotal));
    return { closedTotal, signalsTotal, total, visible };
}

function tradePayloadContextKey(data = {}, trades = {}) {
    const meta = trades.meta || {};
    const totals = tradeHistoryTotals(trades);
    return [
        String(data.mode || data.config?.mode || meta.mode || '').toUpperCase(),
        String(data.simulation_id || meta.simulation_id || ''),
        String(meta.source || ''),
        String(meta.backtest_days || data.config?.backtest_days || ''),
        String(meta.inst_pref || data.config?.inst_pref || ''),
        String(meta.grade_preference || data.config?.signal_grade_preference || ''),
        String(meta.timeframe_entry_policy || data.config?.ut_timeframe_entry_policy || ''),
        String(meta.ut_concurrency_guard ?? data.config?.ut_concurrency_guard ?? ''),
        String(totals.total || 0),
    ].join('|');
}

function syncTradePaginationState(data, trades) {
    if (!trades || typeof trades !== 'object') return;
    const key = tradePayloadContextKey(data || {}, trades);
    if (state.tradePagination.key !== key) {
        state.tradePagination.key = key;
        state.tradePagination.page = 1;
    }
    const windowInfo = trades.window || {};
    const pageSize = Number(windowInfo.limit || windowInfo.page_size || state.tradePagination.pageSize || 100) || 100;
    state.tradePagination.pageSize = Math.max(1, Math.min(2000, pageSize));
    const offset = Number(windowInfo.offset || 0) || 0;
    state.tradePagination.page = Math.max(1, Math.floor(offset / state.tradePagination.pageSize) + 1);
}

function getTradePageInfo(trades) {
    const totals = tradeHistoryTotals(trades || {});
    const windowInfo = (trades || {}).window || {};
    const pageSize = Math.max(1, Number(windowInfo.limit || windowInfo.page_size || state.tradePagination.pageSize || 100) || 100);
    const pageCount = Math.max(1, Number(windowInfo.page_count || Math.ceil((totals.total || totals.visible || 1) / pageSize)) || 1);
    const offset = Math.max(0, Number(windowInfo.offset || (state.tradePagination.page - 1) * pageSize) || 0);
    const page = Math.max(1, Math.min(pageCount, Number(windowInfo.page || Math.floor(offset / pageSize) + 1) || 1));
    return { page, pageSize, pageCount, total: totals.total, visible: totals.visible, offset };
}

function paginationSequence(current, total) {
    if (total <= 7) return Array.from({ length: total }, (_, idx) => idx + 1);
    const pages = new Set([1, total]);
    const start = current <= 4 ? 2 : Math.max(2, current - 2);
    const end = current <= 4 ? Math.min(total - 1, 6) : Math.min(total - 1, current + 2);
    for (let page = start; page <= end; page += 1) pages.add(page);
    return Array.from(pages).sort((a, b) => a - b).reduce((acc, page) => {
        if (acc.length && page - acc[acc.length - 1] > 1) acc.push('...');
        acc.push(page);
        return acc;
    }, []);
}

async function loadTradePage(page) {
    const latest = state.latestData || {};
    const currentTrades = latest.trades || {};
    const info = getTradePageInfo(currentTrades);
    const targetPage = Math.max(1, Math.min(info.pageCount, Number(page) || 1));
    if (state.tradePagination.inFlight || targetPage === info.page) return;

    state.tradePagination.inFlight = true;
    renderTradePagination(currentTrades);
    try {
        const offset = (targetPage - 1) * info.pageSize;
        const response = await fetch(`/api/trades?limit=${info.pageSize}&offset=${offset}`, { cache: 'no-store' });
        if (!response.ok) throw new Error(`Trade page fetch failed: ${response.status}`);
        const pagePayload = await response.json();
        const currentLatest = state.latestData || latest;
        state.latestData = {
            ...currentLatest,
            trades: {
                ...(currentLatest.trades || {}),
                ...pagePayload,
            },
            simulation_id: currentLatest.simulation_id,
        };
        syncTradePaginationState(state.latestData, state.latestData.trades || {});
        updateTradesPanel(state.latestData.trades || {}, latest.simulation_id);
        updatePnlPanel(state.latestData.trades || {});
        updateEquityReplacement(state.latestData.trades || {});
    } catch (error) {
        console.warn('Trade page load failed', error);
    } finally {
        state.tradePagination.inFlight = false;
        renderTradePagination((state.latestData && state.latestData.trades) || currentTrades);
    }
}

function renderTradePagination(trades) {
    const container = document.getElementById('tradePagination');
    if (!container) return;
    const info = getTradePageInfo(trades || {});
    if (info.pageCount <= 1) {
        container.innerHTML = '';
        container.classList.remove('active');
        return;
    }

    const startRow = info.visible > 0 && info.total ? info.offset + 1 : 0;
    const endRow = info.visible > 0 ? Math.min(info.total || info.visible, info.offset + info.visible) : 0;
    const busy = state.tradePagination.inFlight;
    const buttons = paginationSequence(info.page, info.pageCount).map(item => {
        if (item === '...') return '<span class="trade-page-ellipsis">...</span>';
        const active = item === info.page ? ' active' : '';
        return `<button type="button" class="trade-page-btn${active}" data-page="${item}" ${busy ? 'disabled' : ''}>${item}</button>`;
    }).join('');
    container.classList.add('active');
    container.innerHTML = `
        <span class="trade-page-status">${startRow}-${endRow} / ${info.total}</span>
        <div class="trade-page-buttons">
            <button type="button" class="trade-page-btn trade-page-prev" data-page="${info.page - 1}" ${info.page <= 1 || busy ? 'disabled' : ''}>Prev</button>
            ${buttons}
            <button type="button" class="trade-page-btn trade-page-next" data-page="${info.page + 1}" ${info.page >= info.pageCount || busy ? 'disabled' : ''}>Next ›</button>
        </div>
    `;
    container.querySelectorAll('button[data-page]').forEach(button => {
        button.addEventListener('click', () => loadTradePage(Number(button.dataset.page)));
    });
}

function updateDashboard(data) {
    if (!data) return;

    // ══ SYNC SYSTEM POWER & ANALYSING SIGNAL ══
    const powerToggle = document.getElementById('powerToggle');
    const signalBtn = document.getElementById('analysingSignal');
    const analysisText = document.getElementById('analysisText');
    const analysisDot = document.getElementById('analysisDot');
    
    if (powerToggle) {
        const serverSaysOn = data.system_power !== 'OFF';
        if (powerToggle.checked !== serverSaysOn) {
            powerToggle.checked = serverSaysOn;
        }
        
        if (!serverSaysOn) {
            if (signalBtn) {
                signalBtn.className = 'analysing-signal state-offline';
                if (analysisText) analysisText.textContent = 'OFFLINE';
            }
        } else {
            // Check scan_count updates to trigger activity flash
            if (data.scan_count !== undefined) {
                if (data.scan_count > lastSystemScanCount) {
                    lastSystemScanCount = data.scan_count;
                    if (signalBtn) {
                        signalBtn.className = 'analysing-signal state-analysing';
                        if (analysisText) analysisText.textContent = 'ANALYSING';
                        
                        // Revert to IDLE only if no scans occur for 3 seconds
                        clearTimeout(idleTimeout);
                        idleTimeout = setTimeout(() => {
                            if (signalBtn.classList.contains('state-analysing')) {
                                signalBtn.className = 'analysing-signal state-idle';
                                if (analysisText) analysisText.textContent = 'IDLE';
                            }
                        }, 3000);
                    }
                }
            }
        }
    }

    // ══ SYNC GATEWAY STATUS ══
    if (data.gateway_status) {
        updateGatewayStatus(data.gateway_status);
    }
    updateDiagnosticsPanel(data.diagnostics || {});
    
    // ══ SYNC REGIME MODE ══
    const regimeModeSwitch = document.getElementById('regimeModeSwitch');
    if (regimeModeSwitch && data.config && data.config.ut_regime_adaptation !== undefined) {
        const serverSaysOn = data.config.ut_regime_adaptation === true;
        if (regimeModeSwitch.checked !== serverSaysOn) {
            regimeModeSwitch.checked = serverSaysOn;
        }
    }

    // ══ SYNC SL MODE ══
    const slModeSwitch = document.getElementById('slModeSwitch');
    if (slModeSwitch && data.config && data.config.sl_mode !== undefined) {
        const serverSaysNatural = data.config.sl_mode === 'NATURAL';
        if (slModeSwitch.checked !== serverSaysNatural) {
            slModeSwitch.checked = serverSaysNatural;
        }
    }

    // ══ SYNC CONCURRENCY GUARD ══
    const concurGuardSwitch = document.getElementById('concurGuardSwitch');
    if (concurGuardSwitch && data.config && data.config.ut_concurrency_guard !== undefined) {
        const serverSaysOn = data.config.ut_concurrency_guard === true;
        if (concurGuardSwitch.checked !== serverSaysOn) {
            concurGuardSwitch.checked = serverSaysOn;
        }
    }

    if (data.config && data.config.chart_stream_enabled !== undefined) {
        applyChartStreamState(data.config.chart_stream_enabled !== false, { notifyBackend: false, requestSnapshot: false });
    }
    
    state.latestData = data;

    // Safety Alert: If instruments are missing, notify user
    if (!data.instruments || Object.keys(data.instruments).length === 0) {
        console.warn("⚠️ Scanner is still initializing data...");
    }
    
    // 1. Sync Settings Modal (Global) - Only sync if modal is NOT open
    const modal = document.getElementById('settingsModal');
    if (data.config && (!modal || modal.style.display !== 'flex')) {
        const cfg = data.config;
        const setVal = (id, val) => { const el = document.getElementById(id); if (el) el.value = val; };
        setVal('settCapitalTotal', cfg.capital_total);
        setVal('settCapitalFut', cfg.capital_fut);
        setVal('settRiskFut', cfg.risk_fut_pct);
        setVal('settCapitalOpt', cfg.capital_opt);
        setVal('settRiskOpt', cfg.risk_opt_pct);
        setVal('settFutureSL', cfg.fut_sl);
        setVal('settOptionSL', cfg.opt_sl);
        setVal('settFutCost', cfg.fut_cost !== undefined ? cfg.fut_cost : 200);
        setVal('settOptCost', cfg.opt_cost !== undefined ? cfg.opt_cost : 80);
        setVal('settBacktestDays', cfg.backtest_days);
        setVal('settMaxTrades', cfg.max_trades_per_index !== undefined ? cfg.max_trades_per_index : 5);
        setVal('settMaxLosses', cfg.max_consecutive_losses !== undefined ? cfg.max_consecutive_losses : 3);
        setVal('settIndexCooldown', cfg.index_cooldown_minutes !== undefined ? cfg.index_cooldown_minutes : 4);
        if (cfg.lots) {
            setVal('settLotsNifty', cfg.lots.NIFTY || 1);
            setVal('settLotsBN', cfg.lots.BANKNIFTY || 1);
            setVal('settLotsSensex', cfg.lots.SENSEX || 1);
            setVal('settLotsMidcap', cfg.lots.MIDCPNIFTY || 1);
        }
        if (cfg.lots_fut) {
            setVal('settLotsFutNifty', cfg.lots_fut.NIFTY || 1);
            setVal('settLotsFutBN', cfg.lots_fut.BANKNIFTY || 1);
            setVal('settLotsFutSensex', cfg.lots_fut.SENSEX || 1);
            setVal('settLotsFutMidcap', cfg.lots_fut.MIDCPNIFTY || 1);
        }
        const autoEl = document.getElementById('settAutoMode');
        if (autoEl) autoEl.checked = cfg.auto_mode;
        const autoSwitch = document.getElementById('autoModeSwitch');
        if (autoSwitch) autoSwitch.checked = cfg.auto_mode;
        
        const prefEl = document.getElementById('settInstPref');
        if (prefEl) prefEl.value = cfg.inst_pref;
        
        const strikeEl = document.getElementById('settStrikeSelection');
        if (strikeEl) strikeEl.value = cfg.strike_selection || 'BOTH';
        
        const gradeEl = document.getElementById('settGradePreference');
        if (gradeEl) gradeEl.value = cfg.grade_preference || 'auto';
        
        const presetEl = document.getElementById('settUtPreset');
        if (presetEl) presetEl.value = cfg.ut_preset || 'AGGRESSIVE';

        const tfPolicyEl = document.getElementById('topTimeframePolicy');
        if (tfPolicyEl) tfPolicyEl.value = cfg.ut_timeframe_entry_policy || 'INCLUDE_5MIN';

        // Sync Index Checkboxes
        if (cfg.active_indices) {
            const nifty = document.getElementById('checkNifty');
            if (nifty) nifty.checked = cfg.active_indices.includes('NIFTY');
            const bn = document.getElementById('checkBN');
            if (bn) bn.checked = cfg.active_indices.includes('BANKNIFTY');
            const sensex = document.getElementById('checkSensex');
            if (sensex) sensex.checked = cfg.active_indices.includes('SENSEX');
            const midcap = document.getElementById('checkMidcap');
            if (midcap) midcap.checked = cfg.active_indices.includes('MIDCPNIFTY');
        }

        // Sync 3-Way Mode Control
        if (data.mode) {
            document.querySelectorAll('.mode-opt').forEach(btn => {
                if (btn.dataset.mode === data.mode) {
                    btn.classList.add('active');
                } else {
                    btn.classList.remove('active');
                }
            });
        }
    }

    setText('scanCount', data.scan_count || 0);
    setText('scanLatency', (data.latency || 0).toFixed(0) + 'ms');
    const heartbeat = data.dashboard_heartbeat || {};
    const heartbeatTime = heartbeat.timestamp ? new Date(heartbeat.timestamp).getTime() : NaN;
    const heartbeatAgeMs = Number.isFinite(heartbeatTime) ? Math.max(0, Date.now() - heartbeatTime) : null;
    const scanAgeMs = Number.isFinite(Number(heartbeat.scan_age_ms)) ? Number(heartbeat.scan_age_ms) : null;
    const wsAgeMs = state.lastWsUpdateAt ? Math.max(0, Date.now() - state.lastWsUpdateAt) : null;
    const liveAgeMs = heartbeatAgeMs !== null ? heartbeatAgeMs : wsAgeMs;
    const liveSuffix = heartbeatAgeMs !== null
        ? (scanAgeMs !== null ? ` / scan ${(scanAgeMs / 1000).toFixed(1)}s` : '')
        : ' / ws';
    const liveText = liveAgeMs === null ? '--' : `${(liveAgeMs / 1000).toFixed(1)}s${liveSuffix}`;
    setText('liveRefreshAge', liveText);

    // 2. Global Trades & Performance
    try { updateActivityLog(data.activity_log); } catch(e) { console.error("Log Error:", e); }
    const tradePayload = data.trades || {};
    syncTradePaginationState(data, tradePayload);
    try { updateTradesPanel(tradePayload, data.simulation_id); } catch(e) { console.error("Trades Error:", e); }
    try { updatePnlPanel(tradePayload); } catch(e) { console.error("P&L Error:", e); }
    try { updateEquityReplacement(tradePayload); } catch(e) { console.error("Equity Error:", e); }
    try {
        const hasTradeRows =
            (tradePayload.open || []).length ||
            (tradePayload.closed || []).length ||
            (tradePayload.signals || []).length;
        if (!hasTradeRows) {
            updateSignalsFeed(data.instruments || {});
        }
    } catch(e) { console.error("Signals Error:", e); }
    try { updateAnalyticsStrip(data.trades); } catch(e) { console.error("Analytics Error:", e); }

    // 3. Instrument Normalization
    const normalizedInst = {};
    if (data.instruments) {
        for (const key in data.instruments) {
            const normKey = TICKER_MAP[key] || key;
            normalizedInst[normKey] = data.instruments[key];
        }
    }
    
    try { updateInstrumentSelectors(normalizedInst); } catch(e) { console.error("Tabs Error:", e); }
    
    // 4. Active Instrument Updates
    const inst = normalizedInst[state.currentInstrument];
    if (inst) {
        try { updateInfoPanel(inst); } catch(e) { console.error("Info Error:", e); }
        try { updateIntelPanel(inst, data); } catch(e) { console.error("Intel Error:", e); }
        try { updateChart(inst); } catch(e) { console.error("Chart Error:", e); }
    }
}


function setupIntelTabs() {
    const tabs = document.querySelectorAll('.intel-tab');
    if (tabs.length === 0) return;
    tabs.forEach(tab => {
        tab.addEventListener('click', (e) => {
            tabs.forEach(t => t.classList.remove('active'));
            e.currentTarget.classList.add('active');
            
            document.querySelectorAll('.intel-view-pane').forEach(pane => {
                pane.style.display = 'none';
                pane.classList.remove('active');
            });
            
            const targetId = e.currentTarget.dataset.target;
            const targetPane = document.getElementById(targetId);
            if (targetPane) {
                targetPane.style.display = 'block';
                targetPane.classList.add('active');
            }
        });
    });
}

function setupSettingsExperience() {
    const modal = document.getElementById('settingsModal');
    const dialog = modal?.querySelector('.modal');
    const body = modal?.querySelector('.modal-body');
    const header = modal?.querySelector('.modal-header');
    if (!modal || !dialog || !body || body.dataset.compactReady === '1') return;

    dialog.classList.add('settings-modal');
    header?.classList.add('settings-header');
    body.classList.add('settings-body');
    body.dataset.compactReady = '1';

    const saveBtn = document.getElementById('saveSettings');
    const resetBtn = document.getElementById('resetCacheBtn');
    const closeBtn = document.getElementById('closeSettings');
    const findGroup = (id) => {
        const el = document.getElementById(id);
        return el?.closest('.setting-group') || el;
    };
    const makeCard = (id, title, nodes = []) => {
        const section = document.createElement('section');
        section.className = 'settings-card';
        section.id = id;
        const titleEl = document.createElement('div');
        titleEl.className = 'settings-card-title';
        titleEl.textContent = title;
        const grid = document.createElement('div');
        grid.className = 'settings-grid';
        nodes.filter(Boolean).forEach(node => grid.appendChild(node));
        section.append(titleEl, grid);
        return section;
    };

    if (!document.getElementById('settUtPreset')) {
        const presetGroup = document.createElement('div');
        presetGroup.className = 'setting-group';
        presetGroup.innerHTML = `
            <label>UT Bot Preset</label>
            <select id="settUtPreset">
                <option value="AGGRESSIVE">Aggressive</option>
                <option value="BALANCED">Balanced</option>
                <option value="CONSERVATIVE">Conservative</option>
            </select>
        `;
        body.appendChild(presetGroup);
    }
    if (!document.getElementById('settAutoMode')) {
        const autoCard = document.createElement('label');
        autoCard.className = 'settings-switch-card';
        autoCard.innerHTML = `
            <span><strong>Auto Mode</strong><small>Allow system execution when enabled</small></span>
            <input type="checkbox" id="settAutoMode">
        `;
        body.appendChild(autoCard);
    }

    const nav = document.createElement('nav');
    nav.className = 'settings-nav';
    [
        ['settings-indices', 'Indices'],
        ['settings-risk', 'Risk'],
        ['settings-options', 'Options'],
        ['settings-lots', 'Lots'],
        ['settings-limits', 'Limits'],
        ['settings-system', 'System'],
    ].forEach(([href, label]) => {
        const link = document.createElement('a');
        link.href = `#${href}`;
        link.textContent = label;
        nav.appendChild(link);
    });

    const content = document.createElement('div');
    content.className = 'settings-content';

    const indexGrid = body.querySelector('.index-selection-grid');
    if (indexGrid) indexGrid.classList.add('compact');
    content.appendChild(makeCard('settings-indices', 'Active Trading Indices', [indexGrid]));
    content.appendChild(makeCard('settings-risk', 'Capital & Futures Risk', [
        findGroup('settCapitalTotal'),
        findGroup('settCapitalFut'),
        findGroup('settRiskFut'),
        findGroup('settFutureSL'),
        findGroup('settFutCost')
    ]));
    content.appendChild(makeCard('settings-options', 'Options Configuration', [
        findGroup('settCapitalOpt'),
        findGroup('settRiskOpt'),
        findGroup('settOptionSL'),
        findGroup('settOptCost')
    ]));

    const lots = document.createElement('section');
    lots.className = 'settings-card';
    lots.id = 'settings-lots';
    const lotsTitle = document.createElement('div');
    lotsTitle.className = 'settings-card-title';
    lotsTitle.textContent = 'Trading Lots';
    const lotsMatrix = document.createElement('div');
    lotsMatrix.className = 'lots-matrix';
    ['Index', 'Options', 'Futures'].forEach(text => {
        const head = document.createElement('div');
        head.className = 'lots-head';
        head.textContent = text;
        lotsMatrix.appendChild(head);
    });
    [
        ['NIFTY', 'settLotsNifty', 'settLotsFutNifty'],
        ['BANKNIFTY', 'settLotsBN', 'settLotsFutBN'],
        ['SENSEX', 'settLotsSensex', 'settLotsFutSensex'],
        ['MIDCPNIFTY', 'settLotsMidcap', 'settLotsFutMidcap'],
    ].forEach(([name, optId, futId]) => {
        const label = document.createElement('div');
        label.className = 'lots-name';
        label.textContent = name;
        lotsMatrix.appendChild(label);
        [optId, futId].forEach(id => {
            const input = document.getElementById(id);
            if (input) lotsMatrix.appendChild(input);
        });
    });
    lots.append(lotsTitle, lotsMatrix);
    content.appendChild(lots);

    content.appendChild(makeCard('settings-limits', 'Per-Index Trade Limits', [
        findGroup('settMaxTrades'),
        findGroup('settMaxLosses'),
        findGroup('settIndexCooldown'),
    ]));

    content.appendChild(makeCard('settings-system', 'System Controls', [
        findGroup('settBacktestDays'),
        findGroup('settInstPref'),
        findGroup('settStrikeSelection'),
        findGroup('settGradePreference'),
        findGroup('settUtPreset'),
        document.getElementById('settAutoMode')?.closest('.settings-switch-card'),
    ]));
    const cacheCard = makeCard('settings-cache', 'Cache Management', [resetBtn?.closest('.setting-group') || resetBtn]);
    cacheCard.classList.add('settings-actions-card');
    content.appendChild(cacheCard);

    body.innerHTML = '';
    body.append(nav, content);

    const footer = document.createElement('div');
    footer.className = 'settings-footer';
    const cancelBtn = document.createElement('button');
    cancelBtn.className = 'btn-secondary';
    cancelBtn.type = 'button';
    cancelBtn.id = 'cancelSettings';
    cancelBtn.textContent = 'Cancel';
    cancelBtn.addEventListener('click', () => {
        modal.style.display = 'none';
    });
    if (saveBtn) footer.append(cancelBtn, saveBtn);
    dialog.appendChild(footer);

    if (closeBtn && !closeBtn.dataset.closeReady) {
        closeBtn.dataset.closeReady = '1';
        closeBtn.addEventListener('click', () => {
            modal.style.display = 'none';
        });
    }
}

function formatLogTime(rawTime) {
    if (!rawTime) {
        return new Date().toLocaleTimeString('en-IN', {
            timeZone: 'Asia/Kolkata',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
        }) + ' IST';
    }
    const text = String(rawTime).trim();
    if (/IST$/i.test(text)) return text;
    if (/^\d{1,2}:\d{2}(:\d{2})?$/.test(text)) return text.padStart(8, '0') + ' IST';
    const parsed = new Date(text);
    if (!Number.isNaN(parsed.getTime())) {
        return parsed.toLocaleTimeString('en-IN', {
            timeZone: 'Asia/Kolkata',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
        }) + ' IST';
    }
    return text;
}

function cleanLogMessage(message) {
    let text = String(message || '');
    text = text
        .replace(/ðŸ[^\s]*/g, '')
        .replace(/âš[^\s]*/g, '')
        .replace(/âœ[^\s]*/g, '')
        .replace(/Ã¢[^\s]*/g, '')
        .replace(/Â/g, '')
        .replace(/[^\x20-\x7E₹]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();

    text = text
        .replace(/\bManual\s+(LONG|SHORT)\s+Signal:/i, 'Manual $1 signal:')
        .replace(/\bPotential\s+(LONG|SHORT)\s+Signal\s+for\b/i, 'Potential $1 signal for')
        .replace(/\bSignal Rejected:/i, 'Signal rejected:')
        .replace(/\|\s*Reasons:/i, '. Reason:')
        .replace(/\bBelow Grade Pref\b/i, 'below selected grade')
        .replace(/\bMulti-TF\b/i, 'multi-timeframe')
        .replace(/\bRisk-Window\b/i, 'risk window')
        .replace(/\bRange-bound noise\b/i, 'range-bound noise')
        .replace(/\bSystem initialized\b/i, 'System initialised')
        .replace(/\bInitialized\b/i, 'Initialised');

    return text || 'System update received.';
}

function updateActivityLog(logs = []) {
    if (logs && !Array.isArray(logs) && typeof logs === 'object') {
        logs = Object.values(logs);
    }
    if (!Array.isArray(logs)) logs = [];
    state.fullLog = logs
        .filter(Boolean)
        .map(item => ({
            time: item.time || item.timestamp || '',
            msg: item.msg || item.message || '',
            type: item.type || 'system'
        }));
    renderLogs();
}


function generateTapeCardHtml(item) {
    const msgLower = item.cleanMsg.toLowerCase();
    
    if (msgLower.includes('volume pressure') || msgLower.includes('volume spike') || msgLower.includes('vol')) {
        const isBuy = msgLower.includes('buy') || msgLower.includes('long');
        const volClass = isBuy ? 'vol-buy' : 'vol-sell';
        const volColor = isBuy ? 'var(--buy)' : 'var(--sell)';
        const title = isBuy ? 'BUY PRESSURE' : 'SELL PRESSURE';
        
        return {
            className: `tape-card tape-volume ${volClass}`,
            html: `
                <div class="tape-header">
                    <span class="tape-time">${item.timeText}</span>
                    <span class="tape-dir vol-badge" style="color:${volColor}">${title}</span>
                </div>
                <div class="tape-body">${item.cleanMsg}</div>
                <div class="tape-progress"><div class="tape-bar" style="background:${volColor}; width: 85%;"></div></div>
            `
        };
    } else if (msgLower.includes('signal') || msgLower.includes('matched') || item.type === 'trade') {
        const isLong = msgLower.includes('long') || msgLower.includes('buy');
        const isShort = msgLower.includes('short') || msgLower.includes('sell');
        const sigClass = isLong ? 'sig-long' : (isShort ? 'sig-short' : '');
        const dirBadge = isLong ? '<span class="tape-dir long">LONG SIGNAL</span>' : (isShort ? '<span class="tape-dir short">SHORT SIGNAL</span>' : '');
        
        return {
            className: `tape-card tape-signal ${sigClass}`,
            html: `
                <div class="tape-header">
                    <span class="tape-time">${item.timeText}</span>
                    ${dirBadge}
                </div>
                <div class="tape-body">${item.cleanMsg}</div>
            `
        };
    } else {
        return {
            className: `tape-card tape-system`,
            html: `
                <div class="tape-header">
                    <span class="tape-time">${item.timeText}</span>
                    <span class="tape-time" style="opacity:0.6">[${item.typeLabel}]</span>
                </div>
                <div class="tape-body">${item.cleanMsg}</div>
            `
        };
    }
}

function renderLogs() {
    const logEl = document.getElementById('activityLog');
    if (!logEl) return;

    const filtered = state.logFilter === 'all' 
        ? state.fullLog 
        : state.fullLog.filter(item => {
            if (state.logFilter === 'error') return item.type === 'error' || item.type === 'warning';
            return item.type === state.logFilter;
        });

    const dataString = JSON.stringify(filtered);
    if (state.lastRenderedLogData === dataString) return;
    state.lastRenderedLogData = dataString;

    const newItems = filtered.map(item => {
        const type = item.type || 'system';
        const typeLabel = type === 'error' ? 'ALERT' : type.toUpperCase();
        const cleanMsg = cleanLogMessage(item.msg);
        const timeText = formatLogTime(item.time);
        const key = `${timeText}|${type}|${cleanMsg}`;
        return { key, type, typeLabel, cleanMsg, timeText };
    });

    const currentChildren = Array.from(logEl.children);
    const filterChanged = state.lastRenderedFilter !== state.logFilter;
    state.lastRenderedFilter = state.logFilter;

    if (filterChanged || currentChildren.length === 0 || newItems.length === 0) {
        logEl.innerHTML = newItems.length ? newItems.map(item => {
            const card = generateTapeCardHtml(item);
            return `<div class="${card.className}" data-key="${item.key}">${card.html}</div>`;
        }).join('') : `
            <div class="tape-card tape-system" data-key="empty">
                <div class="tape-header"><span class="tape-time">${formatLogTime('')}</span></div>
                <div class="tape-body">Waiting for tape activity...</div>
            </div>
        `;
        return;
    }

    const c0_key = currentChildren[0].getAttribute('data-key');
    const matchIndex = newItems.findIndex(item => item.key === c0_key);

    if (matchIndex === -1) {
        logEl.innerHTML = newItems.map(item => {
            const card = generateTapeCardHtml(item);
            return `<div class="${card.className}" data-key="${item.key}">${card.html}</div>`;
        }).join('');
    } else if (matchIndex > 0) {
        // Prepend new items
        for (let i = matchIndex - 1; i >= 0; i--) {
            const item = newItems[i];
            const div = document.createElement('div');
            const card = generateTapeCardHtml(item);
            div.className = card.className;
            div.setAttribute('data-key', item.key);
            div.innerHTML = card.html;
            logEl.insertBefore(div, logEl.firstChild);
        }
    }

    // Trim extra items from bottom
    while (logEl.children.length > newItems.length) {
        logEl.lastChild.remove();
    }
}

function appendLogItem(container, item) {
    // Legacy - replaced by renderLogs
}


function updateInstrumentSelectors(instruments) {
    INDEX_NAMES.forEach(name => {
        const d = instruments[name];
        if (!d) return;
        
        // Update Price & Change
        const ltp = Number(d.ltp || d.spot_price || 0);
        const pct = Number(d.change_pct || 0);
        const changePoints = resolveIndexChangePoints(d, ltp, pct);
        setText(`price_${name}`, formatPrice(ltp));
        const changeEl = document.getElementById(`change_${name}`);
        if (changeEl) {
            changeEl.textContent = `${formatPointChange(changePoints)} (${pct.toFixed(2)}%)`;
            changeEl.className = 'inst-change ' + (changePoints > 0 ? 'green' : changePoints < 0 ? 'red' : 'neutral');
        }

        // Update Signal
        const tabEl = document.getElementById(`tab_${name}`);
        const signalEl = document.getElementById(`signal_${name}`);
        const mtf = d.mtf || {};
        const signal = (mtf.confluence_signal || 'SCANNING').toUpperCase();
        const stateClass = signal === 'BUY' ? 'state-buy' : signal === 'SELL' ? 'state-sell' : signal === 'SCANNING' ? 'state-scanning' : 'state-hold';
        if (tabEl) {
            tabEl.classList.remove('state-buy', 'state-sell', 'state-hold', 'state-scanning');
            tabEl.classList.add(stateClass);
        }
        if (signalEl) {
            signalEl.textContent = signal;
            signalEl.className = 'inst-signal ' + (signal === 'BUY' ? 'buy' : signal === 'SELL' ? 'sell' : 'neutral');
        }
    });
}

function resolveIndexChangePoints(d, ltp, pct) {
    const direct = [d.change_points, d.change_abs, d.net_change, d.points_change, d.change].find(v => Number.isFinite(Number(v)));
    if (direct !== undefined) return Number(direct);

    const prevClose = Number(d.prev_close || d.previous_close || d.close_previous || 0);
    if (Number.isFinite(prevClose) && prevClose > 0 && Number.isFinite(ltp)) return ltp - prevClose;

    if (!Number.isFinite(ltp) || !Number.isFinite(pct) || Math.abs(100 + pct) < 0.0001) return 0;
    return ltp - (ltp / (1 + pct / 100));
}

function formatPointChange(value) {
    if (!Number.isFinite(value)) return '--';
    const sign = value < 0 ? '-' : '';
    return sign + Math.abs(value).toLocaleString('en-IN', { useGrouping: false, minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function updateInfoPanel(inst) {
    if (!inst) return;
    
    const mtf = inst.mtf || {};
    const tf = state.currentTimeframe;
    const chartObj = (inst.chart && inst.chart[tf]) ? inst.chart[tf] : {};
    const tfState = chartObj.state || {};

    const pos = tfState.position_label || 'FLAT';
    const posEl = document.getElementById('infoPosition');
    if (posEl) {
        posEl.textContent = { LONG: '🟢 LONG', SHORT: '🔴 SHORT', FLAT: '⚪ FLAT' }[pos] || '⚪ FLAT';
        posEl.className = 'info-value ' + (pos === 'LONG' ? 'green' : pos === 'SHORT' ? 'red' : '');
        posEl.textContent = pos;
    }

    setText('infoEntry', formatPrice(tfState.last_entry_price));
    setText('infoExit', formatPrice(tfState.last_exit_price));
    setText('infoStop', formatPrice(tfState.trailing_stop));
    setText('infoStopDist', formatPrice(tfState.stop_distance) + ' pts');
    setText('infoADX', (tfState.adx_value || 0).toFixed(1));
    setText('infoConfluence', (mtf.confluence_score || 0).toFixed(2));
    
    const intel = inst.intelligence || {};
    const agg = intel.aggregate || {};
    setText('infoIntel', agg.score !== undefined ? agg.score.toFixed(0) : '--');
    
    updateMTFLabels(mtf);
}

function updateHardwarePanel(metrics = {}) {
    const ram = metrics.ram || {};
    const disk = metrics.disk || {};
    const gpu = metrics.gpu || {};
    const processRam = Number(metrics.process_ram_mb || 0);
    const processCpu = Number(metrics.process_cpu_pct || 0);
    const threads = Number(metrics.threads || 0);
    const fmtGb = value => Number(value || 0).toLocaleString('en-IN', { maximumFractionDigits: 1 });
    const fmtMb = value => Number(value || 0).toLocaleString('en-IN', { maximumFractionDigits: 0 });

    setText('hwRam', `${Number(ram.used_pct || 0).toFixed(1)}%`);
    setText('hwRamSub', `${fmtMb(ram.used_mb)} / ${fmtMb(ram.total_mb)} MB`);

    setText('hwProcess', `${fmtMb(processRam)} MB`);
    setText('hwProcessSub', `${processCpu.toFixed(1)}% CPU / ${threads || '--'} workers`);

    setText('hwDisk', `${Number(disk.used_pct || 0).toFixed(1)}%`);
    setText('hwDiskSub', `${fmtGb(disk.free_gb)} GB free`);

    if (gpu.available) {
        const bench = gpu.benchmark || {};
        const speedLabel = bench.speedup > 0 ? ` / ${Number(bench.speedup).toFixed(2)}x bench` : '';
        const computeLabel = gpu.compute_available
            ? `compute ${gpu.compute_backend || 'ready'}${speedLabel}`
            : `${gpu.acceleration_mode || 'telemetry_only'}`;
        setText('hwGpu', `${Number(gpu.used_pct || 0).toFixed(1)}%`);
        setText('hwGpuSub', `${gpu.label || 'GPU'} / ${fmtMb(gpu.used_mb)} of ${fmtMb(gpu.total_mb)} MB / ${computeLabel}`);
    } else {
        setText('hwGpu', 'N/A');
        setText('hwGpuSub', gpu.acceleration_mode || 'No NVIDIA telemetry');
    }
}

function updateMTFLabels(mtf) {
    ['1min', '5min', '15min'].forEach(tf => {
        const s = mtf['state_' + tf] || {};
        const dotId = 'dot' + tf.replace('min', 'm');
        const el = document.getElementById(dotId);
        if (el) {
            const pos = (s.position_label || 'FLAT').toLowerCase();
            el.className = 'tf-pos-dot ' + pos;
        }
    });
}


function getSentimentBadge(text) {
    if (!text) return '';
    const txt = String(text).toUpperCase().replace(/_/g, ' ');
    let colorClass = 'neutral';
    
    if (txt.includes('BEAR') || txt.includes('FEAR') || txt.includes('SHORT') || txt.includes('DOWN') || txt.includes('SELL')) colorClass = 'danger';
    else if (txt.includes('BULL') || txt.includes('GREED') || txt.includes('LONG') || txt.includes('UP') || txt.includes('BUY')) colorClass = 'success';
    else if (txt.includes('BREAKOUT') || txt.includes('SURGE')) colorClass = 'warning';
    
    return `<span class="sentiment-badge ${colorClass}">${txt}</span>`;
}

function updateIntelPanel(inst, data) {
    if (!inst) return;
    const intel = (data && data.intelligence) ? data.intelligence[state.currentInstrument] : (inst.intelligence || {});

    // ═══ Manipulation Detection UI ═══
    const alertEl = document.getElementById('manipulationAlert');
    if (alertEl) {
        alertEl.style.display = intel.divergence_alert ? 'block' : 'none';
        if (intel.divergence_alert) {
            alertEl.title = "Order Flow contradicts Price Action — Potential Institutional Trap!";
        }
    }

    const vol = intel.volume || {};
    const oi = intel.oi || {};
    const pcr = intel.pcr || {};
    const regime = intel.regime || {};
    const flow = intel.order_flow || {};

    setText('intelVolRatio', (vol.volume_ratio || 1.0).toFixed(1) + 'x');
    const volSurge = vol.surge_level || 'NORMAL';
    const buyPct = vol.buy_pct !== undefined ? vol.buy_pct : 50;
    let volLabelText = 'BALANCED';
    if (buyPct > 50.5) {
        volLabelText = `${volSurge} (${buyPct.toFixed(0)}% BUY)`;
    } else if (buyPct < 49.5) {
        volLabelText = `${volSurge} (${(100 - buyPct).toFixed(0)}% SELL)`;
    } else {
        volLabelText = `${volSurge} (BALANCED)`;
    }
    setHtml(document.getElementById('intelVolLabel'), getSentimentBadge(volLabelText));
    
    const volBar = document.getElementById('intelVolBar');
    if (volBar) {
        volBar.style.width = buyPct + '%';
        if (buyPct > 50.5) {
            volBar.style.background = 'var(--gradient-bull)';
        } else if (buyPct < 49.5) {
            volBar.style.background = 'var(--gradient-bear)';
        } else {
            volBar.style.background = 'var(--border)';
        }
    }
    
    const oiVal = oi.oi_change_pct ? oi.oi_change_pct.toFixed(1) + '%' : (oi.activity || '--');
    setText('intelOI', oiVal.replace(/_/g, ' '));
    setHtml(document.getElementById('intelOILabel'), getSentimentBadge(oi.oi_sentiment || oi.signal || 'NEUTRAL'));
    
    setText('intelPCR', (pcr.primary_pcr || pcr.pcr_value || pcr.pcr_oi || 0).toFixed(2));
    const pcrSignal = (pcr.contrarian_signal || pcr.signal || pcr.sentiment || 'NEUTRAL').replace(/_/g, ' ');
    setHtml(document.getElementById('intelPCRLabel'), getSentimentBadge(pcrSignal));
    
    setText('intelRegime', (regime.market_regime || regime.regime || 'STABLE').replace(/_/g, ' '));
    setHtml(document.getElementById('intelRegimeLabel'), getSentimentBadge((regime.trend_strength || regime.direction || 'NORMAL').toUpperCase()));

    setText('intelFlow', flow.buy_sell_ratio ? flow.buy_sell_ratio.toFixed(2) + 'x' : '--');
    setHtml(document.getElementById('intelFlowLabel'), getSentimentBadge(flow.signal || 'NEUTRAL'));

    const greeks = intel.greeks || {};
    if (greeks.is_fallback) {
        setText('greekIV', "OFFLINE");
        setText('greekCallDelta', "--");
        setText('greekPutDelta', "--");
        setText('greekTheta', "--");
        setText('greekSummary', "Options data unavailable");
    } else {
        const ivp = greeks.iv_percentile || 50;
        
        // 1. Translation: Pricing
        const pricing = ivp < 30 ? "CHEAP" : (ivp > 70 ? "EXPENSIVE" : "FAIRLY PRICED");
        setText('greekIV', pricing);
        
        // 2. Translation: Power (Rupees per 100 points)
        const callDelta = Number(greeks.call?.delta || 0);
        const deltaMove = (callDelta * 100).toFixed(0);
        setText('greekCallDelta', `+₹${deltaMove}`);
        setText('greekPutDelta', `-₹${deltaMove}`);
        
        // 3. Translation: Time Cost (Rupees per Day)
        const leak = Math.abs(greeks.call?.theta || greeks.total_theta || 0).toFixed(1);
        setText('greekTheta', `₹${leak}/day`);

        // 4. Ultra-compact interpretation
        setText('greekSummary', `₹${deltaMove}/100pts | -₹${leak}/day`);
    }

}

function parseDashboardTimestamp(value) {
    if (value === null || value === undefined || value === '') return 0;
    if (typeof value === 'number' && Number.isFinite(value)) {
        return value > 1e12 ? value : value * 1000;
    }
    const numeric = Number(value);
    if (Number.isFinite(numeric) && String(value).trim() !== '') {
        return numeric > 1e12 ? numeric : numeric * 1000;
    }
    const parsed = Date.parse(String(value));
    return Number.isNaN(parsed) ? 0 : parsed;
}

function getTradeRowEventTimestamp(row) {
    const exitTs = parseDashboardTimestamp(row && (row.exit_timestamp || row.exit_time));
    const entryTs = parseDashboardTimestamp(row && (row.entry_timestamp || row.timestamp || row.entry_time));
    return entryTs || exitTs;
}

function getTradeRowSortRank(row) {
    const status = String(row && row.status ? row.status : '').toUpperCase();
    if (status === 'OPEN' || (row && row.row_kind === 'manager_open')) return 0;
    if (status.indexOf('EXIT') !== -1 || (row && row.row_kind === 'signal')) return 1;
    if (status === 'CLOSED' || (row && row.row_kind === 'manager_closed')) return 2;
    return 3;
}

function getTradePoints(row) {
    if (!row) return null;
    const entry = Number(row.entry_price || 0);
    if (!Number.isFinite(entry) || entry <= 0) return null;

    const exit = Number(row.exit_price || 0);
    const current = Number(row.current_price || 0);
    const mark = exit > 0 ? exit : current;
    if (!Number.isFinite(mark) || mark <= 0) return null;

    const instrumentText = String(row.instrument || '');
    const isOption = row.inst_type === 'OPT' || instrumentText.includes('CE') || instrumentText.includes('PE');
    const direction = String(row.direction || '').toUpperCase();
    const points = isOption || direction !== 'SHORT' ? mark - entry : entry - mark;
    return Number.isFinite(points) ? points : null;
}

function formatTradePoints(points) {
    if (points === null || points === undefined || !Number.isFinite(points)) return '';
    const sign = points < 0 ? '-' : '';
    return `${sign}${Math.abs(points).toLocaleString('en-IN', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    })}`;
}

function collectSessionSignalRows(existingRows = [], backendSignals = []) {
    const baseName = value => String(value || '').split(/\s+/)[0];
    const rowMinuteKey = row => {
        const ts = row.entry_timestamp ? new Date(Number(row.entry_timestamp) * 1000) : null;
        const validTs = ts && !Number.isNaN(ts.getTime());
        const dateText = row.entry_date || (validTs ? ts.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: '2-digit' }) : '');
        const timeText = row.entry_time || (validTs ? ts.toLocaleTimeString('en-GB', { hour12: false }) : '');
        return [
            baseName(row.base_instrument || row.instrument || row.trading_symbol),
            row.timeframe || '',
            row.direction || '',
            dateText,
            String(timeText || '').substring(0, 8)
        ].join('|');
    };
    const realKeys = new Set(existingRows.map(rowMinuteKey));
    const signalMinuteKey = sig => {
        const ts = sig.timestamp ? new Date(sig.timestamp) : null;
        const validTs = ts && !Number.isNaN(ts.getTime());
        const dateText = sig.entry_date || (validTs ? ts.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: '2-digit' }) : '');
        const timeText = sig.entry_time || (validTs ? ts.toLocaleTimeString('en-GB', { hour12: false }) : '');
        return [
            baseName(sig.base_instrument || sig.instrument || sig.trading_symbol),
            sig.timeframe || '',
            sig.direction || '',
            dateText,
            String(timeText || '').substring(0, 8)
        ].join('|');
    };

    const rawRows = [];
    const candidateSignals = [];
    (backendSignals || []).forEach(sig => candidateSignals.push(sig));
    Object.values((state.latestData && state.latestData.instruments) || {}).forEach(inst => {
        (inst.trade_candidates || []).forEach(sig => candidateSignals.push(sig));
    });

    candidateSignals.forEach(sig => {
            if (realKeys.has(signalMinuteKey(sig))) return;
            const rowKey = [
                sig.trading_symbol || sig.instrument || '',
                sig.direction || '',
                sig.timeframe || ''
            ].join('|');
            if (realKeys.has(rowKey)) return;

            const ts = sig.timestamp ? new Date(sig.timestamp) : null;
            const isValidTs = ts && !Number.isNaN(ts.getTime());
            const exitTs = sig.exit_timestamp ? new Date(sig.exit_timestamp) : null;
            const isValidExitTs = exitTs && !Number.isNaN(exitTs.getTime());
            const instrumentName = sig.inst_type === 'OPT' && sig.atm_strike
                ? `${sig.instrument} ${sig.atm_strike} ${sig.option_type || ''}`
                : (sig.trading_symbol || sig.instrument || '--');
            const isExit = sig.is_exit || sig.action === 'EXIT' || sig.status === 'EXIT SIGNAL';
            const isNoEntry = sig.action === 'NO_ENTRY' || String(sig.status || '').includes('NO ENTRY');
            
            // Fix: Filter out ghost 'NO_ENTRY' signals so they do not flash on the dashboard
            if (isNoEntry) return;
            
            const key = [
                sig.instrument || '',
                sig.timeframe || '',
                sig.action || 'ENTRY',
                sig.direction || '',
                sig.trading_symbol || sig.atm_strike || '',
                isValidTs ? Math.floor(ts.getTime() / 1000) : 0,
                isValidExitTs ? Math.floor(exitTs.getTime() / 1000) : 0
            ].join('|');

            rawRows.push({
                row_kind: 'signal',
                id: key,
                lifecycle_id: sig.lifecycle_id || '',
                status: isExit ? 'EXIT_SIGNAL' : (isNoEntry ? 'NO_ENTRY' : 'SIGNAL'),
                base_instrument: sig.instrument || '',
                instrument: instrumentName,
                direction: sig.direction || '--',
                underlying_direction: sig.underlying_direction || sig.direction || '--',
                trade_side: sig.trade_side || '',
                entry_price: sig.price,
                current_price: sig.current_price || sig.price,
                exit_price: sig.exit_price || (isExit ? (sig.current_price || sig.price) : 0),
                trailing_stop: sig.stop,
                target: isExit ? 0 : sig.target,
                pnl: Number(sig.pnl || 0),
                lots: sig.lots || 0,
                lot_size: sig.lot_size || 0,
                grade: sig.grade || '--',
                confidence: sig.confidence || 0,
                rr_ratio: sig.rr || 0,
                atm_strike: sig.atm_strike,
                option_type: sig.option_type,
                inst_type: sig.inst_type || 'FUT',
                timeframe: sig.timeframe || '5min',
                trading_symbol: sig.trading_symbol || '',
                entry_time: isValidTs ? ts.toLocaleTimeString('en-GB', { hour12: false }) : '--',
                entry_date: isValidTs ? ts.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: '2-digit' }) : '--',
                entry_timestamp: isValidTs ? Math.floor(ts.getTime() / 1000) : 0,
                exit_time: isValidExitTs ? exitTs.toLocaleTimeString('en-GB', { hour12: false }) : null,
                exit_date: isValidExitTs ? exitTs.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: '2-digit' }) : null,
                exit_timestamp: isValidExitTs ? Math.floor(exitTs.getTime() / 1000) : 0,
                exit_reason: sig.exit_reason || sig.status || '',
                exec_type: isExit ? 'EXIT' : (isNoEntry ? 'BLOCK' : 'SIG'),
            });
    });

    const rowsNotAlreadyRendered = rawRows.filter(row => !realKeys.has(rowMinuteKey(row)));
    rawRows.length = 0;
    rawRows.push(...rowsNotAlreadyRendered);

    const exitEntryKeys = new Set();
    rawRows.forEach(row => {
        if (row.status !== 'EXIT_SIGNAL') return;
        exitEntryKeys.add(row.lifecycle_id || [
            row.base_instrument || row.instrument,
            row.timeframe,
            row.direction,
            row.entry_timestamp || 0
        ].join('|'));
    });

    const byKey = new Map();
    rawRows.forEach(row => {
        if (row.status === 'SIGNAL') {
            const entryKey = row.lifecycle_id || [
                row.base_instrument || row.instrument,
                row.timeframe,
                row.direction,
                row.entry_timestamp || 0
            ].join('|');
            if (exitEntryKeys.has(entryKey)) return;
        }
        byKey.set(row.id, row);
    });

    return Array.from(byKey.values());
}

function buildSessionSummary(baseSummary, openRows, closedRows, signalRows) {
    const summary = { ...(baseSummary || {}) };
    const backendHasTrades = (summary.total_trades || 0) > 0 || (summary.open_count || 0) > 0 || openRows.length || closedRows.length;
    if (backendHasTrades || !signalRows.length) return summary;

    const exitRows = signalRows.filter(row => row.status === 'EXIT_SIGNAL');
    const exitKeys = new Set(exitRows.map(row => [
        row.base_instrument || row.instrument,
        row.timeframe,
        row.direction,
        row.entry_timestamp || 0,
    ].join('|')));

    const activeRows = signalRows.filter(row => {
        if (row.status !== 'SIGNAL') return false;
        const key = [
            row.base_instrument || row.instrument,
            row.timeframe,
            row.direction,
            row.entry_timestamp || 0,
        ].join('|');
        return !exitKeys.has(key);
    });
    const accountingRows = [...exitRows, ...activeRows];
    const pnls = accountingRows.map(row => Number(row.pnl || 0));
    const wins = pnls.filter(v => v > 0).length;
    const losses = pnls.filter(v => v < 0).length;
    const grossWin = pnls.filter(v => v > 0).reduce((a, b) => a + b, 0);
    const grossLoss = Math.abs(pnls.filter(v => v < 0).reduce((a, b) => a + b, 0));
    const totalPnl = pnls.reduce((a, b) => a + b, 0);
    const futPnl = accountingRows
        .filter(row => {
            const isOpt = row.inst_type === 'OPT' || 
                          (row.instrument && (row.instrument.includes('CE') || row.instrument.includes('PE'))) ||
                          (row.trading_symbol && (row.trading_symbol.includes('CE') || row.trading_symbol.includes('PE')));
            return !isOpt;
        })
        .reduce((sum, row) => sum + Number(row.pnl || 0), 0);
    const optPnl = accountingRows
        .filter(row => {
            const isOpt = row.inst_type === 'OPT' || 
                          (row.instrument && (row.instrument.includes('CE') || row.instrument.includes('PE'))) ||
                          (row.trading_symbol && (row.trading_symbol.includes('CE') || row.trading_symbol.includes('PE')));
            return isOpt;
        })
        .reduce((sum, row) => sum + Number(row.pnl || 0), 0);

    let equity = 0;
    let peak = 0;
    let maxDd = 0;
    accountingRows
        .slice()
        .sort((a, b) => (a.entry_timestamp || 0) - (b.entry_timestamp || 0))
        .forEach(row => {
            equity += Number(row.pnl || 0);
            peak = Math.max(peak, equity);
            maxDd = Math.max(maxDd, peak - equity);
        });

    const avg = pnls.length ? totalPnl / pnls.length : 0;
    const variance = pnls.length > 1
        ? pnls.reduce((sum, val) => sum + Math.pow(val - avg, 2), 0) / (pnls.length - 1)
        : 0;
    const std = Math.sqrt(variance);

    return {
        ...summary,
        daily_pnl: totalPnl,
        fut_pnl: futPnl,
        opt_pnl: optPnl,
        total_trades: accountingRows.length,
        open_count: activeRows.length,
        wins,
        losses,
        win_rate: accountingRows.length ? (wins / accountingRows.length) * 100 : 0,
        profit_factor: grossLoss > 0 ? Math.min(99.99, grossWin / grossLoss) : (grossWin > 0 ? 99.99 : 1),
        sharpe_ratio: std > 0 ? Math.max(-99.99, Math.min(99.99, (avg / std) * Math.sqrt(252))) : 0,
        max_drawdown: maxDd,
    };
}

function updateTradesPanel(trades, simId) {
    const tbody = document.getElementById('tradesBody');
    if (!tbody) return;
    const panelTotals = tradeHistoryTotals(trades || {});
    const pageInfo = getTradePageInfo(trades || {});
    const modeBadge = document.getElementById('tradeDataMode');
    if (modeBadge) {
        const meta = trades.meta || {};
        const restored = meta.restored_signal_rows || 0;
        const simulated = meta.simulated_signal_rows || 0;
        const managerRows = (meta.open_rows || 0) + (meta.closed_rows || 0);
        const source = String(meta.source || '').toLowerCase();
        const expectedRows = restored + simulated || panelTotals.total;
        const visibleLabel = panelTotals.total > panelTotals.visible
            ? ` page ${pageInfo.page}/${pageInfo.pageCount}, showing ${panelTotals.visible}/${panelTotals.total}`
            : '';
        let modeText = `TRADE MANAGER (${managerRows})`;
        let modeClass = 'trade-data-mode live';
        if (source === 'historical_session_plus_simulation') {
            modeText = `HISTORICAL SESSION + SIMULATION (${expectedRows}${visibleLabel})`;
            modeClass = 'trade-data-mode restored';
        } else if (source === 'restored_session_signals') {
            modeText = `RESTORED SESSION SIGNALS (${restored || totals.total}${visibleLabel})`;
            modeClass = 'trade-data-mode restored';
        } else if (restored && !managerRows) {
            modeText = `RESTORED SESSION SIGNALS (${restored}${visibleLabel})`;
            modeClass = 'trade-data-mode restored';
        }
        modeBadge.textContent = modeText;
        modeBadge.className = modeClass;
    }
    renderTradePagination(trades || {});

    // Pulse effect if re-simulation happened
    if (simId && simId !== state.lastSimId) {
        state.lastSimId = simId;
        const panel = document.getElementById('tradesPanel');
        const indicator = document.getElementById('syncIndicator');
        if (panel) {
            panel.style.boxShadow = '0 0 30px var(--buy)';
            setTimeout(() => panel.style.boxShadow = 'none', 1000);
        }
        if (indicator) {
            indicator.innerText = 'RE-SIMULATION COMPLETE';
            indicator.style.color = 'var(--buy)';
            indicator.style.opacity = '1';
            setTimeout(() => {
                indicator.innerText = 'System Synchronized';
                indicator.style.color = 'inherit';
                indicator.style.opacity = '0.6';
            }, 3000);
        }
    }

    const open = (trades.open || []).map(row => ({ ...row, row_kind: row.row_kind || 'manager_open' }));
    const closed = (trades.closed || []).map(row => ({ ...row, row_kind: row.row_kind || 'manager_closed' }));
    const signalRows = collectSessionSignalRows([...open, ...closed], trades.signals || []);
    // Keep the table in reverse-chronological order by actual accepted entry time.
    const all = [...signalRows, ...open, ...closed]
        .map((row, index) => ({ ...row, __renderIndex: index }))
        .sort((a, b) => {
            const tsDiff = getTradeRowEventTimestamp(b) - getTradeRowEventTimestamp(a);
            if (tsDiff !== 0) return tsDiff;
            const rankDiff = getTradeRowSortRank(a) - getTradeRowSortRank(b);
            if (rankDiff !== 0) return rankDiff;
            return (a.__renderIndex || 0) - (b.__renderIndex || 0);
        });
    const isPagedHistory = pageInfo.pageCount > 1 && panelTotals.total > panelTotals.visible;
    const summary = isPagedHistory && trades.summary
        ? trades.summary
        : buildSessionSummary(trades.summary || {}, open, closed, signalRows);
    const makeTradeRowKey = (row, fallbackIndex) => String(row.id || `${row.row_kind || 'row'}:${row.entry_timestamp || row.exit_timestamp || fallbackIndex}`);

    if (all.length === 0) {
        setHtml(tbody, '<tr><td colspan="15" style="text-align:center; padding:20px; opacity:0.5">No trades found. Scanner searching...</td></tr>');
    } else {
        const newKeys = new Set(all.map((t, index) => makeTradeRowKey(t, index)));
        // Remove dead rows
        Array.from(tbody.children).forEach(row => {
            const rowKey = row.getAttribute('data-key');
            if (rowKey && !newKeys.has(rowKey) && rowKey !== 'empty') {
                row.remove();
            }
        });
        // Clear empty state if it exists
        if (tbody.children.length === 1 && !tbody.children[0].hasAttribute('data-key')) {
            tbody.innerHTML = '';
        }

        all.forEach((t, index) => {
            const isSignalRow = t.row_kind === 'signal';
            const isClosedTradeRow = t.row_kind === 'manager_closed' || t.status === 'CLOSED';
            const executionStatus = String(t.execution_status || t.status || '').toUpperCase();
            const executionPending = ['ENTRY_PENDING', 'ENTRY_SUBMITTED', 'EXIT_PENDING', 'RECOVERY_REQUIRED', 'PARTIAL'].includes(executionStatus);
            const pnl = t.pnl || 0;
            const pnlClass = pnl >= 0 ? 'green' : 'red';
            const sideColor = t.direction === 'LONG' ? 'var(--buy)' : 'var(--sell)';
            
            const isOpt = t.inst_type === 'OPT' || t.instrument.includes('CE') || t.instrument.includes('PE');
            let typeLabel = 'FUT';
            let typeClass = 'fut-badge';
            if (isOpt) {
                const strike = t.atm_strike || (t.instrument.match(/\d+/) ? t.instrument.match(/\d+/)[0] : '');
                const optType = t.option_type || (t.instrument.includes('CE') ? 'CE' : 'PE');
                typeLabel = `${strike}${optType}`;
                typeClass = optType === 'CE' ? 'ce-badge' : 'pe-badge';
            }
            const dirLabel = isOpt
                ? (t.trade_side || `BUY ${t.option_type || (t.instrument.includes('PE') ? 'PE' : 'CE')}`)
                : t.direction;

            const conf = (t.confidence || 0) * 100;
            const gradeText = t.grade || '--';
            const targetPrice = t.target ? formatPrice(t.target) : '--';
            const stopPrice = t.trailing_stop ? formatPrice(t.trailing_stop) : '--';
            const exitPrice = t.exit_price ? formatPrice(t.exit_price) : '--';
            const currentPriceText = isClosedTradeRow && !(Number(t.current_price || 0) > 0)
                ? '--'
                : formatPrice(t.current_price || 0);
            
            const eDate = t.entry_date ? t.entry_date.split(' ').slice(0,2).join(' ') : '';
            const xDate = t.exit_date ? t.exit_date.split(' ').slice(0,2).join(' ') : '';
            const eTimeStr = t.entry_time ? `<div class="time-col-wrapper"><span>${t.entry_time.substring(0, 8)}</span><span style="font-size:0.65rem; opacity:0.5">${eDate}</span></div>` : '--:--:--';
            const xTimeStr = t.exit_time ? `<div class="time-col-wrapper"><span>${t.exit_time.substring(0, 8)}</span><span style="font-size:0.65rem; opacity:0.5">${xDate}</span></div>` : '--:--:--';
            const pointsText = formatTradePoints(getTradePoints(t));
            const pnlText = `₹${pnl.toLocaleString()}${pointsText ? `<span class="points-chip">(${pointsText})</span>` : ''}`;

            const rowHtml = `
                <td class="font-bold">${baseInstrumentLabel(t.base_instrument || t.instrument)}</td>
                <td><span class="${typeClass}">${typeLabel}</span></td>
                <td style="color:${sideColor}; font-weight:bold">${dirLabel}</td>
                <td>${formatPrice(t.entry_price)}</td>
                <td class="ltp-cell ${isClosedTradeRow ? '' : 'flash-update'}">${currentPriceText}</td>
                <td style="opacity:${t.exit_price ? '0.95' : '0.45'}">${exitPrice}</td>
                <td style="opacity:0.5">${targetPrice}</td>
                <td style="color:var(--orange); opacity:0.6">${stopPrice}</td>
                <td>1:${(t.rr_ratio || 1.5).toFixed(1)}</td>
                <td class="${pnlClass} font-bold pnl-points-cell">${pnlText}</td>
                <td>
                    <div class="conf-grade-cell">
                        <span class="conf-chip">${conf.toFixed(0)}%</span>
                        <span class="grade-badge grade-${gradeText.replace('+', 'p').replace(/[^a-zA-Z0-9]/g, '-')}">${gradeText}</span>
                        <span style="font-size:0.8rem; opacity:0.6">${t.timeframe || '5min'}</span>
                    </div>
                </td>
                <td><span class="exe-badge ${t.exec_type === 'A' ? 'exe-a' : 'exe-m'}">${t.exec_type || 'A'}</span></td>
                <td class="font-mono">
                    <span class="time-val">${eTimeStr}</span>
                </td>
                <td class="font-mono">
                    <span class="time-val">${xTimeStr}</span>
                </td>
                <td>
                    ${t.is_ghost ? `<div style="color:var(--orange); font-size:0.75rem; font-weight:bold; margin-bottom:4px; text-align:center;">[WATCH]</div>` : ''}
                    <div style="text-align:center;">
                    ${isSignalRow ?
                        `<span style="opacity:0.55; font-size:0.75rem">${t.status === 'EXIT_SIGNAL' ? ((t.exit_reason || '').replace(/_/g, ' ') || 'exit signal') : (t.status === 'NO_ENTRY' ? 'no entry' : 'signal')}</span>` :
                        t.status === 'ORPHANED' ?
                        `<span style="color:var(--sell); font-size:0.75rem">broker orphan</span>` :
                        t.status === 'OPEN' && executionPending ?
                        `<span style="opacity:0.75; font-size:0.75rem">${executionStatus.replace(/_/g, ' ').toLowerCase()}</span>` :
                        t.status === 'OPEN' ?
                        `<button class="exit-btn" onclick="window.closeTrade('${t.id}', ${t.current_price})">EXIT</button>` : 
                        `<span style="opacity:0.65; font-size:0.75rem">closed: ${(t.exit_reason || '').replace(/_/g, ' ')}</span>`
                    }
                    </div>
                </td>
            `;
            
            const rowClass = `${t.status === 'OPEN' || t.status === 'SIGNAL' ? 'active-row' : ''} ${isSignalRow ? 'signal-row' : ''} ${isClosedTradeRow ? 'closed-trade-row' : ''}`;
            const rowKey = makeTradeRowKey(t, index);
            
            let tr = tbody.querySelector(`tr[data-key="${rowKey}"]`);
            if (!tr) {
                tr = document.createElement('tr');
                tr.setAttribute('data-key', rowKey);
                tr.className = rowClass;
                patchRow(tr, rowHtml);
                
                if (tbody.children[index]) {
                    tbody.insertBefore(tr, tbody.children[index]);
                } else {
                    tbody.appendChild(tr);
                }
            } else {
                tr.className = rowClass;
                patchRow(tr, rowHtml);
                // Ensure order
                if (tbody.children[index] !== tr) {
                    tbody.insertBefore(tr, tbody.children[index]);
                }
            }
        });
    }

    const pnlEl = document.getElementById('dailyPnl');
    if (pnlEl) {
        const dp = summary.daily_pnl || 0;
        pnlEl.textContent = '₹' + dp.toLocaleString();
        pnlEl.className = 'h-summary-value ' + (dp >= 0 ? 'green' : 'red');
    }
    
    const futPnlEl = document.getElementById('futPnl');
    if (futPnlEl) {
        const fp = summary.fut_pnl || 0;
        futPnlEl.textContent = '₹' + fp.toLocaleString();
        futPnlEl.className = 'h-summary-value ' + (fp >= 0 ? 'green' : 'red');
    }
    
    const optPnlEl = document.getElementById('optPnl');
    if (optPnlEl) {
        const op = summary.opt_pnl || 0;
        optPnlEl.textContent = '₹' + op.toLocaleString();
        optPnlEl.className = 'h-summary-value ' + (op >= 0 ? 'green' : 'red');
    }
    setText('totalTrades', summary.total_trades || 0);
    setText('winLoss', `${summary.wins || 0}/${summary.losses || 0}`);
    setText('perfWinRate', `WR: ${(summary.win_rate || 0).toFixed(1)}%`);
    setText('perfPF', `PF: ${(summary.profit_factor || 0).toFixed(2)}`);
    setText('perfSR', `SR: ${(summary.sharpe_ratio || 0).toFixed(2)}`);
    setText('perfDD', `DD: ₹${(summary.max_drawdown || 0).toLocaleString('en-IN', {maximumFractionDigits: 0})}`);
}

function updateSignalsFeed(instruments) {
    const tbody = document.getElementById('tradesBody');
    if (!tbody) return;

    const currentRows = Array.from(tbody.querySelectorAll('tr'));
    const hasRealTradeRows = currentRows.some(row => row.cells.length > 1 && !/No (active )?trades/i.test(row.innerText));
    if (hasRealTradeRows) return;

    const byKey = new Map();
    Object.values(instruments || {}).forEach(inst => {
        (inst.trade_candidates || []).forEach(sig => {
            const key = `${sig.instrument}_${sig.timeframe}_${sig.action || 'ENTRY'}_${sig.direction}_${sig.trading_symbol || sig.atm_strike || 'FUT'}`;
            byKey.set(key, { ...sig, source: 'candidate' });
        });
    });

    const signalsArray = Array.from(byKey.values())
        .sort((a, b) => {
            if (a.source === 'candidate' && b.source !== 'candidate') return -1;
            if (a.source !== 'candidate' && b.source === 'candidate') return 1;
            return new Date(b.timestamp || 0) - new Date(a.timestamp || 0);
        });
        
    const isHistorical = state.latestData && state.latestData.mode === 'HISTORICAL';
    const signalsLimited = isHistorical ? signalsArray.slice(0, 50000) : signalsArray.slice(0, 1000);
    const signals = signalsLimited;

    if (signals.length === 0) {
        setHtml(tbody, '<tr><td colspan="15" style="text-align:center; padding:20px; opacity:0.5">No final trade signals yet — scanner is analysing filters...</td></tr>');
        return;
    }

    const newKeys = new Set(signals.map(s => s.instrument + s.timeframe + s.direction + s.timestamp));
    
    // Remove dead rows
    Array.from(tbody.children).forEach(row => {
        const rowKey = row.getAttribute('data-key');
        if (rowKey && !newKeys.has(rowKey) && rowKey !== 'empty') {
            row.remove();
        }
    });
    // Clear empty state if it exists
    if (tbody.children.length === 1 && !tbody.children[0].hasAttribute('data-key')) {
        tbody.innerHTML = '';
    }

    signals.forEach((sig, index) => {
        const isCandidate = sig.source === 'candidate';
        const isExit = sig.is_exit || sig.action === 'EXIT' || sig.status === 'EXIT SIGNAL';
        const direction = sig.direction || (sig.type === 'BUY' ? 'LONG' : 'SHORT');
        const sideColor = isExit ? 'var(--orange)' : (direction === 'LONG' ? 'var(--buy)' : 'var(--sell)');
        const conf = Math.round((sig.confidence || 0) * 100);
        const actionable = isCandidate || sig.is_actionable !== false;
        const grade = sig.grade || (actionable ? 'B' : 'C');
        const ts = sig.timestamp ? new Date(sig.timestamp) : null;
        const timeText = ts && !Number.isNaN(ts.getTime())
            ? ts.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })
            : '--:--';
        const reasons = (sig.reasons || []).join(', ');
        const rowClass = actionable ? 'active-row' : '';
        const exeLabel = isExit ? 'EXIT SIGNAL' : (sig.status || (actionable ? 'TRADE SIGNAL' : 'FILTERED'));
        const exeClass = isExit ? 'exe-m' : (actionable ? 'exe-a' : 'exe-m');
        const isOpt = sig.inst_type === 'OPT';
        const typeLabel = isOpt
            ? `${sig.atm_strike || ''}${sig.option_type || ''}`
            : (sig.inst_type === 'FUT' ? (sig.trading_symbol || 'FUT') : '--');
        const typeClass = isOpt ? (sig.option_type === 'CE' ? 'ce-badge' : 'pe-badge') : 'fut-badge';
        const dirLabel = isOpt
            ? (sig.trade_side || `BUY ${sig.option_type || ''}`.trim())
            : direction;
        const entryPrice = sig.price;
        const currentPrice = sig.current_price || entryPrice;
        const stopPrice = sig.stop;
        const targetPrice = isExit ? '--' : sig.target;
        
        const rowHtml = `
            <td class="font-bold">${baseInstrumentLabel(sig.instrument)}</td>
            <td><span class="${typeClass}">${typeLabel}</span></td>
            <td style="color:${sideColor}; font-weight:bold">${dirLabel}</td>
            <td>${entryPrice ? formatPrice(entryPrice) : '--'}</td>
            <td>${currentPrice ? formatPrice(currentPrice) : '--'}</td>
            <td style="opacity:0.45">--</td>
            <td style="opacity:0.5">${targetPrice ? formatPrice(targetPrice) : '--'}</td>
            <td style="color:var(--orange); opacity:0.6">${stopPrice ? formatPrice(stopPrice) : '--'}</td>
            <td>1:${(sig.rr || 1.5).toFixed(1)}</td>
            <td class="font-bold" style="opacity:0.5">--</td>
            <td>
                <div class="conf-grade-cell">
                    <span class="conf-chip">${conf}%</span>
                    <span class="grade-badge grade-${grade.replace('+', 'p').replace(/[^a-zA-Z0-9]/g, '-')}">${grade}</span>
                    <span style="font-size:0.8rem; opacity:0.6">${sig.timeframe || '5min'}</span>
                </div>
            </td>
            <td><span class="exe-badge ${exeClass}">${exeLabel}</span></td>
            <td class="font-mono">
                <span class="time-val">${timeText}</span>
            </td>
            <td class="font-mono">
                <span class="time-val">--:--</span>
            </td>
            <td style="font-size:0.8rem; opacity:0.6" title="${reasons}">
                ${(sig.status || '').replace(/_/g, ' ') || 'SCANNING'}
            </td>
        `;

        const rowKey = sig.instrument + sig.timeframe + sig.direction + sig.timestamp;
        
        let tr = tbody.querySelector(`tr[data-key="${rowKey}"]`);
        if (!tr) {
            tr = document.createElement('tr');
            tr.setAttribute('data-key', rowKey);
            tr.className = rowClass;
            patchRow(tr, rowHtml);
            
            if (tbody.children[index]) {
                tbody.insertBefore(tr, tbody.children[index]);
            } else {
                tbody.appendChild(tr);
            }
        } else {
            tr.className = rowClass;
            patchRow(tr, rowHtml);
            if (tbody.children[index] !== tr) {
                tbody.insertBefore(tr, tbody.children[index]);
            }
        }
    });
}

function updateChart(data) {
    if (!state.chartStreamEnabled) return;
    const chartData = data.chart || {};
    const tf = state.currentTimeframe;
    const instName = state.currentInstrument;
    const tfData = chartData[tf];

    if (!tfData || !tfData.candles || tfData.candles.length === 0) return;
    document.getElementById('mainChart')?.querySelector('.chart-disabled-placeholder')?.remove();

    // ═══ INCREMENTAL RENDERING OPTIMIZATION ═══
    const lastBar = tfData.candles[tfData.candles.length - 1];
    const chartId = `${instName}_${tf}_${data.simulation_id || 0}`;
    
    const lastBarKey = [
        lastBar.time,
        Number(lastBar.open || 0).toFixed(2),
        Number(lastBar.high || 0).toFixed(2),
        Number(lastBar.low || 0).toFixed(2),
        Number(lastBar.close || 0).toFixed(2),
        Number(lastBar.volume || 0),
        tfData.intel_history?.latest?.time || 0,
        Number(tfData.intel_history?.latest?.primary_pcr || 0).toFixed(3),
        Number(tfData.intel_history?.latest?.net_oi_change || 0),
    ].join('|');

    // Same candle timestamp can still move tick-by-tick; only skip if OHLCV is unchanged too.
    if (state.lastChartId === chartId && state.lastBarKey === lastBarKey) {
        return;
    }
    state.lastBarTime = lastBar.time;
    state.lastBarKey = lastBarKey;
    
    if (state.lastChartId !== chartId) {
        // Full Reset (Instrument/TF changed or Simulation Reset)
        const candles = tfData.candles.map(c => ({
            time: c.time, open: c.open, high: c.high, low: c.low, close: c.close,
        }));
        state.candleSeries.setData(candles);

        const volData = tfData.candles.map(c => ({
            time: c.time, value: c.volume || 0,
            color: c.close >= c.open ? 'rgba(34,197,94,0.5)' : 'rgba(239,68,68,0.5)',
        }));
        state.volumeSeries.setData(volData);

        if (tfData.trailing_stop && tfData.trailing_stop.length > 0) {
            const tsData = tfData.trailing_stop.map(t => ({
                time: t.time, value: t.value || 0
            }));
            state.tsLineSeries.setData(tsData);
        } else {
            state.tsLineSeries.setData([]);
        }
        syncIntelOverlay(tfData, true);
        
        // Clear markers on full sync to prevent ghosting
        if (tfData.markers && tfData.markers.length > 0) {
            state.candleSeries.setMarkers(tfData.markers);
        } else {
            state.candleSeries.setMarkers([]);
        }
        
        state.lastChartId = chartId;
        // console.log(`📊 Full Chart Sync: ${chartId}`);
    } else {
        // Incremental Update (The "Fast Path")
        state.candleSeries.update({
            time: lastBar.time,
            open: lastBar.open,
            high: lastBar.high,
            low: lastBar.low,
            close: lastBar.close
        });
        
        state.volumeSeries.update({
            time: lastBar.time,
            value: lastBar.volume || 0, // Null-Coalescing Fix
            color: lastBar.close >= lastBar.open ? 'rgba(34,197,94,0.5)' : 'rgba(239,68,68,0.5)'
        });

        if (tfData.trailing_stop && tfData.trailing_stop.length > 0) {
            const lastTS = tfData.trailing_stop[tfData.trailing_stop.length - 1];
            state.tsLineSeries.update({
                time: lastTS.time,
                value: lastTS.value
            });
        }
        syncIntelOverlay(tfData, false);

        // Always sync markers on incremental update if they exist
        if (tfData.markers && tfData.markers.length > 0) {
            state.candleSeries.setMarkers(tfData.markers);
        } else {
            state.candleSeries.setMarkers([]);
        }
    }

    const chartStreamSwitch = document.getElementById('chartStreamSwitch');
    if (chartStreamSwitch) {
        chartStreamSwitch.addEventListener('change', () => {
            applyChartStreamState(chartStreamSwitch.checked, { notifyBackend: true, requestSnapshot: true });
            showToast(`Chart Stream: ${chartStreamSwitch.checked ? 'ON' : 'OFF'}`, chartStreamSwitch.checked ? 'info' : 'warning');
        });
    }
}

function syncIntelOverlay(tfData, fullSync) {
    const intel = tfData.intel_history || {};
    const pcr = Array.isArray(intel.pcr) ? intel.pcr.filter(p => Number.isFinite(Number(p.value))) : [];
    const ofr = Array.isArray(intel.ofr) ? intel.ofr.filter(p => Number.isFinite(Number(p.value))) : [];
    const oiDelta = Array.isArray(intel.oi_delta) ? intel.oi_delta : [];
    const support = Array.isArray(intel.support) ? intel.support.filter(p => Number(p.value) > 0) : [];
    const resistance = Array.isArray(intel.resistance) ? intel.resistance.filter(p => Number(p.value) > 0) : [];

    if (fullSync) {
        state.pcrLineSeries?.setData(pcr);
        state.ofrLineSeries?.setData(ofr);
        state.oiDeltaSeries?.setData(oiDelta);
        state.supportWallSeries?.setData(support);
        state.resistanceWallSeries?.setData(resistance);
        return;
    }

    const updateLast = (series, points) => {
        if (!series || !points.length) return;
        const last = points[points.length - 1];
        if (last && last.time !== undefined && Number.isFinite(Number(last.value))) {
            series.update(last);
        }
    };
    updateLast(state.pcrLineSeries, pcr);
    updateLast(state.ofrLineSeries, ofr);
    updateLast(state.oiDeltaSeries, oiDelta);
    updateLast(state.supportWallSeries, support);
    updateLast(state.resistanceWallSeries, resistance);
}

function clearIntelOverlay() {
    state.pcrLineSeries?.setData([]);
    state.ofrLineSeries?.setData([]);
    state.oiDeltaSeries?.setData([]);
    state.supportWallSeries?.setData([]);
    state.resistanceWallSeries?.setData([]);
}

function setupEventListeners() {
    document.querySelectorAll('.inst-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('.inst-tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            state.currentInstrument = tab.dataset.instrument;
            const instEl = document.getElementById('chartInstrument');
            if (instEl) instEl.textContent = state.currentInstrument;
            
            window.sendCommand({
                cmd: 'subscribe_chart',
                instrument: state.currentInstrument,
                tf: state.currentTimeframe
            });

            // Explicitly Clear Series for Anti-Ghosting
            state.candleSeries.setData([]);
            state.volumeSeries.setData([]);
            state.tsLineSeries.setData([]);
            clearIntelOverlay();
            state.candleSeries.setMarkers([]);
            
            // Force Full Re-render on next updateChart call
            state.lastChartId = null;
            state.lastBarTime = null;
            state.lastBarKey = null;
            
            // Safety: Clear series immediately
            if (state.candleSeries) state.candleSeries.setData([]);
            if (state.volumeSeries) state.volumeSeries.setData([]);
            if (state.tsLineSeries) state.tsLineSeries.setData([]);
            clearIntelOverlay();
            
            if (state.latestData.instruments) {
                updateDashboard(state.latestData);
            }
            requestChartSnapshot(state.currentInstrument, state.currentTimeframe, 'instrument-tab');
        });
    });
    
    document.querySelectorAll('.tf-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('.tf-tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            state.currentTimeframe = tab.dataset.tf;
            const tfEl = document.getElementById('chartTimeframe');
            if (tfEl) tfEl.textContent = state.currentTimeframe;
            
            window.sendCommand({
                cmd: 'subscribe_chart',
                instrument: state.currentInstrument,
                tf: state.currentTimeframe
            });

            // Explicitly Clear Series for Anti-Ghosting
            state.candleSeries.setData([]);
            state.volumeSeries.setData([]);
            state.tsLineSeries.setData([]);
            clearIntelOverlay();
            state.candleSeries.setMarkers([]);

            // Force Full Re-render
            state.lastChartId = null;
            state.lastBarTime = null;
            state.lastBarKey = null;

            // Safety: Clear series immediately
            if (state.candleSeries) state.candleSeries.setData([]);
            if (state.volumeSeries) state.volumeSeries.setData([]);
            if (state.tsLineSeries) state.tsLineSeries.setData([]);
            clearIntelOverlay();

            if (state.latestData.instruments) {
                updateDashboard(state.latestData);
            }
            requestChartSnapshot(state.currentInstrument, state.currentTimeframe, 'timeframe-tab');
        });
    });

    // ═══ 3-WAY MODE CONTROL ═══
    document.querySelectorAll('.mode-opt').forEach(btn => {
        btn.addEventListener('click', () => {
            const mode = btn.dataset.mode;
            let confirmRealMode = false;
            let realModeVerification = '';
            if (mode === 'REAL' && state.latestData?.mode !== 'REAL') {
                const confirmed = confirm('REAL mode can place live orders. Continue?');
                if (!confirmed) return;
                confirmRealMode = true;
                realModeVerification = 'YES';
            }
            window.sendCommand({
                cmd: 'configure',
                mode,
                confirm_real_mode: confirmRealMode,
                real_mode_verification: realModeVerification,
                reset: mode === 'HISTORICAL'
            });
            showToast(`Mode change requested: ${mode}`, 'info');
            return;
            
            // ─── Institutional Safety Confirmation for Real Live ───
            if (mode === 'REAL' && state.latestData?.mode !== 'REAL') {
                const confirmed = confirm("⚠️ ATTENTION: You are switching to REAL mode.\n\nOrders will be placed with REAL MONEY on your AngelOne account.\n\nDo you want to proceed?");
                if (!confirmed) return;
                confirmRealMode = true;
            }

            document.querySelectorAll('.mode-opt').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            
            window.sendCommand({ 
                cmd: 'configure', 
                mode: mode,
                confirm_real_mode: confirmRealMode,
                reset: (mode === 'HISTORICAL') // Full reset for historical analysis
            });
            if (state.latestData) {
                state.latestData.mode = mode;
            }
            showToast(`Engine Mode: ${mode === 'REAL' ? 'LIVE TERMINAL' : 'SIGNAL ANALYSIS'}`, mode === 'REAL' ? 'sell' : 'info');
        });
    });

    const settingsBtn = document.getElementById('settingsBtn');
    if (settingsBtn) {
        settingsBtn.addEventListener('click', () => {
            document.getElementById('settingsModal').style.display = 'flex';
        });
    }

    
    const closeSettings = document.getElementById('closeSettings');
    if (closeSettings) {
        closeSettings.addEventListener('click', () => {
            document.getElementById('settingsModal').style.display = 'none';
        });
    }
    
    const saveBtn = document.getElementById('saveSettings');
    if (saveBtn) saveBtn.addEventListener('click', saveSettings);
    
    const resetBtn = document.getElementById('resetCacheBtn');
    if (resetBtn) resetBtn.addEventListener('click', resetCache);

    // ═══ FYERS AUTH MODAL LISTENERS ═══
    const closeFyersAuth = document.getElementById('closeFyersAuth');
    if (closeFyersAuth) {
        closeFyersAuth.addEventListener('click', () => {
            document.getElementById('fyersAuthModal').style.display = 'none';
        });
    }

    const submitFyersAuth = document.getElementById('submitFyersAuth');
    if (submitFyersAuth) {
        submitFyersAuth.addEventListener('click', () => {
            const input = document.getElementById('fyersAuthInput');
            if (input && input.value) {
                submitFyersCode(input.value);
                document.getElementById('fyersAuthModal').style.display = 'none';
                input.value = ''; // Clear for next time
            } else {
                showToast('Please paste the URL first', 'warning');
            }
        });
    }

    const retryFyersLogin = document.getElementById('retryFyersLogin');
    if (retryFyersLogin) {
        retryFyersLogin.addEventListener('click', () => {
            document.getElementById('fyersAuthModal').style.display = 'none';
            startFyersAuthFlow(); // Re-trigger flow
        });
    }

    // ═══ AUTO MODE SWITCH (Top Bar) ═══
    const fyersBadge = document.getElementById('gw_fyers');
    if (fyersBadge) {
        fyersBadge.onclick = startFyersAuthFlow;
        fyersBadge.title = 'Fyers Data Status. Click to refresh login.';
    }

    const autoSwitch = document.getElementById('autoModeSwitch');
    if (autoSwitch) {
        autoSwitch.addEventListener('change', () => {
            const isChecked = autoSwitch.checked;
            let realModeVerification = '';
            let confirmRealMode = false;
            if (isChecked && state.latestData?.mode === 'REAL') {
                const confirmed = confirm('REAL AUTO MODE will allow live broker orders. Enable auto-execution?');
                if (!confirmed) {
                    autoSwitch.checked = false;
                    showToast('REAL Auto Mode not enabled.', 'warning');
                    return;
                }
                realModeVerification = 'YES';
                confirmRealMode = true;
            }
            window.sendCommand({ 
                cmd: 'configure', 
                auto_mode: isChecked,
                confirm_real_mode: confirmRealMode,
                real_mode_verification: realModeVerification
            });
            if (state.latestData && state.latestData.config) {
                state.latestData.config.auto_mode = isChecked;
            }
            showToast(`Auto Mode: ${isChecked ? 'ENABLED' : 'DISABLED'}`, 'info');
        });
    }

    // ═══ LOG FILTERS ═══
    const tfPolicy = document.getElementById('topTimeframePolicy');
    if (tfPolicy) {
        tfPolicy.addEventListener('change', () => {
            const policy = tfPolicy.value || 'INCLUDE_5MIN';
            window.sendCommand({
                cmd: 'configure',
                timeframe_entry_policy: policy
            });
            if (state.latestData && state.latestData.config) {
                state.latestData.config.ut_timeframe_entry_policy = policy;
            }
            showToast(`TF Policy: ${policy === 'INCLUDE_5MIN' ? '5M+15M' : '15M MAIN'}`, 'info');
        });
    }

    document.querySelectorAll('.log-filter').forEach(btn => {
        btn.addEventListener('click', (e) => {
            document.querySelectorAll('.log-filter').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            state.logFilter = btn.dataset.type;
            renderLogs();
        });
    });

    // ═══ KILL ALL BUTTON ═══
    const killAllBtn = document.getElementById('killAllBtn');
    if (killAllBtn) {
        killAllBtn.addEventListener('click', () => {
            if (confirm("⚠️ Are you sure you want to kill ALL open trades?")) {
                window.sendCommand({ cmd: 'kill_all' });
                showToast("Kill All signal sent", "warning");
            }
        });
    }
}

function saveSettings() {
    const capital_fut = parseFloat(document.getElementById('settCapitalFut')?.value) || 250000;
    const risk_fut_pct = parseFloat(document.getElementById('settRiskFut')?.value) || 2.0;
    const capital_opt = parseFloat(document.getElementById('settCapitalOpt')?.value) || 100000;
    const risk_opt_pct = parseFloat(document.getElementById('settRiskOpt')?.value) || 8.0;
    const activeIndices = [];
    if (document.getElementById('checkNifty')?.checked) activeIndices.push('NIFTY');
    if (document.getElementById('checkBN')?.checked) activeIndices.push('BANKNIFTY');
    if (document.getElementById('checkSensex')?.checked) activeIndices.push('SENSEX');
    if (document.getElementById('checkMidcap')?.checked) activeIndices.push('MIDCPNIFTY');

    if (activeIndices.length === 0) {
        showToast("At least one index must be selected!", "error");
        return;
    }

    const wantsAuto = document.getElementById('settAutoMode')?.checked ?? document.getElementById('autoModeSwitch')?.checked;
    let realModeVerification = '';
    let confirmRealMode = false;
    if (wantsAuto && state.latestData?.mode === 'REAL') {
        const confirmed = confirm('REAL AUTO MODE will allow live broker orders. Save this setting?');
        if (!confirmed) {
            showToast('Settings not saved: REAL Auto Mode requires confirmation.', 'warning');
            return;
        }
        realModeVerification = 'YES';
        confirmRealMode = true;
    }

    const config = {
        cmd: 'configure',
        capital_total: parseFloat(document.getElementById('settCapitalTotal')?.value) || 500000,
        capital_fut: parseFloat(document.getElementById('settCapitalFut')?.value) || 250000,
        risk_fut_pct: parseFloat(document.getElementById('settRiskFut')?.value) || 2.0,
        capital_opt: parseFloat(document.getElementById('settCapitalOpt')?.value) || 100000,
        risk_opt_pct: parseFloat(document.getElementById('settRiskOpt')?.value) || 8.0,
        futures_sl_pct: parseFloat(document.getElementById('settFutureSL')?.value) || 0.30,
        options_sl_pct: parseFloat(document.getElementById('settOptionSL')?.value) || 15,
        fut_cost: parseFloat(document.getElementById('settFutCost')?.value) || 200,
        opt_cost: parseFloat(document.getElementById('settOptCost')?.value) || 80,
        backtest_days: parseInt(document.getElementById('settBacktestDays')?.value) || 1,
        auto_mode: wantsAuto,
        confirm_real_mode: confirmRealMode,
        real_mode_verification: realModeVerification,
        inst_pref: document.getElementById('settInstPref')?.value || 'AUTO',
        strike_selection: document.getElementById('settStrikeSelection')?.value || 'BOTH',
        grade_preference: document.getElementById('settGradePreference')?.value || 'auto',
        ut_preset: document.getElementById('settUtPreset')?.value || 'AGGRESSIVE',
        timeframe_entry_policy: document.getElementById('topTimeframePolicy')?.value || 'INCLUDE_5MIN',
        max_trades_per_index: parseInt(document.getElementById('settMaxTrades')?.value) || 5,
        max_consecutive_losses: parseInt(document.getElementById('settMaxLosses')?.value) || 3,
        index_cooldown_minutes: Math.max(0, parseFloat(document.getElementById('settIndexCooldown')?.value) || 0),
        active_indices: activeIndices,
        lots: {
            NIFTY: parseInt(document.getElementById('settLotsNifty')?.value) || 1,
            BANKNIFTY: parseInt(document.getElementById('settLotsBN')?.value) || 1,
            SENSEX: parseInt(document.getElementById('settLotsSensex')?.value) || 1,
            MIDCPNIFTY: parseInt(document.getElementById('settLotsMidcap')?.value) || 1,
        },
        lots_fut: {
            NIFTY: parseInt(document.getElementById('settLotsFutNifty')?.value) || 1,
            BANKNIFTY: parseInt(document.getElementById('settLotsFutBN')?.value) || 1,
            SENSEX: parseInt(document.getElementById('settLotsFutSensex')?.value) || 1,
            MIDCPNIFTY: parseInt(document.getElementById('settLotsFutMidcap')?.value) || 1,
        }
    };
    window.sendCommand(config);
    const modal = document.getElementById('settingsModal');
    if (modal) modal.style.display = 'none';
}

function resetCache() {
    if (!confirm("Are you sure you want to reset cache and re-sync all data? This will pause scanning for a few minutes.")) {
        return;
    }
    
    authFetch('/api/reset_cache', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.status === 'ok') {
            showToast("Cache reset initiated! System is re-syncing data...", "success");
            document.getElementById('settingsModal').style.display = 'none';
        } else {
            showToast("Failed to reset cache: " + data.message, "error");
        }
    })
    .catch(error => {
        console.error('Error resetting cache:', error);
        showToast("Error resetting cache!", "error");
    });
}

function updateMarketTime() {
    const now = new Date();
    const options = { timeZone: 'Asia/Kolkata', hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' };
    const timeStr = now.toLocaleTimeString('en-IN', options);
    const dateStr = now.toLocaleDateString('en-IN', { timeZone: 'Asia/Kolkata', day: '2-digit', month: 'short', year: 'numeric' });
    setText('marketTime', timeStr);
    setText('marketDate', dateStr);

    if (state.latestData) {
        const heartbeat = state.latestData.dashboard_heartbeat || {};
        const heartbeatTime = heartbeat.timestamp ? new Date(heartbeat.timestamp).getTime() : NaN;
        const heartbeatAgeMs = Number.isFinite(heartbeatTime) ? Math.max(0, Date.now() - heartbeatTime) : null;
        const scanAgeMs = Number.isFinite(Number(heartbeat.scan_age_ms)) ? Number(heartbeat.scan_age_ms) : null;
        const wsAgeMs = state.lastWsUpdateAt ? Math.max(0, Date.now() - state.lastWsUpdateAt) : null;
        const liveAgeMs = heartbeatAgeMs !== null ? heartbeatAgeMs : wsAgeMs;
        const liveSuffix = heartbeatAgeMs !== null
            ? (scanAgeMs !== null ? ` / scan ${(scanAgeMs / 1000).toFixed(1)}s` : '')
            : ' / ws';
        const liveText = liveAgeMs === null ? '--' : `${(liveAgeMs / 1000).toFixed(1)}s${liveSuffix}`;
        setText('liveRefreshAge', liveText);
    }
}

function closeTrade(tradeId, price) { window.sendCommand({ cmd: 'close_trade', trade_id: tradeId, price }); }
function setText(id, val) { 
    const el = document.getElementById(id); 
    if (el) el.textContent = val; 
}
function formatPrice(p) { return (!p || p === 0 || isNaN(p)) ? '--' : p.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }
function formatMoney(value, compact = false) {
    const num = Number(value || 0);
    if (!Number.isFinite(num)) return '₹0';
    const sign = num < 0 ? '-' : '';
    const abs = Math.abs(num);
    if (compact && abs >= 100000) return `${sign}₹${(abs / 100000).toFixed(1)}L`;
    if (compact && abs >= 1000) return `${sign}₹${(abs / 1000).toFixed(1)}K`;
    return `${sign}₹${abs.toLocaleString('en-IN', { maximumFractionDigits: 0 })}`;
}
function formatPercent(value) {
    const num = Number(value || 0);
    return `${Number.isFinite(num) ? num.toFixed(1) : '0.0'}%`;
}
function baseInstrumentLabel(value = '') {
    const text = String(value || '').toUpperCase();
    if (text.startsWith('BANKNIFTY')) return 'BANKNIFTY';
    if (text.startsWith('SENSEX')) return 'SENSEX';
    if (text.startsWith('MIDCPNIFTY') || text.startsWith('MIDCAPNIFTY')) return 'MIDCPNIFTY';
    if (text.startsWith('NIFTY')) return 'NIFTY';
    return String(value || '--').split(' ')[0] || '--';
}

function drawEquityCurve(points = []) {
    const canvas = document.getElementById('equityCurveCanvas');
    if (!canvas) return;
    const wrap = canvas.parentElement;
    const width = Math.max(320, wrap?.clientWidth || canvas.clientWidth || 640);
    const height = Math.max(180, wrap?.clientHeight || canvas.clientHeight || 260);
    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.floor(width * dpr);
    canvas.height = Math.floor(height * dpr);
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);

    const pad = { left: 44, right: 14, top: 16, bottom: 28 };
    const plotW = width - pad.left - pad.right;
    const plotH = height - pad.top - pad.bottom;
    ctx.strokeStyle = 'rgba(148, 163, 184, 0.12)';
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
        const y = pad.top + (plotH * i / 4);
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(width - pad.right, y);
        ctx.stroke();
    }
    if (!points.length) {
        ctx.fillStyle = 'rgba(226, 232, 240, 0.55)';
        ctx.font = '12px JetBrains Mono, monospace';
        ctx.fillText('No closed trade equity yet', pad.left, pad.top + 24);
        return;
    }

    const equityVals = points.map(p => Number(p.equity || 0));
    const ddVals = points.map(p => Number(p.drawdown || 0));
    const minEq = Math.min(0, ...equityVals);
    const maxEq = Math.max(1, ...equityVals);
    const maxDd = Math.max(1, ...ddVals);
    const xFor = idx => pad.left + (points.length === 1 ? plotW : (plotW * idx / (points.length - 1)));
    const yEq = value => pad.top + plotH - ((value - minEq) / Math.max(1, maxEq - minEq)) * plotH;
    const yDd = value => pad.top + plotH - (value / maxDd) * Math.min(plotH * 0.42, plotH);

    const zeroY = yEq(0);
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.16)';
    ctx.beginPath();
    ctx.moveTo(pad.left, zeroY);
    ctx.lineTo(width - pad.right, zeroY);
    ctx.stroke();

    ctx.beginPath();
    points.forEach((p, idx) => {
        const x = xFor(idx);
        const y = yEq(Number(p.equity || 0));
        if (idx === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = '#22c55e';
    ctx.lineWidth = 2.2;
    ctx.shadowColor = 'rgba(34, 197, 94, 0.35)';
    ctx.shadowBlur = 8;
    ctx.stroke();
    ctx.shadowBlur = 0;

    ctx.beginPath();
    points.forEach((p, idx) => {
        const x = xFor(idx);
        const y = yDd(Number(p.drawdown || 0));
        if (idx === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = '#ef4444';
    ctx.lineWidth = 1.5;
    ctx.setLineDash([4, 4]);
    ctx.stroke();
    ctx.setLineDash([]);

    ctx.fillStyle = 'rgba(226, 232, 240, 0.65)';
    ctx.font = '10px JetBrains Mono, monospace';
    ctx.fillText(formatMoney(maxEq, true), 4, pad.top + 4);
    ctx.fillText(formatMoney(minEq, true), 4, pad.top + plotH);
    ctx.fillStyle = 'rgba(239, 68, 68, 0.8)';
    ctx.fillText(`DD ${formatMoney(maxDd, true)}`, pad.left, height - 8);
}

function updateEquityReplacement(trades = {}) {
    const points = Array.isArray(trades.equity_curve) ? trades.equity_curve : [];
    const monetary = trades.monetary || {};
    const headline = monetary.headline || {};
    const key = JSON.stringify({
        n: points.length,
        last: points[points.length - 1],
        dd: headline.current_drawdown,
        stream: state.chartStreamEnabled,
    });
    setText('equityNetPnl', `Net ${formatMoney(points.length ? points[points.length - 1].equity : 0, true)}`);
    setText('equityCurrentDd', `DD ${formatMoney(headline.current_drawdown || 0, true)} / ${formatPercent(headline.current_drawdown_pct || 0)}`);
    setText('equityPointCount', `${points.length} equity point${points.length === 1 ? '' : 's'}`);
    setText('equityLastTrade', points.length ? `Last ${points[points.length - 1].trade_id || '--'}` : 'Last trade --');
    if (key === state.lastEquityRenderKey) return;
    state.lastEquityRenderKey = key;
    drawEquityCurve(points);
}

function updatePnlPanel(trades = {}) {
    const monetary = trades.monetary || {};
    const headline = monetary.headline || trades.summary || {};
    const capital = monetary.capital || {};
    const period = monetary.period_pnl || {};
    const summary = trades.summary || {};

    setText('pnlWinRate', formatPercent(headline.win_rate || 0));
    setText('pnlWinMeta', `${summary.wins || 0}W / ${summary.losses || 0}L`);
    const totalTrades = (summary.wins || 0) + (summary.losses || 0);
    setText('pnlProfitFactor', totalTrades > 0 ? Number(headline.profit_factor || 0).toFixed(2) : "N/A");
    setText('pnlSharpe', `Sharpe ${Number(headline.sharpe_ratio || 0).toFixed(2)}`);
    setText('pnlDrawdown', `DD ${formatMoney(headline.max_drawdown || 0, true)} (${formatPercent(headline.max_drawdown_pct || 0)})`);
    setText('pnlCurrentDd', `Current ${formatMoney(headline.current_drawdown || 0, true)} / ${formatPercent(headline.current_drawdown_pct || 0)}`);
    setText('pnlCapital', formatMoney(capital.configured_demat_capital || 0, true));
    setText('pnlAllocation', `F ${formatMoney(capital.futures_allocation || 0, true)} / O ${formatMoney(capital.options_allocation || 0, true)} / Live ${formatMoney(capital.active_trade_allocation || 0, true)}`);
    setText('pnlDaily', formatMoney(period.daily || summary.daily_pnl || 0, true));
    setText('pnlWeekly', formatMoney(period.weekly || 0, true));
    setText('pnlMonthly', formatMoney(period.monthly || 0, true));
    setText('pnlAvgHold', `${Number(headline.avg_hold_minutes || 0).toFixed(1)}m`);

    const breakdowns = monetary.breakdowns || {};
    const makeRows = (title, obj) => {
        const rows = Object.entries(obj || {}).slice(0, 4);
        if (!rows.length) return `<div class="pnl-list-row"><span>${title}</span><small>--</small></div>`;
        return rows.map(([name, value]) => `
            <div class="pnl-list-row">
                <span>${title}: ${name}</span>
                <small>${formatMoney(value.pnl || 0, true)} / ${formatPercent(value.win_rate || 0)}</small>
            </div>
        `).join('');
    };
    const breakdownEl = document.getElementById('pnlBreakdowns');
    if (breakdownEl) {
        breakdownEl.innerHTML = [
            makeRows('IDX', breakdowns.index),
            makeRows('TYPE', breakdowns.instrument_type),
            makeRows('TF', breakdowns.timeframe),
        ].join('');
    }

    const tradeLine = (label, row) => row && row.instrument
        ? `<div class="pnl-list-row"><span>${label}: ${row.instrument} ${row.timeframe || ''}</span><small>${formatMoney(row.pnl || 0, true)}</small></div>`
        : `<div class="pnl-list-row"><span>${label}</span><small>--</small></div>`;
    const extremesEl = document.getElementById('pnlExtremes');
    if (extremesEl) {
        extremesEl.innerHTML = [
            tradeLine('Largest Winner', monetary.largest_winner),
            tradeLine('Largest Loser', monetary.largest_loser),
        ].join('');
    }

    const liveEl = document.getElementById('pnlLivePositions');
    if (liveEl) {
        const live = monetary.live_positions || [];
        liveEl.innerHTML = live.length ? live.slice(0, 4).map(row => `
            <div class="pnl-live-row">
                <b>${row.instrument} ${row.direction || ''} ${row.timeframe || ''}</b>
                <small>P&L ${formatMoney(row.pnl || 0, true)} / Alloc ${formatMoney(row.allocation || 0, true)} / SL ${row.stop || '--'} / TGT ${row.target || '--'}</small>
            </div>
        `).join('') : '<div class="pnl-live-row"><small>No current live/open position.</small></div>';
    }
}

function showToast(message, type = 'info') {
    let container = document.getElementById('toastContainer');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toastContainer';
        container.className = 'toast-container';
        document.body.appendChild(container);
    }
    
    const toast = document.createElement('div');
    toast.className = 'toast ' + type;
    
    let icon = '🔔';
    const typeLower = type.toLowerCase();
    if (typeLower.includes('buy') || typeLower.includes('long')) icon = '📈';
    else if (typeLower.includes('sell') || typeLower.includes('short')) icon = '📉';
    else if (typeLower.includes('error') || typeLower.includes('alert')) icon = '⚠️';
    
    toast.innerHTML = `
        <div class="toast-content">
            <div class="toast-icon">${icon}</div>
            <div class="toast-body">${message}</div>
        </div>
        <div class="toast-progress"><div class="toast-bar"></div></div>
    `;
    
    container.appendChild(toast);
    
    const dismiss = () => {
        toast.style.animation = 'fadeOutToast 0.3s forwards';
        setTimeout(() => toast.remove(), 300);
    };
    
    toast.addEventListener('click', dismiss);
    setTimeout(() => { if(toast.parentElement) dismiss(); }, 5000);
}

async function completeFyersRedirectIfPresent() {
    const params = new URLSearchParams(window.location.search);
    const authCode = params.get('auth_code') || params.get('code');
    if (!authCode) return;

    try {
        showToast('Completing Fyers login...', 'info');
        await submitFyersCode(authCode);
    } finally {
        window.history.replaceState({}, document.title, window.location.pathname);
    }
}

async function startFyersAuthFlow() {
    let loginWindow = null;
    try {
        // Open synchronously from the click event, then replace the URL after
        // the backend generates it. This avoids popup blocking while still
        // letting the modal show errors and a reusable login link.
        loginWindow = window.open('about:blank', '_blank');
        if (loginWindow) {
            loginWindow.opener = null;
            loginWindow.document.write('<!doctype html><title>Fyers Login</title><body style="font-family:sans-serif;background:#070a0f;color:#e7edf6;padding:24px">Preparing Fyers login...</body>');
        }
        const res = await authFetch('/api/fyers_auth', { cache: 'no-store' });
        const data = await res.json();
        if (data.status !== 'ok' || !data.auth_url) {
            if (loginWindow && !loginWindow.closed) loginWindow.close();
            showToast(data.message || 'Unable to start Fyers login', 'error');
            return;
        }

        const loginLink = document.getElementById('openFyersLogin');
        if (loginLink) {
            loginLink.href = data.auth_url;
            loginLink.classList.remove('disabled');
        }

        if (loginWindow && !loginWindow.closed) {
            loginWindow.location.href = data.auth_url;
        } else {
            showToast('Popup blocked. Use the Open Login button in the Fyers modal.', 'warning');
        }
        
        // Show dedicated UI Modal
        const modal = document.getElementById('fyersAuthModal');
        if (modal) {
            modal.style.display = 'flex';
            const input = document.getElementById('fyersAuthInput');
            if (input) input.focus();
        }
    } catch (err) {
        if (loginWindow && !loginWindow.closed) loginWindow.close();
        showToast('Fyers login flow failed: ' + err.message, 'error');
    }
}

async function submitFyersCode(authCode) {
    try {
        const submit = await authFetch('/api/fyers_auth', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ auth_code: authCode })
        });
        const result = await submit.json();
        if (result.status === 'ok') {
            showToast('Fyers token updated. Live Fyers data restored.', 'success');
            window.sendCommand({ cmd: 'get_state' });
        } else {
            showToast(result.message || 'Fyers token update failed', 'error');
        }
    } catch (err) {
        showToast('Submission failed: ' + err.message, 'error');
    }
}

function updateGatewayStatus(status) {
    const gateways = {
        'gw_angel': status.angel,
        'gw_fyers': status.fyers,
        'gw_yahoo': status.yahoo
    };
    
    for (const [id, isOnline] of Object.entries(gateways)) {
        const el = document.getElementById(id);
        if (el) {
            if (isOnline) {
                el.classList.add('online');
                el.classList.remove('offline');
            } else {
                el.classList.add('offline');
                el.classList.remove('online');
            }
        }
    }
    const fyersEl = document.getElementById('gw_fyers');
    if (fyersEl) {
        const authRequired = !!status.fyers_auth_required;
        const refreshDisabled = !!status.fyers_refresh_disabled;
        fyersEl.classList.toggle('auth-required', authRequired || refreshDisabled || !status.fyers);
        fyersEl.title = authRequired
            ? `Fyers login required: ${status.fyers_auth_reason || 'manual authorization needed'}. Click to reconnect.`
            : (refreshDisabled ? 'Fyers refresh requires manual login. Click to reconnect.' : 'Fyers Data Status. Click to refresh login.');
        fyersEl.onclick = startFyersAuthFlow;
        if (authRequired && !state.fyersAuthToastShown) {
            state.fyersAuthToastShown = true;
            showToast('Fyers login required. Click FYERS badge to reconnect.', 'warning');
        }
        if (!authRequired) {
            state.fyersAuthToastShown = false;
        }
    }
}

function updateDiagnosticsPanel(diagnostics) {
    diagnostics = diagnostics || {};
    const rejects = diagnostics.rejects || {};
    const stabilization = diagnostics.stabilization || {};
    const timeouts = diagnostics.timeouts || {};
    const sources = diagnostics.sources || {};
    const latency = diagnostics.latency || {};
    const repaintGuard = diagnostics.repaint_guard || {};
    const exitReasons = diagnostics.exit_reasons || {};
    const dataFreshness = diagnostics.data_freshness || {};
    const sessionStats = diagnostics.session_stats || {};
    const activePeak = diagnostics.active_peak || {};
    const simulation = diagnostics.simulation || {};
    const instrumentSelection = diagnostics.instrument_selection || {};
    updateHardwarePanel(diagnostics.system_metrics || {});
    const isHistorical = (diagnostics.mode || state.latestData?.mode) === 'HISTORICAL';

    const topReject = Object.entries(rejects).sort((a, b) => b[1] - a[1])[0];

    // Sources: A:ON F:ON Y:ON
    const yahooState = sources.yahoo_mode === 'emergency_used' ? 'EMERG' : 'STBY';
    const sourceText = [
        `A:${sources.angel ? 'ON' : 'OFF'}`,
        `F:${sources.fyers ? 'ON' : 'OFF'}`,
        `Y:${sources.yahoo ? yahooState : 'OFF'}`,
    ].join(' ');
    setText('diagSources', sourceText || '--');

    // Data Age: Show oldest instrument age in seconds
    const ages = Object.entries(dataFreshness);
    if (isHistorical) {
        setText('diagDataAge', 'Cached history');
    } else if (ages.length > 0) {
        const ageStrs = ages.map(([inst, age]) => {
            const label = inst.replace('MIDCPNIFTY', 'MC').replace('BANKNIFTY', 'BN').replace('SENSEX', 'SX').replace('NIFTY', 'NF');
            if (age < 0) return `${label}:--`;
            if (age < 60) return `${label}:${Math.round(age)}s`;
            return `${label}:${Math.round(age / 60)}m`;
        });
        setText('diagDataAge', ageStrs.join(' '));
    } else {
        setText('diagDataAge', '--');
    }
    // Color the data age card if any instrument is stale (>60s)
    const dataAgeEl = document.getElementById('diagDataAge');
    if (dataAgeEl) {
        const maxAge = isHistorical ? 0 : Math.max(...Object.values(dataFreshness).filter(v => v >= 0), 0);
        dataAgeEl.style.color = maxAge > 60 ? 'var(--red)' : maxAge > 30 ? 'var(--orange)' : '';
    }

    // Scan Speed: Sync exactly with top-right panel
    const topLatency = document.getElementById('scanLatency') ? document.getElementById('scanLatency').innerText : '--ms';
    const topCount = document.getElementById('scanCount') ? document.getElementById('scanCount').innerText : '0';
    setText('diagScanSpeed', `#${topCount} / ${topLatency}`);

    // Session Stats: entries/exits
    const entries = sessionStats.entries || 0;
    const exits = sessionStats.exits || 0;
    setText('diagSessionLabel', isHistorical ? 'Pre-Gate Select' : 'Session');
    setText(
        'diagSessionStats',
        isHistorical
            ? `${instrumentSelection.OPT || 0} OPT / ${instrumentSelection.FUT || 0} FUT`
            : `${entries} sig / ${exits} exit`
    );

    // Repaint Guard: checked/aborted/passed
    const rgChecked = repaintGuard.checked || 0;
    const rgAborted = repaintGuard.aborted || 0;
    const rgPassed = repaintGuard.passed || 0;
    const rgEl = document.getElementById('diagRepaintGuard');
    if (rgEl) {
        rgEl.textContent = rgChecked === 0 ? 'No checks' : `${rgAborted} abort / ${rgPassed} pass`;
        rgEl.style.color = rgAborted > 0 ? 'var(--orange)' : '';
    }

    // Profit Guard: count of profit-saving vs raw stop exits
    const profitSaves = (exitReasons.SMART_PROFIT_LOCK || 0) +
                        (exitReasons.MAJOR_WIN_GUARD || 0) +
                        (exitReasons.LOW_GAIN_PROTECT || 0);
    const rawStops = exitReasons.STOP_HIT || 0;
    const targets = exitReasons.TARGET_HIT || 0;
    const repaintAborts = exitReasons.REPAINT_ABORT || 0;
    const pgEl = document.getElementById('diagProfitGuard');
    if (pgEl) {
        const parts = [];
        if (profitSaves > 0) parts.push(`${profitSaves} lock`);
        if (targets > 0) parts.push(`${targets} tgt`);
        if (rawStops > 0) parts.push(`${rawStops} sl`);
        if (repaintAborts > 0) parts.push(`${repaintAborts} rp`);
        pgEl.textContent = parts.length > 0 ? parts.join(' / ') : 'No exits';
        pgEl.style.color = profitSaves > rawStops ? 'var(--green)' : rawStops > 0 ? 'var(--orange)' : '';
    }

    // Active Peak: show per-instrument peak vs current PnL
    const peakEntries = Object.entries(activePeak);
    const apEl = document.getElementById('diagActivePeak');
    if (apEl) {
        if (peakEntries.length === 0) {
            apEl.textContent = 'No active';
            apEl.style.color = '';
        } else {
            const strs = peakEntries.map(([inst, data]) => {
                const label = inst.replace('MIDCPNIFTY', 'MC').replace('BANKNIFTY', 'BN').replace('SENSEX', 'SX').replace('NIFTY', 'NF');
                return `${label}:${data.current}/${data.peak}`;
            });
            apEl.textContent = strs.join(' ');
            // Red if current is way below peak
            const worstRatio = Math.min(...peakEntries.map(([, d]) => d.peak > 0 ? d.current / d.peak : 1));
            apEl.style.color = worstRatio < 0.5 ? 'var(--red)' : worstRatio < 0.75 ? 'var(--orange)' : 'var(--green)';
        }
    }

    // Top Reject
    const windowLabel = simulation.backtest_days ? `, ${simulation.backtest_days}d` : '';
    setText('diagTopReject', topReject ? `${topReject[0]} (${topReject[1]}${windowLabel})` : '--');

    setText('diagMode', diagnostics.mode || state.latestData?.mode || '--');

    // Reject List
    const list = document.getElementById('diagRejectList');
    if (!list) return;
    const rows = Object.entries(rejects)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 6)
        .map(([reason, count]) => `<div><span>${reason}</span><b>${count}</b></div>`);
    list.innerHTML = rows.length ? rows.join('') : 'No rejected-signal counters yet.';
}

function updateAnalyticsStrip(trades) {
    const strip = document.getElementById('analyticsStrip');
    if (!strip || !trades || !trades.analytics) return;
    const a = trades.analytics;
    const bestTf = a.by_timeframe ? Object.entries(a.by_timeframe).sort((x,y) => y[1].pnl - x[1].pnl)[0] : null;
    const bestGrade = a.by_grade ? Object.entries(a.by_grade).sort((x,y) => y[1].pnl - x[1].pnl)[0] : null;
    const bestIdx = a.by_instrument ? Object.entries(a.by_instrument).sort((x,y) => y[1].pnl - x[1].pnl)[0] : null;
    const bestType = a.by_type ? Object.entries(a.by_type).sort((x,y) => y[1].pnl - x[1].pnl)[0] : null;
    const topExit = a.by_exit_reason ? Object.entries(a.by_exit_reason).sort((x,y) => y[1].count - x[1].count)[0] : null;
    
    const spans = strip.querySelectorAll('span');
    if (spans[0] && bestTf) spans[0].textContent = `TF: ${bestTf[0]} (₹${bestTf[1].pnl.toLocaleString()})`;
    if (spans[1] && bestGrade) spans[1].textContent = `Grade: ${bestGrade[0]} (₹${bestGrade[1].pnl.toLocaleString()})`;
    if (spans[2] && bestIdx) spans[2].textContent = `Index: ${bestIdx[0]} (₹${bestIdx[1].pnl.toLocaleString()})`;
    if (spans[3] && bestType) spans[3].textContent = `Type: ${bestType[0]} (₹${bestType[1].pnl.toLocaleString()})`;
    if (spans[4] && topExit) spans[4].textContent = `Exit: ${topExit[0].replace(/_/g,' ')} (${topExit[1].count})`;
}

window.closeTrade = closeTrade;

document.addEventListener('DOMContentLoaded', () => {
    const btn = document.getElementById('analysingSignal');
    if (btn) {
        btn.addEventListener('click', () => {
            btn.classList.remove('state-idle', 'state-offline', 'state-analysing');
            btn.classList.add('state-analysing');
            const txt = document.getElementById('analysisText');
            if (txt) txt.textContent = 'FORCING...';
            // Trigger animation restart
            const dot = document.getElementById('analysisDot');
            if (dot) {
                dot.style.animation = 'none';
                dot.offsetHeight; /* trigger reflow */
                dot.style.animation = null; 
            }
            authFetch('/api/recalibrate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
            }).catch(console.error);
        });
    }
});
