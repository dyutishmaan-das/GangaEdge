/* ==========================================================================
   GangaEdge Professional Dashboard Application Logic
   ========================================================================== */

// --- DUAL-NODE & EXTRA SENSOR CONFIGURATION ---
const CONFIG = {
    projectName: 'GangaEdge',
    projectTitle: 'Ganga Canal Water Quality Monitoring',
    author: 'Dyutishmaan Das',
    affiliation: 'IIT Roorkee Summer Internship Project'
};

const LIVE_MODE = true; // Subscribes to HiveMQ Cloud

const MQTT_CONFIG = {
    brokerUrl: 'wss://c27fa0e9f196413ea7ea84e3cb6b1a3d.s1.eu.hivemq.cloud:8884/mqtt',
    username: 'Ganga_Node_A',
    password: 'Luci@2112@#',
    topic: 'ganga-edge/+/sensors',
    clientId: 'gangaedge-dashboard-' + Math.random().toString(16).substr(2, 8)
};

const G_SHEETS_URL = 'https://script.google.com/macros/s/AKfycbzD4xEIK1hTkBQQecZRSRx1iVYQGjxs3Fmh8o9_YcnEzEgcYwQQvIALr8BqR55RGBQI/exec';

// --- INITIAL SENSOR PARAMS CONFIG ---
const SENSOR_CONFIGS = {
    tds: {
        id: 'tds',
        name: 'TDS',
        fullName: 'Total Dissolved Solids',
        unit: ' ppm',
        baseline: 320.0,
        amplitude: 30.0,
        noiseStd: 5.0,
        maxExpectedDelta: 20.0,
        minBound: 0.0,
        maxBound: 1000.0,
        safeMin: 50.0,
        safeMax: 600.0,
        icon: 'fa-flask',
        color: 'rgba(6, 182, 212, 1)',
        cardClass: 'gradient-cyan'
    },
    ph: {
        id: 'ph',
        name: 'pH',
        fullName: 'Acidity / Alkalinity',
        unit: ' pH',
        baseline: 7.2,
        amplitude: 0.2,
        noiseStd: 0.03,
        maxExpectedDelta: 0.1,
        minBound: 3.0,
        maxBound: 11.0,
        safeMin: 6.5,
        safeMax: 8.5,
        icon: 'fa-vial',
        color: 'rgba(20, 184, 166, 1)',
        cardClass: 'gradient-green'
    },
    temp: {
        id: 'temp',
        name: 'Temperature',
        fullName: 'Water Temperature (DS18B20)',
        unit: '°C',
        baseline: 26.5,
        amplitude: 2.0,
        noiseStd: 0.1,
        maxExpectedDelta: 0.5,
        minBound: 0.0,
        maxBound: 50.0,
        safeMin: 10.0,
        safeMax: 35.0,
        icon: 'fa-thermometer-half',
        color: 'rgba(56, 189, 248, 1)',
        cardClass: 'gradient-cyan'
    },
    turb: {
        id: 'turb',
        name: 'Turbidity',
        fullName: 'Water Clarity (Turbidity)',
        unit: ' NTU',
        baseline: 4.5,
        amplitude: 1.5,
        noiseStd: 0.4,
        maxExpectedDelta: 5.0,
        minBound: 0.0,
        maxBound: 400.0,
        safeMin: 0.0,
        safeMax: 80.0,
        icon: 'fa-water',
        color: 'rgba(167, 139, 250, 1)',
        cardClass: 'gradient-cyan'
    }
};

// Node deployment configurations
// Nodes start OFFLINE — they come online only when real MQTT data arrives
let NODES = {
    'node-a': {
        id: 'node-a',
        name: 'Node A: Upstream',
        station: 'ROORKEE-NORTH',
        deviceId: 'EDGE-U-0142',
        installed: '2025-10-12',
        uptimeStart: null,
        status: 'offline',
        sensors: ['tds', 'ph', 'temp', 'turb'],
        trustScores: { tds: 0, ph: 0, temp: 0, turb: 0 },
        lastSeen: 0,
        rssi: -90
    },
    'node-b': {
        id: 'node-b',
        name: 'Node B: Downstream',
        station: 'ROORKEE-SOUTH',
        deviceId: 'EDGE-D-0922',
        installed: '2025-10-14',
        uptimeStart: null,
        status: 'offline',
        sensors: ['tds', 'ph', 'temp', 'turb'],
        trustScores: { tds: 0, ph: 0, temp: 0, turb: 0 },
        lastSeen: 0,
        rssi: -90
    },
    'node-c': {
        id: 'node-c',
        name: 'Node C: Midstream',
        station: 'ROORKEE-CENTRAL',
        deviceId: 'EDGE-M-0451',
        installed: '2025-11-02',
        uptimeStart: null,
        status: 'offline',
        sensors: ['tds', 'ph', 'temp', 'turb'],
        trustScores: { tds: 0, ph: 0, temp: 0, turb: 0 },
        lastSeen: 0,
        rssi: -90
    },
    'node-d': {
        id: 'node-d',
        name: 'Node D: Bridge Site',
        station: 'ROORKEE-BRIDGE',
        deviceId: 'EDGE-B-0711',
        installed: '2025-11-18',
        uptimeStart: null,
        status: 'offline',
        sensors: ['tds', 'ph', 'temp', 'turb'],
        trustScores: { tds: 0, ph: 0, temp: 0, turb: 0 },
        lastSeen: 0,
        rssi: -90
    }
};

// Node offsets for Downstream Simulation
const NODE_OFFSETS = {
    'node-a': { tds: 0, ph: 0, temp: 0, turb: 0 },
    'node-b': { tds: +38.0, ph: -0.12, temp: +0.4, turb: +18.0 },
    'node-c': { tds: +15.0, ph: -0.05, temp: +0.2, turb: +8.0 },
    'node-d': { tds: +45.0, ph: -0.18, temp: +0.6, turb: +25.0 }
};

// --- GLOBAL APPLICATION STATE ---
const state = {
    nodes: NODES,
    simulators: {}, // node_id -> { tds, ph, temp, turb }
    history: {}, // node_id -> { tds:[], ph:[], temp:[], turb:[], trust_tds:[],... }
    selectedNode: 'node-a',
    isCloudOnline: true,
    isSyncing: false,
    weatherContext: 'clear',
    weatherData: { temp: 29.4, wind: 8.1, precip: 0.0, humidity: 78 },
    localQueue: [],
    cloudDb: [],
    systemTick: 0,
    activePage: 'view-overview',
    logFilter: 'all',
    logPaused: false,
    comparisonEnabled: false,
    logs: [] // [{time, msg, type}]
};

const historyLength = 30;
let mqttClient = null;

// --- SENSOR SIMULATOR CLASS ---
class SensorSimulator {
    constructor(config, offset = 0) {
        this.config = config;
        this.offset = offset;
        this.currentFault = 'normal';
        this.tickCounter = Math.floor(Math.random() * 100);
        this.driftAccumulator = 0;
        this.stuckValue = null;
        this.history = [];
        this.filteredHistory = [];
        this.trustEngine = new TrustScoringEngine(config);
    }

    generateValue() {
        this.tickCounter++;
        let rawVal = 0;

        // Sine wave diurnal operation + offset
        const sinVal = this.config.baseline + this.offset + Math.sin(this.tickCounter * 0.06) * this.config.amplitude;
        const normalNoise = (Math.random() - 0.5) * 2 * this.config.noiseStd;
        rawVal = sinVal + normalNoise;

        // Weather impact on turbidity
        if (this.config.id === 'turb' && state.weatherContext === 'rain') {
            rawVal += 15.0 + Math.random() * 8.0; // natural mud churn mud spikes
        }

        // Apply Injected Faults
        switch (this.currentFault) {
            case 'normal':
                this.driftAccumulator = 0;
                this.stuckValue = null;
                break;
            case 'drift':
                this.driftAccumulator += this.config.maxExpectedDelta * 0.35;
                rawVal += this.driftAccumulator;
                break;
            case 'spike':
                if (Math.random() < 0.25) {
                    rawVal += this.config.amplitude * 4.5;
                }
                this.stuckValue = null;
                break;
            case 'stuck':
                if (this.stuckValue === null) {
                    this.stuckValue = rawVal;
                }
                rawVal = this.stuckValue;
                break;
            case 'noise':
                const extNoise = (Math.random() - 0.5) * 2 * (this.config.noiseStd * 12);
                rawVal = sinVal + extNoise;
                this.stuckValue = null;
                break;
        }

        this.history.push(rawVal);
        if (this.history.length > 50) this.history.shift();

        return rawVal;
    }

    setFault(faultName) {
        this.currentFault = faultName;
        this.driftAccumulator = 0;
        this.stuckValue = null;
    }
}

// --- AI TRUST SCORING ENGINE ---
class TrustScoringEngine {
    constructor(config) {
        this.config = config;
        this.window = [];
        this.windowSize = 15;
        this.lastRaw = null;
        this.rollingTrust = 100.0;
        this.consecutiveIdentical = 0;
    }

    calculateTrust(rawVal) {
        this.window.push(rawVal);
        if (this.window.length > this.windowSize) this.window.shift();

        if (this.window.length < 5) {
            this.lastRaw = rawVal;
            return 100;
        }

        // 1. Hard Bounds Check
        if (rawVal < this.config.minBound || rawVal > this.config.maxBound) {
            this.rollingTrust = 0.0;
            this.lastRaw = rawVal;
            return 0;
        }

        // 2. Rolling Z-Score Anomaly detection
        const mean = this.window.reduce((a, b) => a + b, 0) / this.window.length;
        const variance = this.window.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / this.window.length;
        const stdDev = Math.sqrt(variance);

        let zScoreTrust = 100;
        if (stdDev > 0.001) {
            let zScore = Math.abs(rawVal - mean) / stdDev;
            
            // Weather mitigation check: Relax bounds if monsoon active
            if (this.config.id === 'turb' && state.weatherContext === 'rain') {
                zScore = zScore / 3.0; // Reduce Z-score statistical penalty during rain
            }

            if (zScore > 2.0) {
                zScoreTrust = Math.max(0, 100 - ((zScore - 2.0) / 2.5) * 100);
            }
        }

        // 3. Rate of Change (Delta) Check
        let deltaTrust = 100;
        if (this.lastRaw !== null) {
            const delta = Math.abs(rawVal - this.lastRaw);
            let maxExpected = this.config.maxExpectedDelta;

            if (this.config.id === 'turb' && state.weatherContext === 'rain') {
                maxExpected = maxExpected * 4.5;
            }

            if (delta > maxExpected) {
                const excess = delta / maxExpected;
                deltaTrust = Math.max(0, 100 - (excess - 1.0) * 75);
            }
        }

        // 4. Stuck Value Check
        let stuckTrust = 100;
        if (this.lastRaw !== null && Math.abs(rawVal - this.lastRaw) < 0.0001) {
            this.consecutiveIdentical++;
        } else {
            this.consecutiveIdentical = 0;
        }

        if (this.consecutiveIdentical >= 5) {
            stuckTrust = Math.max(0, 100 - (this.consecutiveIdentical - 4) * 25);
        }

        // Weighted Trust calculation
        const rawScore = (zScoreTrust * 0.40) + (deltaTrust * 0.30) + (stuckTrust * 0.30);

        // Alpha decay filters
        let decayAlpha = rawScore < this.rollingTrust ? 0.55 : 0.25;
        if (this.config.id === 'turb' && state.weatherContext === 'rain') {
            decayAlpha = rawScore < this.rollingTrust ? 0.18 : 0.25; // slower trust drop in rains
        }

        this.rollingTrust = (decayAlpha * rawScore) + ((1 - decayAlpha) * this.rollingTrust);
        this.rollingTrust = Math.max(0, Math.min(100, this.rollingTrust));

        this.lastRaw = rawVal;
        return Math.round(this.rollingTrust);
    }
}

// --- LOCAL DECISION DECISION AVOIDANCE ENGINE ---
class LocalDecisionEngine {
    static processReading(sensor, rawVal, trustScore) {
        let status = 'trusted';
        let filteredVal = rawVal;
        let actionMsg = '';

        if (trustScore >= 80) {
            status = 'trusted';
            filteredVal = rawVal;
        } else if (trustScore >= 50) {
            status = 'suspect';
            const prevFiltered = sensor.filteredHistory.length > 0 ? sensor.filteredHistory[sensor.filteredHistory.length - 1] : rawVal;
            filteredVal = (0.35 * rawVal) + (0.65 * prevFiltered); // EMA filter
            actionMsg = `[WARNING] ${sensor.config.name} trust degraded to ${trustScore}%. Applying noise filter.`;
        } else {
            status = 'degraded';
            // decoupling fallback forecast
            const trusted = sensor.filteredHistory.slice(-6);
            if (trusted.length > 0) {
                filteredVal = trusted.reduce((a, b) => a + b, 0) / trusted.length;
            } else {
                filteredVal = sensor.config.baseline + sensor.offset;
            }
            actionMsg = `[CRITICAL] Anomaly detected on ${sensor.config.name}! Decoupling sensor. Injecting fallback: ${filteredVal.toFixed(1)}${sensor.config.unit}.`;
        }

        sensor.filteredHistory.push(filteredVal);
        if (sensor.filteredHistory.length > 50) sensor.filteredHistory.shift();

        return { status, filteredVal, actionMsg };
    }
}

// --- WEATHER RETRIEVAL (OPEN-METEO) ---
async function fetchWeather() {
    const url = "https://api.open-meteo.com/v1/forecast?latitude=29.8543&longitude=77.8880&current=temperature_2m,precipitation,rain,weathercode,windspeed_10m,relative_humidity_2m&timezone=Asia%2FKolkata";
    try {
        const res = await fetch(url);
        if (!res.ok) throw new Error("HTTP Status " + res.status);
        const data = await res.json();
        
        if (data && data.current) {
            const cur = data.current;
            state.weatherData.temp = cur.temperature_2m;
            state.weatherData.wind = cur.windspeed_10m;
            state.weatherData.precip = cur.precipitation;
            state.weatherData.humidity = cur.relative_humidity_2m;
            
            // WMO code checking for Rain
            const code = cur.weathercode;
            const isRaining = [51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82, 95, 96, 99].includes(code);
            
            if (isRaining || cur.precipitation > 0.5) {
                state.weatherContext = 'rain';
            } else {
                state.weatherContext = 'clear';
            }
            updateWeatherUI();
        }
    } catch (err) {
        logEvent(`[WEATHER ERROR] Failed to fetch weather API: ${err.message}`, 'system');
    }
}

function updateWeatherUI() {
    const iconEl = document.getElementById('weather-icon');
    const tempEl = document.getElementById('weather-temp');
    const windEl = document.getElementById('weather-wind');
    const rainEl = document.getElementById('weather-rain');
    const badgeEl = document.getElementById('weather-badge');
    const precipValEl = document.getElementById('weather-precip-val');
    const humidityValEl = document.getElementById('weather-humidity-val');
    const alertsIndicator = document.getElementById('alerts-indicator');
    const alertsTooltip = document.getElementById('alerts-tooltip');

    const tempStr = `${state.weatherData.temp.toFixed(1)}°C`;
    const windStr = `${state.weatherData.wind.toFixed(1)} km/h`;
    const rainStr = `${state.weatherData.precip.toFixed(1)} mm`;

    if (iconEl) iconEl.innerText = state.weatherContext === 'rain' ? '🌧️' : '☀️';
    if (tempEl) tempEl.innerText = tempStr;
    if (windEl) windEl.innerText = windStr;
    if (rainEl) rainEl.innerText = rainStr;
    if (precipValEl) precipValEl.innerText = rainStr;
    if (humidityValEl) humidityValEl.innerText = `${state.weatherData.humidity}%`;

    const badge = document.getElementById('weather-badge');
    const banner = document.getElementById('mitigation-banner');

    if (state.weatherContext === 'rain') {
        if (badge) {
            badge.innerText = 'MONSOON ACTIVE';
            badge.className = 'ml-1 text-[9px] font-extrabold px-1.5 py-0.5 rounded bg-red-500/15 text-magenta-accent';
        }
        if (banner) {
            banner.innerHTML = `
                <span class="font-bold text-red-400 block mb-1"><i class="fa-solid fa-cloud-showers-heavy text-magenta-accent animate-bounce"></i> Monsoon Mitigated</span>
                <p class="text-light-gray leading-relaxed text-[11px]">Rain detected. Turbidity SVM thresholds expanded +15% and decay rate reduced to prevent natural mud false alarms.</p>
            `;
            banner.className = 'mt-4 p-3 bg-slate-800/60 border border-red-500/30 rounded-lg text-xs';
        }
        if (alertsIndicator) alertsIndicator.classList.remove('hidden');
        if (alertsTooltip) {
            alertsTooltip.innerText = `WEATHER ALERT: Rain detected (${rainStr}). Turbidity thresholds relaxed.`;
            document.getElementById('alerts-btn').classList.add('bell-pulse');
        }
    } else {
        if (badge) {
            badge.innerText = 'CLEAR';
            badge.className = 'ml-1 text-[9px] font-extrabold px-1.5 py-0.5 rounded bg-emerald-500/15 text-soft-green';
        }
        if (banner) {
            banner.innerHTML = `
                <span class="font-bold text-blue-400 block mb-1"><i class="fa-solid fa-cloud-sun"></i> Ambient Nominal</span>
                <p class="text-light-gray leading-relaxed text-[11px]">Normal statistical Z-score and delta-rate check bounds active for all parameters.</p>
            `;
            banner.className = 'mt-4 p-3 bg-panel-bg/60 border border-gray-700/30 rounded-lg text-xs';
        }
        if (alertsIndicator) alertsIndicator.classList.add('hidden');
        if (alertsTooltip) {
            alertsTooltip.innerText = 'System Status: Nominal. Normal turbidity thresholds active.';
            document.getElementById('alerts-btn').classList.remove('bell-pulse');
        }
    }
}

// --- GOOGLE SHEETS TELEMETRY DATABASE FETCH ---
async function fetchGoogleSheetsHistory() {
    logEvent('[SYSTEM] Fetching historical telemetry records from Google Sheets database...', 'info');
    try {
        const response = await fetch(G_SHEETS_URL);
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        
        if (Array.isArray(data) && data.length > 0) {
            logEvent(`[SYSTEM] Loaded ${data.length} historical records from Google Sheets.`, 'success');
            
            // Clear current simulator histories for all nodes before seeding real data
            Object.keys(state.nodes).forEach(nodeId => {
                state.history[nodeId] = {
                    tds: [], ph: [], temp: [], turb: [],
                    trust_tds: [], trust_ph: [], trust_temp: [], trust_turb: []
                };
            });

            data.forEach(row => {
                const device = row["Device"] || "";
                let nodeId = "node-a"; // Fallback to Node A
                
                if (device.toLowerCase().includes("node-b") || device.toLowerCase().includes("node-b")) {
                    nodeId = "node-b";
                } else if (device.toLowerCase().includes("node-c")) {
                    nodeId = "node-c";
                } else if (device.toLowerCase().includes("node-d")) {
                    nodeId = "node-d";
                }

                if (state.history[nodeId]) {
                    const tempVal = parseFloat(row["Temperature (°C)"]);
                    const phVal = parseFloat(row["pH"]);
                    const tdsVal = parseFloat(row["TDS (ppm)"]);
                    const turbVal = parseFloat(row["Turbidity (NTU)"]);

                    const trustTds = parseFloat(row["Trust_TDS (%)"]) || 100;
                    const trustPh = parseFloat(row["Trust_pH (%)"]) || 100;
                    const trustTemp = parseFloat(row["Trust_Temp (%)"]) || 100;
                    const trustTurb = parseFloat(row["Trust_Turb (%)"]) || 100;

                    if (!isNaN(tempVal)) state.history[nodeId].temp.push(tempVal);
                    if (!isNaN(phVal)) state.history[nodeId].ph.push(phVal);
                    if (!isNaN(tdsVal)) state.history[nodeId].tds.push(tdsVal);
                    if (!isNaN(turbVal)) state.history[nodeId].turb.push(turbVal);

                    state.history[nodeId].trust_temp.push(trustTemp);
                    state.history[nodeId].trust_ph.push(trustPh);
                    state.history[nodeId].trust_tds.push(trustTds);
                    state.history[nodeId].trust_turb.push(trustTurb);
                }
            });

            // Restrain array sizes to historyLength
            Object.keys(state.nodes).forEach(nodeId => {
                const h = state.history[nodeId];
                Object.keys(h).forEach(metric => {
                    if (h[metric].length > historyLength) {
                        h[metric] = h[metric].slice(-historyLength);
                    }
                });
                
                // Keep the node's simulator filteredHistory synced with latest value
                const sims = state.simulators[nodeId];
                if (sims) {
                    Object.keys(sims).forEach(metric => {
                        const hList = h[metric];
                        if (hList && hList.length > 0) {
                            sims[metric].filteredHistory = [...hList];
                        }
                    });
                }
            });

            // Refresh UI
            renderSensorCards();
            updateSensorsPage();
            updateOverviewPage();
            updateComparisonTable();
        } else {
            logEvent('[SYSTEM] Google Sheets database empty. Pre-seeding simulator defaults.', 'warn');
        }
    } catch (err) {
        logEvent(`[SYSTEM ERROR] Failed to load Google Sheets history: ${err.message}`, 'error');
    }
}

// --- MQTT UPLINK HANDLERS (LIVE DATA RECEIPT) ---
function connectLiveMQTT() {
    logEvent('[MQTT] Connecting to HiveMQ Cloud WebSocket broker...', 'system');

    mqttClient = mqtt.connect(MQTT_CONFIG.brokerUrl, {
        username: MQTT_CONFIG.username,
        password: MQTT_CONFIG.password,
        clientId: MQTT_CONFIG.clientId,
        protocol: 'wss',
        reconnectPeriod: 6000,
        connectTimeout: 12000
    });

    mqttClient.on('connect', () => {
        logEvent(`[MQTT] ✓ Connected! Subscribing to topic: ${MQTT_CONFIG.topic}`, 'info');
        mqttClient.subscribe(MQTT_CONFIG.topic, (err) => {
            if (err) logEvent(`[MQTT] Subscription error: ${err.message}`, 'error');
            else logEvent('[MQTT] ✓ Subscription active. Listening for live ESP32 nodes...', 'info');
        });
    });

    mqttClient.on('message', (topic, message) => {
        try {
            const payload = JSON.parse(message.toString());
            // Deduce node from topic
            const isNodeB = topic.includes('node-b') || (payload.device && payload.device.includes('node-B'));
            const nodeId = isNodeB ? 'node-b' : 'node-a';
            handleLivePacket(nodeId, payload);
        } catch (err) {
            logEvent(`[MQTT] Parse Failure: ${err.message}`, 'error');
        }
    });

    mqttClient.on('error', (err) => {
        logEvent(`[MQTT CONNECTION ERROR] ${err.message}`, 'error');
    });

    mqttClient.on('offline', () => {
        logEvent('[MQTT] Connection lost. Checking backup buffers.', 'warn');
    });
}

function handleLivePacket(nodeId, data) {
    const node = state.nodes[nodeId];
    if (!node) return;

    // Bring node online when we receive real data
    if (node.status === 'offline') {
        node.uptimeStart = Date.now();
        logEvent(`[MQTT] Node ${node.name} is now ONLINE (live data received).`, 'success');
    }
    node.status = 'online';
    node.lastSeen = Date.now();
    node.rssi = data.rssi || -60;

    // Map parameters
    const params = ['tds', 'ph', 'temp', 'turb'];
    params.forEach(p => {
        if (data[p]) {
            const raw = parseFloat(data[p].raw);
            const trust = parseInt(data[p].trust);
            node.trustScores[p] = trust;

            // Push into history
            if (state.history[nodeId] && state.history[nodeId][p]) {
                state.history[nodeId][p].push(raw);
                state.history[nodeId][`trust_${p}`].push(trust);
                if (state.history[nodeId][p].length > historyLength) {
                    state.history[nodeId][p].shift();
                    state.history[nodeId][`trust_${p}`].shift();
                }
            }
        }
    });

    // ML Anomaly warning logs
    if (data.mlAnomaly) {
        logEvent(`[TinyML Anomaly ${node.name}] Anomaly flag TRUE (Cause: ${data.mlCause || 'Unknown'}). Decoupling bounds shifted.`, 'error');
    }
}

// --- LIVE DATA LOOP (ONLY PROCESSES NODES WITH RECENT MQTT DATA) ---
function runSimulatedTick() {
    state.systemTick++;
    let activeFaultsCount = 0;

    // Check for nodes that have gone stale (no MQTT packet in 15s) and mark them offline
    Object.keys(state.nodes).forEach(nodeId => {
        const node = state.nodes[nodeId];
        if (node.status === 'online' && node.lastSeen > 0 && (Date.now() - node.lastSeen > 15000)) {
            node.status = 'offline';
            logEvent(`[MQTT] Node ${node.name} went OFFLINE (no data for 15s).`, 'warn');
        }
    });

    // Process values for online nodes that have recent MQTT data
    Object.keys(state.nodes).forEach(nodeId => {
        const node = state.nodes[nodeId];

        if (node.status === 'offline') {
            return;
        }

        // Only process if we have recent live data (within 15 seconds)
        const hasRecentData = node.lastSeen > 0 && (Date.now() - node.lastSeen < 15000);
        if (!hasRecentData) return;

        const tickData = [];
        const sims = state.simulators[nodeId];

        node.sensors.forEach(sensorId => {
            const sim = sims[sensorId];
            let raw = 0;
            let trust = 0;
            let filteredVal = 0;
            let status = 'trusted';
            let actionMsg = '';

            // Use the last value pushed by MQTT (no simulation)
            const hList = state.history[nodeId][sensorId];
            const tList = state.history[nodeId][`trust_${sensorId}`];
            raw = hList.length > 0 ? hList[hList.length - 1] : sim.config.baseline;
            trust = tList.length > 0 ? tList[tList.length - 1] : 100;
            filteredVal = raw;
            status = trust < 50 ? 'degraded' : trust < 80 ? 'suspect' : 'trusted';

            node.trustScores[sensorId] = trust;
            if (status !== 'trusted') activeFaultsCount++;

            tickData.push({
                id: sensorId,
                name: sim.config.name,
                raw: raw,
                filtered: filteredVal,
                trust: trust,
                status: status,
                unit: sim.config.unit
            });

            // Log fault anomalies
            if (actionMsg && !isLive && nodeId === state.selectedNode) {
                logEvent(`[${node.name}] ${actionMsg}`, status === 'suspect' ? 'warn' : 'error');
            }
        });

        // Trigger network buffering or sync simulation (ONLY for selectedNode for console demonstration)
        if (nodeId === 'node-a') {
            tickData.forEach(item => {
                const telemetryPacket = {
                    timestamp: new Date().toLocaleTimeString(),
                    node: nodeId,
                    sensorName: item.name,
                    rawVal: item.raw,
                    filteredVal: item.filtered,
                    trust: item.trust,
                    status: item.status,
                    unit: item.unit
                };

                if (state.isCloudOnline) {
                    state.cloudDb.unshift(telemetryPacket);
                    if (state.cloudDb.length > 60) state.cloudDb.pop();
                } else {
                    state.localQueue.push(telemetryPacket);
                    if (state.localQueue.length > 500) {
                        state.localQueue.shift();
                        logEvent('[BUFFER FULL] SPIFFS Outage Queue capacity reached! Dropping oldest telemetry packet.', 'error');
                    }
                }
            });
        }
    });

    // Update global Stats & widgets
    updateStatsDOM();
    
    // Draw current active page contents
    if (state.activePage === 'view-overview') {
        updateOverviewPage();
    } else if (state.activePage === 'view-sensors') {
        updateSensorsPage();
    } else if (state.activePage === 'view-analytics') {
        updateAnalyticsPage();
    }
}

// --- BATCH CLOUD SYNC DRAINING ---
function triggerBatchSync() {
    if (state.localQueue.length === 0) {
        setSyncStatus('idle');
        return;
    }

    state.isSyncing = true;
    setSyncStatus('syncing');

    const syncInterval = setInterval(() => {
        if (!state.isCloudOnline) {
            clearInterval(syncInterval);
            state.isSyncing = false;
            setSyncStatus('buffering');
            return;
        }

        if (state.localQueue.length > 0) {
            const packet = state.localQueue.shift();
            state.cloudDb.unshift(packet);
            if (state.cloudDb.length > 60) state.cloudDb.pop();

            updateStatsDOM();
        } else {
            clearInterval(syncInterval);
            state.isSyncing = false;
            setSyncStatus('idle');
            logEvent('[SYNC SERVICE] ✓ Edge autonomous sync complete. Buffered registers draining successful.', 'info');
        }
    }, 200); // 5Hz (1 every 200ms)
}

function setSyncStatus(status) {
    const icon = document.getElementById('sync-icon');
    const label = document.getElementById('sync-status-text');
    const flowLine = document.getElementById('flow-edge-to-cloud');
    const cloudCircle = document.getElementById('cloud-circle');
    const cloudMeta = document.getElementById('cloud-meta-text');

    if (!icon || !label) return;

    if (status === 'idle') {
        icon.className = 'status-dot-active';
        label.innerText = 'Live Sync';
        label.className = 'font-extrabold uppercase text-[10px] text-emerald-400 tracking-wider';
        if (flowLine) flowLine.setAttribute('class', 'flow-line-active');
        if (cloudCircle) cloudCircle.setAttribute('stroke', 'var(--accent-cyan)');
        if (cloudMeta) cloudMeta.innerText = 'Connected';
    } else if (status === 'syncing') {
        icon.className = 'status-dot-active';
        label.innerText = 'Syncing...';
        label.className = 'font-extrabold uppercase text-[10px] text-blue-400 tracking-wider';
        if (flowLine) flowLine.setAttribute('class', 'flow-line-active');
    } else if (status === 'buffering') {
        icon.className = 'status-dot-offline';
        label.innerText = 'Buffering';
        label.className = 'font-extrabold uppercase text-[10px] text-red-400 tracking-wider';
        if (flowLine) flowLine.setAttribute('class', 'flow-line-inactive');
        if (cloudCircle) cloudCircle.setAttribute('stroke', 'var(--text-muted)');
        if (cloudMeta) cloudMeta.innerText = 'Unreachable';
    }
}

// --- TERMINAL LOGSTREAM SYSTEM ---
function logEvent(msg, type = 'info') {
    const timestamp = new Date().toLocaleTimeString();
    const logItem = { time: timestamp, msg, type };
    state.logs.push(logItem);
    if (state.logs.length > 200) state.logs.shift();

    if (state.logPaused) return;

    // Render to small stream inside overview
    const smallStream = document.getElementById('console-stream');
    if (smallStream && state.activePage === 'view-overview') {
        const p = document.createElement('div');
        p.className = `p-1 border-l-2 border-gray-800 my-1 ${type === 'error' ? 'text-magenta-accent border-l-magenta-accent' : type === 'warn' ? 'text-yellow-400 border-l-yellow-400' : 'text-light-gray'}`;
        p.innerHTML = `<span>[${timestamp}]</span> ${msg}`;
        smallStream.appendChild(p);
        
        if (smallStream.childElementCount > 40) {
            smallStream.removeChild(smallStream.firstChild);
        }
        smallStream.scrollTop = smallStream.scrollHeight;
    }

    // Render to large terminal page
    const mainTerm = document.getElementById('main-terminal');
    if (mainTerm && state.activePage === 'view-console') {
        if (shouldDisplayLog(type)) {
            const div = document.createElement('div');
            div.className = `my-1 ${type === 'error' ? 'log-error' : type === 'warn' ? 'log-warn' : type === 'system' ? 'log-system' : 'log-info'}`;
            div.innerText = `[${timestamp}] ${msg}`;
            mainTerm.appendChild(div);
            
            if (mainTerm.childElementCount > 150) {
                mainTerm.removeChild(mainTerm.firstChild);
            }
            mainTerm.scrollTop = mainTerm.scrollHeight;
        }
    }
}

function shouldDisplayLog(type) {
    if (state.logFilter === 'all') return true;
    return state.logFilter === type;
}

function renderFullConsoleLogs() {
    const mainTerm = document.getElementById('main-terminal');
    if (!mainTerm) return;
    mainTerm.innerHTML = '';
    
    state.logs.forEach(log => {
        if (shouldDisplayLog(log.type)) {
            const div = document.createElement('div');
            div.className = `my-1 ${log.type === 'error' ? 'log-error' : log.type === 'warn' ? 'log-warn' : log.type === 'system' ? 'log-system' : 'log-info'}`;
            div.innerText = `[${log.time}] ${log.msg}`;
            mainTerm.appendChild(div);
        }
    });
    mainTerm.scrollTop = mainTerm.scrollHeight;
}

// --- DOM RENDER WIDGETS ---

function updateStatsDOM() {
    // Stat cards Overview
    const actSens = document.getElementById('stat-active-sensors');
    const bufCount = document.getElementById('stat-buffered');
    const cldCount = document.getElementById('stat-cloud');
    const riskEl = document.getElementById('stat-risk');

    const activeNodes = Object.values(state.nodes).filter(n => n.status === 'online').length;
    const totalNodes = Object.keys(state.nodes).length;

    if (actSens) actSens.innerText = `${activeNodes} / ${totalNodes} Nodes`;
    if (bufCount) bufCount.innerText = state.localQueue.length;
    if (cldCount) cldCount.innerText = state.cloudDb.length;
    
    // Risk State calculations
    let degradedCount = 0;
    Object.values(state.nodes).forEach(n => {
        if (n.status === 'online') {
            Object.values(n.trustScores).forEach(score => {
                if (score < 50) degradedCount++;
            });
        }
    });

    if (riskEl) {
        if (degradedCount > 0) {
            riskEl.innerText = 'WARNING';
            riskEl.className = 'text-3xl font-extrabold mt-1 text-red-400';
        } else {
            riskEl.innerText = 'NOMINAL';
            riskEl.className = 'text-3xl font-extrabold mt-1 text-white';
        }
    }

    const qBadge = document.getElementById('queue-size-badge');
    const cBadge = document.getElementById('cloud-size-badge');
    if (qBadge) qBadge.innerText = `${state.localQueue.length} records buffered`;
    if (cBadge) cBadge.innerText = `${state.cloudDb.length} records synced`;
}

// Overview page charts and topology updating
const miniChartInstances = {};

function initOverviewCharts() {
    const chartIds = ['miniChartTds', 'miniChartPh', 'miniChartTemp', 'miniChartTurb'];
    const metrics = ['tds', 'ph', 'temp', 'turb'];
    const colors = ['#06b6d4', '#14b8a6', '#38bdf8', '#a78bfa'];

    chartIds.forEach((id, idx) => {
        const canvas = document.getElementById(id);
        if (!canvas) return;
        
        miniChartInstances[metrics[idx]] = new Chart(canvas.getContext('2d'), {
            type: 'line',
            data: {
                labels: Array.from({length: 15}, (_, i) => `-${(15-i)*2}s`),
                datasets: [{
                    data: Array(15).fill(0),
                    borderColor: colors[idx],
                    backgroundColor: colors[idx] + '15',
                    borderWidth: 1.5,
                    pointRadius: 0,
                    tension: 0.35,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    x: { display: false },
                    y: { grid: { color: 'rgba(6, 182, 212, 0.06)' }, ticks: { color: '#64748b', font: { size: 8 } } }
                }
            }
        });
    });
}

function updateOverviewPage() {
    // Update mini-charts values
    const metrics = ['tds', 'ph', 'temp', 'turb'];
    metrics.forEach(m => {
        const dropdown = document.getElementById(`node-select-${m}`);
        const selectedNodeId = dropdown ? dropdown.value : 'node-a';
        
        const chart = miniChartInstances[m];
        if (chart && state.history[selectedNodeId] && state.history[selectedNodeId][m]) {
            const fullHistory = state.history[selectedNodeId][m];
            chart.data.datasets[0].data = fullHistory.slice(-15);
            chart.update('none');
        }
    });

    // Update Interactive Topology Visual States
    Object.keys(state.nodes).forEach(nodeId => {
        const node = state.nodes[nodeId];
        const circle = document.getElementById(`node-${nodeId.split('-')[1]}-circle`);
        const icon = document.getElementById(`node-${nodeId.split('-')[1]}-icon`);
        const trustLabel = document.getElementById(`${nodeId}-topo-trust`);
        
        if (circle) {
            if (node.status === 'online') {
                // compute average trust
                const scores = Object.values(node.trustScores);
                const avgTrust = Math.round(scores.reduce((a,b)=>a+b,0)/scores.length);
                
                if (avgTrust >= 80) circle.setAttribute('stroke', '#10b981'); // nominal green
                else if (avgTrust >= 50) circle.setAttribute('stroke', '#f59e0b'); // degraded yellow
                else circle.setAttribute('stroke', '#ef4444'); // alert red
                
                if (icon) icon.setAttribute('fill', circle.getAttribute('stroke'));
                if (trustLabel) {
                    trustLabel.innerText = `${avgTrust}% Avg Trust`;
                    trustLabel.setAttribute('fill', circle.getAttribute('stroke'));
                }
            } else {
                circle.setAttribute('stroke', '#6b7280'); // offline grey
                if (icon) icon.setAttribute('fill', '#6b7280');
                if (trustLabel) {
                    trustLabel.innerText = 'Offline';
                    trustLabel.setAttribute('fill', '#6b7280');
                }
            }
        }
    });
}

// Telemetry Sensors page rendering
function renderSensorCards() {
    const container = document.getElementById('sensor-container');
    if (!container) return;
    container.innerHTML = '';

    const node = state.nodes[state.selectedNode];
    if (!node) return;

    node.sensors.forEach(s => {
        const cfg = SENSOR_CONFIGS[s];
        if (!cfg) return;

        const card = document.createElement('div');
        card.className = `glass-card ${cfg.cardClass} flex flex-col justify-between`;
        card.id = `card-metric-${s}`;

        card.innerHTML = `
            <div>
                <div class="flex justify-between items-center mb-2">
                    <span class="text-xs font-bold uppercase tracking-wider text-light-gray"><i class="fa-solid ${cfg.icon} mr-1.5 text-[14px]"></i> ${cfg.name}</span>
                    <span class="text-[9px] font-extrabold uppercase px-2 py-0.5 rounded bg-soft-green/20 text-soft-green" id="sensor-badge-${s}">Nominal</span>
                </div>
                <div class="text-xs text-light-gray mb-3">${cfg.fullName}</div>
                <div class="flex justify-between items-baseline my-4">
                    <div>
                        <span class="text-3xl font-extrabold text-white" id="sensor-raw-${s}">--</span>
                        <span class="text-xs text-light-gray ml-0.5">${cfg.unit}</span>
                    </div>
                    <div class="text-right">
                        <span class="block text-[9px] font-bold text-light-gray uppercase">Edge Filtered Output</span>
                        <span class="text-sm font-bold text-cyan-accent" id="sensor-filt-${s}">--</span>
                    </div>
                </div>
                <div class="flex justify-between text-[10px] text-light-gray mb-4 border-t border-gray-700/20 pt-2">
                    <span>Safe WHO bounds: ${cfg.safeMin} - ${cfg.safeMax}</span>
                    <span class="font-extrabold" id="sensor-flag-${s}">✓ OK</span>
                </div>

                <!-- Sparkline SVG Sparkline container -->
                <div class="h-10 mb-4 bg-navy-bg/30 border border-[var(--border-color)] rounded-lg overflow-hidden flex items-end">
                    <canvas class="sparkline-canvas" id="sparkline-canvas-${s}" height="40" width="220"></canvas>
                </div>

                <!-- Trust score progress bar -->
                <div class="mb-4">
                    <div class="flex justify-between items-center text-[10px] mb-1.5 font-bold text-light-gray">
                        <span>Edge AI Trust Score</span>
                        <span id="sensor-trust-val-${s}">100%</span>
                    </div>
                    <div class="h-1.5 bg-navy-bg rounded-full overflow-hidden">
                        <div class="h-full bg-cyan-accent" id="sensor-trust-bar-${s}" style="width: 100%; transition: width 0.3s ease;"></div>
                    </div>
                </div>
            </div>

            <!-- Fault Injection panel -->
            <div class="border-t border-gray-700/20 pt-3">
                <span class="block text-[9px] font-extrabold text-light-gray uppercase tracking-wider mb-2">Simulate Hardware Fault</span>
                <div class="grid grid-cols-5 gap-1.5">
                    <button class="btn-fault active" data-fault="normal" onclick="injectNodeFault('${s}', 'normal')">Nominal</button>
                    <button class="btn-fault" data-fault="noise" onclick="injectNodeFault('${s}', 'noise')">Noise</button>
                    <button class="btn-fault" data-fault="spike" onclick="injectNodeFault('${s}', 'spike')">Spike</button>
                    <button class="btn-fault" data-fault="stuck" onclick="injectNodeFault('${s}', 'stuck')">Stuck</button>
                    <button class="btn-fault" data-fault="drift" onclick="injectNodeFault('${s}', 'drift')">Drift</button>
                </div>
            </div>
        `;
        container.appendChild(card);
    });
}

function updateSensorsPage() {
    const node = state.nodes[state.selectedNode];
    if (!node) return;

    node.sensors.forEach(s => {
        const sim = state.simulators[state.selectedNode][s];
        const historyData = state.history[state.selectedNode][s];
        const trustHistory = state.history[state.selectedNode][`trust_${s}`];

        if (historyData.length === 0) return;

        const rawVal = historyData[historyData.length - 1];
        const trustVal = trustHistory[trustHistory.length - 1];
        const filteredVal = sim.filteredHistory.length > 0 ? sim.filteredHistory[sim.filteredHistory.length - 1] : rawVal;

        const rawEl = document.getElementById(`sensor-raw-${s}`);
        const filtEl = document.getElementById(`sensor-filt-${s}`);
        const flagEl = document.getElementById(`sensor-flag-${s}`);
        const trustValEl = document.getElementById(`sensor-trust-val-${s}`);
        const trustBarEl = document.getElementById(`sensor-trust-bar-${s}`);
        const badgeEl = document.getElementById(`sensor-badge-${s}`);

        const decimals = s === 'ph' ? 2 : s === 'turb' ? 1 : 0;

        // Raw Value update
        if (rawEl) {
            rawEl.innerText = rawVal.toFixed(decimals);
            // Red alert if out of safe bounds
            const isUnsafe = rawVal < sim.config.safeMin || rawVal > sim.config.safeMax;
            rawEl.className = isUnsafe ? 'text-3xl font-extrabold text-magenta-accent animate-pulse' : 'text-3xl font-extrabold text-white';
            if (flagEl) {
                flagEl.innerText = isUnsafe ? '⚠ OUT OF BOUNDS' : '✓ OK';
                flagEl.className = isUnsafe ? 'font-extrabold text-magenta-accent' : 'font-extrabold text-soft-green';
            }
        }

        // Filtered Output update
        if (filtEl) filtEl.innerText = `${filteredVal.toFixed(decimals)}${sim.config.unit}`;

        // Trust score bar update
        if (trustValEl) trustValEl.innerText = `${trustVal}%`;
        if (trustBarEl) {
            trustBarEl.style.width = `${trustVal}%`;
            // Color shifts
            if (trustVal >= 80) trustBarEl.style.backgroundColor = 'var(--accent-green)';
            else if (trustVal >= 50) trustBarEl.style.backgroundColor = 'var(--accent-orange)';
            else trustBarEl.style.backgroundColor = 'var(--accent-red)';
        }

        // Status badge updates
        if (badgeEl) {
            if (trustVal >= 80) {
                badgeEl.innerText = 'Nominal';
                badgeEl.className = 'text-[9px] font-extrabold uppercase px-2 py-0.5 rounded bg-soft-green/20 text-soft-green';
            } else if (trustVal >= 50) {
                badgeEl.innerText = 'Noise Mitigated';
                badgeEl.className = 'text-[9px] font-extrabold uppercase px-2 py-0.5 rounded bg-yellow-500/20 text-yellow-500';
            } else {
                badgeEl.innerText = 'Sensor decoupled';
                badgeEl.className = 'text-[9px] font-extrabold uppercase px-2 py-0.5 rounded bg-magenta-accent/20 text-magenta-accent';
            }
        }

        // Render mini sparklines via HTML5 Canvas
        drawSparkline(`sparkline-canvas-${s}`, historyData.slice(-20), SENSOR_CONFIGS[s].color);
        
        // Synchronize selected fault injection buttons
        const card = document.getElementById(`card-metric-${s}`);
        if (card) {
            const buttons = card.querySelectorAll('.btn-fault');
            buttons.forEach(btn => {
                if (btn.getAttribute('data-fault') === sim.currentFault) {
                    btn.classList.add('active');
                } else {
                    btn.classList.remove('active');
                }
            });
        }
    });

    // Update Upstream vs Downstream comparative table values
    if (state.comparisonEnabled) {
        updateComparisonTable();
    }
}

function drawSparkline(canvasId, dataHistory, color) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    
    // Clear canvas
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    if (dataHistory.length < 2) return;

    ctx.beginPath();
    const min = Math.min(...dataHistory);
    const max = Math.max(...dataHistory);
    const range = (max - min) === 0 ? 1.0 : (max - min);

    for (let i = 0; i < dataHistory.length; i++) {
        const x = (i / (dataHistory.length - 1)) * canvas.width;
        // Map Y coords with padding
        const y = canvas.height - ((dataHistory[i] - min) / range) * (canvas.height - 6) - 3;
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
    }
    
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.8;
    ctx.stroke();

    // Fill area below sparkline
    ctx.lineTo(canvas.width, canvas.height);
    ctx.lineTo(0, canvas.height);
    ctx.closePath();
    
    const gradient = ctx.createLinearGradient(0, 0, 0, canvas.height);
    gradient.addColorStop(0, color.replace('1)', '0.15)'));
    gradient.addColorStop(1, color.replace('1)', '0)'));
    ctx.fillStyle = gradient;
    ctx.fill();
}

window.injectNodeFault = function(sensorId, faultName) {
    const sim = state.simulators[state.selectedNode][sensorId];
    if (!sim) return;

    sim.setFault(faultName);
    
    if (faultName === 'normal') {
        logEvent(`[${state.nodes[state.selectedNode].name}] Clear fault on ${sim.config.name} sensor. Calibrating...`, 'info');
    } else {
        logEvent(`[${state.nodes[state.selectedNode].name}] Injected ${faultName.toUpperCase()} fault on ${sim.config.name}.`, 'warn');
    }
    updateSensorsPage();
};

function updateComparisonTable() {
    const checkboxes = document.querySelectorAll('.telemetry-node-cb:checked');
    const selectedNodes = Array.from(checkboxes).map(cb => cb.value);

    const thead = document.getElementById('comparison-table-head');
    const tbody = document.getElementById('comparison-table-body');
    if (!thead || !tbody) return;

    if (selectedNodes.length === 0) {
        thead.innerHTML = '';
        tbody.innerHTML = '<tr><td class="py-4 text-center text-light-gray" colspan="5">No nodes selected for comparison</td></tr>';
        return;
    }

    // Build Table Header dynamically
    let headerHTML = '<tr class="text-light-gray border-b border-[var(--border-color)]"><th class="py-2">Parameter</th>';
    selectedNodes.forEach(nodeId => {
        const node = state.nodes[nodeId];
        headerHTML += `<th class="py-2">${node ? node.name : nodeId}</th>`;
    });
    headerHTML += '<th class="py-2">Variance</th><th class="py-2">Status</th></tr>';
    thead.innerHTML = headerHTML;

    // Build Table Body dynamically
    const params = ['tds', 'ph', 'temp', 'turb'];
    const paramNames = { tds: 'Total Dissolved Solids (TDS)', ph: 'Acidity Level (pH)', temp: 'Water Temperature', turb: 'Turbidity Clarity' };
    let tbodyHTML = '';

    params.forEach(p => {
        const unit = SENSOR_CONFIGS[p].unit;
        const fixed = p === 'ph' ? 2 : p === 'turb' ? 1 : 0;
        
        let rowHTML = `<tr><td class="py-3 font-semibold text-[var(--text-primary)]">${paramNames[p]}</td>`;
        
        let minVal = Infinity;
        let maxVal = -Infinity;

        selectedNodes.forEach(nodeId => {
            const hist = state.history[nodeId] && state.history[nodeId][p];
            const val = hist ? (hist.slice(-1)[0] || 0) : 0;
            if (val < minVal) minVal = val;
            if (val > maxVal) maxVal = val;
            
            const colorClass = p === 'ph' ? 'text-soft-green' : 'text-cyan-accent';
            rowHTML += `<td class="py-3 font-mono ${colorClass}">${val.toFixed(fixed)}${unit}</td>`;
        });

        const delta = maxVal - minVal;
        rowHTML += `<td class="py-3 font-mono font-bold">${delta.toFixed(fixed)}${unit}</td>`;

        // Highlight logic based on divergence
        const isNegativeAlert = (p === 'ph' && delta > 1.0) || (p !== 'ph' && delta > (SENSOR_CONFIGS[p].max * 0.15));
        if (selectedNodes.length < 2) {
            rowHTML += `<td class="py-3"><span class="text-light-gray font-semibold">N/A</span></td>`;
        } else if (isNegativeAlert) {
            rowHTML += `<td class="py-3"><span class="text-magenta-accent font-semibold"><i class="fa-solid fa-triangle-exclamation"></i> High Variance</span></td>`;
        } else {
            rowHTML += `<td class="py-3"><span class="text-soft-green font-semibold"><i class="fa-solid fa-circle-check"></i> Consistent</span></td>`;
        }

        rowHTML += '</tr>';
        tbodyHTML += rowHTML;
    });

    tbody.innerHTML = tbodyHTML;

    // Simple trend badge based on number of nodes
    const badge = document.getElementById('overall-trend-badge');
    if (badge) {
        if (selectedNodes.length > 1) {
            badge.innerText = 'Multi-Node Active';
            badge.className = 'text-xs px-2 py-0.5 rounded bg-cyan-accent/20 text-cyan-accent font-extrabold uppercase tracking-wider';
        } else {
            badge.innerText = 'Single View';
            badge.className = 'text-xs px-2 py-0.5 rounded bg-gray-600/20 text-gray-400 font-extrabold uppercase tracking-wider';
        }
    }
}

// Analytics large chart rendering
let analyticsChartInstance = null;

function initAnalyticsPageChart() {
    const canvas = document.getElementById('analyticsChart');
    if (!canvas) return;
    
    analyticsChartInstance = new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: {
            labels: Array.from({length: historyLength}, (_, i) => `-${(historyLength-i)*2}s`),
            datasets: []
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { intersect: false, mode: 'index' },
            scales: {
                x: { grid: { color: 'rgba(255,255,255,0.03)' }, ticks: { color: '#9ca3af', font: { size: 9 } } },
                y: { grid: { color: 'rgba(255,255,255,0.03)' }, ticks: { color: '#9ca3af', font: { size: 9 } } }
            },
            plugins: {
                legend: {
                    display: true,
                    labels: { color: '#9ca3af', font: { size: 10 } }
                }
            }
        }
    });
}

function updateAnalyticsPage() {
    if (!analyticsChartInstance) return;

    const metric = document.getElementById('analytics-metric-select').value;
    const compareAll = document.getElementById('analytics-compare-nodes').checked;
    const selectedNodeId = document.getElementById('analytics-node-select').value;

    const labels = Array.from({length: historyLength}, (_, i) => `-${(historyLength-i)*2}s`);
    analyticsChartInstance.data.labels = labels;

    // Reset datasets
    analyticsChartInstance.data.datasets = [];

    const colors = {
        'node-a': '#3b82f6',
        'node-b': '#10b981',
        'node-c': '#60a5fa',
        'node-d': '#3b82f6',
        'tds': '#3b82f6',
        'ph': '#10b981',
        'temp': '#f59e0b',
        'turb': '#60a5fa'
    };

    if (compareAll) {
        // Compare one metric across all 4 nodes
        Object.keys(state.nodes).forEach(nodeId => {
            const node = state.nodes[nodeId];
            if (node.status === 'offline') return;

            let dataPath = [];
            let labelSuffix = '';
            if (metric === 'trust') {
                // Plot average trust score of all sensors
                for (let i = 0; i < historyLength; i++) {
                    let total = 0;
                    const sensors = ['tds', 'ph', 'temp', 'turb'];
                    sensors.forEach(s => {
                        const arr = state.history[nodeId][`trust_${s}`];
                        total += arr[arr.length - historyLength + i] || 100;
                    });
                    dataPath.push(Math.round(total / 4));
                }
                labelSuffix = 'Avg Trust Score';
            } else {
                dataPath = state.history[nodeId][metric].slice(-historyLength);
                labelSuffix = SENSOR_CONFIGS[metric].name;
            }

            analyticsChartInstance.data.datasets.push({
                label: `${node.name} (${labelSuffix})`,
                data: dataPath,
                borderColor: colors[nodeId],
                backgroundColor: colors[nodeId] + '05',
                borderWidth: 2,
                pointRadius: 0,
                tension: 0.3,
                fill: true
            });
        });
    } else {
        // Show all metrics of the SINGLE selected node
        const node = state.nodes[selectedNodeId];
        if (node.status === 'offline') {
            analyticsChartInstance.update();
            return;
        }

        if (metric === 'trust') {
            // Plot trust scores of all 4 sensors
            ['tds', 'ph', 'temp', 'turb'].forEach(s => {
                const data = state.history[selectedNodeId][`trust_${s}`].slice(-historyLength);
                analyticsChartInstance.data.datasets.push({
                    label: `${SENSOR_CONFIGS[s].name} AI Trust`,
                    data: data,
                    borderColor: colors[s],
                    backgroundColor: colors[s] + '05',
                    borderWidth: 2.2,
                    pointRadius: 0,
                    tension: 0.35,
                    fill: true
                });
            });
        } else {
            // Plot raw values + edge filtered values side-by-side or overlapped
            const rawData = state.history[selectedNodeId][metric].slice(-historyLength);
            const sims = state.simulators[selectedNodeId];
            const filteredData = sims[metric].filteredHistory.slice(-historyLength);

            analyticsChartInstance.data.datasets.push({
                label: `${SENSOR_CONFIGS[metric].name} Raw Value`,
                data: rawData,
                borderColor: colors[metric],
                backgroundColor: 'transparent',
                borderWidth: 1.5,
                borderDash: [5, 5],
                pointRadius: 0,
                tension: 0.2
            });

            analyticsChartInstance.data.datasets.push({
                label: `${SENSOR_CONFIGS[metric].name} Edge Output`,
                data: filteredData,
                borderColor: colors[metric],
                backgroundColor: colors[metric] + '08',
                borderWidth: 2.5,
                pointRadius: 0,
                tension: 0.35,
                fill: true
            });
        }
    }

    analyticsChartInstance.update('none');
}

// Nodes Directory Card builder
function renderNodesDirectory() {
    const grid = document.getElementById('nodes-directory-grid');
    if (!grid) return;
    grid.innerHTML = '';

    const filter = document.getElementById('node-search-filter').value;

    Object.keys(state.nodes).forEach(nodeId => {
        const node = state.nodes[nodeId];
        
        // Filter criteria
        if (filter === 'online' && node.status !== 'online') return;
        if (filter === 'offline' && node.status !== 'offline') return;

        const card = document.createElement('div');
        card.className = `glass-card group hover:scale-[1.01] flex flex-col justify-between border-l-4 ${node.status === 'online' ? 'border-l-soft-green' : 'border-l-gray-600'}`;

        // Compute average trust
        let avgTrust = 0;
        if (node.status === 'online') {
            const scores = Object.values(node.trustScores);
            avgTrust = Math.round(scores.reduce((a,b)=>a+b,0)/scores.length);
        }

        // Calculate active sensors status
        let sensorsHtml = '';
        node.sensors.forEach(s => {
            const tScore = node.status === 'online' ? node.trustScores[s] : 0;
            let dotColor = 'bg-gray-600';
            if (node.status === 'online') {
                dotColor = tScore >= 80 ? 'bg-soft-green' : tScore >= 50 ? 'bg-yellow-500' : 'bg-magenta-accent';
            }
            sensorsHtml += `
                <div class="flex items-center justify-between text-[10px] my-1">
                    <span class="text-light-gray"><i class="fa-solid ${SENSOR_CONFIGS[s].icon} mr-1"></i> ${SENSOR_CONFIGS[s].name}</span>
                    <div class="flex items-center gap-1.5">
                        <span class="w-1.5 h-1.5 rounded-full ${dotColor}"></span>
                        <span class="font-bold text-white font-mono">${node.status === 'online' ? tScore + '%' : 'Offline'}</span>
                    </div>
                </div>
            `;
        });

        // Compute active time
        let uptimeStr = 'Unreachable';
        if (node.status === 'online' && node.uptimeStart) {
            const activeMins = Math.round((Date.now() - node.uptimeStart) / 60000);
            uptimeStr = `${Math.floor(activeMins/60)}h ${activeMins%60}m`;
        }

        card.innerHTML = `
            <div>
                <div class="flex justify-between items-start mb-2">
                    <div class="cursor-pointer" onclick="selectAndJumpToTelemetry('${nodeId}')">
                        <h4 class="font-bold text-white group-hover:text-cyan-accent transition-colors">${node.name}</h4>
                        <span class="text-[9px] text-light-gray uppercase tracking-widest block mt-0.5">ID: ${node.deviceId} &bull; ${node.station}</span>
                    </div>
                    <div class="flex items-center gap-2">
                        <span class="text-[9px] font-extrabold uppercase px-2 py-0.5 rounded ${node.status === 'online' ? 'bg-soft-green/20 text-soft-green' : 'bg-gray-600/20 text-gray-500'}">
                            ${node.status.toUpperCase()}
                        </span>
                        <button class="w-7 h-7 rounded-full bg-red-500/10 border border-red-500/30 flex items-center justify-center text-red-400 hover:bg-red-500/25 hover:text-red-300 transition-all" 
                            onclick="event.stopPropagation(); deleteNode('${nodeId}')" title="Remove Node">
                            <i class="fa-solid fa-trash-can text-[10px]"></i>
                        </button>
                    </div>
                </div>

                <div class="grid grid-cols-2 gap-2 my-4 pt-3 border-t border-gray-700/20">
                    <div>
                        <span class="text-[9px] text-light-gray block uppercase">Deployed</span>
                        <span class="text-xs font-bold text-white">${node.installed}</span>
                    </div>
                    <div>
                        <span class="text-[9px] text-light-gray block uppercase">Active Timer</span>
                        <span class="text-xs font-bold text-cyan-accent">${uptimeStr}</span>
                    </div>
                </div>

                <div class="pt-3 border-t border-gray-700/20">
                    <span class="text-[9px] text-light-gray uppercase tracking-wider block mb-2">Sensors Pipeline</span>
                    ${sensorsHtml}
                </div>
            </div>

            <!-- Trust Circle Gauge footer -->
            ${node.status === 'online' ? `
            <div class="flex items-center justify-between mt-4 bg-navy-bg/30 p-2 rounded-lg border border-[var(--border-color)]">
                <span class="text-[10px] text-light-gray font-bold">NODE TRUST SCORE</span>
                <span class="font-mono font-extrabold text-sm ${avgTrust >= 80 ? 'text-soft-green' : avgTrust >= 50 ? 'text-yellow-500' : 'text-magenta-accent'}">${avgTrust}%</span>
            </div>
            ` : `
            <div class="flex items-center justify-between mt-4 bg-navy-bg/30 p-2 rounded-lg border border-[var(--border-color)]">
                <span class="text-[10px] text-light-gray font-bold">STATUS</span>
                <span class="font-mono font-extrabold text-sm text-gray-500">WAITING FOR DATA</span>
            </div>
            `}
        `;

        grid.appendChild(card);
    });
}

window.selectAndJumpToTelemetry = function(nodeId) {
    state.selectedNode = nodeId;
    // Check the corresponding checkbox if it exists
    const cb = document.querySelector(`.telemetry-node-cb[value="${nodeId}"]`);
    if (cb) cb.checked = true;
    
    // Trigger SPA Routing to Telemetry
    const btn = document.querySelector('[data-page="view-sensors"]');
    if (btn) btn.click();
};

// Delete node and clean up all associated state
window.deleteNode = function(nodeId) {
    const node = state.nodes[nodeId];
    if (!node) return;

    const nodeName = node.name;
    
    // Confirm deletion
    if (!confirm(`Remove ${nodeName} from the dashboard?\n\nThis will delete all its telemetry history and configuration.`)) {
        return;
    }

    // Remove from all state objects
    delete state.nodes[nodeId];
    delete NODES[nodeId];
    delete state.simulators[nodeId];
    delete state.history[nodeId];
    delete NODE_OFFSETS[nodeId];

    // If deleted node was the selected node, switch to first available
    if (state.selectedNode === nodeId) {
        const remaining = Object.keys(state.nodes);
        state.selectedNode = remaining.length > 0 ? remaining[0] : null;
    }

    // Remove checkbox from telemetry sidebar
    const cb = document.querySelector(`.telemetry-node-cb[value="${nodeId}"]`);
    if (cb) {
        const label = cb.closest('label');
        if (label) label.remove();
    }

    logEvent(`[ASSETS MANAGER] ✗ Node ${nodeName} removed from deployment roster.`, 'warn');
    
    // Re-render affected views
    renderNodesDirectory();
    populateAllDropdowns();
    updateStatsDOM();
    updateOverviewPage();
    if (state.activePage === 'view-sensors') {
        renderSensorCards();
        updateSensorsPage();
    }
};

// Add new node dynamically
document.getElementById('add-node-btn').addEventListener('click', () => {
    const letters = ['E', 'F', 'G', 'H'];
    const idx = Object.keys(state.nodes).length - 4;
    if (idx >= letters.length) {
        logEvent('[ASSETS MANAGER] Maximum capacity reached! Deployment pool exhausted.', 'warn');
        return;
    }

    const key = `node-${letters[idx].toLowerCase()}`;
    const name = `Node ${letters[idx]}: Canal Branch`;
    const deviceId = `EDGE-C-0${Math.floor(100+Math.random()*900)}`;
    const installDate = new Date().toISOString().split('T')[0];

    // Configure new Node state
    NODES[key] = {
        id: key,
        name: name,
        station: 'ROORKEE-BRANCH',
        deviceId: deviceId,
        installed: installDate,
        uptimeStart: Date.now(),
        status: 'online',
        sensors: ['tds', 'ph', 'temp', 'turb'],
        trustScores: { tds: 100, ph: 100, temp: 100, turb: 100 },
        lastSeen: Date.now(),
        rssi: -58
    };

    // Configure simulators & offset
    NODE_OFFSETS[key] = { tds: +10.0, ph: +0.05, temp: -0.3, turb: +5.0 };
    initNodeState(key);

    logEvent(`[ASSETS MANAGER] ✓ Successfully provisioned new asset card: ${name} (ID: ${deviceId})`, 'info');
    renderNodesDirectory();
    populateAllDropdowns();

    // Add a new checkbox for the node in telemetry sidebar
    const cbContainer = document.getElementById('telemetry-node-checkboxes');
    if (cbContainer) {
        const label = document.createElement('label');
        label.className = 'flex items-center gap-2 text-sm text-[var(--text-primary)] cursor-pointer';
        label.innerHTML = `<input type="checkbox" value="${key}" class="telemetry-node-cb rounded bg-navy-bg border-[var(--border-color)] text-cyan-accent focus:ring-0" checked> ${name}`;
        cbContainer.appendChild(label);
        // Bind change listener
        label.querySelector('input').addEventListener('change', () => {
            const checked = document.querySelectorAll('.telemetry-node-cb:checked');
            if (checked.length > 0) state.selectedNode = checked[0].value;
            renderSensorCards();
            updateSensorsPage();
        });
    }
});

// --- SPA TAB SELECT ROUTING ENGINE ---
function setupSPARoutes() {
    const buttons = document.querySelectorAll('.nav-btn');
    buttons.forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.preventDefault();
            
            buttons.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            
            const targetPage = btn.getAttribute('data-page');
            state.activePage = targetPage;
            
            document.querySelectorAll('.page-view').forEach(view => {
                view.classList.add('hidden');
            });
            
            const targetView = document.getElementById(targetPage);
            if (targetView) targetView.classList.remove('hidden');

            logEvent(`[SPA ROUTER] Navigated to ${btn.innerText} page.`, 'system');

            // Force renders
            if (targetPage === 'view-overview') {
                updateOverviewPage();
            } else if (targetPage === 'view-sensors') {
                renderSensorCards();
                updateSensorsPage();
            } else if (targetPage === 'view-analytics') {
                updateAnalyticsPage();
            } else if (targetPage === 'view-nodes') {
                renderNodesDirectory();
            } else if (targetPage === 'view-console') {
                renderFullConsoleLogs();
            }
        });
    });
}

// --- SETUP EVENT EVENT LISTENERS ---
function setupEventListeners() {
    // Cloud Toggle Outage switch
    const cloudToggle = document.getElementById('cloud-toggle');
    if (cloudToggle) {
        cloudToggle.addEventListener('change', (e) => {
            state.isCloudOnline = e.target.checked;
            const headerIcon = document.getElementById('cloud-icon-header');
            
            if (state.isCloudOnline) {
                if (headerIcon) headerIcon.className = 'fa-solid fa-cloud-arrow-up text-cyan-accent';
                logEvent('[UPLINK RESTORED] Cloud WebSocket connection established. Draining offline buffers...', 'info');
                triggerBatchSync();
            } else {
                if (headerIcon) headerIcon.className = 'fa-solid fa-cloud-arrow-up text-magenta-accent animate-pulse';
                logEvent('[NETWORK FAULT] Outage detected! Disconnected from cloud server. Buffering telemetry to SPIFFS queue...', 'error');
                setSyncStatus('buffering');
            }
        });
    }

    // Telemetry Page Node Select checkboxes
    const nodeCheckboxes = document.querySelectorAll('.telemetry-node-cb');
    nodeCheckboxes.forEach(cb => {
        cb.addEventListener('change', () => {
            const checked = document.querySelectorAll('.telemetry-node-cb:checked');
            if (checked.length > 0) {
                state.selectedNode = checked[0].value;
            }
            logEvent(`[TELEMETRY] Selection updated`, 'system');
            renderSensorCards();
            updateSensorsPage();
        });
    });

    // Comparison view toggle
    const compareToggle = document.getElementById('compare-nodes-toggle');
    const compContainer = document.getElementById('comparison-view-container');
    if (compareToggle) {
        compareToggle.addEventListener('change', (e) => {
            state.comparisonEnabled = e.target.checked;
            if (state.comparisonEnabled) {
                if (compContainer) compContainer.classList.remove('hidden');
                updateComparisonTable();
            } else {
                if (compContainer) compContainer.classList.add('hidden');
            }
        });
    }

    // Console filters
    document.getElementById('filter-all').addEventListener('click', (e) => setLogFilter('all', e.target));
    document.getElementById('filter-info').addEventListener('click', (e) => setLogFilter('info', e.target));
    document.getElementById('filter-warn').addEventListener('click', (e) => setLogFilter('warn', e.target));
    document.getElementById('filter-error').addEventListener('click', (e) => setLogFilter('error', e.target));

    // Console Action buttons
    document.getElementById('clear-console-btn').addEventListener('click', () => {
        const mainTerm = document.getElementById('main-terminal');
        if (mainTerm) mainTerm.innerHTML = '<div class="log-system">[TERMINAL REINITIALIZED]</div>';
        state.logs = [];
    });

    const pauseBtn = document.getElementById('pause-stream-btn');
    if (pauseBtn) {
        pauseBtn.addEventListener('click', () => {
            state.logPaused = !state.logPaused;
            pauseBtn.innerHTML = state.logPaused ? '<i class="fa-solid fa-play mr-1.5"></i>Resume' : '<i class="fa-solid fa-pause mr-1.5"></i>Pause';
            if (!state.logPaused) renderFullConsoleLogs();
        });
    }

    // Export console logs to file
    document.getElementById('export-log-btn').addEventListener('click', () => {
        const text = state.logs.map(log => `[${log.time}] [${log.type.toUpperCase()}] ${log.msg}`).join('\n');
        const blob = new Blob([text], { type: 'text/plain' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = `gangaedge_audit_log_${Date.now()}.txt`;
        a.click();
        logEvent('[AUDIT EXPORT] Telemetry diagnostic logs exported to diagnostic file.', 'system');
    });

    // Theme Switch toggle
    const themeBtn = document.getElementById('theme-toggle');
    if (themeBtn) {
        themeBtn.addEventListener('click', () => {
            const html = document.documentElement;
            if (html.classList.contains('dark')) {
                html.classList.remove('dark');
                html.classList.add('light');
                localStorage.setItem('theme', 'light');
                logEvent('[THEME SWITCH] Loaded Light palette.', 'system');
            } else {
                html.classList.remove('light');
                html.classList.add('dark');
                localStorage.setItem('theme', 'dark');
                logEvent('[THEME SWITCH] Loaded Dark palette.', 'system');
            }
            updateChartThemes();
        });
    }

    // Analytics dropdown listeners
    document.getElementById('analytics-node-select').addEventListener('change', () => updateAnalyticsPage());
    document.getElementById('analytics-metric-select').addEventListener('change', () => updateAnalyticsPage());
    document.getElementById('analytics-compare-nodes').addEventListener('change', () => updateAnalyticsPage());

    // Directory Node search/filter
    document.getElementById('node-search-filter').addEventListener('change', () => renderNodesDirectory());
}

function setLogFilter(filter, button) {
    state.logFilter = filter;
    
    // Switch active button colors
    const btns = button.parentNode.querySelectorAll('button');
    btns.forEach(b => {
        b.className = 'px-3 py-1 font-bold rounded-md text-light-gray';
    });
    button.className = 'px-3 py-1 font-bold rounded-md bg-cyan-accent text-slate-900';

    logEvent(`[CONSOLE] Log filters shifted to: ${filter.toUpperCase()}`, 'system');
    renderFullConsoleLogs();
}

function updateChartThemes() {
    const isLight = document.documentElement.classList.contains('light');
    const labelColor = isLight ? '#475569' : '#64748b';
    const gridColor = isLight ? 'rgba(148, 163, 184, 0.18)' : 'rgba(6, 182, 212, 0.06)';
    const chartBg = isLight ? 'rgba(255,255,255,0)' : 'rgba(0,0,0,0)';

    const miniChartOpts = {
        gridColor,
        tickColor: labelColor,
        bgColor: chartBg
    };

    // Update Overview mini charts styling
    Object.values(miniChartInstances).forEach(c => {
        c.options.scales.y.grid.color = miniChartOpts.gridColor;
        c.options.scales.y.ticks.color = miniChartOpts.tickColor;
        c.update('none');
    });

    // Update Analytics Chart styling
    if (analyticsChartInstance) {
        analyticsChartInstance.options.scales.x.grid.color = gridColor;
        analyticsChartInstance.options.scales.y.grid.color = gridColor;
        analyticsChartInstance.options.scales.x.ticks.color = labelColor;
        analyticsChartInstance.options.scales.y.ticks.color = labelColor;
        analyticsChartInstance.options.plugins.legend.labels.color = labelColor;
        analyticsChartInstance.update('none');
    }
}


function populateAllDropdowns() {
    // Populate drop downs from state (excluding telemetry which uses checkboxes now)
    const selects = [
        'node-select-tds', 'node-select-ph', 'node-select-temp', 'node-select-turb',
        'analytics-node-select'
    ];

    selects.forEach(id => {
        const el = document.getElementById(id);
        if (!el) return;
        
        // save current selection
        const val = el.value;
        el.innerHTML = '';

        Object.keys(state.nodes).forEach(key => {
            const opt = document.createElement('option');
            opt.value = key;
            opt.innerText = state.nodes[key].name;
            el.appendChild(opt);
        });

        if (val && state.nodes[val]) el.value = val;
    });
}

function initNodeState(nodeId) {
    state.simulators[nodeId] = {};
    state.history[nodeId] = {
        tds: [], ph: [], temp: [], turb: [],
        trust_tds: [], trust_ph: [], trust_temp: [], trust_turb: []
    };

    // Initialize simulators without pre-seeding fake data
    // Real data comes from MQTT or Google Sheets only
    Object.keys(SENSOR_CONFIGS).forEach(s => {
        const offset = (NODE_OFFSETS[nodeId] && NODE_OFFSETS[nodeId][s]) || 0;
        state.simulators[nodeId][s] = new SensorSimulator(SENSOR_CONFIGS[s], offset);
        // No pre-seeding — arrays start empty, filled by live MQTT or Google Sheets sync
    });
}

// --- INITIALIZATION ---
function init() {
    // 1. Pre-seed nodes state
    Object.keys(state.nodes).forEach(nodeId => {
        initNodeState(nodeId);
    });

    // 2. Setup SPA routes and event UI handlers
    setupSPARoutes();
    setupEventListeners();

    // 3. Populate dropdown options
    populateAllDropdowns();

    // 4. Initialize charts
    initOverviewCharts();
    initAnalyticsPageChart();
    
    // Fetch historical telemetry from Google Sheets database
    fetchGoogleSheetsHistory();
    
    // 5. Sync checks
    updateStatsDOM();
    setSyncStatus('idle');

    // 6. Large Terminal log pre-seed
    logEvent('[SYSTEM INIT] GangaEdge commands center running.', 'system');
    logEvent('[SYSTEM INIT] ESP32-WROOM-32E nodes listening status active.', 'system');
    logEvent('[SYSTEM INIT] One-Class SVM TinyML anomaly core calibration nominal.', 'system');
    logEvent('[WEATHER SERVICE] Listening to Roorkee Canal station updates.', 'system');

    // 7. Live Weather fetch
    fetchWeather();
    setInterval(fetchWeather, 300000); // 5 minutes polling

    // 8. Start telemetry loop
    setInterval(runSimulatedTick, 1500); // Fast 1.5s refresh for visual dashboard performance

    // 9. HiveMQ broker WebSocket Connection
    if (LIVE_MODE) {
        connectLiveMQTT();
    }
    
    // Load local stored theme preference
    if (localStorage.getItem('theme') === 'light') {
        document.documentElement.classList.remove('dark');
        document.documentElement.classList.add('light');
        updateChartThemes();
    }
}

// Bootstrapper
window.addEventListener('DOMContentLoaded', init);
