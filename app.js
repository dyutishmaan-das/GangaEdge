/* ==========================================================================
   IoT-Edge - Resilient IoT Edge & Trust Scoring JS Engine
   ========================================================================== */

// --- CONFIGURATION ---
const CONFIG = {
    projectName: 'GangaEdge',
    projectTitle: 'Ganga Canal Water Quality Monitoring',
    author: 'Dyutishmaan Das',
    affiliation: 'IIT Roorkee Summer Internship Project'
};

// --- LIVE MODE CONFIGURATION ---
// Set to true to receive REAL data from ESP32 via HiveMQ Cloud
// Set to false to use the built-in simulator for demo/testing
const LIVE_MODE = true;

const MQTT_CONFIG = {
    // HiveMQ Cloud WebSocket endpoint (port 8884 for wss://)
    brokerUrl: 'wss://c27fa0e9f196413ea7ea84e3cb6b1a3d.s1.eu.hivemq.cloud:8884/mqtt',
    username: 'Ganga_Node_A',
    password: 'Luci@2112@#',
    topic: 'ganga-edge/sensors',
    clientId: 'gangaedge-dashboard-' + Math.random().toString(16).substr(2, 8)
};

// --- GLOBAL STATE ---
let isCloudOnline = true;
let isSyncing = false;
let weatherContext = 'clear';
let systemTick = 0;
const historyLength = 30; // Chart history seconds

let localQueue = [];
let cloudDb = [];
let sensors = [];
let trustChartInstance = null;
let mqttClient = null;  // Live MQTT client instance

// --- SENSOR CONFIGURATION ---
// Calibrated to actual ESP32-WROOM-32E prototype hardware sensors
// Reference ranges: WHO drinking water guidelines & FAO irrigation water standards
const SENSOR_CONFIGS = [
    {
        id: 'tds',
        name: 'TDS',
        fullName: 'Total Dissolved Solids',
        unit: ' ppm',
        // Normal irrigation water: 200–500 ppm | Drinking: 50–300 ppm | WHO limit: 600 ppm
        baseline: 320.0,
        amplitude: 45.0,
        noiseStd: 8.0,
        // TDS changes slowly (dissolution/dilution process)
        maxExpectedDelta: 25.0,
        minBound: 0.0,
        maxBound: 1200.0,   // >1200 ppm: dangerously saline for crops
        safeMin: 50.0,
        safeMax: 600.0,
        icon: 'fa-flask',
        colorHsl: 'hsl(217, 91%, 60%)',
        colorRgba: 'rgba(59, 130, 246, 0.08)'
    },
    {
        id: 'ph',
        name: 'pH',
        fullName: 'Acidity / Alkalinity',
        unit: ' pH',
        // Ideal irrigation: 6.5–7.5 | Drinking water: 6.5–8.5 | WHO guideline
        baseline: 7.1,
        amplitude: 0.25,
        noiseStd: 0.04,
        // pH in water bodies changes slowly (buffering capacity)
        maxExpectedDelta: 0.15,
        minBound: 3.0,      // Below 3: corrosive acid
        maxBound: 11.0,     // Above 11: caustic alkali
        safeMin: 6.5,
        safeMax: 8.5,
        icon: 'fa-vial',
        colorHsl: 'hsl(152, 76%, 50%)',
        colorRgba: 'rgba(16, 185, 129, 0.08)'
    },
    {
        id: 'temp',
        name: 'Temperature',
        fullName: 'Water Temperature (DS18B20)',
        unit: '°C',
        // Typical surface water / irrigation channel temperature
        baseline: 26.5,
        amplitude: 2.8,
        noiseStd: 0.12,
        // DS18B20 resolution 0.0625°C — water temp changes slowly
        maxExpectedDelta: 0.8,
        minBound: 0.0,      // Freezing point
        maxBound: 50.0,     // Above 50°C: sensor damage threshold
        safeMin: 5.0,
        safeMax: 35.0,
        icon: 'fa-thermometer-half',
        colorHsl: 'hsl(37, 98%, 53%)',
        colorRgba: 'rgba(245, 158, 11, 0.08)'
    },
    {
        id: 'turb',
        name: 'Turbidity',
        fullName: 'Water Clarity (Turbidity)',
        unit: ' NTU',
        // WHO drinking water guideline: <1 NTU | Irrigation acceptable: <100 NTU
        baseline: 4.5,
        amplitude: 1.8,
        noiseStd: 0.5,
        // Turbidity can spike quickly after rainfall or sediment disturbance
        maxExpectedDelta: 8.0,
        minBound: 0.0,
        maxBound: 500.0,    // Above 500 NTU: visibly muddy, sensors saturate
        safeMin: 0.0,
        safeMax: 100.0,
        icon: 'fa-water',
        colorHsl: 'hsl(280, 85%, 60%)',
        colorRgba: 'rgba(168, 85, 247, 0.08)'
    }
];

// --- SENSOR SIMULATOR CLASS ---
class SensorSimulator {
    constructor(config) {
        this.config = config;
        this.currentFault = 'normal';
        this.tickCounter = 0;
        this.driftAccumulator = 0;
        this.stuckValue = null;
        this.history = [];
        this.filteredHistory = [];
        this.trustEngine = new TrustScoringEngine(config);
    }

    generateValue() {
        this.tickCounter++;
        let rawVal = 0;

        // Base sine wave fluctuation representing normal diurnal/cyclical operations
        const sinVal = this.config.baseline + Math.sin(this.tickCounter * 0.05) * this.config.amplitude;
        
        // Add default normal thermal/electrical noise
        const normalNoise = (Math.random() - 0.5) * 2 * this.config.noiseStd;
        rawVal = sinVal + normalNoise;

        // Apply Injected Fault Types
        switch (this.currentFault) {
            case 'normal':
                this.driftAccumulator = 0;
                this.stuckValue = null;
                break;
                
            case 'drift':
                // Steady linear deterioration (e.g. sensor decalibration or build-up)
                this.driftAccumulator += this.config.maxExpectedDelta * 0.5;
                rawVal += this.driftAccumulator;
                break;
                
            case 'spike':
                // Single huge transient pulse (e.g., electrical surge or physical impact)
                // Randomly occurs 30% of the time, otherwise remains normal
                if (Math.random() < 0.3) {
                    rawVal += this.config.amplitude * 4;
                }
                this.stuckValue = null;
                break;
                
            case 'stuck':
                // Sensor gets frozen (e.g., frozen ADC, lockup, dead communication)
                if (this.stuckValue === null) {
                    this.stuckValue = rawVal;
                }
                rawVal = this.stuckValue;
                break;
                
            case 'noise':
                // Highly erratic readings (e.g. loose connection, extreme EMF)
                const extremeNoise = (Math.random() - 0.5) * 2 * (this.config.noiseStd * 10);
                rawVal = sinVal + extremeNoise;
                this.stuckValue = null;
                break;
        }

        // Store history (max 50 points)
        this.history.push(rawVal);
        if (this.history.length > 50) this.history.shift();

        return rawVal;
    }

    setFault(faultName) {
        this.currentFault = faultName;
        // Reset counters when switching faults
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

        // If not enough data, trust is high until calibrated
        if (this.window.length < 5) {
            this.lastRaw = rawVal;
            return 100;
        }

        // Factor 1: Hard Bounds Checks
        if (rawVal < this.config.minBound || rawVal > this.config.maxBound) {
            this.rollingTrust = 0.0;
            this.lastRaw = rawVal;
            return 0; // Immediate failure
        }

        // Factor 2: Rolling Statistics & Z-Score
        const mean = this.window.reduce((a, b) => a + b, 0) / this.window.length;
        const variance = this.window.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / this.window.length;
        const stdDev = Math.sqrt(variance);

        let zScoreTrust = 100;
        if (stdDev > 0.001) {
            let zScore = Math.abs(rawVal - mean) / stdDev;
            
            // Weather mitigation: Relax Z-score statistical bounds during rain for turbidity
            if (this.config.id === 'turb' && typeof weatherContext !== 'undefined' && weatherContext === 'rain') {
                zScore = zScore / 2.5; // significantly reduce Z-score penalty
            }

            // Z-score thresholding
            if (zScore > 2.0) {
                // Decay trust score linearly between Z=2 (100% trust) and Z=4.5 (0% trust)
                zScoreTrust = Math.max(0, 100 - ((zScore - 2.0) / 2.5) * 100);
            }
        }

        // Factor 3: Rate of Change (Delta) Check
        let deltaTrust = 100;
        if (this.lastRaw !== null) {
            const delta = Math.abs(rawVal - this.lastRaw);
            let maxExpected = this.config.maxExpectedDelta;

            // Weather mitigation: Allow 4x faster rate of change for turbidity during rain
            if (this.config.id === 'turb' && typeof weatherContext !== 'undefined' && weatherContext === 'rain') {
                maxExpected = maxExpected * 4.0;
            }

            if (delta > maxExpected) {
                // Decay trust based on how much it exceeds the max allowed delta
                const excess = delta / maxExpected;
                deltaTrust = Math.max(0, 100 - (excess - 1.0) * 80);
            }
        }

        // Factor 4: Stuck Value Check (Zero variance over time)
        let stuckTrust = 100;
        if (this.lastRaw !== null && Math.abs(rawVal - this.lastRaw) < 0.0001) {
            this.consecutiveIdentical++;
        } else {
            this.consecutiveIdentical = 0;
        }

        // If stuck for more than 5 consecutive ticks, start dropping trust quickly
        if (this.consecutiveIdentical >= 6) {
            stuckTrust = Math.max(0, 100 - (this.consecutiveIdentical - 5) * 20);
        }

        // Combine Factors into a raw weighted score
        // 40% Z-score statistical alignment, 30% rate-of-change, 30% stuck/frozen checking
        const rawScore = (zScoreTrust * 0.40) + (deltaTrust * 0.30) + (stuckTrust * 0.30);

        // Exponential smoothing to prevent sudden flickering, but allow fast decay
        let decayAlpha = rawScore < this.rollingTrust ? 0.6 : 0.25; // Decays faster than it recovers

        // Weather mitigation: Slow down decay rate during rain for turbidity
        if (this.config.id === 'turb' && typeof weatherContext !== 'undefined' && weatherContext === 'rain') {
            decayAlpha = rawScore < this.rollingTrust ? 0.20 : 0.25;
        }

        this.rollingTrust = (decayAlpha * rawScore) + ((1 - decayAlpha) * this.rollingTrust);

        // Cap trust bounds
        this.rollingTrust = Math.max(0, Math.min(100, this.rollingTrust));

        this.lastRaw = rawVal;
        return Math.round(this.rollingTrust);
    }
}

// --- LOCAL DECISION ENGINE ---
class LocalDecisionEngine {
    static processReading(sensor, rawVal, trustScore) {
        let status = 'trusted';
        let filteredVal = rawVal;
        let actionMsg = '';

        if (trustScore >= 80) {
            status = 'trusted';
            filteredVal = rawVal;
        } 
        else if (trustScore >= 50) {
            status = 'suspect';
            // Smooth reading: Apply exponential moving average to filter out high noise
            const prevFiltered = sensor.filteredHistory.length > 0 ? sensor.filteredHistory[sensor.filteredHistory.length - 1] : rawVal;
            filteredVal = (0.4 * rawVal) + (0.6 * prevFiltered);
            actionMsg = `[WARNING] ${sensor.config.name} trust degraded to ${trustScore}%. Applying EMA noise filter.`;
        } 
        else {
            status = 'degraded';
            // Critical degradation: Decouple sensor and inject forecasted backup value
            // Calculated as the average of the last 5 trusted readings (or baseline if none exist)
            const trustedReadings = sensor.filteredHistory.slice(-5);
            if (trustedReadings.length > 0) {
                filteredVal = trustedReadings.reduce((a, b) => a + b, 0) / trustedReadings.length;
            } else {
                filteredVal = sensor.config.baseline;
            }
            actionMsg = `[CRITICAL] ${sensor.config.name} trust score at ${trustScore}%! DECOUPLING sensor. Injecting local fallback: ${filteredVal.toFixed(2)}${sensor.config.unit}.`;
        }

        sensor.filteredHistory.push(filteredVal);
        if (sensor.filteredHistory.length > 50) sensor.filteredHistory.shift();

        return { status, filteredVal, actionMsg };
    }
}

// --- WEATHER INTEGRATION ---
function getWeatherInfo(code) {
    if (code === 0) return { icon: '☀️', desc: 'Clear Sky' };
    if ([1, 2, 3].includes(code)) return { icon: '⛅', desc: 'Partly Cloudy' };
    if ([45, 48].includes(code)) return { icon: '🌫️', desc: 'Foggy' };
    if ([51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82].includes(code)) return { icon: '🌧️', desc: 'Rainy' };
    if ([71, 73, 75, 77, 85, 86].includes(code)) return { icon: '❄️', desc: 'Snowy' };
    if ([95, 96, 99].includes(code)) return { icon: '⛈️', desc: 'Thunderstorm' };
    return { icon: '⛅', desc: 'Cloudy' };
}

async function fetchWeather() {
    const url = "https://api.open-meteo.com/v1/forecast?latitude=29.8543&longitude=77.8880&current=temperature_2m,precipitation,rain,weathercode,windspeed_10m&timezone=Asia%2FKolkata";
    try {
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        
        if (data && data.current) {
            const current = data.current;
            const temp = current.temperature_2m;
            const precip = current.precipitation;
            const rain = current.rain;
            const code = current.weathercode;
            const wind = current.windspeed_10m;
            
            // Map weathercode to WMO description
            const info = getWeatherInfo(code);
            
            // Update UI elements
            document.getElementById('weather-icon').innerText = info.icon;
            document.getElementById('weather-temp').innerText = temp.toFixed(1);
            document.getElementById('weather-precipitation').innerText = `${precip.toFixed(1)} mm`;
            document.getElementById('weather-rain').innerText = `${rain.toFixed(1)} mm`;
            document.getElementById('weather-wind').innerText = `${wind.toFixed(1)} km/h`;
            
            // Check weather context: rain > 0.5mm OR precipitation > 1.0mm
            if (rain > 0.5 || precip > 1.0) {
                weatherContext = 'rain';
                const badge = document.getElementById('weather-badge');
                if (badge) {
                    badge.innerText = 'MONSOON ACTIVE';
                    badge.className = 'badge red';
                }
                
                const banner = document.getElementById('weather-banner-turb');
                if (banner) banner.style.display = 'flex';
                
                appendTerminal(`[WEATHER ALERT] Rain detected in Roorkee (${rain.toFixed(1)} mm). Relaxing turbidity anomaly thresholds.`, 'info');
            } else {
                weatherContext = 'clear';
                const badge = document.getElementById('weather-badge');
                if (badge) {
                    badge.innerText = 'CLEAR';
                    badge.className = 'badge green';
                }
                
                const banner = document.getElementById('weather-banner-turb');
                if (banner) banner.style.display = 'none';
            }
        }
    } catch (error) {
        console.error("Failed to fetch weather data: ", error);
        appendTerminal(`[WEATHER ERROR] Failed to fetch live weather data: ${error.message}`, 'system');
    }
}

// =============================================================================
// --- DUAL-NODE POLLUTION FLOW ANALYSIS (Node A Upstream → Node B Downstream) ---
// =============================================================================

// Node B downstream baselines: slightly elevated due to agricultural runoff,
// sediment accumulation, and canal-side effluents between the two monitoring points.
const NODE_B_OFFSETS = {
    tds:  { baselineDelta: +38.0, noiseMult: 1.15 },   // +38 ppm avg from runoff
    ph:   { baselineDelta: -0.12, noiseMult: 1.08 },   // slightly acidic from CO₂
    temp: { baselineDelta: +0.4,  noiseMult: 1.0  },   // slightly warmer downstream
    turb: { baselineDelta: +18.0, noiseMult: 1.25 }    // more suspended sediment
};

// Sensor ranges used for progress bar % calculation
const SENSOR_DISPLAY_RANGES = {
    tds:  { min: 0,  max: 900 },
    ph:   { min: 4,  max: 10 },
    temp: { min: 15, max: 40 },
    turb: { min: 0,  max: 200 }
};

// TRANSIT_TICKS: How many 1-second ticks represent the flow transit time A→B.
// For a canal with ~0.4 m/s flow speed and ~360 m spacing between nodes,
// water takes ~900 s ≈ 15 minutes. For the demo we use 15 ticks (15 s).
const TRANSIT_TICKS = 15;

class DualNodeAnalyzer {
    constructor() {
        // Ring buffers — hold last TRANSIT_TICKS readings for Node A
        this.bufferA = { tds: [], ph: [], temp: [], turb: [] };
        // Latest Node B values (generated each tick)
        this.lastB = { tds: null, ph: null, temp: null, turb: null };
        // Smoothed deltas (exponential moving average, α=0.3)
        this.smoothDelta = { tds: 0, ph: 0, temp: 0, turb: 0 };
        this.ready = false;
    }

    /** Feed current Node A tick data (object with id → raw value) */
    updateNodeA(tickData) {
        tickData.forEach(d => {
            if (this.bufferA[d.id] !== undefined) {
                this.bufferA[d.id].push(d.raw);
                if (this.bufferA[d.id].length > TRANSIT_TICKS + 2) {
                    this.bufferA[d.id].shift();
                }
            }
        });
        if (this.bufferA.tds.length >= TRANSIT_TICKS) this.ready = true;
    }

    /** Simulate Node B reading: Node A baseline-shifted + independent noise */
    simulateNodeB(tickData) {
        tickData.forEach(d => {
            if (NODE_B_OFFSETS[d.id] !== undefined) {
                const off = NODE_B_OFFSETS[d.id];
                // Correlated with Node A but with its own noise
                const noise = (Math.random() - 0.5) * 2 * off.noiseMult;
                let val = d.raw + off.baselineDelta + noise;
                // Weather boosts turbidity at both nodes if raining
                if (d.id === 'turb' && weatherContext === 'rain') val += 12;
                this.lastB[d.id] = val;
            }
        });
    }

    /**
     * Compute per-sensor deltas using transit-time-aligned comparison:
     * compare current Node B reading vs Node A reading from TRANSIT_TICKS ago.
     */
    computeDeltas() {
        const deltas = {};
        const α = 0.3; // EMA smoothing factor
        ['tds', 'ph', 'temp', 'turb'].forEach(id => {
            const bufA = this.bufferA[id];
            const refA = bufA.length >= TRANSIT_TICKS ? bufA[bufA.length - TRANSIT_TICKS] : bufA[0];
            const valB = this.lastB[id];
            if (refA !== undefined && valB !== null) {
                const raw = valB - refA;
                this.smoothDelta[id] = α * raw + (1 - α) * this.smoothDelta[id];
            }
            deltas[id] = this.smoothDelta[id];
        });
        return deltas;
    }

    /**
     * Weighted pollution index:
     * Weights: TDS 35%, Turbidity 35%, pH 20%, Temperature 10%
     * Positive = pollution increasing downstream; Negative = decreasing.
     */
    computePollutionIndex(deltas) {
        const norm = {
            tds:  deltas.tds  / 200,   // scale: 200 ppm range
            ph:   -deltas.ph  / 2,     // pH drop → acidification → more polluted
            temp: deltas.temp / 5,
            turb: deltas.turb / 80
        };
        return 0.35 * norm.tds + 0.20 * norm.ph + 0.10 * norm.temp + 0.35 * norm.turb;
    }

    /** Render all dual-node DOM elements */
    render(tickData) {
        this.updateNodeA(tickData);
        this.simulateNodeB(tickData);
        const deltas = this.computeDeltas();
        const pollIdx = this.computePollutionIndex(deltas);

        // ── Transit time display ──
        document.getElementById('transit-time-display').textContent = `Transit: ${TRANSIT_TICKS}s`;

        // ── Overall trend badge ──
        const badge = document.getElementById('overall-trend-badge');
        if (pollIdx > 0.08) {
            badge.textContent = '⬆ POLLUTION INCREASING';
            badge.className = 'badge badge-red';
        } else if (pollIdx < -0.08) {
            badge.textContent = '⬇ POLLUTION DECREASING';
            badge.className = 'badge badge-green';
        } else {
            badge.textContent = '↔ STABLE';
            badge.className = 'badge badge-amber';
        }

        // ── Per-sensor rows ──
        const sensorMeta = {
            tds:  { unit: 'ppm',  fixed: 0 },
            ph:   { unit: '',     fixed: 2 },
            temp: { unit: '°C',   fixed: 1 },
            turb: { unit: 'NTU',  fixed: 1 }
        };

        tickData.forEach(d => {
            const id = d.id;
            const meta = sensorMeta[id];
            if (!meta) return;

            const valA = d.raw;
            const valB = this.lastB[id];
            const delta = deltas[id];
            const range = SENSOR_DISPLAY_RANGES[id];

            if (valB === null) return;

            // Progress bar widths (clamped 2–98%)
            const pctA = Math.min(98, Math.max(2, ((valA - range.min) / (range.max - range.min)) * 100));
            const pctB = Math.min(98, Math.max(2, ((valB - range.min) / (range.max - range.min)) * 100));

            const barA = document.getElementById(`${id}-bar-a`);
            const barB = document.getElementById(`${id}-bar-b`);
            const valElA = document.getElementById(`${id}-val-a`);
            const valElB = document.getElementById(`${id}-val-b`);
            const deltaEl = document.getElementById(`${id}-delta`);

            if (barA) barA.style.width = pctA.toFixed(1) + '%';
            if (barB) {
                barB.style.width = pctB.toFixed(1) + '%';
                // Color node B bar: red if higher pollution, green if cleaner
                const increasing = id === 'ph' ? delta < -0.05 : delta > 0;
                barB.style.background = increasing
                    ? 'linear-gradient(90deg, hsl(0,78%,55%), hsl(15,90%,50%))'
                    : 'linear-gradient(90deg, hsl(142,70%,42%), hsl(160,70%,38%))';
            }
            if (valElA) valElA.textContent = valA.toFixed(meta.fixed) + meta.unit;
            if (valElB) valElB.textContent = valB.toFixed(meta.fixed) + meta.unit;

            if (deltaEl) {
                const sign = delta >= 0 ? '+' : '';
                const arrow = delta > 0.02 ? ' ↑' : delta < -0.02 ? ' ↓' : ' ↔';
                const isWorseDownstream = id === 'ph' ? delta < -0.05 : delta > 0.05;
                deltaEl.textContent = `${sign}${delta.toFixed(meta.fixed)}${meta.unit}${arrow}`;
                deltaEl.className = isWorseDownstream
                    ? 'comp-delta text-red'
                    : Math.abs(delta) < 0.05 ? 'comp-delta text-amber' : 'comp-delta text-green';
            }
        });
    }
}

// Singleton analyzer instance
const dualNodeAnalyzer = new DualNodeAnalyzer();

// --- LIVE MQTT CONNECTION ---
function connectLiveMQTT() {
    appendTerminal(`[MQTT] Connecting to HiveMQ Cloud via WebSocket...`, 'system');

    mqttClient = mqtt.connect(MQTT_CONFIG.brokerUrl, {
        username: MQTT_CONFIG.username,
        password: MQTT_CONFIG.password,
        clientId: MQTT_CONFIG.clientId,
        protocol: 'wss',
        reconnectPeriod: 5000,
        connectTimeout: 10000
    });

    mqttClient.on('connect', () => {
        appendTerminal(`[MQTT] ✓ Connected to HiveMQ Cloud! Subscribing to '${MQTT_CONFIG.topic}'...`, 'info');
        mqttClient.subscribe(MQTT_CONFIG.topic, (err) => {
            if (!err) {
                appendTerminal(`[MQTT] ✓ Subscribed. Waiting for live ESP32 data...`, 'info');
                // Update header to show LIVE status
                const edgeLabel = document.querySelector('#node-sensors .node-meta');
                if (edgeLabel) edgeLabel.textContent = 'LIVE ESP32 Data';
            } else {
                appendTerminal(`[MQTT] ✗ Subscribe failed: ${err.message}`, 'error');
            }
        });
    });

    mqttClient.on('message', (topic, message) => {
        try {
            const payload = JSON.parse(message.toString());
            handleLivePacket(payload);
        } catch (e) {
            appendTerminal(`[MQTT] Parse error: ${e.message}`, 'error');
        }
    });

    mqttClient.on('error', (err) => {
        appendTerminal(`[MQTT] Connection error: ${err.message}`, 'error');
    });

    mqttClient.on('reconnect', () => {
        appendTerminal(`[MQTT] Reconnecting to HiveMQ Cloud...`, 'warn');
    });

    mqttClient.on('offline', () => {
        appendTerminal(`[MQTT] Broker connection lost. Buffering locally.`, 'warn');
    });
}

// --- HANDLE LIVE ESP32 PACKET ---
function handleLivePacket(data) {
    systemTick++;
    let activeFaultsCount = 0;

    // Map ESP32 JSON fields to the dashboard's sensor rendering format
    const sensorMap = [
        { key: 'tds',  idx: 0 },
        { key: 'ph',   idx: 1 },
        { key: 'temp', idx: 2 },
        { key: 'turb', idx: 3 }
    ];

    const tickData = [];

    sensorMap.forEach(({ key, idx }) => {
        const sensorData = data[key];
        if (!sensorData) return;

        const sensor = sensors[idx];
        const raw = parseFloat(sensorData.raw);
        const filtered = parseFloat(sensorData.filtered);
        const trust = parseInt(sensorData.trust);
        const status = sensorData.status; // 'trusted', 'suspect', 'degraded'

        tickData.push({
            id: sensor.config.id,
            name: sensor.config.name,
            raw: raw,
            filtered: filtered,
            trust: trust,
            status: status,
            unit: sensor.config.unit
        });

        if (status !== 'trusted') activeFaultsCount++;

        // Update DOM elements for sensor cards (reuse existing rendering)
        updateSensorDOM(sensor, raw, filtered, trust, status);
    });

    // Update active fault statistics
    document.getElementById('stat-faults').innerText = `${activeFaultsCount} currently degraded/suspect`;

    // ML Anomaly terminal logging
    if (data.mlAnomaly) {
        const conf = data.mlConfidence || {};
        appendTerminal(
            `[TinyML] ⚠ ANOMALY → Cause: ${data.mlCause} | Conf: TDS:${conf.tds||0}% pH:${conf.ph||0}% Temp:${conf.temp||0}% Turb:${conf.turb||0}%`,
            'error'
        );
    } else if (systemTick % 3 === 0) {
        appendTerminal(`[ESP32 LIVE] All parameters within learned baseline. Device: ${data.device || 'node-A'}`, 'info');
    }

    // Cloud DB record
    const packetTime = new Date().toLocaleTimeString();
    tickData.forEach(td => {
        const pkt = { timestamp: packetTime, sensorId: td.id, sensorName: td.name,
                      rawVal: td.raw, filteredVal: td.filtered, trust: td.trust,
                      status: td.status, unit: td.unit };
        cloudDb.unshift(pkt);
        if (cloudDb.length > 50) cloudDb.pop();
    });
    updateCloudDbDOM();

    // Update statistics panels
    document.getElementById('stat-buffer').innerText = localQueue.length;
    document.getElementById('stat-cloud-records').innerText = cloudDb.length;
    document.getElementById('queue-size-badge').innerText = `${localQueue.length} items`;
    document.getElementById('cloud-size-badge').innerText = `${cloudDb.length} entries`;

    // Animate data flow particles
    spawnDataPacket('particles-edge', 'normal');
    setTimeout(() => spawnDataPacket('particles-cloud', 'cloud'), 600);

    // Update trust history chart
    updateChart(tickData);

    // Dual-node pollution flow analysis
    dualNodeAnalyzer.render(tickData);
}

// --- INITIALIZATION ---
function init() {
    // Dynamically inject configuration values
    document.title = `${CONFIG.projectName} - Weather-Aware Resilient IoT Edge Command Center`;
    document.getElementById('brand-name').innerText = CONFIG.projectName;
    document.getElementById('brand-subtitle').innerHTML = `${CONFIG.projectTitle} &bull; Weather-Aware Trust Scoring`;
    document.getElementById('edge-node-title').innerText = `${CONFIG.projectName} Server`;
    document.getElementById('footer-text').innerHTML = `${CONFIG.projectName} Framework Proof-of-Concept &bull; Developed by ${CONFIG.author} &bull; ${CONFIG.affiliation}`;

    // Initialize virtual terminal content
    const term = document.getElementById('edge-terminal');
    term.innerHTML = '';
    appendTerminal(`[SYSTEM INIT] ${CONFIG.projectName} Server initialized. Water Quality Monitoring Mode ACTIVE.`, 'system');
    appendTerminal(`[SYSTEM INIT] Sensors online: TDS (ppm) | pH (0–14) | Temperature (°C) | Turbidity (NTU)`, 'system');
    appendTerminal(`[SYSTEM INIT] AI Trust Scoring loaded — Z-Score, Delta-Rate, Stuck-Value, Bound-Check thresholds calibrated.`, 'system');
    appendTerminal(`[SYSTEM INIT] WHO & FAO water quality reference ranges registered. Local fallback values ready.`, 'system');
    appendTerminal(`[WEATHER INIT] Weather-Aware monitoring enabled for Roorkee Canal (Lat 29.8543, Lon 77.8880).`, 'system');
    appendTerminal(`[DUAL-NODE INIT] Node A (Upstream) + Node B (Downstream) pollution flow analysis ACTIVE. Transit window: ${TRANSIT_TICKS}s.`, 'system');

    // Generate Sensor instances (needed for config references in both modes)
    SENSOR_CONFIGS.forEach(cfg => {
        sensors.push(new SensorSimulator(cfg));
    });

    // Render static elements
    renderSensorCards();
    setupEventListeners();
    initChart();
    
    // Fetch live weather context (Roorkee coords)
    fetchWeather();
    setInterval(fetchWeather, 600000); // Poll every 10 minutes

    if (LIVE_MODE) {
        // ── LIVE MODE: Connect to HiveMQ Cloud and receive real ESP32 data ──
        appendTerminal(`[MODE] ★ LIVE MODE ACTIVE — Connecting to real ESP32 via HiveMQ Cloud...`, 'system');
        connectLiveMQTT();
        // The systemTickLoop is NOT started — data arrives via MQTT messages
    } else {
        // ── SIMULATOR MODE: Use built-in sensor simulators for demo ──
        appendTerminal(`[MODE] Simulator mode — generating synthetic sensor data.`, 'system');
        setInterval(systemTickLoop, 1000);
    }
}

// --- MAIN LOOP ---
function systemTickLoop() {
    systemTick++;
    let activeFaultsCount = 0;
    
    // 1. Read sensors, compute AI trust, process local decisions
    const tickData = [];
    
    sensors.forEach((sensor, index) => {
        const raw = sensor.generateValue();
        const trust = sensor.trustEngine.calculateTrust(raw);
        const { status, filteredVal, actionMsg } = LocalDecisionEngine.processReading(sensor, raw, trust);
        
        tickData.push({
            id: sensor.config.id,
            name: sensor.config.name,
            raw: raw,
            filtered: filteredVal,
            trust: trust,
            status: status,
            unit: sensor.config.unit
        });

        if (status !== 'trusted') activeFaultsCount++;

        // Update DOM elements for sensor cards
        updateSensorDOM(sensor, raw, filteredVal, trust, status);

        // Log actions to the virtual Edge Terminal
        if (actionMsg !== '') {
            appendTerminal(actionMsg, status === 'suspect' ? 'warn' : 'error');
        } else if (systemTick % 4 === 0) {
            // General Info logs every few ticks
            appendTerminal(`[INFO] ${sensor.config.name} nominal. Value: ${filteredVal.toFixed(2)}${sensor.config.unit} (Trust: ${trust}%)`, 'info');
        }
    });

    // Update active fault statistics
    document.getElementById('stat-faults').innerText = `${activeFaultsCount} currently degraded/suspect`;

    // 2. Queue or sync data based on connectivity status
    const packetTime = new Date().toLocaleTimeString();
    
    tickData.forEach(data => {
        const telemetryPacket = {
            timestamp: packetTime,
            sensorId: data.id,
            sensorName: data.name,
            rawVal: data.raw,
            filteredVal: data.filtered,
            trust: data.trust,
            status: data.status,
            unit: data.unit
        };

        if (isCloudOnline) {
            // Uplink is healthy: Transmit data straight to Cloud
            cloudDb.unshift(telemetryPacket);
            if (cloudDb.length > 50) cloudDb.pop();
            
            // Render cloud db update
            updateCloudDbDOM();
            
            // Visual flow animations (green/blue packets)
            spawnDataPacket('particles-edge', data.status === 'degraded' ? 'faulty' : 'normal');
            setTimeout(() => {
                spawnDataPacket('particles-cloud', data.status === 'degraded' ? 'faulty' : 'cloud');
            }, 600);
        } else {
            // Outage Mode: Buffer telemetry in local queue
            localQueue.push(telemetryPacket);
            if (localQueue.length > 500) {
                localQueue.shift(); // Hard limit on buffer
                appendTerminal(`[BUFFER FULL] Outage buffer capacity reached! Dropping oldest telemetry packet.`, 'error');
            }
            
            updateEdgeQueueDOM();
            spawnDataPacket('particles-edge', data.status === 'degraded' ? 'faulty' : 'normal');
            // Packet flow stops at Edge node - no particles to Cloud
        }
    });

    // Update statistics panels
    document.getElementById('stat-buffer').innerText = localQueue.length;
    document.getElementById('stat-cloud-records').innerText = cloudDb.length;
    document.getElementById('queue-size-badge').innerText = `${localQueue.length} items`;
    document.getElementById('cloud-size-badge').innerText = `${cloudDb.length} entries`;

    // 3. Update trust history chart
    updateChart(tickData);

    // 4. Dual-node pollution flow analysis (Node A → Node B)
    dualNodeAnalyzer.render(tickData);
}

// --- EVENT HANDLERS ---
function setupEventListeners() {
    // Cloud Outage Toggle Switch
    const cloudToggle = document.getElementById('cloud-toggle');
    const cloudStatusText = document.getElementById('cloud-status-text');
    const syncStatusText = document.getElementById('sync-status-text');
    const syncIcon = document.getElementById('sync-icon');
    const flowLine = document.getElementById('flow-edge-to-cloud');
    const cloudNode = document.getElementById('node-cloud');
    const cloudMeta = document.getElementById('cloud-meta-text');

    cloudToggle.addEventListener('change', (e) => {
        isCloudOnline = e.target.checked;
        if (isCloudOnline) {
            // Reconnected
            cloudStatusText.innerText = "CONNECTED";
            cloudStatusText.className = "lbl-bottom text-green";
            flowLine.classList.remove('offline');
            cloudNode.classList.remove('disconnected');
            cloudMeta.innerText = "Sync Target";
            
            appendTerminal(`[OUTAGE RECOVERY] Cloud uplink connection RESTORED. Triggering automatic batch sync...`, 'info');
            triggerBatchSync();
        } else {
            // Outage started
            cloudStatusText.innerText = "OFFLINE (OUTAGE)";
            cloudStatusText.className = "lbl-bottom text-red";
            syncStatusText.innerText = "BUFFERING";
            syncIcon.className = "sync-state-dot buffering";
            flowLine.classList.add('offline');
            cloudNode.classList.add('disconnected');
            cloudMeta.innerText = "CONNECTION LOSS";
            
            appendTerminal(`[NETWORK OUTAGE] Cloud uplink lost! ${CONFIG.projectName} entering Edge-Autonomous mode. Buffering local telemetry to queue.`, 'warn');
        }
    });

    // Clear Terminal button
    document.getElementById('clear-terminal').addEventListener('click', () => {
        const term = document.getElementById('edge-terminal');
        term.innerHTML = '<div class="terminal-line system-line">[SYSTEM LOGS CLEARED]</div>';
    });
}

// --- SYNC ENGINE ---
function triggerBatchSync() {
    if (localQueue.length === 0) {
        document.getElementById('sync-status-text').innerText = "IDLE";
        document.getElementById('sync-icon').className = "sync-state-dot idle";
        return;
    }

    isSyncing = true;
    document.getElementById('sync-status-text').innerText = "SYNCING";
    document.getElementById('sync-icon').className = "sync-state-dot syncing";

    // 5 Hz syncing - 1 packet popped every 200ms
    const syncInterval = setInterval(() => {
        if (!isCloudOnline) {
            // Outage occurred mid-sync
            clearInterval(syncInterval);
            isSyncing = false;
            return;
        }

        if (localQueue.length > 0) {
            const packet = localQueue.shift();
            cloudDb.unshift(packet);
            if (cloudDb.length > 100) cloudDb.pop(); // keep DB tidy

            // Update stats
            document.getElementById('stat-buffer').innerText = localQueue.length;
            document.getElementById('stat-cloud-records').innerText = cloudDb.length;
            document.getElementById('queue-size-badge').innerText = `${localQueue.length} items`;
            document.getElementById('cloud-size-badge').innerText = `${cloudDb.length} entries`;

            updateEdgeQueueDOM();
            updateCloudDbDOM();

            // Spawn visual sync packets going to cloud
            spawnDataPacket('particles-cloud', packet.status === 'degraded' ? 'faulty' : 'cloud');
        } else {
            // Sync complete
            clearInterval(syncInterval);
            isSyncing = false;
            document.getElementById('sync-status-text').innerText = "IDLE";
            document.getElementById('sync-icon').className = "sync-state-dot idle";
            appendTerminal(`[SYNC COMPLETE] Successfully uploaded all offline buffered telemetry records.`, 'info');
        }
    }, 200);
}

// --- UI RENDERING METHODS ---

function renderSensorCards() {
    const container = document.getElementById('sensor-container');
    container.innerHTML = '';

    sensors.forEach((sensor, index) => {
        const cfg = sensor.config;
        // Determine decimal places per sensor type
        const decimals = cfg.id === 'ph' ? 2 : cfg.id === 'turb' ? 1 : cfg.id === 'tds' ? 0 : 1;
        const card = document.createElement('div');
        card.className = 'sensor-card trusted';
        card.id = `sensor-card-${cfg.id}`;
        card.innerHTML = `
            <div class="sensor-card-header">
                <span class="sensor-name"><i class="fa-solid ${cfg.icon}"></i> ${cfg.name}</span>
                <span id="badge-${cfg.id}" class="sensor-badge trusted">Trusted</span>
            </div>
            <div class="sensor-full-name">${cfg.fullName}</div>
            ${cfg.id === 'turb' ? `<div id="weather-banner-turb" class="weather-banner" style="display: none;"><i class="fa-solid fa-cloud-showers-heavy text-amber"></i> Rain Detected — Turbidity spike expected. Trust penalty reduced.</div>` : ''}
            <div class="sensor-readings">
                <span id="raw-${cfg.id}" class="sensor-raw">--</span>
                <div class="sensor-filtered">
                    <span class="lbl-top">Edge Output</span>
                    <span id="filtered-${cfg.id}">--</span>
                </div>
            </div>
            <div class="safe-range-row">
                <span class="safe-range-label">Safe Range</span>
                <span class="safe-range-value">${cfg.safeMin} – ${cfg.safeMax}${cfg.unit}</span>
                <span id="range-flag-${cfg.id}" class="range-flag"></span>
            </div>
            <div class="sensor-trust-section">
                <div class="trust-label-row">
                    <span>AI Trust Score</span>
                    <span id="trust-val-${cfg.id}">100%</span>
                </div>
                <div class="trust-bar-bg">
                    <div id="trust-bar-${cfg.id}" class="trust-bar-fill" style="width: 100%"></div>
                </div>
            </div>
            <div class="fault-controls">
                <span class="fault-title">Fault Injection Model</span>
                <div class="fault-btn-group">
                    <button class="btn-fault active" data-fault="normal" onclick="injectFault('${cfg.id}', 'normal', this)">Normal</button>
                    <button class="btn-fault" data-fault="noise" onclick="injectFault('${cfg.id}', 'noise', this)">Noise</button>
                    <button class="btn-fault" data-fault="spike" onclick="injectFault('${cfg.id}', 'spike', this)">Spike</button>
                    <button class="btn-fault" data-fault="stuck" onclick="injectFault('${cfg.id}', 'stuck', this)">Stuck</button>
                    <button class="btn-fault" style="grid-column: span 2" data-fault="drift" onclick="injectFault('${cfg.id}', 'drift', this)">Linear Drift</button>
                </div>
            </div>
        `;
        container.appendChild(card);
    });
}

// Bind to window to allow button onclick triggers
window.injectFault = function(sensorId, faultName, btnElement) {
    const sensor = sensors.find(s => s.config.id === sensorId);
    if (!sensor) return;

    sensor.setFault(faultName);
    
    // Toggle active classes in card UI
    const card = document.getElementById(`sensor-card-${sensorId}`);
    const buttons = card.querySelectorAll('.btn-fault');
    buttons.forEach(btn => btn.classList.remove('active'));
    btnElement.classList.add('active');

    if (faultName === 'normal') {
        appendTerminal(`[FAULT CLEAR] ${sensor.config.name} set to normal mode. Recalibrating trust levels.`, 'info');
    } else {
        appendTerminal(`[FAULT INJECT] Activating ${faultName.toUpperCase()} fault on ${sensor.config.name} sensor.`, 'warn');
    }
};

function updateSensorDOM(sensor, raw, filtered, trust, status) {
    const id = sensor.config.id;
    const cfg = sensor.config;
    const rawEl = document.getElementById(`raw-${id}`);
    const filtEl = document.getElementById(`filtered-${id}`);
    const trustValEl = document.getElementById(`trust-val-${id}`);
    const trustBarEl = document.getElementById(`trust-bar-${id}`);
    const badgeEl = document.getElementById(`badge-${id}`);
    const cardEl = document.getElementById(`sensor-card-${id}`);
    const rangeFlagEl = document.getElementById(`range-flag-${id}`);

    // Determine decimal precision per sensor
    const dec = id === 'ph' ? 2 : id === 'turb' ? 1 : id === 'tds' ? 0 : 1;

    // Update raw value text
    rawEl.innerText = `${raw.toFixed(dec)}${cfg.unit}`;

    // Safe range check — color raw value red if outside safe band
    const outsideSafe = raw < cfg.safeMin || raw > cfg.safeMax;
    rawEl.style.color = outsideSafe ? 'var(--red)' : 'var(--text-primary)';
    if (rangeFlagEl) {
        if (outsideSafe) {
            rangeFlagEl.innerText = '⚠ UNSAFE';
            rangeFlagEl.style.color = 'var(--red)';
            appendTerminal(`[ALERT] ${cfg.name} reading (${raw.toFixed(dec)}${cfg.unit}) outside safe range (${cfg.safeMin}-${cfg.safeMax}${cfg.unit}).`, 'warn');
        } else {
            rangeFlagEl.innerText = '✓ OK';
            rangeFlagEl.style.color = 'var(--green)';
        }
    }

    // Update filtered value text (Edge decisions)
    filtEl.innerText = `${filtered.toFixed(dec)}${cfg.unit}`;
    
    // Update Trust numerical label & color fill
    trustValEl.innerText = `${trust}%`;
    trustBarEl.style.width = `${trust}%`;
    
    // Update dynamic classes depending on status
    if (id === 'turb' && typeof weatherContext !== 'undefined' && weatherContext === 'rain' && (status === 'suspect' || status === 'degraded')) {
        cardEl.className = `sensor-card ${status} rain-mitigated`;
    } else {
        cardEl.className = `sensor-card ${status}`;
    }
    
    badgeEl.innerText = status === 'degraded' ? 'Degraded/Faulty' : status;
    badgeEl.className = `sensor-badge ${status}`;

    // Color gradient shifts on trust score
    if (trust >= 80) {
        trustBarEl.style.backgroundColor = 'var(--green)';
    } else if (trust >= 50) {
        trustBarEl.style.backgroundColor = 'var(--amber)';
    } else {
        trustBarEl.style.backgroundColor = 'var(--red)';
    }
}

function spawnDataPacket(containerId, packetClass) {
    const container = document.getElementById(containerId);
    if (!container) return;

    const packet = document.createElement('div');
    packet.className = `packet ${packetClass}-packet`;
    container.appendChild(packet);

    // Remove packet after animation ends
    setTimeout(() => {
        packet.remove();
    }, 1500);
}

function appendTerminal(msg, type) {
    const term = document.getElementById('edge-terminal');
    const line = document.createElement('div');
    const time = new Date().toLocaleTimeString();
    
    // Style specific output parts
    let styledMsg = msg;
    if (type === 'warn') {
        line.className = 'terminal-line warn-line';
        styledMsg = msg.replace(/(\b\d+\.?\d*%\b|\b\w+ degraded\b)/g, '<span class="action-val">$1</span>');
    } else if (type === 'error') {
        line.className = 'terminal-line error-line';
        styledMsg = msg.replace(/(DECOUPLING|CRITICAL|stuck|\b\d+\.?\d*%\b)/g, '<span class="action-val">$1</span>');
    } else if (type === 'info') {
        line.className = 'terminal-line info-line';
        styledMsg = msg.replace(/(\b\d+\.?\d*°C\b|\b\d+\.?\d*mm\/s\b|\b\d+\.?\d*kPa\b|\b\d+\.?\d*V\b|passed|upload|RESTORED)/gi, '<span class="action-val">$1</span>');
    } else {
        line.className = 'terminal-line system-line';
    }

    line.innerHTML = `[${time}] ${styledMsg}`;
    term.appendChild(line);
    
    // Auto-scroll to bottom
    term.scrollTop = term.scrollHeight;
}

function updateEdgeQueueDOM() {
    const body = document.getElementById('edge-queue-body');
    if (localQueue.length === 0) {
        body.innerHTML = '<tr><td colspan="4" class="empty-placeholder">Queue empty. Edge transmitting directly to Cloud.</td></tr>';
        return;
    }

    // Show only the 10 newest items in the buffer for visual rendering efficiency
    let rowsHtml = '';
    const slice = localQueue.slice(-10).reverse();
    slice.forEach(pkt => {
        const statusBadge = `<span class="sensor-badge ${pkt.status}">${pkt.status}</span>`;
        rowsHtml += `
            <tr>
                <td>${pkt.timestamp}</td>
                <td>${pkt.sensorName}</td>
                <td>${pkt.filteredVal.toFixed(2)}${pkt.unit}</td>
                <td>${statusBadge}</td>
            </tr>
        `;
    });
    body.innerHTML = rowsHtml;
}

function updateCloudDbDOM() {
    const body = document.getElementById('cloud-db-body');
    if (cloudDb.length === 0) {
        body.innerHTML = '<tr><td colspan="4" class="empty-placeholder">No cloud telemetry logged yet.</td></tr>';
        return;
    }

    // Show only the 10 newest entries in the Cloud DB
    let rowsHtml = '';
    const slice = cloudDb.slice(0, 10);
    slice.forEach(pkt => {
        rowsHtml += `
            <tr>
                <td>${pkt.timestamp}</td>
                <td>${pkt.sensorName}</td>
                <td>${pkt.filteredVal.toFixed(2)}${pkt.unit}</td>
                <td style="font-family:var(--font-mono); font-weight:700;" class="${pkt.trust < 50 ? 'text-red' : pkt.trust < 80 ? 'text-amber' : 'text-green'}">${pkt.trust}%</td>
            </tr>
        `;
    });
    body.innerHTML = rowsHtml;
}

// --- CHART MANAGEMENT ---
let chartLabels = [];
let chartDataSets = { tds: [], ph: [], temp: [], turb: [] };

function initChart() {
    const ctx = document.getElementById('trustChart').getContext('2d');
    
    // Seed labels (30 seconds) with 100% initial trust
    for (let i = historyLength; i > 0; i--) {
        chartLabels.push(`-${i}s`);
        chartDataSets.tds.push(100);
        chartDataSets.ph.push(100);
        chartDataSets.temp.push(100);
        chartDataSets.turb.push(100);
    }
    chartLabels.push('Now');

    trustChartInstance = new Chart(ctx, {
        type: 'line',
        data: {
            labels: chartLabels,
            datasets: [
                {
                    label: 'TDS (ppm)',
                    borderColor: 'hsl(217, 91%, 60%)',
                    backgroundColor: 'rgba(59, 130, 246, 0.06)',
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.3,
                    data: chartDataSets.tds
                },
                {
                    label: 'pH',
                    borderColor: 'hsl(152, 76%, 50%)',
                    backgroundColor: 'rgba(16, 185, 129, 0.06)',
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.3,
                    data: chartDataSets.ph
                },
                {
                    label: 'Temperature (°C)',
                    borderColor: 'hsl(37, 98%, 53%)',
                    backgroundColor: 'rgba(245, 158, 11, 0.06)',
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.3,
                    data: chartDataSets.temp
                },
                {
                    label: 'Turbidity (NTU)',
                    borderColor: 'hsl(280, 85%, 60%)',
                    backgroundColor: 'rgba(168, 85, 247, 0.06)',
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.3,
                    data: chartDataSets.turb
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false } // Custom legend in HTML
            },
            scales: {
                x: {
                    grid: { color: 'rgba(255, 255, 255, 0.03)' },
                    ticks: { color: 'hsl(215, 16%, 47%)', font: { size: 9 } }
                },
                y: {
                    min: 0,
                    max: 105,
                    grid: { color: 'rgba(255, 255, 255, 0.03)' },
                    ticks: { color: 'hsl(215, 16%, 47%)', font: { size: 9, family: 'Fira Code' } }
                }
            }
        }
    });
}

function updateChart(tickData) {
    if (!trustChartInstance) return;

    // Shift data into rolling window per sensor ID
    tickData.forEach(d => {
        if (chartDataSets[d.id] !== undefined) {
            chartDataSets[d.id].push(d.trust);
            if (chartDataSets[d.id].length > historyLength + 1) {
                chartDataSets[d.id].shift();
            }
        }
    });

    // Update dataset references to match new sensor IDs
    trustChartInstance.data.datasets[0].data = chartDataSets.tds;
    trustChartInstance.data.datasets[1].data = chartDataSets.ph;
    trustChartInstance.data.datasets[2].data = chartDataSets.temp;
    trustChartInstance.data.datasets[3].data = chartDataSets.turb;
    
    trustChartInstance.update('none'); // silent update without reset transitions
}

// Start app
window.addEventListener('DOMContentLoaded', init);
