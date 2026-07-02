/*
 * =============================================================================
 *  GangaEdge Resilient Water Quality Monitor — ESP32-WROOM-32E Firmware
 * =============================================================================
 *  Sensors  : TDS (GPIO 2) | pH (GPIO 15) | Turbidity OUT (GPIO 14) |
 *             Turbidity PWM (GPIO 18) | DS18B20 Temperature (GPIO 26)
 *  Features : AI Trust Scoring (Z-Score + Delta-Rate + Stuck-Value + Bounds)
 *             WiFi + MQTT Cloud Publishing
 *             SPIFFS Offline Buffer (survives WiFi/cloud outages)
 *             Automatic Queue-Drain Sync on Reconnection
 *  Libraries: OneWire, DallasTemperature, PubSubClient,
 *             ArduinoJson (v6), SPIFFS
 * =============================================================================
 *  Install via Arduino Library Manager:
 *    - OneWire             by Paul Stoffregen
 *    - DallasTemperature   by Miles Burton
 *    - PubSubClient        by Nick O'Leary
 *    - ArduinoJson         by Benoit Blanchon  (v6.x)
 * =============================================================================
 */

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include <PubSubClient.h>
#include <SPIFFS.h>
#include <ArduinoJson.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include "OneClassSVM.h"

// ─────────────────────────────────────────────────────────────────────────────
//  NODE CONFIGURATION
// ─────────────────────────────────────────────────────────────────────────────
//#define NODE_A  // Comment out this line to compile for Node B (Downstream)

// ─────────────────────────────────────────────────────────────────────────────
//  USER CONFIGURATION  ←  Edit these before flashing
// ─────────────────────────────────────────────────────────────────────────────
const char* WIFI_SSID       = "esp";          // ← your WiFi name
const char* WIFI_PASS       = "Luci@13579";      // ← your WiFi password

// Free public HiveMQ broker — no signup needed for testing
// Replace with your own Mosquitto/EMQX broker IP for production
const char* MQTT_BROKER     = "c27fa0e9f196413ea7ea84e3cb6b1a3d.s1.eu.hivemq.cloud";
const int   MQTT_PORT       = 8883;

#ifdef NODE_A
const char* MQTT_CLIENT_ID  = "ganga-edge-node-A"; // must be unique on broker
const char* MQTT_TOPIC_DATA = "ganga-edge/node-a/sensors";    // live telemetry
const char* MQTT_TOPIC_STAT = "ganga-edge/node-a/status";     // device heartbeat
const char* MQTT_USER       = "Ganga_Node_A";
const char* MQTT_PASS       = "Luci@2112@#";
#else
const char* MQTT_CLIENT_ID  = "ganga-edge-node-B"; // must be unique on broker
const char* MQTT_TOPIC_DATA = "ganga-edge/node-b/sensors";    // live telemetry
const char* MQTT_TOPIC_STAT = "ganga-edge/node-b/status";     // device heartbeat
const char* MQTT_USER       = "Ganga_Node_B";
const char* MQTT_PASS       = "Luci@2112@#";
#endif
// DUAL-NODE ARCHITECTURE:
// Node A (upstream): publishes to 'ganga-edge/node-a/sensors'
// Node B (downstream): publishes to 'ganga-edge/node-b/sensors'
// The dashboard subscribes to both and computes pollution flow deltas.
// For single-node testing, the current topic 'ganga-edge/sensors' is used.
const char* G_SHEETS_WEBHOOK = "https://script.google.com/macros/s/AKfycbzD4xEIK1hTkBQQecZRSRx1iVYQGjxs3Fmh8o9_YcnEzEgcYwQQvIALr8BqR55RGBQI/exec";

// ─────────────────────────────────────────────────────────────────────────────
//  PIN DEFINITIONS  (from your existing prototype)
// ─────────────────────────────────────────────────────────────────────────────
#define TDS_PIN        34
#define PH_PIN         35
#define TURB_OUT_PIN   33
#define TURB_PWM_PIN   27
#define TEMP_PIN       32

// ─────────────────────────────────────────────────────────────────────────────
//  TRUST SCORING CONFIGURATION
// ─────────────────────────────────────────────────────────────────────────────
#define TRUST_WINDOW_SIZE   10    // rolling window depth (readings)
#define TRUST_ZSCORE_WARN   2.0f  // Z-score above which trust starts falling
#define TRUST_ZSCORE_FAIL   4.5f  // Z-score at which trust hits 0%
#define TRUST_STUCK_THRESH  6     // consecutive identical ticks → stuck fault
#define TRUST_DECAY_ALPHA   0.60f // fast decay when anomaly detected
#define TRUST_RECOVER_ALPHA 0.25f // slow recovery (prevents jitter)

// Safe operating bands — WHO drinking water / FAO irrigation standards
struct SensorBounds {
  float minBound;   // hard lower limit (sensor damage / physical impossibility)
  float maxBound;   // hard upper limit
  float safeMin;    // WHO/FAO safe minimum
  float safeMax;    // WHO/FAO safe maximum
  float maxDelta;   // max allowed change per reading cycle
};

const SensorBounds BOUNDS_TDS  = { 0.0f,  1200.0f,  50.0f,  600.0f,  25.0f };
const SensorBounds BOUNDS_PH   = { 3.0f,  11.0f,    6.5f,   8.5f,    0.15f };
const SensorBounds BOUNDS_TEMP = { 0.0f,  50.0f,    5.0f,   35.0f,   0.8f  };
const SensorBounds BOUNDS_TURB = { 0.0f,  500.0f,   0.0f,   100.0f,  8.0f  };

// ─────────────────────────────────────────────────────────────────────────────
//  TRUST SCORING ENGINE — mirrors JS TrustScoringEngine class exactly
// ─────────────────────────────────────────────────────────────────────────────
struct TrustEngine {
  float   window[TRUST_WINDOW_SIZE];
  int     windowCount  = 0;
  int     windowHead   = 0;       // circular buffer head index
  float   lastRaw      = NAN;
  float   rollingTrust = 100.0f;
  int     stuckCount   = 0;

  void reset() {
    windowCount  = 0;
    windowHead   = 0;
    lastRaw      = NAN;
    rollingTrust = 100.0f;
    stuckCount   = 0;
  }

  float calculate(float rawVal, const SensorBounds& b) {

    // ── Factor 0: Hard Physical Bounds (instant zero) ────────────────────────
    if (rawVal < b.minBound || rawVal > b.maxBound) {
      rollingTrust = 0.0f;
      lastRaw = rawVal;
      return 0.0f;
    }

    // Push into circular window
    window[windowHead] = rawVal;
    windowHead = (windowHead + 1) % TRUST_WINDOW_SIZE;
    if (windowCount < TRUST_WINDOW_SIZE) windowCount++;

    if (windowCount < 4) {
      // Not enough data to score — assume fully trusted
      lastRaw = rawVal;
      return 100.0f;
    }

    // ── Factor 1: Z-Score (statistical deviation from rolling mean) ──────────
    float mean = 0.0f;
    for (int i = 0; i < windowCount; i++) mean += window[i];
    mean /= windowCount;

    float variance = 0.0f;
    for (int i = 0; i < windowCount; i++)
      variance += (window[i] - mean) * (window[i] - mean);
    float stdDev = sqrtf(variance / windowCount);

    float zScoreTrust = 100.0f;
    if (stdDev > 0.0001f) {
      float zScore = fabsf(rawVal - mean) / stdDev;
      if (zScore > TRUST_ZSCORE_WARN) {
        // Linear decay: 100% at Z=2.0 → 0% at Z=4.5
        float range = TRUST_ZSCORE_FAIL - TRUST_ZSCORE_WARN;
        zScoreTrust = constrain(
          100.0f - ((zScore - TRUST_ZSCORE_WARN) / range) * 100.0f,
          0.0f, 100.0f);
      }
    }

    // ── Factor 2: Rate-of-Change (delta) check ───────────────────────────────
    float deltaTrust = 100.0f;
    if (!isnan(lastRaw)) {
      float delta = fabsf(rawVal - lastRaw);
      if (delta > b.maxDelta) {
        float excess = delta / b.maxDelta;   // e.g. 1.5 → 50% over limit
        deltaTrust = constrain(100.0f - (excess - 1.0f) * 80.0f, 0.0f, 100.0f);
      }
    }

    // ── Factor 3: Stuck-Value (zero-variance freeze) detector ───────────────
    float stuckTrust = 100.0f;
    if (!isnan(lastRaw) && fabsf(rawVal - lastRaw) < 0.0001f) {
      stuckCount++;
    } else {
      stuckCount = 0;
    }
    if (stuckCount >= TRUST_STUCK_THRESH) {
      stuckTrust = constrain(
        100.0f - (float)(stuckCount - (TRUST_STUCK_THRESH - 1)) * 20.0f,
        0.0f, 100.0f);
    }

    // ── Combine: weighted average (Z=40%, Delta=30%, Stuck=30%) ─────────────
    float rawScore = zScoreTrust * 0.40f
                   + deltaTrust  * 0.30f
                   + stuckTrust  * 0.30f;

    // ── Asymmetric EMA: fast decay, slow recovery ────────────────────────────
    float alpha = (rawScore < rollingTrust) ? TRUST_DECAY_ALPHA : TRUST_RECOVER_ALPHA;
    rollingTrust = alpha * rawScore + (1.0f - alpha) * rollingTrust;
    rollingTrust = constrain(rollingTrust, 0.0f, 100.0f);

    lastRaw = rawVal;
    return rollingTrust;
  }

  // Local decision: what value should the control loop actually use?
  // Returns filtered/fallback value and sets status string
  float decide(float rawVal, float filteredHistory[], int histLen, const char** statusOut) {
    if (rollingTrust >= 80.0f) {
      *statusOut = "trusted";
      return rawVal;
    } else if (rollingTrust >= 50.0f) {
      *statusOut = "suspect";
      // EMA noise filter: blend new reading with last good output
      float prev = (histLen > 0) ? filteredHistory[histLen - 1] : rawVal;
      return 0.40f * rawVal + 0.60f * prev;
    } else {
      *statusOut = "degraded";
      // Decouple sensor — return average of last 5 trusted filtered readings
      if (histLen == 0) return rawVal; // fallback if no history yet
      int count = min(histLen, 5);
      float sum = 0;
      for (int i = histLen - count; i < histLen; i++) sum += filteredHistory[i];
      return sum / count;
    }
  }
};

// ─────────────────────────────────────────────────────────────────────────────
//  ML MODEL INSTANTIATION
// ─────────────────────────────────────────────────────────────────────────────
Eloquent::ML::Port::GangaEdgeAnomalyDetector mlModel;

// StandardScaler feature scaling coefficients (from train_model.py)
const float SCALER_MEAN[4] = {19.042683f, 8.011034f, 379.420069f, 19.079919f};
const float SCALER_SCALE[4] = {2.484539f, 0.117477f, 8.124620f, 29.208018f};

// ─────────────────────────────────────────────────────────────────────────────
//  ML ANOMALY ATTRIBUTION ENGINE
//  Trained baseline means sourced from real GEMStat Roorkee.xlsx dataset
// ─────────────────────────────────────────────────────────────────────────────
const float ML_MEAN_TEMP = 19.04f;   // deg C  (GEMStat mean)
const float ML_MEAN_PH   =  8.01f;   // pH
const float ML_MEAN_TDS  = 371.95f;  // ppm
const float ML_MEAN_TURB = 10.86f;   // NTU

// Max possible deviations used to normalise each sensor onto the same 0-1 scale
const float ML_MAX_DEV_TEMP = 50.0f;    // full sensor range
const float ML_MAX_DEV_PH   =  8.0f;   // 3 to 11
const float ML_MAX_DEV_TDS  = 1200.0f;
const float ML_MAX_DEV_TURB = 500.0f;

// Attribution result container
struct MLAttribution {
  const char* primaryCause;   // name of the most likely offending sensor
  float confTDS;              // % probability TDS caused the anomaly
  float confPH;
  float confTemp;
  float confTurb;
  float devTDS;               // raw absolute deviation from trained mean
  float devPH;
  float devTemp;
  float devTurb;
};

// Compute which sensor contributed most to the ML anomaly decision
MLAttribution computeMLAttribution(float temp, float ph, float tds, float turb) {
  // Step 1: Absolute deviation from each trained baseline mean
  float dTemp = fabsf(temp - ML_MEAN_TEMP);
  float dPH   = fabsf(ph   - ML_MEAN_PH);
  float dTDS  = fabsf(tds  - ML_MEAN_TDS);
  float dTurb = fabsf(turb - ML_MEAN_TURB);

  // Step 2: Normalise each deviation by its maximum possible value (0-1 scale)
  float nTemp = constrain(dTemp / ML_MAX_DEV_TEMP, 0.0f, 1.0f);
  float nPH   = constrain(dPH   / ML_MAX_DEV_PH,  0.0f, 1.0f);
  float nTDS  = constrain(dTDS  / ML_MAX_DEV_TDS, 0.0f, 1.0f);
  float nTurb = constrain(dTurb / ML_MAX_DEV_TURB,0.0f, 1.0f);

  // Step 3: Convert to percentage share of total deviation
  MLAttribution attr;
  float total = nTemp + nPH + nTDS + nTurb;
  if (total < 0.0001f) {
    // All sensors at baseline (edge case)
    attr.confTemp = 25.0f; attr.confPH = 25.0f;
    attr.confTDS  = 25.0f; attr.confTurb = 25.0f;
  } else {
    attr.confTemp = (nTemp / total) * 100.0f;
    attr.confPH   = (nPH   / total) * 100.0f;
    attr.confTDS  = (nTDS  / total) * 100.0f;
    attr.confTurb = (nTurb / total) * 100.0f;
  }

  // Step 4: Store raw deviations for JSON logging
  attr.devTemp = dTemp;
  attr.devPH   = dPH;
  attr.devTDS  = dTDS;
  attr.devTurb = dTurb;

  // Step 5: Identify primary cause (sensor with highest normalised deviation)
  float maxN = max(max(nTemp, nPH), max(nTDS, nTurb));
  if      (maxN == nTDS)  attr.primaryCause = "TDS";
  else if (maxN == nPH)   attr.primaryCause = "pH";
  else if (maxN == nTurb) attr.primaryCause = "Turbidity";
  else                    attr.primaryCause = "Temperature";

  return attr;
}

// ─────────────────────────────────────────────────────────────────────────────
//  OFFLINE BUFFER (SPIFFS)
// ─────────────────────────────────────────────────────────────────────────────
#define BUFFER_FILE       "/queue.jsonl"  // newline-delimited JSON records
#define MAX_BUFFER_LINES  200             // max packets stored offline (~20KB)

// Append one JSON string as a line to the SPIFFS buffer file
bool bufferAppend(const String& jsonLine) {
  File f = SPIFFS.open(BUFFER_FILE, FILE_APPEND);
  if (!f) { Serial.println("[BUFFER] ERROR: Cannot open queue file!"); return false; }
  f.println(jsonLine);
  f.close();
  return true;
}

// Count lines in the buffer file
int bufferCount() {
  File f = SPIFFS.open(BUFFER_FILE, FILE_READ);
  if (!f) return 0;
  int count = 0;
  while (f.available()) {
    String line = f.readStringUntil('\n');
    if (line.length() > 2) count++;  // skip blank lines
  }
  f.close();
  return count;
}

// Read and remove the FIRST line from the buffer (FIFO pop)
String bufferPop() {
  File f = SPIFFS.open(BUFFER_FILE, FILE_READ);
  if (!f) return "";

  String firstLine = "";
  String remainder = "";
  bool first = true;

  while (f.available()) {
    String line = f.readStringUntil('\n');
    line.trim();
    if (line.length() < 3) continue;
    if (first) { firstLine = line; first = false; }
    else        { remainder += line + "\n"; }
  }
  f.close();

  // Write back remainder
  File fw = SPIFFS.open(BUFFER_FILE, FILE_WRITE);
  if (fw) { fw.print(remainder); fw.close(); }

  return firstLine;
}

// ─────────────────────────────────────────────────────────────────────────────
//  GLOBALS
// ─────────────────────────────────────────────────────────────────────────────
WiFiClientSecure wifiClient;
PubSubClient mqtt(wifiClient);

TrustEngine  trustTDS, trustPH, trustTemp, trustTurb;

// Per-sensor filtered history (last 10 filtered values)
#define HIST_LEN 10
float histTDS[HIST_LEN]  = {0}; int histTDSi  = 0;
float histPH[HIST_LEN]   = {0}; int histPHi   = 0;
float histTemp[HIST_LEN] = {0}; int histTempi = 0;
float histTurb[HIST_LEN] = {0}; int histTurbi = 0;

// DS18B20
OneWire         oneWire(TEMP_PIN);
DallasTemperature tempSensor(&oneWire);

// Turbidity calibration
const float VOLTAGE_DIVIDER_RATIO = 0.5f;
const int   SAMPLING_COUNT        = 100;
float       zeroVoltage           = 3.8f;

unsigned long lastReadingMs = 0;
const unsigned long READ_INTERVAL_MS = 15000;   // sensor read every 15s (4/min)
bool cloudOnline = false;

// ─────────────────────────────────────────────────────────────────────────────
//  SENSOR READING FUNCTIONS  (unchanged from your original firmware)
// ─────────────────────────────────────────────────────────────────────────────
float readPHVoltage() {
  long sum = 0;
  for (int i = 0; i < 100; i++) {
    sum += analogRead(PH_PIN);
    delay(2);
  }
  float adc     = sum / 100.0f;
  float voltage = adc * 3.3f / 4095.0f + 0.20f;  // ESP32 ADC correction
  return voltage;
}

float readTDS() {
  int   adc     = analogRead(TDS_PIN);
  float voltage = adc * 3.3f / 4095.0f;
  float tds     = (133.42f * voltage * voltage * voltage
                 - 255.86f * voltage * voltage
                 +  857.39f * voltage) * 0.5f;
  return tds;
}

float readTurbidity() {
  float adcSum = 0;
  for (int i = 0; i < SAMPLING_COUNT; i++) {
    adcSum += analogRead(TURB_OUT_PIN);
    delay(2);
  }
  float adcVoltage     = (adcSum / SAMPLING_COUNT) * (3.3f / 4095.0f);
  float measuredVoltage = adcVoltage / VOLTAGE_DIVIDER_RATIO;
  float ntu             = 112.0f * (zeroVoltage - measuredVoltage);
  return max(ntu, 0.0f);
}

float readTemperature() {
  tempSensor.requestTemperatures();
  return tempSensor.getTempCByIndex(0);
}

// ─────────────────────────────────────────────────────────────────────────────
//  HELPER: push value into fixed-size circular history array
// ─────────────────────────────────────────────────────────────────────────────
void histPush(float* arr, int& idx, float val) {
  arr[idx % HIST_LEN] = val;
  idx++;
}

// Return last N values as a flat array (most recent at end)
// Simple approach: just pass arr[] directly since we track by modular idx
int histAvailable(int idx) { return min(idx, HIST_LEN); }

// ─────────────────────────────────────────────────────────────────────────────
//  NETWORK: WiFi + MQTT connection management
// ─────────────────────────────────────────────────────────────────────────────
void connectWiFi() {
  if (WiFi.status() == WL_CONNECTED) return;
  Serial.printf("\n[WiFi] Connecting to %s", WIFI_SSID);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  int attempts = 0;
  while (WiFi.status() != WL_CONNECTED && attempts < 20) {
    delay(500); Serial.print("."); attempts++;
  }
  if (WiFi.status() == WL_CONNECTED) {
    Serial.printf("\n[WiFi] Connected. IP: %s\n", WiFi.localIP().toString().c_str());
  } else {
    Serial.println("\n[WiFi] Failed — entering offline buffer mode.");
  }
}

bool connectMQTT() {
  if (mqtt.connected()) return true;
  Serial.printf("[MQTT] Connecting to %s ...", MQTT_BROKER);
  if (mqtt.connect(MQTT_CLIENT_ID, MQTT_USER, MQTT_PASS)) {
    Serial.println(" OK");
    // Publish a status heartbeat on connect
    String statusPayload = "{\"status\":\"online\",\"device\":\"" + String(MQTT_CLIENT_ID) + "\"}";
    mqtt.publish(MQTT_TOPIC_STAT, statusPayload.c_str());
    return true;
  }
  Serial.printf(" FAILED (rc=%d)\n", mqtt.state());
  return false;
}

// ─────────────────────────────────────────────────────────────────────────────
//  PUBLISH: build JSON and either publish live or queue offline
// ─────────────────────────────────────────────────────────────────────────────
void publishOrBuffer(
    float tdsR,  float tdsF,  float tdsT,  const char* tdsS,
    float phR,   float phF,   float phT,   const char* phS,
    float tempR, float tempF, float tempT, const char* tempS,
    float turbR, float turbF, float turbT, const char* turbS,
    bool mlAnomaly, MLAttribution attr)
{
  // Build JSON payload  (1024 bytes to fit attribution sub-objects)
  StaticJsonDocument<1024> doc;
  doc["device"]    = MQTT_CLIENT_ID;
  doc["ts"]        = millis();
  doc["mlAnomaly"] = mlAnomaly;

  // Attribution fields — always logged so dashboard can chart confidence trends
  doc["mlCause"] = mlAnomaly ? attr.primaryCause : "none";

  JsonObject mlConf = doc.createNestedObject("mlConfidence");
  mlConf["tds"]  = (int)attr.confTDS;
  mlConf["ph"]   = (int)attr.confPH;
  mlConf["temp"] = (int)attr.confTemp;
  mlConf["turb"] = (int)attr.confTurb;

  JsonObject mlDev = doc.createNestedObject("mlDeviations");
  mlDev["tds_ppm"]  = serialized(String(attr.devTDS,  1));
  mlDev["ph"]       = serialized(String(attr.devPH,   2));
  mlDev["temp_c"]   = serialized(String(attr.devTemp, 1));
  mlDev["turb_ntu"] = serialized(String(attr.devTurb, 1));

  JsonObject s1 = doc.createNestedObject("tds");
  s1["raw"] = serialized(String(tdsR, 0)); s1["filtered"] = serialized(String(tdsF, 0));
  s1["trust"] = (int)tdsT; s1["status"] = tdsS;

  JsonObject s2 = doc.createNestedObject("ph");
  s2["raw"] = serialized(String(phR, 2)); s2["filtered"] = serialized(String(phF, 2));
  s2["trust"] = (int)phT; s2["status"] = phS;

  JsonObject s3 = doc.createNestedObject("temp");
  s3["raw"] = serialized(String(tempR, 1)); s3["filtered"] = serialized(String(tempF, 1));
  s3["trust"] = (int)tempT; s3["status"] = tempS;

  JsonObject s4 = doc.createNestedObject("turb");
  s4["raw"] = serialized(String(turbR, 1)); s4["filtered"] = serialized(String(turbF, 1));
  s4["trust"] = (int)turbT; s4["status"] = turbS;

  String payload;
  serializeJson(doc, payload);

  if (cloudOnline && mqtt.connected()) {
    // ── ONLINE MODE: publish directly ──────────────────────────────────────
    if (mqtt.publish(MQTT_TOPIC_DATA, payload.c_str())) {
      Serial.println("[MQTT] ✓ Published live telemetry.");
    } else {
      Serial.println("[MQTT] Publish failed — buffering.");
      bufferAppend(payload);
    }
  } else {
    // ── OFFLINE MODE: save to SPIFFS queue ─────────────────────────────────
    int queueSize = bufferCount();
    if (queueSize < MAX_BUFFER_LINES) {
      bufferAppend(payload);
      Serial.printf("[BUFFER] Queued offline (total: %d packets).\n", queueSize + 1);
    } else {
      Serial.println("[BUFFER] ⚠ Buffer full! Dropping oldest telemetry.");
      // Drop oldest by reading one and discarding
      bufferPop();
      bufferAppend(payload);
    }
  }

  // ── POST TO GOOGLE SHEETS ──────────────────────────────────────────────
  if (WiFi.status() == WL_CONNECTED) {
    HTTPClient http;
    http.begin(G_SHEETS_WEBHOOK);
    http.addHeader("Content-Type", "application/json");
    
    int httpResponseCode = http.POST(payload);
    
    if (httpResponseCode > 0) {
      Serial.printf("[HTTP] Google Sheets POST successful, Code: %d\n", httpResponseCode);
    } else {
      Serial.printf("[HTTP] Google Sheets POST failed, Error: %s\n", http.errorToString(httpResponseCode).c_str());
    }
    http.end();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
//  SYNC: drain SPIFFS buffer to MQTT when reconnected (5 Hz = 200ms / packet)
// ─────────────────────────────────────────────────────────────────────────────
void drainOfflineBuffer() {
  int remaining = bufferCount();
  if (remaining == 0) return;

  Serial.printf("[SYNC] Draining %d offline packets to cloud...\n", remaining);
  int synced = 0;

  while (bufferCount() > 0) {
    if (!mqtt.connected()) { connectMQTT(); }
    if (!mqtt.connected()) break;    // abort if MQTT drops mid-sync

    String packet = bufferPop();
    if (packet.length() < 3) continue;

    if (mqtt.publish(MQTT_TOPIC_DATA, packet.c_str())) {
      synced++;
      Serial.printf("[SYNC] ✓ Synced packet %d\n", synced);
    } else {
      // Re-queue on failure and stop
      bufferAppend(packet);
      Serial.println("[SYNC] Publish failed mid-sync — stopping.");
      break;
    }
    delay(200);  // 5 Hz throttle to avoid broker flooding
    mqtt.loop();
  }

  Serial.printf("[SYNC] Done. %d packets synced, %d remaining.\n",
                synced, bufferCount());
}

// ─────────────────────────────────────────────────────────────────────────────
//  SETUP
// ─────────────────────────────────────────────────────────────────────────────
void setup() {
  Serial.begin(115200);
  delay(500);
  Serial.println("\n═══════════════════════════════════════════");
  Serial.println("  GangaEdge Water Quality Monitor — BOOT");
  Serial.println("═══════════════════════════════════════════");

  // ── ADC configuration ──────────────────────────────────────────────────────
  analogSetPinAttenuation(PH_PIN,       ADC_11db);
  analogSetPinAttenuation(TDS_PIN,      ADC_11db);
  analogSetPinAttenuation(TURB_OUT_PIN, ADC_11db);

  // ── DS18B20 ───────────────────────────────────────────────────────────────
  tempSensor.begin();

  // ── Turbidity PWM drive ───────────────────────────────────────────────────
  pinMode(TURB_PWM_PIN, OUTPUT);
  ledcAttach(TURB_PWM_PIN, 5000, 12);
  ledcWrite(TURB_PWM_PIN, 2048);   // 50% duty cycle
  delay(2000);

  // ── Turbidity clean-water zero-point calibration ──────────────────────────
  Serial.println("[CAL] Calibrating turbidity zero-point (clean water assumed)...");
  float adcSum = 0;
  for (int i = 0; i < SAMPLING_COUNT; i++) {
    adcSum += analogRead(TURB_OUT_PIN);
    delay(10);
  }
  float adcVoltage = (adcSum / SAMPLING_COUNT) * (3.3f / 4095.0f);
  zeroVoltage      = adcVoltage / VOLTAGE_DIVIDER_RATIO;
  Serial.printf("[CAL] Turbidity zero voltage: %.3f V\n", zeroVoltage);

  // ── SPIFFS ────────────────────────────────────────────────────────────────
  if (!SPIFFS.begin(true)) {
    Serial.println("[SPIFFS] MOUNT FAILED — offline buffering disabled!");
  } else {
    Serial.printf("[SPIFFS] Mounted. Offline queue: %d packets pending.\n",
                  bufferCount());
  }

  // ── WiFi + MQTT ───────────────────────────────────────────────────────────
  connectWiFi();
  wifiClient.setInsecure(); // Required to connect to HiveMQ Cloud TLS port 8883 without storing Root CA
  mqtt.setServer(MQTT_BROKER, MQTT_PORT);
  mqtt.setBufferSize(512);

  bool mqttOk = connectMQTT();
  cloudOnline = (WiFi.status() == WL_CONNECTED && mqttOk);

  if (cloudOnline) {
    Serial.println("[NET] Cloud uplink ONLINE.");
    drainOfflineBuffer();   // clear any previously stored packets immediately
  } else {
    Serial.println("[NET] Cloud uplink OFFLINE — entering Edge-Autonomous mode.");
  }

  Serial.println("[SYSTEM] Ready. Starting sensor loop.\n");
}

// ─────────────────────────────────────────────────────────────────────────────
//  LOOP
// ─────────────────────────────────────────────────────────────────────────────
void loop() {
  // Keep MQTT alive
  if (mqtt.connected()) mqtt.loop();

  // ── Periodic connectivity check & sync trigger ────────────────────────────
  bool wifiOk = (WiFi.status() == WL_CONNECTED);
  if (!wifiOk) connectWiFi();

  bool mqttOk = wifiOk && mqtt.connected();
  if (wifiOk && !mqttOk) mqttOk = connectMQTT();

  bool prevCloudOnline = cloudOnline;
  cloudOnline = wifiOk && mqttOk;

  // ── Outage recovery: drain buffer when cloud just came back online ────────
  if (!prevCloudOnline && cloudOnline) {
    Serial.println("[NET] ✓ Cloud uplink RESTORED! Triggering offline sync...");
    drainOfflineBuffer();
  }

  // ── Sensor reading cycle (every READ_INTERVAL_MS) ─────────────────────────
  if (millis() - lastReadingMs < READ_INTERVAL_MS) return;
  lastReadingMs = millis();

  // ──────────────────────── Read Raw Sensor Values ─────────────────────────
  float rawTemp = readTemperature();
  float rawTDS  = readTDS();
  float rawPH   = 5.77f * readPHVoltage() - 4.60f;  // linear pH calibration
  float rawTurb = readTurbidity();

  // ──────────────────────── AI Trust Scoring ───────────────────────────────
  float trustTDSval  = trustTDS.calculate(rawTDS,  BOUNDS_TDS);
  float trustPHval   = trustPH.calculate(rawPH,    BOUNDS_PH);
  float trustTempval = trustTemp.calculate(rawTemp, BOUNDS_TEMP);
  float trustTurbval = trustTurb.calculate(rawTurb, BOUNDS_TURB);

  // ──────────────────────── Edge Local Decision ────────────────────────────
  const char *staTDS, *staPH, *staTemp, *staTurb;

  float filtTDS  = trustTDS.decide(rawTDS,   histTDS,  histAvailable(histTDSi),  &staTDS);
  float filtPH   = trustPH.decide(rawPH,     histPH,   histAvailable(histPHi),   &staPH);
  float filtTemp = trustTemp.decide(rawTemp,  histTemp, histAvailable(histTempi), &staTemp);
  float filtTurb = trustTurb.decide(rawTurb,  histTurb, histAvailable(histTurbi), &staTurb);

  // Push filtered values into history for next cycle
  histPush(histTDS,  histTDSi,  filtTDS);
  histPush(histPH,   histPHi,   filtPH);
  histPush(histTemp, histTempi, filtTemp);
  histPush(histTurb, histTurbi, filtTurb);

  // ──────────────────────── ML Inference + Attribution ────────────────────
  float mlFeatures[4] = {rawTemp, rawPH, rawTDS, rawTurb};
  // Apply StandardScaler normalization (must match Python training pipeline)
  for (int i = 0; i < 4; i++) {
    mlFeatures[i] = (mlFeatures[i] - SCALER_MEAN[i]) / SCALER_SCALE[i];
  }
  bool mlAnomaly = (mlModel.predict(mlFeatures) == 1);
  MLAttribution attr = computeMLAttribution(rawTemp, rawPH, rawTDS, rawTurb);

  Serial.println("────────────────────────────────────────────");
  if (mlAnomaly) {
    Serial.printf("  [TinyML] ANOMALY DETECTED  -->  Primary Cause: %s\n", attr.primaryCause);
    Serial.printf("  [TinyML] Confidence  TDS:%2.0f%%  pH:%2.0f%%  Temp:%2.0f%%  Turb:%2.0f%%\n",
                  attr.confTDS, attr.confPH, attr.confTemp, attr.confTurb);
    Serial.printf("  [TinyML] Deviations  TDS:%.1fppm  pH:%.2f  Temp:%.1fC  Turb:%.1fNTU\n",
                  attr.devTDS, attr.devPH, attr.devTemp, attr.devTurb);
  } else {
    Serial.println("  [TinyML] Normal (0)  —  All parameters within learned baseline");
  }
  Serial.printf("  TDS         Raw: %4.0f ppm  Filtered: %4.0f ppm  Trust: %3.0f%%  [%s]\n",
                rawTDS,  filtTDS,  trustTDSval,  staTDS);
  Serial.printf("  pH          Raw: %.2f       Filtered: %.2f       Trust: %3.0f%%  [%s]\n",
                rawPH,   filtPH,   trustPHval,   staPH);
  Serial.printf("  Temperature Raw: %.1f °C    Filtered: %.1f °C    Trust: %3.0f%%  [%s]\n",
                rawTemp, filtTemp, trustTempval, staTemp);
  Serial.printf("  Turbidity   Raw: %.1f NTU   Filtered: %.1f NTU   Trust: %3.0f%%  [%s]\n",
                rawTurb, filtTurb, trustTurbval, staTurb);
  Serial.printf("  Cloud: %s  |  Buffer: %d packets pending\n",
                cloudOnline ? "ONLINE ✓" : "OFFLINE ⚠", bufferCount());
  Serial.println("────────────────────────────────────────────\n");

  // ──────────────────────── Warn on Safe-Range Breaches ────────────────────
  if (filtTDS  < BOUNDS_TDS.safeMin  || filtTDS  > BOUNDS_TDS.safeMax)
    Serial.printf("  ⚠ TDS UNSAFE: %.0f ppm (safe: %.0f–%.0f)\n",
                  filtTDS, BOUNDS_TDS.safeMin, BOUNDS_TDS.safeMax);
  if (filtPH   < BOUNDS_PH.safeMin   || filtPH   > BOUNDS_PH.safeMax)
    Serial.printf("  ⚠ pH UNSAFE: %.2f (safe: %.1f–%.1f)\n",
                  filtPH, BOUNDS_PH.safeMin, BOUNDS_PH.safeMax);
  if (filtTemp < BOUNDS_TEMP.safeMin || filtTemp > BOUNDS_TEMP.safeMax)
    Serial.printf("  ⚠ Temperature UNSAFE: %.1f °C (safe: %.0f–%.0f)\n",
                  filtTemp, BOUNDS_TEMP.safeMin, BOUNDS_TEMP.safeMax);
  if (filtTurb > BOUNDS_TURB.safeMax)
    Serial.printf("  ⚠ Turbidity UNSAFE: %.1f NTU (safe: <%.0f)\n",
                  filtTurb, BOUNDS_TURB.safeMax);

  // ──────────────────────── Publish or Buffer ───────────────────────────────
  publishOrBuffer(
    rawTDS,  filtTDS,  trustTDSval,  staTDS,
    rawPH,   filtPH,   trustPHval,   staPH,
    rawTemp, filtTemp, trustTempval, staTemp,
    rawTurb, filtTurb, trustTurbval, staTurb,
    mlAnomaly, attr
  );
}
