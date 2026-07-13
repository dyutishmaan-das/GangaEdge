/**
 * Google Apps Script for ESP32 Telemetry
 * 
 * DEPLOYMENT INSTRUCTIONS:
 * 1. Open Google Sheets and create a new spreadsheet.
 * 2. Go to Extensions > Apps Script.
 * 3. Delete any existing code and paste this entire script.
 * 4. Save the script (Ctrl+S or Cmd+S).
 * 5. Click "Deploy" > "New deployment" in the top right corner.
 * 6. Select type: "Web app".
 * 7. Set "Execute as" to "Me".
 * 8. Set "Who has access" to "Anyone".
 * 9. Click "Deploy" and authorize the app if prompted.
 * 10. Copy the generated "Web app URL" (this is your Webhook URL).
 * 11. Paste the URL into your ESP32 firmware: const char* G_SHEETS_WEBHOOK = "YOUR_WEBHOOK_URL_HERE";
 *
 * COLUMN LAYOUT (in order):
 *  A  Timestamp
 *  B  Device
 *  C  Temperature (°C)       — filtered sensor reading
 *  D  pH                     — filtered sensor reading
 *  E  TDS (ppm)              — filtered sensor reading
 *  F  Turbidity (NTU)        — filtered sensor reading
 *  G  Trust_TDS (%)          — per-sensor trust score
 *  H  Trust_pH (%)
 *  I  Trust_Temp (%)
 *  J  Trust_Turb (%)
 *  K  Trust_Avg (%)          — average trust score across all sensors
 *  L  mlAnomaly              — true / false
 *  M  mlCause                — e.g. "TDS", "pH", "none"
 *  N  mlConf_TDS (%)         — ML confidence that TDS caused anomaly
 *  O  mlConf_pH (%)
 *  P  mlConf_Temp (%)
 *  Q  mlConf_Turb (%)
 *  R  mlDev_TDS_ppm          — deviation from expected value
 *  S  mlDev_pH
 *  T  mlDev_Temp_C
 *  U  mlDev_Turb_NTU
 */

// ─── Header definition ───────────────────────────────────────────────────────
var HEADERS = [
  "Timestamp",
  "Device",
  "Temperature (°C)",
  "pH",
  "TDS (ppm)",
  "Turbidity (NTU)",
  "Trust_TDS (%)",
  "Trust_pH (%)",
  "Trust_Temp (%)",
  "Trust_Turb (%)",
  "Trust_Avg (%)",
  "mlAnomaly",
  "mlCause",
  "mlConf_TDS (%)",
  "mlConf_pH (%)",
  "mlConf_Temp (%)",
  "mlConf_Turb (%)",
  "mlDev_TDS_ppm",
  "mlDev_pH",
  "mlDev_Temp_C",
  "mlDev_Turb_NTU"
];

// ─── Helper: safe nested get ─────────────────────────────────────────────────
/**
 * Safely retrieves a value from a nested object path.
 * Returns `fallback` (default "") if any key along the path is missing/null.
 * 
 * Example: safeGet(data, ["mlConfidence", "tds"]) → data.mlConfidence.tds
 */
function safeGet(obj, keys, fallback) {
  if (fallback === undefined) fallback = "";
  var current = obj;
  for (var i = 0; i < keys.length; i++) {
    if (current == null || typeof current !== "object" || !(keys[i] in current)) {
      return fallback;
    }
    current = current[keys[i]];
  }
  return current !== null && current !== undefined ? current : fallback;
}

// ─── Main handler ─────────────────────────────────────────────────────────────
function doPost(e) {
  try {
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();

    // ── Parse incoming JSON payload from the ESP32 ──────────────────────────
    var jsonString = e.postData.contents;
    var doc = JSON.parse(jsonString);

    // ── Timestamp & device ──────────────────────────────────────────────────
    var timestamp = new Date();
    var device    = doc.device || "Unknown";

    // ── Sensor filtered readings ────────────────────────────────────────────
    var temp = safeGet(doc, ["temp", "filtered"]);
    var ph   = safeGet(doc, ["ph",   "filtered"]);
    var tds  = safeGet(doc, ["tds",  "filtered"]);
    var turb = safeGet(doc, ["turb", "filtered"]);

    // ── Per-sensor trust scores ─────────────────────────────────────────────
    var trustTDS  = safeGet(doc, ["tds",  "trust"]);
    var trustPH   = safeGet(doc, ["ph",   "trust"]);
    var trustTemp = safeGet(doc, ["temp", "trust"]);
    var trustTurb = safeGet(doc, ["turb", "trust"]);

    // Average trust score (only computed when all four are present)
    var trustAvg = "";
    if (trustTDS !== "" && trustPH !== "" && trustTemp !== "" && trustTurb !== "") {
      trustAvg = (trustTDS + trustPH + trustTemp + trustTurb) / 4;
    }

    // ── ML attribution fields ───────────────────────────────────────────────
    // doc.mlAnomaly  → boolean (true/false)
    // doc.mlCause    → string  (e.g. "TDS", "pH", "none")
    // doc.mlConfidence.tds  / .ph / .temp / .turb  → number (0–100 %)
    // doc.mlDeviation.tds   / .ph / .temp / .turb  → number (deviation value)
    var mlAnomaly  = safeGet(doc, ["mlAnomaly"]);
    var mlCause    = safeGet(doc, ["mlCause"]);
    var mlConfTDS  = safeGet(doc, ["mlConfidence", "tds"]);
    var mlConfPH   = safeGet(doc, ["mlConfidence", "ph"]);
    var mlConfTemp = safeGet(doc, ["mlConfidence", "temp"]);
    var mlConfTurb = safeGet(doc, ["mlConfidence", "turb"]);
    var mlDevTDS   = safeGet(doc, ["mlDeviations", "tds_ppm"]);
    var mlDevPH    = safeGet(doc, ["mlDeviations", "ph"]);
    var mlDevTemp  = safeGet(doc, ["mlDeviations", "temp_c"]);
    var mlDevTurb  = safeGet(doc, ["mlDeviations", "turb_ntu"]);

    // ── Create header row on first run ──────────────────────────────────────
    if (sheet.getLastRow() === 0) {
      sheet.appendRow(HEADERS);

      // Optional: bold the header row for readability
      sheet.getRange(1, 1, 1, HEADERS.length).setFontWeight("bold");
    }

    // ── Append data row ─────────────────────────────────────────────────────
    sheet.appendRow([
      timestamp,   // A  Timestamp
      device,      // B  Device
      temp,        // C  Temperature (°C)
      ph,          // D  pH
      tds,         // E  TDS (ppm)
      turb,        // F  Turbidity (NTU)
      trustTDS,    // G  Trust_TDS (%)
      trustPH,     // H  Trust_pH (%)
      trustTemp,   // I  Trust_Temp (%)
      trustTurb,   // J  Trust_Turb (%)
      trustAvg,    // K  Trust_Avg (%)
      mlAnomaly,   // L  mlAnomaly
      mlCause,     // M  mlCause
      mlConfTDS,   // N  mlConf_TDS (%)
      mlConfPH,    // O  mlConf_pH (%)
      mlConfTemp,  // P  mlConf_Temp (%)
      mlConfTurb,  // Q  mlConf_Turb (%)
      mlDevTDS,    // R  mlDev_TDS_ppm
      mlDevPH,     // S  mlDev_pH
      mlDevTemp,   // T  mlDev_Temp_C
      mlDevTurb    // U  mlDev_Turb_NTU
    ]);

    // ── Success response ────────────────────────────────────────────────────
    return ContentService
      .createTextOutput(JSON.stringify({ status: "success", message: "Data logged successfully" }))
      .setMimeType(ContentService.MimeType.JSON);

  } catch (error) {
    // ── Error response ──────────────────────────────────────────────────────
    return ContentService
      .createTextOutput(JSON.stringify({ status: "error", message: error.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

// ─── GET handler for Dashboard History retrieval ──────────────────────────────
function doGet(e) {
  try {
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    var lastRow = sheet.getLastRow();
    
    if (lastRow <= 1) {
      return ContentService
        .createTextOutput(JSON.stringify([]))
        .setMimeType(ContentService.MimeType.JSON);
    }
    
    // Read the headers (Row 1)
    var headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    
    // Read last 100 rows to prevent execution timeout / high payload size
    var limit = 100;
    var startRow = Math.max(2, lastRow - limit + 1);
    var numRows = lastRow - startRow + 1;
    
    var data = sheet.getRange(startRow, 1, numRows, sheet.getLastColumn()).getValues();
    var records = [];
    
    for (var i = 0; i < data.length; i++) {
      var rowObj = {};
      for (var j = 0; j < headers.length; j++) {
        rowObj[headers[j]] = data[i][j];
      }
      records.push(rowObj);
    }
    
    return ContentService
      .createTextOutput(JSON.stringify(records))
      .setMimeType(ContentService.MimeType.JSON);
      
  } catch (error) {
    return ContentService
      .createTextOutput(JSON.stringify({ status: "error", message: error.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}
