/**
 * Node.js Server for Firestore Storage + In-Memory Cache (optimized)
 * Stores last 2 days in server memory, full history optionally in Firestore
 * Sends notifications to all users with the same deviceId when condition is met
 */

require("dotenv").config();
const express = require("express");
const admin = require("firebase-admin");
const cors = require("cors");
const twilio = require("twilio");

const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;
const client = twilio(accountSid, authToken);

async function makeCall(toNumber, messageUrl) {
  try {
    const call = await client.calls.create({
      url: messageUrl, // TwiML Bin or XML file URL
      to: toNumber, // Destination phone number
      from: "+15513654561", // Your Twilio number
    });
    console.log("✅ Call started, SID:", call.sid);
    return call.sid;
  } catch (err) {
    console.error("❌ Call failed:", err.message);
    throw err;
  }
}

async function makeCallsSequentially(numbers, messageUrl) {
  for (const num of numbers) {
    try {
      const sid = await makeCall(num, messageUrl);
      console.log(`📞 Call to ${num} started (SID: ${sid})`);
      // wait a bit before next call (optional)
      await new Promise((resolve) => setTimeout(resolve, 60000));
    } catch (err) {
      console.error(`❌ Failed to call ${num}:`, err.message);
    }
  }
}

// ===== CONFIG =====
const PORT = 8080;
const TWO_DAYS = 2 * 24 * 60 * 60 * 1000; // 2 days in ms

// ===== GLOBAL HARDCODED SENSOR VALUES =====
const HARDCODED_SENSORS = {
  temperature: 29.5,
  turbidity: 15.7,
  ph: 7.1,
  do: 6.9,
  tds: 260.3,
};

// ===== FIREBASE SETUP =====
if (!process.env.FIREBASE_SERVICE_ACCOUNT_BASE64) {
  console.error("❌ Missing FIREBASE_SERVICE_ACCOUNT_BASE64 in .env");
  process.exit(1);
}
const serviceAccount = JSON.parse(
  Buffer.from(process.env.FIREBASE_SERVICE_ACCOUNT_BASE64, "base64").toString(
    "utf8",
  ),
);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const firestore = admin.firestore();

// ===== EXPRESS SETUP =====
const app = express();
app.use(cors());
app.use(express.json());

// ===== HELPERS =====
const getFormattedTimestamp = () => {
  const now = new Date();
  const pad = (n) => (n < 10 ? "0" + n : n);
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(
    now.getDate(),
  )}_${pad(now.getHours())}-${pad(now.getMinutes())}-${pad(now.getSeconds())}`;
};

const log = (msg, type = "INFO") => {
  console.log(`[${new Date().toISOString()}] [${type}] ${msg}`);
};

function formatData(device_id, payload) {
  const formatted = {
    device_id,
    line1: Number(payload.line1) || 0,
    line2: Number(payload.line2) || 0,
    relay1_status: payload.relay1_status ?? 0,
    relay2_status: payload.relay2_status ?? 0,

    // copy any other keys (optional)
    ...payload,

    // hard override these keys
    ...HARDCODED_SENSORS,

    timestamp: admin.firestore.Timestamp.now(),
  };
  return formatted;
}

// ===== IN-MEMORY CACHES =====
const liveDataCache = {}; // { deviceId: { data, lastUpdated } }
const historyCache = {}; // { deviceId: [last 2 days data] }
const userSettingsCache = {}; // { deviceId: { noAeratorsLine1, noAeratorsLine2, perAerator_currentLine1, perAerator_currentLine2 } }
const alertStateCache = {}; // { deviceId: { active: true/false, lastAlert: timestamp } }
const alertCounterCache = {}; // { deviceId: count }

// ===== API ROUTES =====

// Store incoming data
const mqtt = require("mqtt");

// ===== MQTT CONFIG =====
const MQTT_BROKER = process.env.MQTT_BROKER || "mqtt://broker.emqx.io";
const MQTT_TOPIC = process.env.MQTT_TOPIC || "PMS1/data";
const mqttClient = mqtt.connect(MQTT_BROKER);

// Cache to store device alert active state

async function processDeviceData(data, source = "http") {
  if (!data.device_id) throw new Error("device_id required");

  const deviceId = data.device_id;
  const docId = getFormattedTimestamp();
  const formatted = formatData(deviceId, data);

  // ==========================================================
  // 1️⃣ Store data in Firestore + Update live & history caches
  // ==========================================================
  await firestore.collection(deviceId).doc(docId).set(formatted);

  liveDataCache[deviceId] = { data: formatted, lastUpdated: Date.now() };
  if (!historyCache[deviceId]) historyCache[deviceId] = [];
  historyCache[deviceId].push(formatted);
  const cutoff = Date.now() - TWO_DAYS;
  historyCache[deviceId] = historyCache[deviceId].filter(
    (item) => item.timestamp.toMillis() >= cutoff,
  );
  log(`[${source.toUpperCase()}] Data stored: ${deviceId}/${docId}`);

  // ==========================================================
  // 2️⃣ Check alert condition
  // ==========================================================
  let alertTriggered = false;
  let alertMsg = "";

  // Load device settings (cached)
  let deviceSettings = userSettingsCache[deviceId];
  if (!deviceSettings) {
    const settingsSnap = await firestore
      .collection("users")
      .where("deviceId", "==", deviceId)
      .get();

    if (!settingsSnap.empty) {
      const user = settingsSnap.docs[0].data();
      deviceSettings = {
        noAeratorsLine1: user.noAeratorsLine1 || 0,
        noAeratorsLine2: user.noAeratorsLine2 || 0,
        perAerator_currentLine1: user.perAerator_currentLine1 || 1,
        perAerator_currentLine2: user.perAerator_currentLine2 || 1,
      };
      userSettingsCache[deviceId] = deviceSettings;
      log(`Cached settings for deviceId=${deviceId}`);
    }
  }

  // Skip if no settings found
  if (!deviceSettings) return { formatted, alertTriggered, alertMsg };

  // Calculate aerator ratios
  const ratio1 = Math.round(
    formatted.line1 / (deviceSettings.perAerator_currentLine1 || 1),
  );
  const ratio2 = Math.round(
    formatted.line2 / (deviceSettings.perAerator_currentLine2 || 1),
  );

  // Calculate not working aerators explicitly
  const notWorkingLine1 = Math.max(0, (deviceSettings.noAeratorsLine1 || 0) - ratio1);
  const notWorkingLine2 = Math.max(0, (deviceSettings.noAeratorsLine2 || 0) - ratio2);

  // Check thresholds: breach if not working aerators is >= 3 (more than 3 or 3)
  const line1Low = notWorkingLine1 >= 3;
  const line2Low = notWorkingLine2 >= 3;

  if (line1Low && line2Low) {
    alertMsg =
      `Both Line 1 and Line 2 aerators are running low. ` +
      `(Line1: ${ratio1}/${deviceSettings.noAeratorsLine1}, ` +
      `Line2: ${ratio2}/${deviceSettings.noAeratorsLine2})`;
  } else if (line1Low) {
    alertMsg = `Line 1 aerators running low: ${ratio1}, expected ≥ ${deviceSettings.noAeratorsLine1}.`;
  } else if (line2Low) {
    alertMsg = `Line 2 aerators running low: ${ratio2}, expected ≥ ${deviceSettings.noAeratorsLine2}.`;
  }

  // ==========================================================
  // 3️⃣ Alert handling (only once per trigger)
  // ==========================================================
  const currentlyAlerting = !!alertMsg;

  // initialize counter
  if (!alertCounterCache[deviceId]) {
    alertCounterCache[deviceId] = 0;
  }

  if (currentlyAlerting) {
    alertCounterCache[deviceId] += 1;
  } else {
    alertCounterCache[deviceId] = 0; // reset if normal
  }

  // trigger only after 8 consecutive detections (initial breach + next 7 messages)
  const shouldTriggerAlert = alertCounterCache[deviceId] >= 8;
  const previouslyAlerting = alertStateCache[deviceId] || false;

  if (shouldTriggerAlert && !previouslyAlerting) {
    // ---- NEW ALERT ----
    log(`🚨 New alert for ${deviceId}: ${alertMsg}`);
    alertStateCache[deviceId] = true; // mark as active

    // --- Get all users for this device ---
    const usersSnap = await firestore
      .collection("users")
      .where("deviceId", "==", deviceId)
      .get();

    // --- Send FCM first ---
    for (const doc of usersSnap.docs) {
      const user = doc.data();
      if (!user.fcmToken) continue;

      const message = {
        token: user.fcmToken,
        data: {
          title: "⚠️ Aerator Alert!",
          body: `Device ${deviceId}: ${alertMsg}`,
          alarm: "1",
        },
        android: {
          priority: "HIGH",
        },
      };

      try {
        await admin.messaging().send(message);
        log(`📨 FCM sent to ${doc.id}`);
      } catch (err) {
        log(`❌ FCM failed for ${doc.id}: ${err.message}`, "WARN");
      }
    }

    // --- Then make Twilio calls ---
    try {
      await makeCallsSequentially(
        ["+917661912957", "+918897618973"],
        "https://handler.twilio.com/twiml/EH07a7a07e1fe048421184ab40a80757e4",
      );
      log(`📞 Twilio calls placed for ${deviceId}`);
    } catch (err) {
      log(`❌ Twilio call failed: ${err.message}`, "WARN");
    }

    alertTriggered = true;
  } else if (!currentlyAlerting && previouslyAlerting) {
    // ---- RECOVERY ----
    log(`✅ Device ${deviceId} recovered — resetting alert state.`);
    alertStateCache[deviceId] = false; // reset
  } else if (!currentlyAlerting) {
    log(`✅ No alert for ${deviceId}`);
  }

  // ==========================================================
  // 4️⃣ Return status summary
  // ==========================================================
  return { formatted, alertTriggered, alertMsg };
}

function publishCommand(command) {
  const topic = "PMS/cmd";
  const payload = JSON.stringify(command);

  mqttClient.publish(topic, payload, { qos: 1 }, (err) => {
    if (err) {
      log(`❌ Failed to publish command: ${err.message}`, "ERROR");
    } else {
      log(`📢 Command published to ${topic}: ${payload}`);
    }
  });
}

app.post("/api/control-response", (req, res) => {
  const { deviceId, code } = req.body;

  if (!deviceId || !code) {
    return res
      .status(400)
      .json({ status: "error", error: "deviceId and code required" });
  }

  responseControl[deviceId] = { code };

  log(`Frontend set response code for ${deviceId}: ${code}`);

  res.json({
    status: "success",
    deviceId,
    code,
  });
});

const responseControl = {};
// ===== HTTP ENDPOINT (reuses processDeviceData) =====
app.post("/api/data", async (req, res) => {
  try {
    const { device_id } = req.body;

    let respCode = 200;

    // Use custom response code once if set
    if (responseControl[device_id]) {
      respCode = responseControl[device_id].code || 200;
      delete responseControl[device_id];
    }

    const { formatted, alertSent, alertMsg, noAlertNeeded } =
      await processDeviceData(req.body, "http");

    res.status(respCode).json({
      status: "success",
      stored: formatted,
      alertSent,
      noAlertNeeded,
      alertMsg,
    });
  } catch (err) {
    log(`❌ Error saving HTTP data: ${err}`, "ERROR");
    res.status(500).json({ status: "error", error: err.message });
  }
});

// ===== MQTT HANDLER =====
// ===== MQTT CONFIG =====

// ===== MQTT DEBUG HANDLER =====
mqttClient.on("connect", () => {
  console.log("✅ Connected to MQTT broker");
  mqttClient.subscribe(MQTT_TOPIC, (err) => {
    if (err) {
      console.error("❌ Failed to subscribe:", err);
    } else {
      console.log(`📡 Subscribed to topic: ${MQTT_TOPIC}`);
    }
  });
});

mqttClient.on("message", async (topic, message) => {
  try {
    const parsed = JSON.parse(message.toString());

    // 🔥 Store into Firestore + cache
    const { formatted, alertSent, alertMsg, noAlertNeeded } =
      await processDeviceData(parsed, "mqtt");

    console.log("MQTT Stored to Firestore:", formatted.device_id);
    if (alertMsg) {
      console.log("   🚨 Alert Triggered:", alertMsg);
      console.log("   📲 FCM sent?", alertSent);
    } else {
      console.log("   ✅ No alert needed");
    }
  } catch (err) {
    console.error("   ❌ Failed to parse/process message:", err.message);
  }
});

mqttClient.on("error", (err) => {
  console.error("❌ MQTT Error:", err);
});

mqttClient.on("close", () => {
  console.log("⚠️ MQTT connection closed");
});

mqttClient.on("reconnect", () => {
  console.log("🔄 Reconnecting to MQTT broker...");
});

// ===== PATCH USERS BY deviceId (update calculation values) =====
app.patch("/api/users/update", async (req, res) => {
  try {
    const {
      deviceId,
      noAeratorsLine1,
      noAeratorsLine2,
      perAerator_currentLine1,
      perAerator_currentLine2,
    } = req.body;
    if (!deviceId)
      return res
        .status(400)
        .json({ status: "error", error: "deviceId required" });

    const updates = {};
    if (noAeratorsLine1 !== undefined)
      updates.noAeratorsLine1 = noAeratorsLine1;
    if (noAeratorsLine2 !== undefined)
      updates.noAeratorsLine2 = noAeratorsLine2;
    if (perAerator_currentLine1 !== undefined)
      updates.perAerator_currentLine1 = perAerator_currentLine1;
    if (perAerator_currentLine2 !== undefined)
      updates.perAerator_currentLine2 = perAerator_currentLine2;

    if (!Object.keys(updates).length)
      return res
        .status(400)
        .json({ status: "error", error: "Provide at least one field" });

    updates.updatedAt = new Date().toISOString();

    const snapshot = await firestore
      .collection("users")
      .where("deviceId", "==", deviceId)
      .get();
    if (snapshot.empty)
      return res
        .status(404)
        .json({ status: "error", error: "No users found with this deviceId" });

    const batch = firestore.batch();
    snapshot.forEach((doc) => batch.update(doc.ref, updates));
    await batch.commit();

    // Update cache for calculation
    if (!userSettingsCache[deviceId]) userSettingsCache[deviceId] = {};
    Object.assign(userSettingsCache[deviceId], updates);

    res.json({
      status: "success",
      message: `Updated ${snapshot.size} user(s)`,
      cache: userSettingsCache[deviceId],
    });
    log(`Updated ${snapshot.size} user(s) for deviceId=${deviceId}`);
  } catch (err) {
    log(`Error patching users: ${err}`, "ERROR");
    res.status(500).json({ status: "error", error: err.message });
  }
});

// ===== FETCH HISTORY =====
app.get("/api/history/:deviceId", (req, res) => {
  const { deviceId } = req.params;
  if (!deviceId)
    return res
      .status(400)
      .json({ status: "error", error: "deviceId required" });

  const history = historyCache[deviceId] || [];
  res.json({ status: history.length ? "ok" : "no_data", data: history });
});

// ===== FETCH LIVE DATA =====
app.get("/api/data/:deviceId", (req, res) => {
  const { deviceId } = req.params;
  if (!deviceId)
    return res
      .status(400)
      .json({ status: "error", error: "deviceId required" });

  const cacheEntry = liveDataCache[deviceId];
  if (!cacheEntry || Date.now() - cacheEntry.lastUpdated > 30000) {
    return res.json({ status: "no_data", data: "--" });
  }

  res.json({ status: "ok", data: cacheEntry.data });
});

// ===== RELAY CONTROL =====
app.post("/api/relays", async (req, res) => {
  try {
    const { device_id, ...relays } = req.body;
    if (!device_id)
      return res
        .status(400)
        .json({ status: "error", error: "device_id required" });

    const docId = getFormattedTimestamp();
    await firestore
      .collection(`${device_id}_control`)
      .doc(docId)
      .set({
        device_id,
        ...relays,
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
      });

    res.json({ status: "success", device_id, updated: relays });
    log(`Relay data saved for ${device_id}`);
  } catch (err) {
    log(`Error saving relay data: ${err}`, "ERROR");
    res.status(500).json({ status: "error", error: err.message });
  }
});

// ===== TEST NOTIFICATION API (matches /api/data payload) =====
app.post("/api/test-notification", async (req, res) => {
  try {
    const { fcmToken, title, body } = req.body;

    if (!fcmToken || !title || !body) {
      return res.status(400).json({
        status: "error",
        error: "fcmToken, title, and body are required",
      });
    }

    // Same payload as in /api/data alerts
    const message = {
      token: fcmToken,
      data: {
        title: title,
        body: body,
        alarm: "1",
      },
      android: {
        priority: "HIGH",
      },
    };

    const response = await admin.messaging().send(message);

    log(`✅ Test notification sent to token: ${fcmToken}`);
    res.json({
      status: "success",
      validity: "valid",
      messageId: response,
      payload: message,
    });
  } catch (err) {
    if (
      err.code === "messaging/invalid-argument" ||
      err.code === "messaging/registration-token-not-registered"
    ) {
      log(`❌ Invalid or expired FCM token: ${req.body.fcmToken}`, "ERROR");
      return res.status(400).json({
        status: "error",
        validity: "invalid",
        error: "Invalid or expired FCM token",
      });
    }

    log(`❌ Failed to send test notification: ${err.message}`, "ERROR");
    res.status(500).json({
      status: "error",
      validity: "unknown",
      error: err.message,
    });
  }
});

// ===== GET GUARDIAN NUMBERS BY USER ID =====
app.get("/api/guardians/user/:userId", async (req, res) => {
  try {
    const { userId } = req.params;
    if (!userId) {
      return res
        .status(400)
        .json({ status: "error", error: "userId required" });
    }

    // Query Firestore for the user with this userId
    const doc = await firestore.collection("users").doc(userId).get();

    if (!doc.exists) {
      return res.status(404).json({ status: "error", error: "User not found" });
    }

    const data = doc.data();

    const guardians = {
      userName: data.name || "",
      guardianNumber1: data.guardianNumber1 || "",
      guardianNumber2: data.guardianNumber2 || "",
    };

    res.json({ status: "success", userId, guardians });
  } catch (err) {
    console.error(
      `Error fetching guardians for user ${req.params.userId}:`,
      err,
    );
    res.status(500).json({ status: "error", error: err.message });
  }
});

// ===== START SERVER =====
app.listen(PORT, () => log(`Server running on port ${PORT}`));