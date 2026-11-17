
#include <BluetoothSerial.h>
#include <DHT.h>
#include <Preferences.h>
#include <ArduinoJson.h>

// ---------------- PIN CONFIGURATION ----------------
#define DHT_PIN 27
#define SOIL_PIN 34
#define DHT_TYPE DHT11


// ---------------- OBJECTS ----------------
BluetoothSerial SerialBT;
DHT dht(DHT_PIN, DHT_TYPE);
Preferences prefs;

// ---------------- CALIBRATION ----------------
int dryValue = 3000;  // Initial placeholder
int wetValue = 1500;  // Initial placeholder

float tempOffset = 0.0;
float humidityOffset = 0.0;

unsigned long lastRead = 0;
const unsigned long READ_INTERVAL = 2000;

// ---------------- FUNCTIONS ----------------

int readSoilRaw() {
  long total = 0;

  for (int i = 0; i < 10; i++) {
    total += analogRead(SOIL_PIN);
    delay(10);
  }

  return total / 10;
}

int getSoilPercentage(int raw) {
  if (dryValue == wetValue) return 0;

  float percentage =
    100.0 * (dryValue - raw) / (float)(dryValue - wetValue);

  return constrain((int)round(percentage), 0, 100);
}

void saveCalibration() {
  prefs.putInt("dry", dryValue);
  prefs.putInt("wet", wetValue);
  prefs.putFloat("toff", tempOffset);
  prefs.putFloat("hoff", humidityOffset);
}

void loadCalibration() {
  dryValue = prefs.getInt("dry", 3000);
  wetValue = prefs.getInt("wet", 1500);

  tempOffset = prefs.getFloat("toff", 0.0);
  humidityOffset = prefs.getFloat("hoff", 0.0);
}

// Average several samples for soil calibration
int captureSoilCalibration() {
  long total = 0;

  for (int i = 0; i < 50; i++) {
    total += analogRead(SOIL_PIN);
    delay(100);
  }

  return total / 50;
}

// ---------------- COMMAND HANDLER ----------------

void handleCommand(String message) {
  JsonDocument doc;

  DeserializationError error =
    deserializeJson(doc, message);

  if (error) return;

  const char* cmd = doc["cmd"];

  if (!cmd) return;

  if (strcmp(cmd, "soil_calibrate_dry") == 0) {
    dryValue = captureSoilCalibration();
    saveCalibration();

    SerialBT.printf(
      "{\"type\":\"calibration\",\"sensor\":\"soil\","
      "\"stage\":\"dry\",\"raw\":%d}\n",
      dryValue
    );
  }

  else if (strcmp(cmd, "soil_calibrate_wet") == 0) {
    wetValue = captureSoilCalibration();
    saveCalibration();

    SerialBT.printf(
      "{\"type\":\"calibration\",\"sensor\":\"soil\","
      "\"stage\":\"wet\",\"raw\":%d}\n",
      wetValue
    );
  }

  else if (strcmp(cmd, "set_offsets") == 0) {
    if (doc["temperature_offset"].is<float>() ||
        doc["temperature_offset"].is<int>()) {
      tempOffset = doc["temperature_offset"].as<float>();
    }

    if (doc["humidity_offset"].is<float>() ||
        doc["humidity_offset"].is<int>()) {
      humidityOffset = doc["humidity_offset"].as<float>();
    }

    saveCalibration();

    SerialBT.println(
      "{\"type\":\"ack\",\"message\":\"Offsets saved\"}"
    );
  }

  else if (strcmp(cmd, "get_calibration") == 0) {
    SerialBT.printf(
      "{\"type\":\"calibration_status\","
      "\"dry\":%d,\"wet\":%d,"
      "\"temperature_offset\":%.2f,"
      "\"humidity_offset\":%.2f}\n",
      dryValue, wetValue,
      tempOffset, humidityOffset
    );
  }
}

// ---------------- SETUP ----------------

void setup() {
  Serial.begin(115200);

  dht.begin();

  analogReadResolution(12);
  analogSetPinAttenuation(SOIL_PIN, ADC_11db);

  prefs.begin("plant", false);
  loadCalibration();

  SerialBT.begin("PlantMonitor");

  Serial.println("Plant Monitor Started");
  Serial.println("Bluetooth: PlantMonitor");
}

// ---------------- MAIN LOOP ----------------

void loop() {

  // Receive commands from Raspberry Pi
  if (SerialBT.available()) {
    String message = SerialBT.readStringUntil('\n');
    message.trim();

    if (message.length() > 0) {
      handleCommand(message);
    }
  }

  // Send sensor readings periodically
  if (millis() - lastRead >= READ_INTERVAL) {
    lastRead = millis();

    float temperature = dht.readTemperature();
    float humidity = dht.readHumidity();

    int soilRaw = readSoilRaw();

    if (!isnan(temperature) && !isnan(humidity)) {

      temperature += tempOffset;
      humidity += humidityOffset;

      int soilPercent = getSoilPercentage(soilRaw);

      SerialBT.printf(
        "{\"type\":\"sensor\","
        "\"temperature\":%.1f,"
        "\"humidity\":%.1f,"
        "\"soil_raw\":%d,"
        "\"soil_percent\":%d}\n",
        temperature,
        humidity,
        soilRaw,
        soilPercent
      );

      Serial.printf(
        "Temp: %.1f C | Humidity: %.1f %% | "
        "Soil: %d %% | Raw: %d\n",
        temperature, humidity,
        soilPercent, soilRaw
      );
    }
  }
}
