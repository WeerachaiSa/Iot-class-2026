/*
* iot-class-udp.ino
* iot-class from ESP32 Cucumber RIS transmitting via UDP
* Sensors: SHT41 (Temp/Humid), BMP280 (Pressure), MPU6050 (IMU)
*/

#include <Wire.h>
#include <Adafruit_BMP280.h>
#include <Adafruit_MPU6050.h>
#include <SensirionI2cSht4x.h>
#include <WiFi.h>
#include <WiFiUdp.h>      // เรียกใช้งานไลบรารี UDP ของ ESP32
#include <ArduinoJson.h>
#include <Adafruit_NeoPixel.h>

// Sensirion SHt4x
#define SDA_PIN 41
#define SCL_PIN 40
#define CLOCK_FEQ 100000
#define LED_BUILTIN 2 
SensirionI2cSht4x sht4x;

// NeoPixel
#define LEDPIN 18
#define NUMPIXELS 1
Adafruit_NeoPixel pixels(NUMPIXELS, LEDPIN, NEO_RGB + NEO_KHZ800);

// สถานะระบบสำหรับควบคุมไฟ NeoPixel
enum SystemState {
  STATE_SENSING,      // กำลังอ่านค่าเซนเซอร์ -> สีฟ้า (BLUE)
  STATE_TX_SUCCESS,   // ส่งข้อมูล UDP สำเร็จ -> สีเขียว (GREEN)
  STATE_NETWORK_WAIT, // Wi-Fi หลุด/กำลังเชื่อมต่อ -> สีเหลือง (ORANGE)
  STATE_ERROR         // เกิดข้อผิดพลาดของฮาร์ดแวร์ -> สีแดง (RED)
};

SystemState currentState = STATE_NETWORK_WAIT;

// *** การตั้งค่าเครือข่ายและ UDP Gateway ***
const char* ssid         = "Net_FDT";
const char* password     = "Cdti2358";
const char* gateway_ip   = "172.16.46.53"; // IP ของ IoT Gateway (หรือคอมพิวเตอร์ที่เปิดรับ UDP)
const uint16_t gateway_port = 5005;          // Port ที่ Gateway เปิดรอรับข้อมูล

// ประกาศออบเจกต์ WiFiUDP
WiFiUDP udpClient;

#ifdef NO_ERROR
#undef NO_ERROR
#endif
#define NO_ERROR 0

// Sensors
Adafruit_BMP280 bmp;
Adafruit_MPU6050 mpu;

static char errorMessage[64];
static int16_t error;

// ตัวแปรควบคุมเวลา Non-blocking
unsigned long prev_sensor_millis = 0;
unsigned long prev_blink_millis = 0;
bool ledToggleState = false;

void setupHardware() {
    Wire.begin(SDA_PIN, SCL_PIN, CLOCK_FEQ);
    pixels.begin();
    pixels.setBrightness(40);
  
    // prepare BMP280 sensor
    if (bmp.begin(0x76)) {
      Serial.println("BMP280 sensor ready");
    } else {
      Serial.println("BMP280 sensor fail!");
      currentState = STATE_ERROR;
    }

    // Sensirion setup
    sht4x.begin(Wire, SHT40_I2C_ADDR_44);
    sht4x.softReset();
    delay(10);
    
    uint32_t serialNumber = 0;
    error = sht4x.serialNumber(serialNumber);
    if (error != NO_ERROR) {
      Serial.print("Error trying to execute serialNumber(): ");
      errorToString(error, errorMessage, sizeof errorMessage);
      Serial.println(errorMessage);
      currentState = STATE_ERROR;
      return;
    }

    Serial.print("serialNumber: ");
    Serial.println(serialNumber);

    // prepare MPU6050 sensor
    if (mpu.begin()) { 
       Serial.println("MPU6050 sensor ready");
    } else {
       Serial.println("MPU6050 sensor fail!");
       currentState = STATE_ERROR;
    }

    pinMode(LED_BUILTIN, OUTPUT);
    digitalWrite(LED_BUILTIN, HIGH);
}

// --- เพิ่มส่วนนี้ไว้ด้านบนนอก setup() หรือใน setup() ก็ได้ ---
IPAddress local_IP(172, 16, 46,54);  // หมายเลข IP ที่คุณต้องการให้บอร์ด ESP32 ใช้ (ห้ามซ้ำกับอุปกรณ์อื่นในวง)
IPAddress gateway(172,16,46,254);    // IP ของ Gateway/Router ตัวหลัก
IPAddress subnet(255, 255, 255, 0);   // Subnet Mask ส่วนใหญ่เป็นค่านี้
IPAddress primaryDNS(8, 8, 8, 8);     // DNS Server ตัวหลัก (เช่นของ Google)
IPAddress secondaryDNS(8, 8, 4, 4);   // DNS Server สำรอง (ใส่หรือไม่ใส่ก็ได้)

void setup() {
  Serial.begin(115200);
  setupHardware();
  Serial.println("Starting UDP Node");
  randomSeed(analogRead(0));

  // ----------------------------------------------------
  // เพิ่มการตั้งค่า Static IP ตรงนี้ (ก่อน WiFi.begin)
  // ----------------------------------------------------
  if (!WiFi.config(local_IP, gateway, subnet, primaryDNS, secondaryDNS)) {
    Serial.println("STA Failed to configure Static IP");
  }
  // Connect to Wi-Fi
  WiFi.begin(ssid, password);
  Serial.print("\r\nConnecting to ");
  Serial.print(ssid); Serial.print(" ...");
  
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
    pixels.setPixelColor(0, pixels.Color(30, 20, 0)); // สีเหลืองกระพริบตอนต่อไวไฟ
    pixels.show();
    delay(100);
    pixels.setPixelColor(0, pixels.Color(0, 0, 0));
    pixels.show();
  }
  Serial.print(" Connected! IP address: ");
  Serial.println(WiFi.localIP());

  currentState = STATE_SENSING;
}

void loop() {
  unsigned long currentTime = millis();
  
  sensors_event_t temp;
  sensors_event_t a, g;

  // ตรวจสอบสถานะ Wi-Fi ตลอดเวลา (Non-blocking Network Monitor)
  if (WiFi.status() != WL_CONNECTED) {
    if (currentState != STATE_ERROR) {
      currentState = STATE_NETWORK_WAIT;
    }
  }

  // ----------------------------------------------------
  // TASK 1: อ่านค่าเซนเซอร์และยิง UDP Packet (ทุก ๆ 5 วินาที)
  // ----------------------------------------------------
  if ((currentTime - prev_sensor_millis) > 5000) {
    prev_sensor_millis = currentTime;
    
    // แสดงสถานะเริ่มต้นก่อนเริ่มประมวลผลในรอบนั้นๆ
    Serial.println("\n==============================================");
    Serial.print("[Debug] Starting TX Cycle. Current State: ");
    switch (currentState) {
      case STATE_SENSING:      Serial.println("STATE_SENSING (Blue)"); break;
      case STATE_TX_SUCCESS:   Serial.println("STATE_TX_SUCCESS (Green)"); break;
      case STATE_NETWORK_WAIT: Serial.println("STATE_NETWORK_WAIT (Orange)"); break;
      case STATE_ERROR:        Serial.println("STATE_ERROR (Red)"); break;
    }

    // หากระบบ Wi-Fi ไม่พร้อม ให้ข้ามลูปการส่งข้อมูลไปก่อน
    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("[Warning] Wi-Fi Disconnected. Skipping TX...");
      return;
    }

    if (currentState != STATE_ERROR) {
      currentState = STATE_SENSING;
    }

    // 1. อ่านค่าเซนเซอร์ทางกายภาพทั้งหมด
    float pressure = bmp.readPressure();

    uint16_t sht_error;
    char sht_errorMessage[256];
    float temperature = 0.0;
    float humidity = 0.0;
    
    sht_error = sht4x.measureHighPrecision(temperature, humidity);
    if (sht_error) {
      Serial.print("Error SHT41: ");
      errorToString(sht_error, sht_errorMessage, 256);
      Serial.println(sht_errorMessage);
      currentState = STATE_ERROR;
    }

    mpu.getEvent(&a, &g, &temp);
    float ax = a.acceleration.x;
    float ay = a.acceleration.y;
    float az = a.acceleration.z;
    float gx = g.gyro.x;
    float gy = g.gyro.y;
    float gz = g.gyro.z;

    unsigned int b = random(2900, 3000); // จำลองแบตเตอรี่

    // 2. จัดเตรียม JSON Payloadด้วย ArduinoJson
    DynamicJsonDocument doc(1024);
    doc["id"] = "99999999";
    doc["name"] = "iot_sensor_99";
    
    JsonObject payload = doc.createNestedObject("payload");
    payload["temperature"] = temperature;
    payload["humidity"] = humidity;
    payload["pressure"] = pressure;
    payload["accel_x"] = ax;
    payload["accel_y"] = ay;
    payload["accel_z"] = az;
    payload["batt_mv"] = b;

    String jsonPayload;
    serializeJson(doc, jsonPayload);

    // 3. เริ่มกระบวนการส่ง UDP Packet (ไม่มีขั้นตอน Handshake)
    // เริ่มส่งไปยัง IP และ Port ปลายทาง
    udpClient.beginPacket(gateway_ip, gateway_port);
    
    // เขียนข้อมูล String ลงใน Buffer ของ UDP
    udpClient.print(jsonPayload);
    
    // ยิงแพ็กเก็ตออกไปทันที (Fire)
    int tx_result = udpClient.endPacket();

    if (tx_result == 1) { // endPacket จะส่งกลับค่า 1 หากยิงแพ็กเก็ตออกจากบอร์ดสำเร็จ
      Serial.println("----------------------------------------------");
      Serial.println("UDP Packet Sent Successfully!");
      Serial.print("Current State: ");
      switch (currentState) {
        case STATE_SENSING:      Serial.println("STATE_SENSING (Blue)"); break;
        case STATE_TX_SUCCESS:   Serial.println("STATE_TX_SUCCESS (Green)"); break;
        case STATE_NETWORK_WAIT: Serial.println("STATE_NETWORK_WAIT (Orange)"); break;
        case STATE_ERROR:        Serial.println("STATE_ERROR (Red)"); break;
      }
      
      Serial.print("Destination: "); Serial.print(gateway_ip); Serial.print(":"); Serial.println(gateway_port);
      Serial.println(jsonPayload);
      
      if (currentState != STATE_ERROR) {
        currentState = STATE_TX_SUCCESS; // อัปเดตเป็นไฟสีเขียว
      }
    } else {
      Serial.println("UDP Transmission Failed locally!");
      if (currentState != STATE_ERROR) {
        currentState = STATE_ERROR;
      }
    }
  }

  // ----------------------------------------------------
  // TASK 2: ควบคุมการกระพริบของ NeoPixel (ทุกๆ 500ms) - Non-blocking
  // ----------------------------------------------------
  if ((currentTime - prev_blink_millis) > 500) {
    prev_blink_millis = currentTime;
    ledToggleState = !ledToggleState;

    if (ledToggleState) {
      switch (currentState) {
        case STATE_SENSING:
          pixels.setPixelColor(0, pixels.Color(0, 0, 40));    // สีฟ้า (BLUE)
          break;
        case STATE_TX_SUCCESS:
          pixels.setPixelColor(0, pixels.Color(0, 40, 0));    // สีเขียว (GREEN)
          break;
        case STATE_NETWORK_WAIT:
          pixels.setPixelColor(0, pixels.Color(40, 25, 0));   // สีส้ม/เหลือง (ORANGE)
          break;
        case STATE_ERROR:
          pixels.setPixelColor(0, pixels.Color(40, 0, 0));    // สีแดง (RED)
          break;
      }
    } else {
      pixels.setPixelColor(0, pixels.Color(0, 0, 0)); // ดับไฟ
    }
    pixels.show();
  }
}