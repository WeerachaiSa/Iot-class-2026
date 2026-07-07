import asyncio
import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime, timezone
import re

# ==================================================================
#         ส่วนกำหนดค่าสำหรับนักเรียน (Student Gateway Config)
# ==================================================================
STUDENT_ID = "68123456789"  # แก้ไขเป็นรหัสนักศึกษาของตนเอง

# === กำหนดค่าเชื่อมต่อเครือข่าย ===
UDP_PORT = 5005
MQTT_BROKER = "172.16.2.117"    # ไอพีของ MQTT Broker (หรือตามที่ระบุในสถาปัตยกรรม 172.16.2.117)
MQTT_PORT = 1883

# === กำหนดการทำ Data Aggregation ===
# กำหนดช่วงเวลาเก็บข้อมูลเพื่อหาค่าเฉลี่ยของเซนเซอร์แต่ละตัว (วินาที)
# นักศึกษาสามารถทดลองเปลี่ยนตัวแปรนี้ (เช่น 5, 30, 60) เพื่อวัด Traffic บน Grafana
AGGREGATION_WINDOW_SEC = 60  

# กำหนด Client ID อ้างอิงตามข้อกำหนดของนักเรียนแต่ละคน
MQTT_CLIENT_ID = f"GW_{STUDENT_ID}"
# ==================================================================

# === ตัวแปรสำหรับคำนวณทราฟฟิกเครือข่ายสะสม (Telemetry Traffic Counters) ===
total_udp_payload_bytes = 0    # ปริมาณข้อมูล JSON UDP สะสม (Bytes)
total_udp_network_bytes = 0    # ปริมาณแพ็กเก็ต UDP บนเครือข่ายสะสม (+46 Bytes Overhead per Packet)
total_udp_packets_count = 0    # จำนวนแพ็กเก็ต UDP ที่ได้รับทั้งหมด

total_mqtt_payload_bytes = 0   # ปริมาณข้อมูล JSON MQTT สะสม (Bytes)
total_mqtt_network_bytes = 0   # ปริมาณแพ็กเก็ต MQTT บนเครือข่ายสะสม
total_mqtt_messages_count = 0  # จำนวนข้อความ MQTT ที่ส่งออกทั้งหมด

# บัฟเฟอร์สำหรับเก็บรวบรวมค่าจากเซนเซอร์แต่ละตัวแยกออกจากกัน (Per-Sensor Buffer)
device_buffers = {}

# สร้าง Client สำหรับเชื่อมต่อ MQTT
try:
    mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID, callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
except AttributeError:
    # สำหรับ Paho-MQTT v1.x รุ่นเก่า
    mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID)

class AsyncUDPReceiverProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport
        print("==================================================================")
        print(f" IoT Gateway ({MQTT_CLIENT_ID}) [ACTIVE]")
        print(f" - รอรับข้อมูล UDP ที่พอร์ต: {UDP_PORT}")
        print(f" - ช่วงเวลาทำ Aggregation: {AGGREGATION_WINDOW_SEC} วินาที")
        print("==================================================================")

    def datagram_received(self, data, addr):
        global device_buffers
        global total_udp_payload_bytes, total_udp_network_bytes, total_udp_packets_count
        try:
            # 1. คำนวณขนาดไบต์ของ UDP Packet ที่ได้รับ
            udp_payload_size = len(data)                          # ขนาดของ JSON string ในระดับ Application Layer
            udp_network_size = udp_payload_size + 46              # ขนาด On-the-wire (+ Ethernet + IP + UDP Headers)
            
            # สะสมค่าสถิติ
            total_udp_payload_bytes += udp_payload_size
            total_udp_network_bytes += udp_network_size
            total_udp_packets_count += 1

            # 2. แปลงบิตข้อมูลที่ได้รับจาก UDP เป็น String (UTF-8)
            message = data.decode('utf-8')
            data_json = json.loads(message)

            sensor_id = data_json.get("id")
            if not sensor_id:
                print(f" [ข้ามแพ็กเก็ต] ได้รับข้อมูลจาก {addr} แต่ไม่มีฟิลด์ 'id' ระบุตัวตน")
                return

            # 3. ตรวจสอบและสร้างกล่องเก็บบัฟเฟอร์เฉพาะของเซนเซอร์ตัวนั้น ๆ
            if sensor_id not in device_buffers:
                device_buffers[sensor_id] = {
                    "name": data_json.get("name", f"NAME_{sensor_id}"),
                    "place_id": data_json.get("place_id", f"PLACE_{sensor_id}"),
                    "temperatures": [],
                    "humidities": [],
                    "pressures": []
                }
                print(f"\n[NEW SENSOR] พบเซนเซอร์ใหม่ในระบบ: {sensor_id} จากที่อยู่ IP {addr}")

            # 4. นำข้อมูลลงกล่องบันทึกข้อมูลดิบของเซนเซอร์ตัวนั้น ๆ
            if "payload" in data_json:
                payload = data_json["payload"]
                buffer = device_buffers[sensor_id]
                
                if "temperature" in payload:
                    buffer["temperatures"].append(float(payload["temperature"]))
                if "humidity" in payload:
                    buffer["humidities"].append(float(payload["humidity"]))
                if "pressure" in payload:
                    buffer["pressures"].append(float(payload["pressure"]))
                
                current_count = len(buffer["temperatures"])
                
                # แสดงผลการคำนวณไบต์ของ UDP ขาเข้าทาง Console
                print(f"[UDP IN] {sensor_id} (รวม {current_count} ค่า) | IP: {addr}")
                print(f"   -> ข้อมูลดิบ (Payload): {udp_payload_size} Bytes")
                print(f"   -> แพ็กเก็ตเครือข่ายจริง (On-the-wire): {udp_network_size} Bytes (รวม Overhead 46B)")

        except json.JSONDecodeError:
            print(f" [ผิดพลาด] ข้อมูลที่ได้รับจาก {addr} ไม่ใช่รูปแบบ JSON ที่ถูกต้อง")
        except Exception as e:
            print(f" เกิดข้อผิดพลาดในการประมวลผลข้อมูลขาเข้า: {e}")

async def aggregation_task():
    """ ทาสก์สำหรับคำนวณหาค่าเฉลี่ยและยิง MQTT ออกไปแยกรายเซนเซอร์ """
    global device_buffers
    global total_mqtt_payload_bytes, total_mqtt_network_bytes, total_mqtt_messages_count
    
    while True:
        # รอให้ครบกำหนดเวลาตาม Window ขนาดวินาทีที่ตั้งไว้
        await asyncio.sleep(AGGREGATION_WINDOW_SEC)
        
        # คัดแยกประมวลผลทีละเซนเซอร์ที่เก็บสะสมไว้ในบัฟเฟอร์
        for sensor_id in list(device_buffers.keys()):
            buffer = device_buffers[sensor_id]
            total_samples = len(buffer["temperatures"])
            
            # ถ้าในรอบเวลานี้มีข้อมูลของอุปกรณ์ชิ้นนี้ส่งเข้ามาจริง
            if total_samples > 0:
                print(f"\n--- [Aggregation Triggered] ประมวลผลเซนเซอร์: {sensor_id} ({total_samples} แพ็กเก็ตดิบ) ---")
                
                # 1. คำนวณค่าเฉลี่ยของเซนเซอร์ตัวนี้
                avg_temp = round(sum(buffer["temperatures"]) / total_samples, 2)
                avg_humid = round(sum(buffer["humidities"]) / total_samples, 2)
                avg_press = round(sum(buffer["pressures"]) / total_samples, 2)
                
                # 2. ถอดรหัสนักศึกษาออกจากรหัสเซนเซอร์เพื่อจับคู่ส่ง Topic (เช่น ID_68123456789 -> 68123456789)
                student_id_match = re.search(r'\d+', sensor_id)
                student_id = student_id_match.group() if student_id_match else STUDENT_ID
                
                target_topic = f"v1/{student_id}"
                
                # 3. จัดเตรียม JSON Payload ตัวใหม่พร้อมระบุเวลา (Timestamp & Date) ตามโครงสร้างที่ถูกต้อง
                now = datetime.now(timezone.utc)
                aggregated_payload = {
                    "id": sensor_id,
                    "name": buffer["name"],
                    "place_id": buffer["place_id"],
                    "payload": {
                        "temperature": avg_temp,
                        "humidity": avg_humid,
                        "pressure": avg_press,
                        "timestamp": int(time.time()),
                        "date": now.strftime('%Y-%m-%dT%H:%M:%S+00:00')  # รูปแบบ ISO 8601
                    }
                }
                
                mqtt_message = json.dumps(aggregated_payload, indent=4)
                
                # 4. คำนวณขนาดไบต์ของ MQTT Publish Message ที่ละเอียดขึ้นตามมาตรฐาน
                # Payload Size
                mqtt_payload_size = len(mqtt_message.encode('utf-8'))
                
                # คำนวณหาขนาดเฟรม MQTT จริงระดับ Socket (ไม่รวม TCP/IP Header ชั่วคราวเพื่อให้ใกล้เคียง VerneMQ Metrics)
                # MQTT Header = Fixed Header (2B) + Topic Length (2B) + Topic String + Packet ID (2B สำหรับ QoS 1) + Payload
                mqtt_packet_overhead = 2 + 2 + len(target_topic.encode('utf-8')) + 2
                mqtt_total_bytes = mqtt_payload_size + mqtt_packet_overhead
                
                # ขนาดสะสม On-the-wire ระดับเครือข่ายทั้งหมด (+ TCP Header 20B + IP Header 20B + Ethernet 18B = 58B)
                mqtt_network_size = mqtt_total_bytes + 58            
                
                # สะสมค่าสถิติ
                total_mqtt_payload_bytes += mqtt_payload_size
                total_mqtt_network_bytes += mqtt_network_size
                total_mqtt_messages_count += 1
                
                # 5. ทำการส่งข้อมูล (Publish) ออกไปยัง Topic ส่วนตัวของเซนเซอร์ตัวนั้น ๆ
                if mqtt_client.is_connected():
                    mqtt_client.publish(target_topic, mqtt_message, qos=1)
                    print(f"[MQTT OUT] ส่งค่าเฉลี่ยไปยัง Topic '{target_topic}' สำเร็จ (QoS 1)")
                    print(f"   -> ขนาดสตริง (Payload): {mqtt_payload_size} Bytes")
                    print(f"   -> คาดการณ์ขนาดในระดับ Socket ของ VerneMQ: {mqtt_total_bytes} Bytes")
                    print(f"   -> ขนาดแพ็กเก็ตเครือข่ายจริง (On-the-wire): {mqtt_network_size} Bytes (รวม TCP/IP)")
                    print(mqtt_message)
                else:
                    print(f" [ผิดพลาด] ไม่สามารถส่งข้อความของ {sensor_id} ได้เนื่องจากขาดการเชื่อมต่อ MQTT!")
                
                # 6. เคลียร์ค่าที่ใช้เสร็จในบัฟเฟอร์ เพื่อเตรียมรอสะสมในรอบต่อไป
                buffer["temperatures"].clear()
                buffer["humidities"].clear()
                buffer["pressures"].clear()
                
            else:
                # กรณีเซนเซอร์ตัวเดิมไม่มีข้อมูลส่งเข้ามาเลยในรอบวินาทีนี้
                print(f" -> [ไม่มีข้อมูล] เซนเซอร์ {sensor_id} เงียบไปในรอบเวลานี้")
        
        # 7. พิมพ์ตารางรายงานเปรียบเทียบทราฟฟิกรวมสะสมทั้งหมดของเครื่อง Gateway (Observability Dashboard on Console)
        if total_udp_packets_count > 0:
            print("\n==================================================================")
            print("        รายงานวิเคราะห์ปริมาณแบนด์วิดท์เครือข่ายสะสม (Gateway Stats)")
            print("==================================================================")
            print(f" [ฝั่ง LOCAL LAN ขาเข้าจากอุปกรณ์เซนเซอร์]")
            print(f"   - จำนวนแพ็กเก็ต UDP ที่ได้รับ: {total_udp_packets_count} Packets")
            print(f"   - ข้อมูลดิบรวมสะสม (Raw Payload): {total_udp_payload_bytes} Bytes")
            print(f"   - ทราฟฟิกเครือข่ายจริงสะสม (On-the-wire): {total_udp_network_bytes} Bytes")
            print(" ----------------------------------------------------------------")
            print(f" [ฝั่ง WAN ขาออกไปยังคลาวด์ภายนอก MQTT Broker]")
            print(f"   - จำนวนข้อความ MQTT ที่ส่งออก: {total_mqtt_messages_count} Messages")
            print(f"   - ข้อมูลส่งออกรวมสะสม (Raw Payload): {total_mqtt_payload_bytes} Bytes")
            print(f"   - ประมาณการสะสมในระดับ Socket ของ VerneMQ: {total_mqtt_payload_bytes + (total_mqtt_messages_count * (8 + len(target_topic)))} Bytes")
            print(f"   - ทราฟฟิกเครือข่ายจริงสะสม (On-the-wire): {total_mqtt_network_bytes} Bytes")
            print(" ----------------------------------------------------------------")
            # คำนวณหาอัตราการประหยัด Traffic ขาออก (WAN Bandwidth Saved Rate)
            bandwidth_saved = 0.0
            if total_udp_network_bytes > 0:
                bandwidth_saved = (1.0 - (total_mqtt_network_bytes / total_udp_network_bytes)) * 100.0
            
            # คำนวณหาอัตราเฉลี่ยต่อวินาที (Bytes/Sec) เพื่อประมาณการค่าเปรียบเทียบบน Grafana ทั้ง 2 แบบ
            # การคำนวณ: (ขนาดข้อความล่าสุด + ขนาด Overhead) / วงรอบวินาทีที่ใช้ดึงข้อมูล (30 วินาที หรือ 60 วินาที)
            avg_rate_30s = (mqtt_total_bytes) / 30.0 if AGGREGATION_WINDOW_SEC <= 30 else (mqtt_total_bytes) / AGGREGATION_WINDOW_SEC
            avg_rate_60s = (mqtt_total_bytes) / 60.0 if AGGREGATION_WINDOW_SEC <= 60 else (mqtt_total_bytes) / AGGREGATION_WINDOW_SEC
            
            print(f" ** อัตราการประหยัดเครือข่าย (WAN Saved Rate): {bandwidth_saved:.2f} %")
            print(f" ** [เปรียบเทียบความถูกต้องของ Metrics]")
            print(f"   - หากตั้ง Grafana rate(...[30s]): จะเหวี่ยงและเกิด Spike สูงถึง ~{avg_rate_30s:.2f} Bytes/sec")
            print(f"   - หากตั้ง Grafana rate(...[60s]): กราฟจะเสถียรและตรงตามจริงที่ ~{avg_rate_60s:.2f} Bytes/sec")
            print("==================================================================")

async def main():
    # 1. เริ่มทำการเชื่อมต่อกับ MQTT Broker ส่วนกลาง
    print(f"กำลังเชื่อมต่อกับ MQTT Broker ที่ {MQTT_BROKER}:{MQTT_PORT}...")
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()
        print("เชื่อมต่อ MQTT Broker สำเร็จ!")
    except Exception as e:
        print(f"ไม่สามารถเชื่อมต่อ MQTT Broker ได้: {e}")
        return

    # 2. ทำการเปิดพอร์ตรับข้อมูล UDP (Port 5005)
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: AsyncUDPReceiverProtocol(),
        local_addr=('0.0.0.0', UDP_PORT)
    )

    # 3. รัน Task ประมวลผลข้อมูล Aggregation แยกเซนเซอร์ ควบคู่ไปด้วยแบบไม่ขัดจังหวะกัน
    asyncio.create_task(aggregation_task())

    try:
        # รักษาลูปให้สคริปต์รันทำงานอย่างต่อเนื่องแบบ Non-blocking
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        print("\nกำลังยกเลิกการทำงาน...")
    finally:
        transport.close()
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("ปิดการทำงานของ Gateway เรียบร้อยแล้ว")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nปิดโปรแกรมสำเร็จด้วยคีย์บอร์ด")