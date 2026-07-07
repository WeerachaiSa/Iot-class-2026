import asyncio
import json
import random

# ==================================================================
#         ส่วนกำหนดค่าสำหรับนักเรียน (Student Simulation Config)
# ==================================================================
STUDENT_ID = "68123456789"  # แก้ไขเป็นรหัสนักศึกษาของตนเองเพื่อระบุตัวตน

# === กำหนดการเชื่อมต่อไปยัง Gateway ===
GATEWAY_IP = "172.16.2.117"    # ไอพีของเครื่อง Gateway (ใช้ '127.0.0.1' หากรันบนคอมพิวเตอร์เครื่องเดียวกัน)
GATEWAY_PORT = 5005         # พอร์ต UDP ของฝั่ง Gateway

# === ตั้งค่าความถี่ในการส่งข้อมูลดิบ ===
SEND_INTERVAL_SEC = 5       # ความถี่การจำลองส่งข้อมูลจากเซนเซอร์ทุก ๆ 5 วินาที
# ==================================================================

class AsyncUDPSenderProtocol(asyncio.DatagramProtocol):
    def __init__(self, message, target_addr):
        self.message = message
        self.target_addr = target_addr

    def connection_made(self, transport):
        # ส่งข้อมูล JSON ที่แปลงเป็น bytes แล้วไปยังเซิร์ฟเวอร์เกตเวย์ปลายทาง
        transport.sendto(self.message.encode('utf-8'), self.target_addr)
        transport.close()

def generate_mock_sensor_data():
    """ จำลองการอ่านค่าจากเซนเซอร์ต่างๆ และจัดรูปแบบตามโครงสร้างของบอร์ด ESP32 """
    temperature = round(random.uniform(25.0, 28.5), 2)  # สุ่มค่าอุณหภูมิห้องทดลอง
    humidity = round(random.uniform(40.0, 55.0), 2)     # สุ่มค่าความชื้น
    pressure = round(random.uniform(100100.0, 100300.0), 2)  # สุ่มค่าความกดอากาศ (Pa)

    # ประกอบโครงสร้าง JSON Object ให้สมบูรณ์ตรงตามที่ระบุใน iot_class_2025.ino
    sensor_frame = {
        "id": f"ID_{STUDENT_ID}",
        "name": f"NAME_{STUDENT_ID}",
        "place_id": f"PLACE_{STUDENT_ID}",
        "payload": {
            "temperature": temperature,
            "humidity": humidity,
            "pressure": pressure
        }
    }
    return sensor_frame

async def send_sensor_data(target_addr):
    loop = asyncio.get_running_loop()

    # 1. ดึงข้อมูล JSON จำลองล่าสุด
    data = generate_mock_sensor_data()

    # 2. แปลง Dictionary ให้เป็น String รูปแบบ JSON
    json_payload = json.dumps(data)

    # 3. แสดงผลบนหน้า Console ของนักเรียนเพื่อตรวจสอบ
    print(f"\n[UDP OUT] -> ส่งข้อมูลสำเร็จไปยัง {target_addr[0]}:{target_addr[1]}")
    print(f"   Payload: Temp: {data['payload']['temperature']} C | Humid: {data['payload']['humidity']} % | Press: {data['payload']['pressure']} Pa")

    # 4. เรียกทำงานผ่าน Async UDP Endpoint เพื่อยิง Packet ออกไป
    await loop.create_datagram_endpoint(
        lambda: AsyncUDPSenderProtocol(json_payload, target_addr),
        remote_addr=target_addr
    )

async def main():
    target_addr = (GATEWAY_IP, GATEWAY_PORT)
    packet_count = 0
    
    print("==================================================================")
    print(f" Asyncio UDP Sensor Simulator [ACTIVE] (ID: ID_{STUDENT_ID})")
    print(f" - ส่งข้อมูลไปยัง Gateway: {GATEWAY_IP}:{GATEWAY_PORT}")
    print(f" - ความถี่ในการทำงาน: ทุก ๆ {SEND_INTERVAL_SEC} วินาที (วนลูปต่อเนื่อง)")
    print(" - กด Ctrl+C เพื่อหยุดการทำงาน")
    print("==================================================================")
    
    try:
        # ปรับเปลี่ยนลูปให้ทำงานตลอดไป (Infinite Loop) เพื่อจำลองเซนเซอร์ที่ส่งค่าแบบ Real-time
        while True:
            packet_count += 1
            await send_sensor_data(target_addr)
            # พักตามเวลาที่กำหนด (5 วินาที) ก่อนส่งรอบถัดไป
            await asyncio.sleep(SEND_INTERVAL_SEC)
            
    except asyncio.CancelledError:
        print("\nยกเลิกการส่งข้อมูล...")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOP] ปิดโปรแกรมจำลองเซนเซอร์สำเร็จ")
