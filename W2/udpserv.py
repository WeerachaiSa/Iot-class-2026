import asyncio

class AsyncUDPReceiverProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport
        print("UDP Receiver เริ่มทำงานและพร้อมรับข้อมูลแล้ว...")

    def datagram_received(self, data, addr):
        # ฟังก์ชันนี้จะถูกเรียกทำงานโดยอัตโนมัติเมื่อมีข้อมูลส่งเข้ามา
        try:
            message = data.decode('utf-8')
            print(f"ได้รับข้อมูลจาก {addr}: {message}")
        except Exception as e:
            print(f"เกิดข้อผิดพลาดในการอ่านข้อมูล: {e}")

async def main():
    print("กำลังเปิดพอร์ต UDP...")
    loop = asyncio.get_running_loop()
    
    # สร้างจุดเชื่อมต่อ UDP บน IP '0.0.0.0' และ Port 5005
    # โดยผูกเข้ากับ Protocol ที่เราเขียนไว้ด้านบน
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: AsyncUDPReceiverProtocol(),
        local_addr=('0.0.0.0', 5005)
    )

    try:
        # เปิดให้ทำงานทิ้งไว้ตลอดไป (สามารถรันงาน async อื่นๆ ควบคู่ไปด้วยได้)
        while True:
            await asyncio.sleep(3600)  # นอนรอครั้งละ 1 ชั่วโมงไปเรื่อยๆ
    except asyncio.CancelledError:
        print("\nยกเลิกการทำงาน...")
    finally:
        transport.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nปิดโปรแกรมสำเร็จ")