#!/usr/bin/env python3
"""
Простий Producer для енергетичних даних з Kafka
"""

 
from kafka import KafkaProducer # Імпортуємо KafkaProducer
import json # Імпортуємо JSON для серіалізації
import time # Імпортуємо time для затримок
import random # Імпортуємо random для генерації випадкових даних
from datetime import datetime # Імпортуємо datetime для часових міток

 
def create_producer():
    """Створюємо Kafka producer з налаштуваннями"""
    print("🔌 Підключаємся до Kafka...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['127.0.0.1:9092'], # Адреса Kafka брокера
            # Перетворюємо Python об'єкти в JSON
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'), # UTF-8 кодування
            # Налаштування для надійності
            acks='all',  # Чекаємо підтвердження від всіх реплік
            retries=3,   # Повторюємо спробу 3 рази при невдачі
            request_timeout_ms=30000, # Час очікування відповіді від брокера
            retry_backoff_ms=500, # Час очікування між спробами
            # Налаштування для продуктивності
            batch_size=16384, # 16KB пакетів
            linger_ms=100, # Чекаємо 100мс перед відправкою, щоб зібрати більше повідомлень
            compression_type='gzip',  # Стиснення для зменшення розміру повідомлень
            buffer_memory=33554432,  # 32MB буфер для повідомлень
            max_in_flight_requests_per_connection=5  # Підтримка порядку при повторних спробах
        )
        
        print("✅ Підключення до Kafka успішне!")
        return producer 
        
    except Exception as e:
        print(f"❌ Помилка підключення: {e}")
        print("Перевірте чи запущено Kafka на localhost:9092")
        return None # Якщо не вдалося підключитися, повертаємо None

 
def generate_power_data():
    """Генеруємо дані електростанції"""
    stations = [
        {"name": "Київська ТЕС", "type": "thermal", "max_power": 1200},
        {"name": "Дніпровська ГЕС", "type": "hydro", "max_power": 800},
        {"name": "Сонячна ферма", "type": "solar", "max_power": 150},
        {"name": "Вітряна ферма", "type": "wind", "max_power": 200}
    ] # Список станцій
    
    station = random.choice(stations) # Вибираємо випадкову станцію
    
    # Генеруємо реалістичні дані залежно від типу
    if station["type"] == "solar":
        # Сонячні панелі залежать від часу доби
        hour = datetime.now().hour # Поточна година
        if 6 <= hour <= 18:
            power_factor = random.uniform(0.7, 0.95) # Вдень
        else:
            power_factor = random.uniform(0.0, 0.1) # Вночі
    elif station["type"] == "wind":
        # Вітряки залежать від швидкості вітру
        wind_speed = random.uniform(0, 15)  # м/с
        if wind_speed < 3:
            power_factor = 0 # Слабкий вітер
        elif wind_speed > 12:
            power_factor = random.uniform(0.8, 1.0) # Сильний вітер
        else:
            power_factor = (wind_speed / 12) ** 2 # Квадратична залежність
    else:
        # ТЕС та ГЕС - більш стабільні
        power_factor = random.uniform(0.75, 0.95)  # Висока потужність
    
    current_power = station["max_power"] * power_factor # Потужність в МВт
    
    data = {
        "station_name": station["name"],
        "station_type": station["type"],
        "timestamp": datetime.now().isoformat(),
        "power_output_mw": round(current_power, 2),
        "voltage_kv": round(random.uniform(218, 222), 1),
        "frequency_hz": round(random.uniform(49.9, 50.1), 2),
        "efficiency_percent": round(random.uniform(82, 88), 1),
        "kafka_version": "3.7.1" 
    } 
    
    return data 

 
def main():
    """Основна функція"""
    producer = create_producer() # Підключаємося до Kafka
    if not producer:
        return # Якщо не вдалося підключитися, виходимо
    
    print("🚀 Починаємо відправку даних через Kafka...")
    print("📊 Натисніть Ctrl+C для зупинки\n")
    
    message_count = 0 # Лічильник повідомлень
    
    try:
        while True:
            # Генеруємо дані
            power_data = generate_power_data()
            
            # Відправляємо в Kafka
            try:
                future = producer.send('power-station-data-tkachenko', power_data) # Відправляємо повідомлення
                # Блокуємося, щоб отримати підтвердження від брокера
                record_metadata = future.get(timeout=10) # Чекаємо максимум 10 секунд
                message_count += 1 # Збільшуємо лічильник
                
                print(f"📤 [{message_count}] {power_data['station_name']} - {power_data['power_output_mw']} МВт")
                print(f"   Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
                
            except Exception as e:
                print(f"❌ Помилка: {e}")

 
            time.sleep(3) # Затримка між повідомленнями 3 секунди
            
    except KeyboardInterrupt:
        print(f"\n🛑 Зупинено. Всього відправлено {message_count} повідомлень")
    
    finally:
        # Важливо! Закриваємо producer коректно
        producer.flush() # Відправляємо всі буферовані повідомлення
        producer.close() # Закриваємо з'єднання
        print("🔌 З'єднання з Kafka 3.7.1 закрито")

 
if __name__ == "__main__":
    main() # Запускаємо основну функцію
