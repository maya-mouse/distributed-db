#!/usr/bin/env python3
"""
Простий Consumer для енергетичних даних з Kafka
"""

 
from kafka import KafkaConsumer # Імпортуємо KafkaConsumer
import json # Імпортуємо JSON для десеріалізації
from datetime import datetime # Імпортуємо datetime для часових міток
import time # Імпортуємо time для унікальної групи

 
def create_consumer():
    """Створюємо Kafka consumer з налаштуваннями"""
    print("🔌 Підключаємся до Kafka як Consumer...")
    
    try:
        consumer = KafkaConsumer(
            'power-station-data-tkachenko',  # Topic який читаємо
            bootstrap_servers=['127.0.0.1:9092'],  # Адреса Kafka брокера
            auto_offset_reset='latest',  # Починаємо з останніх повідомлень
            group_id=f'energy-monitor-{int(time.time())}',#'energy-monitor-group',  # Група consumers
            # Перетворюємо JSON назад в Python об'єкти
            value_deserializer=lambda m: json.loads(m.decode('utf-8')), # UTF-8 декодування
            # Налаштування для надійності
            enable_auto_commit=True, # Автоматичний коміт збережених офсетів
            auto_commit_interval_ms=5000, # Комітимо кожні 5 секунд
            session_timeout_ms=30000, # 30 секунд
            heartbeat_interval_ms=10000, # 10 секунд
            max_poll_records=500, # Максимум 500 повідомлень за раз
            # Налаштування для продуктивності
            fetch_min_bytes=1024, # Мінімум 1KB для отримання
            fetch_max_wait_ms=1000 # Чекаємо до 1 секунди для збору даних
        )
        
        print("✅ Consumer для Kafka готовий до роботи!")
        return consumer # Повертаємо створений consumer
        
    except Exception as e:
        print(f"❌ Помилка підключення: {e}")
        return None

 
def analyze_power_data(data):
    """Аналізуємо енергетичні дані та повертаємо статуси"""
    try:
        station = data.get('station_name', 'Невідома станція') # Назва станції
        station_type = data.get('station_type', 'unknown') # Тип станції
        power = data.get('power_output_mw', 0) # Потужність в МВт
        voltage = data.get('voltage_kv', 0) # Напруга в кВ
        frequency = data.get('frequency_hz', 0) # Частота в Гц
        efficiency = data.get('efficiency_percent', 0) # ККД в %
        kafka_version = data.get('kafka_version', 'unknown') # Версія Kafka
        
        # Статус за потужністю
        if power == 0:
            power_status = "🔴 ВІДКЛЮЧЕНА"
        elif power < 100:
            power_status = "🟡 НИЗЬКА ПОТУЖНІСТЬ"
        elif power > 1000:
            power_status = "🟢 ВИСОКА ПОТУЖНІСТЬ"
        else:
            power_status = "🟢 НОРМАЛЬНА ПОТУЖНІСТЬ"
        
        # Статус за напругою
        if 219 <= voltage <= 221:
            voltage_status = "🟢 НОРМА"
        else:
            voltage_status = "🟠 ВІДХИЛЕННЯ"
        
        # Статус за частотою
        if 49.9 <= frequency <= 50.1:
            frequency_status = "🟢 НОРМА"
        else:
            frequency_status = "🔴 КРИТИЧНО"
        
        # Статус за ефективністю
        if efficiency >= 85:
            efficiency_status = "🟢 ВІДМІННО"
        elif efficiency >= 80:
            efficiency_status = "🟡 ДОБРЕ"
        else:
            efficiency_status = "🟠 ПОГАНО"
        
        return {
            'power_status': power_status,
            'voltage_status': voltage_status,
            'frequency_status': frequency_status,
            'efficiency_status': efficiency_status,
            'kafka_version': kafka_version
        }
        
    except Exception as e:
        print(f"❌ Помилка аналізу: {e}")
        return None

 
def process_power_data(data):
    """Обробляємо отримані дані"""
    try:
        station = data.get('station_name', 'Невідома станція') # Назва станції
        station_type = data.get('station_type', 'unknown') # Тип станції
        power = data.get('power_output_mw', 0) # Потужність в МВт
        voltage = data.get('voltage_kv', 0) # Напруга в кВ
        frequency = data.get('frequency_hz', 0) # Частота в Гц
        efficiency = data.get('efficiency_percent', 0) # ККД в %
        timestamp = data.get('timestamp', 'unknown') # Часова мітка
        
        # Аналізуємо дані
        analysis = analyze_power_data(data)
        
        # Виводимо результат
        print(f"\n📋 === МОНІТОРИНГ ЕНЕРГОСИСТЕМИ ===")
        print(f"🏭 Станція: {station} ({station_type.upper()})")
        print(f"🕐 Час: {timestamp}")
        print(f"⚡ Потужність: {power} МВт - {analysis['power_status']}")
        print(f"🔌 Напруга: {voltage} кВ - {analysis['voltage_status']}")
        print(f"📊 Частота: {frequency} Гц - {analysis['frequency_status']}")
        print(f"⚙️ ККД: {efficiency}% - {analysis['efficiency_status']}")
        if analysis['kafka_version'] != 'unknown':
            print(f"🚀 Kafka версія: {analysis['kafka_version']}")
        
        # Попередження та рекомендації
        warnings = []
        if power < 50:
            warnings.append("⚠️ Критично низька потужність - перевірити систему")
        if voltage < 218 or voltage > 222:
            warnings.append("⚠️ Напруга поза допустимими межами")
        if frequency < 49.8 or frequency > 50.2:
            warnings.append("🚨 КРИТИЧНО: Частота поза межами - негайні дії!")
        if efficiency < 75:
            warnings.append("⚠️ Низький ККД - потрібне технічне обслуговування")
        
        if warnings:
            print("\n🚨 ПОПЕРЕДЖЕННЯ:")
            for warning in warnings:
                print(f"   {warning}")
        
        # Рекомендації по типу станції
        if station_type == "solar" and power < 50:
            print("💡 Можливо хмарна погода - це нормально для сонячних станцій")
        elif station_type == "wind" and power < 50:
            print("💡 Можливо низька швидкість вітру - це нормально для вітряків")
        
        print("-" * 55)
        
    except Exception as e:
        print(f"❌ Помилка обробки: {e}")
        print(f"📝 Сирі дані: {data}")

 
def main():
    """Основна функція"""
    consumer = create_consumer() # Підключаємося до Kafka
    if not consumer:
        return # Якщо не вдалося підключитися, виходимо
    
    print("👀 Очікуємо дані від електростанцій...")
    print("🛑 Натисніть Ctrl+C для зупинки\n")
    
    message_count = 0 # Лічильник повідомлень
    
    try:
        for message in consumer: # Ітеруємося по повідомленнях
            message_count += 1 # Збільшуємо лічильник
            
            print(f"📨 Повідомлення #{message_count}")
            print(f"📍 Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")
            
            # Обробляємо отримані дані
            process_power_data(message.value)
            
    except KeyboardInterrupt:
        print(f"\n🛑 Consumer зупинено. Оброблено {message_count} повідомлень")
    
    finally:
        consumer.close() # Важливо! Закриваємо consumer коректно
        print("🔌 З'єднання закрито")

 
if __name__ == "__main__":
    main() # Запускаємо основну функцію
