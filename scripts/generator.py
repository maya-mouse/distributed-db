from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from datetime import datetime, timedelta
import random
import time

# --- 1. Підключення до кластера ---
try:
    # Запуск таймера для вимірювання часу виконання всього скрипту
    start_script_time = time.time()
    
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('nuclear_monitoring')
    print("Підключення до Cassandra успішне")

    # --- 2. Генерація та вставка тестових даних ---
    
    # Визначення констант для моделювання
    BLOCKS = {
        1: "Rivne NPP", 2: "Rivne NPP", 
        3: "Zaporizhzhia NPP", 4: "Zaporizhzhia NPP"
    }
    TIME_INTERVAL_MINUTES = 15
    HOURS_TO_GENERATE = 24
    
    insert_stmt = session.prepare("""
        INSERT INTO reactor_readings_by_block 
        (station_name, reactor_block_id, timestamp, temperature, pressure, power_mw, radiation_level)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    
    total_records = 0
    start_time_data = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=HOURS_TO_GENERATE)
    
    # --- ВИМІРЮВАННЯ ЧАСУ ВСТАВКИ ---
    start_insert_time = time.time()
    
    for block_id, station_name in BLOCKS.items():
        batch = BatchStatement() # Використання Batch для пакетної вставки
        for i in range(HOURS_TO_GENERATE * 60 // TIME_INTERVAL_MINUTES):
            current_time = start_time_data + timedelta(minutes=i * TIME_INTERVAL_MINUTES)
            
            # Моделювання: Блок 3 має вищу базову потужність
            base_power = 950 if block_id in [1, 2] else 980 
            power_mw = base_power + random.uniform(-10.0, 5.0) 
            temp = 310.0 + random.uniform(-1.5, 1.5)
            pressure = 15.5 + random.uniform(-0.1, 0.1)
            radiation = 0.05 + random.uniform(0.00, 0.005)
            
            batch.add(insert_stmt, (station_name, block_id, current_time, temp, pressure, power_mw, radiation))
            total_records += 1
            
        session.execute(batch)
        
    end_insert_time = time.time()
    insertion_time = end_insert_time - start_insert_time
    
    print(f"\n--- РЕЗУЛЬТАТИ ВСТАВКИ ДАНИХ ---")
    print(f"Успішно вставлено {total_records} записів")
    print(f"Час вставки (загальний): {insertion_time:.4f} секунд")


    # --- 3. Базовий аналіз (Підваріант Б: Порівняльний) ---
    
    block_id_1 = 1
    block_id_2 = 3
    
    print("\n--- АНАЛІЗ (ПОРІВНЯННЯ) ---")
    
    def calculate_average_power(block_id):
        # --- ВИМІРЮВАННЯ ЧАСУ ЧИТАННЯ ---
        start_read_time = time.time()
        
        # Запит ефективний: використовує Partition Key (block_id)
        rows = session.execute(f"SELECT power_mw FROM reactor_readings_by_block WHERE reactor_block_id = {block_id}")
        
        end_read_time = time.time()
        read_time = end_read_time - start_read_time
        
        powers = [row.power_mw for row in rows if row.power_mw is not None]
        
        # Виведення часу читання для аналізу ключової структури
        print(f"Час читання для Блоку {block_id}: {read_time:.5f} секунд.")
        
        if not powers:
            return 0.0
        
        return sum(powers) / len(powers)

    avg_power_1 = calculate_average_power(block_id_1)
    avg_power_2 = calculate_average_power(block_id_2)

    # Виведення результатів порівняння
    name_1 = f"Блок {block_id_1} ({BLOCKS[block_id_1]})"
    name_2 = f"Блок {block_id_2} ({BLOCKS[block_id_2]})"
    
    print(f"\n{name_1}: Середня потужність = {avg_power_1:.2f} МВт")
    print(f"{name_2}: Середня потужність = {avg_power_2:.2f} МВт")
    
    # Порівняльний висновок
    if avg_power_1 > avg_power_2:
        comparison = f"{name_1} ({avg_power_1:.2f} МВт) генерував більше енергії, ніж {name_2} ({avg_power_2:.2f} МВт)."
    else:
        comparison = f"{name_2} ({avg_power_2:.2f} МВт) генерував більше енергії, ніж {name_1} ({avg_power_1:.2f} МВт)."

    print(f"\n**Порівняння:** {comparison}")
    print(f"Висновок Б: Модель показала, що Блок 3, який представляє Запорізьку АЕС, має вищу базову генерацію ({avg_power_2:.2f} МВт), що може бути пов'язано з проєктною потужністю.")
    
except Exception as e:
    print(f"\n❌ Помилка виконання: {e}")
finally:
    if 'cluster' in locals() and cluster:
        cluster.shutdown()
    
    end_script_time = time.time()
    total_execution_time = end_script_time - start_script_time
    print(f"\nЗагальний час виконання скрипту (включно з підключенням): {total_execution_time:.4f} секунд.")