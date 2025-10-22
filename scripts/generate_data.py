import random
import datetime
import json
import string
import time

def generate_nuclear_data(device_id_prefix="REACTOR_ZA", record_size=256):
    """Генерує реалістичне повідомлення для АЕС (Варіант 5)."""
    
    # Задані діапазони параметрів для АЕС (Варіант 5)
    neutron_flux = round(random.uniform(90.0, 100.0), 2) # 90.0-100.0%
    pressure = round(random.uniform(150.0, 160.0), 1)   # 150.0-160.0 атм
    
    # Випадковий вибір статусу та позиції стрижнів
    rod_positions = ["75pct", "85pct", "95pct", "100pct"]
    statuses = ["normal", "maintenance", "shutdown"]
    
    # Базовий JSON-об'єкт (поля, які дають 144 байти)
    base_data = {
        "device_id": f"{device_id_prefix}_{random.choice(['1', '2'])}", # REACTOR_ZA_1, REACTOR_ZA_2, etc.
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z'),
        "power_output": round(random.uniform(800.0, 1000.0), 1),
        "efficiency": round(random.uniform(32.0, 35.0), 1),
        "temperature": round(random.uniform(285.0, 295.0), 1),
        "voltage": round(random.uniform(21000.0, 23000.0), 1),
        "current": round(random.uniform(20000.0, 28000.0), 1),
        "status": random.choice(statuses),
        "location": {"lat": 49.0, "lon": 34.0}, # Усереднена локація
        "maintenance_hours": random.randint(1000, 8760),
        "neutron_flux": neutron_flux,         # Критичний параметр 1
        "pressure": pressure,                 # Критичний параметр 2
        "rod_position": random.choice(rod_positions) # Критичний параметр 3
    }

    # Серіалізувати та розрахувати розмір
    json_string = json.dumps(base_data)
    current_size = len(json_string.encode('utf-8'))
    
    # Додати padding для досягнення цільового розміру 256 байт
    if current_size < record_size:
        padding_needed = record_size - current_size - len('"padding":""')
        # Додаємо унікальний рядок для padding, щоб стиснення було складнішим
        padding_value = ''.join(random.choices(string.ascii_letters + string.digits, k=padding_needed))
        base_data["reserved_padding"] = padding_value
        
    return base_data

# Приклад генерації даних для перевірки:
print(json.dumps(generate_nuclear_data(), indent=2))