from cassandra.cluster import Cluster
from cassandra.query import BatchStatement # Залишаємо тільки для вставки агрегатів
from datetime import datetime, timedelta, date
import random
import time
import math

# --- КОНФІГУРАЦІЯ ТЕСТУ ---
KEYSPACE = 'nuclear_optimization'
CONTACT_POINTS = ['127.0.0.1'] 
NUM_REACTORS = 4
DAYS_TO_GENERATE = 30 
MINUTES_IN_SIMULATION = DAYS_TO_GENERATE * 24 * 60 # 43200 хвилин
# ЗАГАЛЬНА КІЛЬКІСТЬ ЗАПИСІВ (3 таблиці * 43200 хв * 4 реактори) = 518,400
ITERATIONS_PER_QUERY = 50 

# --- МЕТОДИ BUCKETING ---
def get_bucket_minute(ts):
    """Формат: YYYYMMDDHHmm"""
    return ts.strftime('%Y%m%d%H%M')

def get_bucket_date(ts):
    """Формат: YYYY-MM-DD (для типу DATE)"""
    return ts.date()

# --- ФУНКЦІЯ ВИМІРЮВАННЯ ЛАТЕНТНОСТІ ---
def measure_latency(session, query, *params):
    times = []
    # 5 ітерацій на розігрів кешу
    for _ in range(5):
        try:
            session.execute(query, params)
        except Exception:
            pass 
            
    # Вимірювання робочих ітерацій
    for _ in range(ITERATIONS_PER_QUERY):
        start = time.perf_counter()
        try:
            session.execute(query, params)
        except Exception as e:
            # Фіксуємо 5 секунд у разі повільного ALLOW FILTERING або помилки
            times.append(5000.0) 
            continue
            
        end = time.perf_counter()
        times.append((end - start) * 1000) # в мілісекундах
        
    avg = sum(times) / len(times)
    times.sort()
    p95 = times[int(len(times) * 0.95)]
    p99 = times[int(len(times) * 0.99)]
    return avg, p95, p99

# --- ОСНОВНА ЛОГІКА СКРИПТУ ---
def run_benchmark():
    cluster = None
    try:
        cluster = Cluster(CONTACT_POINTS)
        session = cluster.connect(KEYSPACE)
        print(f"\n✅ Підключення до Cassandra ({KEYSPACE}) успішне.")

        # --- PREPARED STATEMENTS ---
        insert_minute_stmt = session.prepare("INSERT INTO telemetry_minute_bucket (reactor_id, bucket_minute, timestamp, neutron_flux, pressure, rod_position) VALUES (?, ?, ?, ?, ?, ?)")
        insert_daily_stmt = session.prepare("INSERT INTO telemetry_daily_raw (reactor_id, bucket_date, timestamp, neutron_flux, pressure, rod_position) VALUES (?, ?, ?, ?, ?, ?)")
        insert_simple_stmt = session.prepare("INSERT INTO telemetry_simple (reactor_id, timestamp, neutron_flux, pressure, rod_position) VALUES (?, ?, ?, ?, ?)")
        agg_insert_stmt = session.prepare("INSERT INTO aggregates_hourly (reactor_id, bucket_hour, avg_flux, max_pressure, num_samples) VALUES (?, ?, ?, ?, ?)")
        
        # ==========================================================
        # ЕТАП 2: ГЕНЕРАЦІЯ ТА ВСТАВКА ДАНИХ (1.7M)
        # ВСТАВКА: ОДНА ОПЕРАЦІЯ ЗА РАЗ (Уникнення Batch too large)
        # ==========================================================
        print("\n=== ЕТАП 2: ГЕНЕРАЦІЯ ТА ВСТАВКА ДАНИХ (30 днів, 1/хвилину) ===")
        start_time_data = datetime.now().replace(second=0, microsecond=0) - timedelta(minutes=MINUTES_IN_SIMULATION)
        
        total_inserts = 0
        hourly_agg = {} 

        start_insert_time = time.perf_counter()
        
        for reactor_id in range(1, NUM_REACTORS + 1):
            
            for i in range(MINUTES_IN_SIMULATION): # Ітерація за хвилинами
                current_time = start_time_data + timedelta(minutes=i)
                
                # Моделювання даних АЕС
                flux = round(random.uniform(90.0, 100.0), 2) 
                pressure = round(random.uniform(150.0, 160.0), 1)
                rod_pos = random.choice(['75pct', '85pct', '95pct', '100pct'])
                
                bucket_minute_val = get_bucket_minute(current_time)
                bucket_date_val = get_bucket_date(current_time)
                bucket_hour_val = current_time.replace(minute=0, second=0)

                # --- 1. Вставка в СХЕМИ (ПРЯМЕ ВИКОНАННЯ) ---
                session.execute(insert_minute_stmt, (reactor_id, bucket_minute_val, current_time, flux, pressure, rod_pos))
                session.execute(insert_daily_stmt, (reactor_id, bucket_date_val, current_time, flux, pressure, rod_pos))
                session.execute(insert_simple_stmt, (reactor_id, current_time, flux, pressure, rod_pos))

                # --- 2. Агрегація (для СХЕМИ 3.2) ---
                key = (reactor_id, bucket_hour_val)
                if key not in hourly_agg:
                    hourly_agg[key] = {'flux_sum': 0.0, 'pressure_max': 0.0, 'count': 0}

                hourly_agg[key]['flux_sum'] += flux
                hourly_agg[key]['pressure_max'] = max(hourly_agg[key]['pressure_max'], pressure)
                hourly_agg[key]['count'] += 1
                
                total_inserts += 3 
                
                # Відображення прогресу кожні 10000 ітерацій
                if total_inserts % 10000 == 0:
                    print(f"  Прогрес: {total_inserts // 3} / {MINUTES_IN_SIMULATION * NUM_REACTORS // 3} хвилин...")


        # --- ВСТАВКА АГРЕГАТІВ (СХЕМА 3.2, ВИКОРИСТОВУЄМО BATCH ДЛЯ ЕФЕКТИВНОСТІ) ---
        print("\n  Вставка агрегатів (розбиваємо на пакети по 100 записів)...")
        agg_batch = BatchStatement()
        agg_inserts = 0
        BATCH_AGGREGATE_SIZE = 100 # Новий ліміт для агрегатів
        
        # Створюємо список агрегатів для ітерації
        aggregate_list = list(hourly_agg.items())
        
        for (r_id, h_bucket), data in aggregate_list:
            if data['count'] > 0:
                avg_flux = round(data['flux_sum'] / data['count'], 2)
                agg_batch.add(agg_insert_stmt, (r_id, h_bucket, avg_flux, data['pressure_max'], data['count']))
                agg_inserts += 1

                # Виконання Batch кожні 100 записів
                if agg_inserts % BATCH_AGGREGATE_SIZE == 0:
                    session.execute(agg_batch)
                    agg_batch = BatchStatement()
        
        # Виконання фінального Batch (залишку)
        if len(agg_batch) > 0:
            session.execute(agg_batch)


        end_insert_time = time.perf_counter()
        insertion_time = end_insert_time - start_insert_time
        
        print(f"  Успішно вставлено {total_inserts} записів (загалом).")
        print(f"  Кількість агрегованих записів (Hourly): {agg_inserts}.")
        print(f"  Час вставки (загальний): {insertion_time:.2f} секунд. Throughput: {total_inserts / insertion_time:.0f} rec/sec")


        # ==========================================================
        # ЕТАП 4, ЧАСТИНА 1: СТВОРЕННЯ MATERIALIZED VIEW
        # ==========================================================
        print("\n=== ЕТАП 4: СТВОРЕННЯ MATERIALIZED VIEW (Focus Б) ===")
        
        mv_name = "telemetry_high_pressure_mv"

        # Видалення MV, якщо існує (потрібно для повторних запусків)
        session.execute(f"DROP MATERIALIZED VIEW IF EXISTS nuclear_optimization.{mv_name}")
        
        # СТВОРЕННЯ MV ДЛЯ ФІЛЬТРАЦІЇ ВИСОКОГО ТИСКУ (Q4)
        try:
             session.execute(f"""
                CREATE MATERIALIZED VIEW {mv_name} AS
                    SELECT reactor_id, bucket_minute, pressure, timestamp, neutron_flux
                    FROM telemetry_minute_bucket
                    WHERE reactor_id IS NOT NULL AND bucket_minute IS NOT NULL AND pressure IS NOT NULL
                    PRIMARY KEY ((reactor_id, bucket_minute), pressure, timestamp)
                    WITH CLUSTERING ORDER BY (pressure DESC);
            """)
             print(f"  ✅ Materialized View '{mv_name}' успішно створено.")
        except Exception as e:
             print(f"  ❌ Помилка створення MV: {e}")
             print("     (Увага: Можливо, MV вимкнені у конфігурації сервера)")


        # ==========================================================
        # ЕТАП 3 & 4: ТЕСТУВАННЯ ПРОДУКТИВНОСТІ ЗАПИТІВ (Q1, Q3, Q4)
        # ==========================================================
        print("\n=== ЕТАП 3 & 4: ТЕСТУВАННЯ ПРОДУКТИВНОСТІ (Latency) ===")
        
        # Визначаємо параметри для тестування
        test_reactor_id = 1
        # Вибираємо випадкову хвилину для тестування Q1/Q4
        test_minute_offset = random.randint(300, MINUTES_IN_SIMULATION - 300) 
        test_time_q = start_time_data + timedelta(minutes=test_minute_offset)
        test_minute = get_bucket_minute(test_time_q)
        test_date = get_bucket_date(test_time_q)

        results = []
        
        # Q1: Latest Data (Схема 2 - Minute Bucketing) - ІДЕАЛЬНА ПАРТИЦІЯ
        q1_query = f"SELECT * FROM telemetry_minute_bucket WHERE reactor_id = {test_reactor_id} AND bucket_minute = '{test_minute}' LIMIT 60"
        avg, p95, p99 = measure_latency(session, q1_query)
        results.append(('Q1 (Minute Bucket)', 'Minute (2)', avg, p95, p99))
        
        # Q3: Daily Aggregation (Схема 3.2 - Aggregates) - ІДЕАЛЬНА ДЛЯ ЗВІТІВ
        q3_query = f"SELECT avg_flux, max_pressure FROM aggregates_hourly WHERE reactor_id = {test_reactor_id} LIMIT 100"
        avg, p95, p99 = measure_latency(session, q3_query)
        results.append(('Q3 (Aggregates)', 'Aggregates (3.2)', avg, p95, p99))


        # --- ТЕСТУВАННЯ Q4: MV vs. ALLOW FILTERING ---
        
        # Q4.1: Filtered Query (MV Optimized - Focus Б)
        q4_mv_query = f"SELECT * FROM {mv_name} WHERE reactor_id = {test_reactor_id} AND bucket_minute = '{test_minute}' AND pressure > 155.0 LIMIT 10"
        try:
             avg_mv, p95_mv, p99_mv = measure_latency(session, q4_mv_query)
        except Exception:
             avg_mv, p95_mv, p99_mv = 5000.0, 5000.0, 5000.0 # Якщо MV не створилося, фіксуємо таймаут

        results.append(('Q4.1 (MV Optimized)', 'MV', avg_mv, p95_mv, p99_mv))

        # Q4.2: Filtered Query (ALLOW FILTERING - Антипатерн)
        # Використовуємо велику партицію (Daily Raw) для демонстрації високої затримки
        q4_af_query = f"SELECT * FROM telemetry_daily_raw WHERE reactor_id = {test_reactor_id} AND bucket_date = '{test_date}' AND pressure > 155.0 ALLOW FILTERING"
        avg_af, p95_af, p99_af = measure_latency(session, q4_af_query)
        results.append(('Q4.2 (ALLOW FILTERING)', 'Daily Raw (3.1)', avg_af, p95_af, p99_af))


        # --- ВИВЕДЕННЯ ТАБЛИЦІ РЕЗУЛЬТАТІВ ---
        print("\n--- РЕЗУЛЬТАТИ LATENCY (мс) ---")
        print(f"{'Запит (Схема)':<24} | {'Avg Latency':<15} | {'P95 Latency':<15} | {'P99 Latency':<15}")
        print("-" * 75)
        for name, table, avg, p95, p99 in results:
             print(f"{name:<24} | {avg:.3f} ms | {p95:.3f} ms | {p99:.3f} ms")

        # АНАЛІЗ FOCUS Б (ПОРІВНЯННЯ MV)
        if avg_mv < 5000.0: # Тільки якщо MV був успішно створений і протестований
            improvement = avg_af / avg_mv
            print(f"\n✅ АНАЛІЗ FOCUS Б (MV vs. ALLOW FILTERING):")
            print(f"   Запит з MV ({avg_mv:.3f} мс) виконується у {improvement:.1f} разів швидше, ніж ALLOW FILTERING ({avg_af:.3f} мс).")
        else:
             print("\n⚠️ АНАЛІЗ FOCUS Б: MV не було створено, порівняння не виконано.")


    except Exception as e:
        print(f"\n ❌  Критична Помилка виконання: {e}")
        print("\n Перевірте, чи всі CREATE TABLE команди виконані у cqlsh та чи всі стовпці у таблицях мають правильний тип даних (DECIMAL/TEXT/TIMESTAMP).")
    finally:
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    run_benchmark()