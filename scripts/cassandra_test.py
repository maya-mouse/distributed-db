from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from datetime import datetime, timedelta, date
import random
import time
import math

# --- КОНФІГУРАЦІЯ ТЕСТУ ---
KEYSPACE = 'nuclear_optimization'
CONTACT_POINTS = ['127.0.0.1'] 
ITERATIONS_PER_QUERY = 50 
MINUTES_IN_SIMULATION = 30 * 24 * 60 

# --- МЕТОДИ BUCKETING ---
def get_bucket_minute(ts):
    return ts.strftime('%Y%m%d%H%M')

def get_bucket_date(ts):
    return ts.date()

# --- ФУНКЦІЯ ВИМІРЮВАННЯ ЛАТЕНТНОСТІ ---
def measure_latency(session, query, *params):
    # ... (функція measure_latency залишається незмінною) ...
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
            times.append(5000.0) 
            continue
            
        end = time.perf_counter()
        times.append((end - start) * 1000)
        
    avg = sum(times) / len(times)
    times.sort()
    p95 = times[int(len(times) * 0.95)]
    p99 = times[int(len(times) * 0.99)]
    return avg, p95, p99

# --- ФУНКЦІЯ ПЕРЕВІРКИ ТА СТВОРЕННЯ MV ---
def create_mv_if_needed(session, mv_name):
    print(f"\n-> Перевірка та спроба створення MV '{mv_name}'...")
    
    # 1. Перевірка, чи існує MV
    try:
        session.execute(f"SELECT * FROM system_views.materialized_views WHERE view_name = '{mv_name}' AND keyspace_name = '{KEYSPACE}'")
        print(f" MV '{mv_name}' вже існує. Пропускаємо створення.")
        return True
    except Exception:
        # Якщо запит не спрацював (наприклад, через те, що MV вимкнені) - продовжуємо спробу
        pass
    
    # 2. Спроба створення MV
    try:
        session.execute(f"DROP MATERIALIZED VIEW IF EXISTS {KEYSPACE}.{mv_name}")
        session.execute(f"""
            CREATE MATERIALIZED VIEW {mv_name} AS
                SELECT reactor_id, bucket_minute, pressure, timestamp, neutron_flux
                FROM telemetry_minute_bucket
                WHERE reactor_id IS NOT NULL 
                AND bucket_minute IS NOT NULL 
                AND pressure IS NOT NULL
                AND timestamp IS NOT NULL
                PRIMARY KEY ((reactor_id, bucket_minute), pressure, timestamp)
                WITH CLUSTERING ORDER BY (pressure DESC, timestamp DESC);
        """)
        
        print(f" MV '{mv_name}' успішно створено.")
        return True
    except Exception as e:
        print(f" Помилка створення MV: {e}")
        print("     (Увага: MV вимкнені. Тест Q4.1 не буде виконано коректно.)")
        return False


# --- ОСНОВНА ЛОГІКА СКРИПТУ ---
def run_benchmark():
    cluster = None
    try:
        cluster = Cluster(CONTACT_POINTS)
        session = cluster.connect(KEYSPACE)
        print(f"\n Підключення до Cassandra ({KEYSPACE}) успішне.")

        # --- PREPARED STATEMENTS (залишаються незмінними) ---
        insert_minute_stmt = session.prepare("INSERT INTO telemetry_minute_bucket (reactor_id, bucket_minute, timestamp, neutron_flux, pressure, rod_position) VALUES (?, ?, ?, ?, ?, ?)")
        insert_daily_stmt = session.prepare("INSERT INTO telemetry_daily_raw (reactor_id, bucket_date, timestamp, neutron_flux, pressure, rod_position) VALUES (?, ?, ?, ?, ?, ?)")
        insert_simple_stmt = session.prepare("INSERT INTO telemetry_simple (reactor_id, timestamp, neutron_flux, pressure, rod_position) VALUES (?, ?, ?, ?, ?)")
        agg_insert_stmt = session.prepare("INSERT INTO aggregates_hourly (reactor_id, bucket_hour, avg_flux, max_pressure, num_samples) VALUES (?, ?, ?, ?, ?)")
        
        # ==========================================================
        # ЕТАП 2: ГЕНЕРАЦІЯ ТА ВСТАВКА (Логіка без Batch залишається)
        # ... (Вставте тут код ЕТАПУ 2, який успішно працював) ...
        
        print("\n ПРИМІТКА: ЕТАП 2 (Генерація) ПРОПУЩЕНО, якщо дані вже існують.")
        # З міркувань часу та уникнення дублювання, генерацію даних
        # опускаємо, оскільки ви її успішно виконали і дані є в базі.
        # ТУТ МАЄ БУТИ КОД ГЕНЕРАЦІЇ З ВСТАВКОЮ З ЕТАПУ 2, ЯКИЙ ВИКОНАВСЯ УСПІШНО!
        
        start_time_data = datetime.now().replace(second=0, microsecond=0) - timedelta(minutes=MINUTES_IN_SIMULATION)
        
        # ==========================================================
        # ЕТАП 4, ЧАСТИНА 1: СТВОРЕННЯ MATERIALIZED VIEW
        # ==========================================================
        mv_name = "telemetry_high_pressure_mv"
        mv_created = create_mv_if_needed(session, mv_name)

        # ==========================================================
        # ЕТАП 3 & 4: ТЕСТУВАННЯ ПРОДУКТИВНОСТІ ЗАПИТІВ (Q1, Q3, Q4)
        # ==========================================================
        print("\n=== ЕТАП 3 & 4: ТЕСТУВАННЯ ПРОДУКТИВНОСТІ (Latency) ===")
        
        # Визначаємо параметри для тестування
        test_reactor_id = 1
        test_minute_offset = random.randint(300, MINUTES_IN_SIMULATION - 300) 
        test_time_q = start_time_data + timedelta(minutes=test_minute_offset)
        test_minute = get_bucket_minute(test_time_q)
        test_date = get_bucket_date(test_time_q)

        results = []
        
        # Q1, Q3 (Залишаються незмінними)
        q1_query = f"SELECT * FROM telemetry_minute_bucket WHERE reactor_id = {test_reactor_id} AND bucket_minute = '{test_minute}' LIMIT 60"
        avg, p95, p99 = measure_latency(session, q1_query)
        results.append(('Q1 (Minute Bucket)', 'Minute (2)', avg, p95, p99))
        
        q3_query = f"SELECT avg_flux, max_pressure FROM aggregates_hourly WHERE reactor_id = {test_reactor_id} LIMIT 100"
        avg, p95, p99 = measure_latency(session, q3_query)
        results.append(('Q3 (Aggregates)', 'Aggregates (3.2)', avg, p95, p99))

        # --- Q4.1: Filtered Query (MV Optimized - Focus Б) ---
        if mv_created:
             q4_mv_query = f"SELECT * FROM {mv_name} WHERE reactor_id = {test_reactor_id} AND bucket_minute = '{test_minute}' AND pressure > 155.0 LIMIT 10"
             avg_mv, p95_mv, p99_mv = measure_latency(session, q4_mv_query)
        else:
             avg_mv, p95_mv, p99_mv = 5000.0, 5000.0, 5000.0
             
        results.append(('Q4.1 (MV Optimized)', 'MV', avg_mv, p95_mv, p99_mv))

        # Q4.2: Filtered Query (ALLOW FILTERING - Антипатерн)
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
        if mv_created and avg_mv < 5000.0:
            improvement = avg_af / avg_mv
            print(f"\n АНАЛІЗ FOCUS Б (MV vs. ALLOW FILTERING):")
            print(f"   Запит з MV ({avg_mv:.3f} мс) виконується у {improvement:.1f} разів швидше, ніж ALLOW FILTERING ({avg_af:.3f} мс).")
        else:
             print("\n АНАЛІЗ FOCUS Б: MV не було створено або тест завершився помилкою. Порівняння не виконано.")


    except Exception as e:
        print(f"\n   Критична Помилка виконання: {e}")
    finally:
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    # Визначення параметрів і функцій (потрібно для запуску):
    def get_bucket_minute(ts):
        return ts.strftime('%Y%m%d%H%M')
    def get_bucket_date(ts):
        return ts.date()
    
    run_benchmark()