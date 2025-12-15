from prometheus_client import Counter, Gauge, start_http_server
import time
import random

PROMETHEUS_PORT = 8000
SCRAPE_INTERVAL = 5 

NOMINAL_POWER = 980.0
REACTORS = ['Rivne-3', 'Rivne-4', 'ZAP-5', 'ZAP-6']

CRASH_ITERATION_LIMIT = 100
SIMULATION_FREEZE_TIME = 90

BURNUP_ACCUMULATOR = {r: 0.0 for r in REACTORS}
EFFICIENCY_STATE = {r: 99.5 for r in REACTORS}

reactor_power = Gauge(
    'reactor_power_mw',
    'Поточна потужність реактора в МВт',
    ['reactor_id']
)

reactor_burnup = Gauge(
    'reactor_burnup_pct',
    'Вигорання палива (%)',
    ['reactor_id']
)

reactor_efficiency = Gauge(
    'reactor_efficiency_pct',
    'Ефективність реактора (%)',
    ['reactor_id']
)

energy_generated_total = Counter(
    'reactor_energy_generated_total',
    'Накопичена генерація (МВт*с)',
    ['reactor_id']
)

reactor_usage_factor = Gauge(
    'reactor_usage_factor',
    'Коефіцієнт використання реактора',
    ['reactor_id']
)

producer_heartbeat = Gauge(
    'producer_heartbeat',
    'Last successful producer update timestamp'
)

def update_burnup(reactor_id: str) -> float:
    current = BURNUP_ACCUMULATOR[reactor_id]

    if current < 70:
        delta = random.uniform(0.2, 0.4)
    elif current < 90:
        delta = random.uniform(0.05, 0.12)
    elif current < 100:
        delta = random.uniform(0.01, 0.03)
    else:
        delta = random.uniform(-0.2, -0.05)

    new_value = max(0, min(current + delta, 102))
    BURNUP_ACCUMULATOR[reactor_id] = new_value
    return new_value


def update_efficiency(reactor_id: str, burnup: float) -> float:
    current = EFFICIENCY_STATE[reactor_id]

    if burnup < 70:
        target = random.uniform(98.5, 100.0)
    elif burnup < 90:
        target = random.uniform(95.0, 98.0)
    else:
        target = random.uniform(92.0, 96.0)

    inertia = random.uniform(0.05, 0.15)
    new_efficiency = current + (target - current) * inertia

    EFFICIENCY_STATE[reactor_id] = new_efficiency
    return new_efficiency


def generate_and_publish_metrics():
    producer_heartbeat.set_to_current_time()

    for reactor_id in REACTORS:
        burnup = update_burnup(reactor_id)
        efficiency = update_efficiency(reactor_id, burnup)

        base_power = random.uniform(NOMINAL_POWER - 8, NOMINAL_POWER + 4)
        power = base_power * (efficiency / 100.0)

        reactor_power.labels(reactor_id=reactor_id).set(power)
        reactor_burnup.labels(reactor_id=reactor_id).set(burnup)
        reactor_efficiency.labels(reactor_id=reactor_id).set(efficiency)

        energy_generated_total.labels(
            reactor_id=reactor_id
        ).inc(power * SCRAPE_INTERVAL)

        reactor_usage_factor.labels(
            reactor_id=reactor_id
        ).set(power / NOMINAL_POWER)

        print(
            f"[{reactor_id}] "
            f"Power={power:.1f}MW | "
            f"Efficiency={efficiency:.2f}% | "
            f"Burnup={burnup:.2f}%"
        )

def main():
    start_http_server(PROMETHEUS_PORT)
    print(f" Metrics exposed at http://localhost:{PROMETHEUS_PORT}/metrics")

    iteration_count = 0

    while True:
        if iteration_count >= CRASH_ITERATION_LIMIT:
            print("\n SIMULATION: Producer OFFLINE (heartbeat stops)")
            time.sleep(SIMULATION_FREEZE_TIME)
            print("\n Producer ONLINE again\n")
            iteration_count = 0
            continue

        generate_and_publish_metrics()
        time.sleep(SCRAPE_INTERVAL)

        iteration_count += 1
        if iteration_count % 10 == 0:
            print(f"--- Progress {iteration_count}/{CRASH_ITERATION_LIMIT} ---")


if __name__ == "__main__":
    main()
