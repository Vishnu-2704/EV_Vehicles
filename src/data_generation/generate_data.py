import csv
import random
from datetime import datetime, timedelta

# Number of records
NUM_CUSTOMERS = 1000
NUM_VEHICLES = 1500
NUM_STATIONS = 50
NUM_TRANSACTIONS = 100000

# Output files
CUSTOMER_FILE = "data/raw/customers.csv"
VEHICLE_FILE = "data/raw/vehicles.csv"
STATION_FILE = "data/raw/stations.csv"
TRANSACTION_FILE = "data/raw/charging_transactions.csv"


# -----------------------------
# 1. Generate Customers
# -----------------------------
def generate_customers():
    customers = []

    first_names = [
        "Rahul", "Priya", "Vishnu", "Arjun", "Sneha",
        "Anil", "Kiran", "Divya", "Ravi", "Neha"
    ]

    cities = [
        "Hyderabad", "Bangalore", "Chennai",
        "Mumbai", "Pune", "Delhi", "Kochi"
    ]

    for i in range(1, NUM_CUSTOMERS + 1):
        customer = {
            "customer_id": f"C{i:05d}",
            "customer_name": random.choice(first_names),
            "city": random.choice(cities)
        }

        customers.append(customer)

    return customers


# -----------------------------
# 2. Generate Vehicles
# -----------------------------
def generate_vehicles():
    vehicles = []

    models = [
        "Tata Nexon EV",
        "MG ZS EV",
        "Hyundai Ioniq 5",
        "Tata Tiago EV",
        "Mahindra XUV400",
        "BYD Atto 3"
    ]

    vehicle_types = [
        "Hatchback",
        "SUV",
        "Sedan"
    ]

    for i in range(1, NUM_VEHICLES + 1):
        vehicle = {
            "vehicle_id": f"V{i:05d}",
            "customer_id": f"C{random.randint(1, NUM_CUSTOMERS):05d}",
            "vehicle_model": random.choice(models),
            "vehicle_type": random.choice(vehicle_types),
            "battery_capacity_kwh": random.choice(
                [30, 40, 50, 60, 70, 80]
            )
        }

        vehicles.append(vehicle)

    return vehicles


# -----------------------------
# 3. Generate Charging Stations
# -----------------------------
def generate_stations():
    stations = []

    cities = [
        "Hyderabad", "Bangalore", "Chennai",
        "Mumbai", "Pune", "Delhi", "Kochi"
    ]

    charger_types = [
        "AC",
        "DC Fast",
        "DC Ultra Fast"
    ]

    for i in range(1, NUM_STATIONS + 1):
        station = {
            "station_id": f"S{i:03d}",
            "station_name": f"EV Station {i}",
            "city": random.choice(cities),
            "charger_type": random.choice(charger_types),
            "power_kw": random.choice([7, 22, 50, 100, 150, 250])
        }

        stations.append(station)

    return stations


# -----------------------------
# 4. Generate Transactions
# -----------------------------
def generate_transactions():
    transactions = []

    start_date = datetime(2025, 1, 1)

    for i in range(1, NUM_TRANSACTIONS + 1):

        start_time = start_date + timedelta(
            minutes=random.randint(0, 365 * 24 * 60)
        )

        duration_minutes = random.randint(20, 180)

        end_time = start_time + timedelta(
            minutes=duration_minutes
        )

        energy_kwh = round(
            random.uniform(5, 80),
            2
        )

        price_per_kwh = random.uniform(8, 15)

        amount = round(
            energy_kwh * price_per_kwh,
            2
        )

        transaction = {
            "transaction_id": f"T{i:06d}",
            "customer_id": f"C{random.randint(1, NUM_CUSTOMERS):05d}",
            "vehicle_id": f"V{random.randint(1, NUM_VEHICLES):05d}",
            "station_id": f"S{random.randint(1, NUM_STATIONS):03d}",
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "energy_kwh": energy_kwh,
            "amount_paid": amount,
            "status": random.choice([
                "Completed",
                "Completed",
                "Completed",
                "Cancelled"
            ])
        }

        transactions.append(transaction)

    return transactions


# -----------------------------
# 5. Write CSV File
# -----------------------------
def write_csv(filename, data):

    with open(filename, "w", newline="", encoding="utf-8") as file:

        writer = csv.DictWriter(
            file,
            fieldnames=data[0].keys()
        )

        writer.writeheader()
        writer.writerows(data)

    print(f"Created: {filename}")


# -----------------------------
# Main Program
# -----------------------------
if __name__ == "__main__":

    print("Generating EV charging data...")

    customers = generate_customers()
    vehicles = generate_vehicles()
    stations = generate_stations()
    transactions = generate_transactions()

    write_csv(CUSTOMER_FILE, customers)
    write_csv(VEHICLE_FILE, vehicles)
    write_csv(STATION_FILE, stations)
    write_csv(TRANSACTION_FILE, transactions)

    print()
    print("Data generation completed!")
