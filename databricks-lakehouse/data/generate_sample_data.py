"""Generate sample landing files for the lakehouse demo.

Output (under data/landing/):
  customers/customers_2026-01-01.csv   initial snapshot (60 customers)
  customers/customers_2026-03-01.csv   changes: 12 moved/re-segmented, 5 new, 8 unchanged  -> exercises SCD Type 2
  orders/orders_part1.csv              Jan-Feb orders
  orders/orders_part2.csv              Mar-Apr orders + 10 corrections of part1 orders -> exercises MERGE upsert
Dirty rows (duplicates, null keys, bad quantities, messy text) are injected on purpose.
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(7)
OUT = Path(__file__).parent / "landing"

SEGMENTS = ["Enterprise", "SMB", "Consumer"]
CITIES = ["Hyderabad", "Mumbai", "Bengaluru", "London", "New York", "Berlin", "Sydney"]
COUNTRY_OF = {"Hyderabad": "India", "Mumbai": "India", "Bengaluru": "India",
              "London": "UK", "New York": "USA", "Berlin": "Germany", "Sydney": "Australia"}
PRODUCTS = {
    "Laptop": ("Electronics", 950.0), "Phone": ("Electronics", 600.0),
    "Headphones": ("Electronics", 120.0), "Desk": ("Furniture", 250.0),
    "Chair": ("Furniture", 150.0), "Notebook": ("Stationery", 4.5),
    "Pen Set": ("Stationery", 12.0),
}
STATUSES = ["completed", "completed", "completed", "shipped", "cancelled"]
SPLIT_DATE = date(2026, 3, 1)


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"  {path.relative_to(OUT.parent)}: {len(rows)} rows")


def customer(i: int, city: str, segment: str, updated: date) -> dict:
    return {"customer_id": f"C{i:03d}", "name": f"Customer {i:03d}",
            "email": f"customer{i:03d}@example.com", "city": city,
            "segment": segment, "updated_at": updated.isoformat()}


def make_customers():
    day1 = date(2026, 1, 1)
    snapshot = [customer(i, random.choice(CITIES), random.choice(SEGMENTS), day1) for i in range(1, 61)]
    write_csv(OUT / "customers" / "customers_2026-01-01.csv", snapshot)

    day2 = SPLIT_DATE
    changed = []
    for row in random.sample(snapshot, 12):
        new_city = random.choice([c for c in CITIES if c != row["city"]])
        new_seg = random.choice([s for s in SEGMENTS if s != row["segment"]])
        changed.append(customer(int(row["customer_id"][1:]), new_city, new_seg, day2))
    unchanged_pool = [r for r in snapshot if r["customer_id"] not in {c["customer_id"] for c in changed}]
    unchanged = [dict(r, updated_at=day2.isoformat()) for r in random.sample(unchanged_pool, 8)]
    new = [customer(i, random.choice(CITIES), random.choice(SEGMENTS), day2) for i in range(61, 66)]
    write_csv(OUT / "customers" / "customers_2026-03-01.csv", changed + unchanged + new)


def make_order(i: int, d: date) -> dict:
    product = random.choice(list(PRODUCTS))
    category, price = PRODUCTS[product]
    max_cust = 65 if d >= SPLIT_DATE else 60  # new customers only order after they exist
    return {
        "order_id": f"ORD{i:05d}", "customer_id": f"C{random.randint(1, max_cust):03d}",
        "product": product, "category": category, "quantity": random.randint(1, 5),
        "unit_price": round(price * random.uniform(0.9, 1.1), 2), "order_date": d.isoformat(),
        "country": random.choice(list(set(COUNTRY_OF.values()))), "status": random.choice(STATUSES),
    }


def add_dirt(rows: list[dict]) -> list[dict]:
    rows = rows + random.sample(rows, 8)                       # duplicates
    for r in random.sample(rows, 4):
        r["customer_id"] = ""                                  # null keys
    for r in random.sample(rows, 4):
        r["quantity"] = random.choice([0, -1, -2])             # invalid quantity
    for r in random.sample(rows, 15):
        r["country"], r["status"] = f"  {r['country'].upper()} ", r["status"].upper()  # messy text
    random.shuffle(rows)
    return rows


def rand_date(start: date, days: int) -> date:
    return start + timedelta(days=random.randint(0, days))


def make_orders():
    part1 = [make_order(i, rand_date(date(2026, 1, 1), 58)) for i in range(1, 301)]
    part2 = [make_order(i, rand_date(SPLIT_DATE, 60)) for i in range(301, 551)]

    # Corrections: same order_id re-sent in the later file with a changed status -> must UPDATE, not duplicate
    corrections = []
    for r in random.sample(part1, 10):
        fixed = dict(r)
        fixed["status"] = "cancelled" if r["status"] != "cancelled" else "completed"
        corrections.append(fixed)

    write_csv(OUT / "orders" / "orders_part1.csv", add_dirt(part1))
    write_csv(OUT / "orders" / "orders_part2.csv", add_dirt(part2) + corrections)


if __name__ == "__main__":
    print("Generating landing files:")
    make_customers()
    make_orders()
