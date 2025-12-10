import json
import os
from typing import Dict, List, Optional, Any
from datetime import datetime

DATA_DIR = 'data'
DRIVERS_FILE = os.path.join(DATA_DIR, 'drivers.json')
ORDERS_FILE = os.path.join(DATA_DIR, 'orders.json')

def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def load_json(filepath: str) -> Dict:
    ensure_data_dir()
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_json(filepath: str, data: Dict):
    ensure_data_dir()
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_drivers() -> Dict[str, Dict]:
    return load_json(DRIVERS_FILE)

def save_driver(user_id: int, driver_data: Dict):
    drivers = get_drivers()
    drivers[str(user_id)] = driver_data
    save_json(DRIVERS_FILE, drivers)

def get_driver(user_id: int) -> Optional[Dict]:
    drivers = get_drivers()
    return drivers.get(str(user_id))

def delete_driver(user_id: int) -> bool:
    drivers = get_drivers()
    if str(user_id) in drivers:
        del drivers[str(user_id)]
        save_json(DRIVERS_FILE, drivers)
        return True
    return False

def get_orders() -> Dict[str, Dict]:
    return load_json(ORDERS_FILE)

def save_order(order_id: str, order_data: Dict):
    orders = get_orders()
    orders[order_id] = order_data
    save_json(ORDERS_FILE, orders)

def get_active_drivers() -> List[Dict]:
    drivers = get_drivers()
    active = []
    for user_id, data in drivers.items():
        if data.get('active', True):
            data['user_id'] = int(user_id)
            active.append(data)
    return active
