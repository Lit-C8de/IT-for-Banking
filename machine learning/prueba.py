import pyodbc
import time
import random
from faker import Faker
from datetime import datetime

# =======================
# CONFIGURACIÓN
# =======================
SERVER = r'DESKTOP-CD1H8R9\SQLEXPRESS'
DATABASE = 'bank_transactions'
USERNAME = 'sa'
PASSWORD = '123456'

TOTAL_SECONDS = 10          # duración de la simulación
TX_PER_SECOND = 10          # transacciones por segundo
TOTAL_TX = TOTAL_SECONDS * TX_PER_SECOND

USE_FAST_EXECUTEMANY = True

# =======================
# CONEXIÓN
# =======================
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"
)

try:
    conn = pyodbc.connect(conn_str, autocommit=False, timeout=5)
    cursor = conn.cursor()
    if USE_FAST_EXECUTEMANY:
        cursor.fast_executemany = True
    print("✅ Conectado a SQL Server.")
except Exception as e:
    print("❌ Error conectando a la base de datos:", e)
    raise SystemExit(1)

# =======================
# DATOS MAESTROS
# =======================
def fetch_column_list(query, col_index=0):
    cursor.execute(query)
    rows = cursor.fetchall()
    return [row[col_index] for row in rows]

accounts = fetch_column_list("SELECT account_id FROM accounts")
channels = fetch_column_list("SELECT channel_code FROM channels")
terminals = fetch_column_list("SELECT terminal_id FROM terminals")
merchants = fetch_column_list("SELECT merchant_id FROM merchants")
switches = fetch_column_list("SELECT switch_id FROM switches")

if len(accounts) < 2:
    print("❌ Error: Se requieren al menos 2 cuentas en 'accounts'.")
    conn.close()
    raise SystemExit(1)

if not channels or not terminals or not merchants or not switches:
    print("❌ Error: asegúrate de que existan datos en channels, terminals, merchants y switches.")
    conn.close()
    raise SystemExit(1)

print(f"ℹ️ Cuentas: {len(accounts)}, Canales: {len(channels)}, Terminals: {len(terminals)}, Merchants: {len(merchants)}, Switches: {len(switches)}")

# =======================
# UTILIDADES
# =======================
faker = Faker()
random.seed(42)
Faker.seed(42)

def detect_fraud(transaction):
    """
    Regla simple de detección (solo para mostrar en pantalla, no guardar en SQL):
      - Monto mayor a 1200
      - Transacciones rápidas entre mismas cuentas
      - Canales o terminales repetitivos
    """
    amount = transaction["amount"]
    tx_type = transaction["transaction_type"]
    channel = transaction["channel"]
    terminal = transaction["terminal_id"]

    if amount > 1200:
        return (1, f"🚨 Monto alto ({amount}) PEN")
    elif tx_type == "Transferencia" and random.random() < 0.05:
        return (1, "🚨 Transferencia sospechosa a cuenta frecuente")
    elif random.random() < 0.02:
        return (1, "🚨 Patrón inusual de canal o terminal")
    else:
        return (0, None)

def generate_transaction_row(idx):
    """Genera una transacción SIN etiquetas guardadas."""
    acc_src = random.choice(accounts)
    acc_dest = random.choice(accounts)
    while acc_dest == acc_src:
        acc_dest = random.choice(accounts)

    amount = round(random.uniform(10, 15000), 2)
    tx_type = random.choice(["Pago", "Transferencia"])
    channel = random.choice(channels)
    terminal = random.choice(terminals)
    merchant = random.choice(merchants)
    switch = random.choice(switches)
    timestamp = datetime.now()
    card_masked = f"**** **** **** {random.randint(1000,9999)}"
    auth_method = random.choice(["PIN", "OTP", "Biometría"])
    processing_time_ms = random.randint(100, 900)
    response_code = '00'
    status = 'PENDIENTE'

    transaction_id = f"RTX{int(time.time()) % 100000}_{idx:03d}"

    # estructura tipo diccionario (para análisis)
    tx = {
        "transaction_id": transaction_id,
        "timestamp": timestamp,
        "account_id": acc_src,
        "destination_account": acc_dest,
        "amount": amount,
        "currency": 'PEN',
        "transaction_type": tx_type,
        "channel": channel,
        "terminal_id": terminal,
        "merchant_id": merchant,
        "card_number_masked": card_masked,
        "auth_method": auth_method,
        "response_code": response_code,
        "status": status,
        "processing_time_ms": processing_time_ms,
        "switch_id": switch,
        "is_suspicious": None,
        "fraud_pattern": None
    }

    return tx

def generate_transaction_features(transaction):
    """Genera variables sintéticas de comportamiento."""
    timestamp = transaction["timestamp"]
    amount = transaction["amount"]
    transaction_id = transaction["transaction_id"]

    hour_of_day = timestamp.hour
    day_of_week = timestamp.weekday()
    is_holiday = 1 if day_of_week in (5, 6) else 0
    avg_txn_amount_7d = round(amount * random.uniform(0.7, 1.2), 2)
    txn_count_24h = random.randint(1, 10)
    previous_response_code = random.choice(['00', '05', '91', '96'])
    return (transaction_id, avg_txn_amount_7d, txn_count_24h, hour_of_day, day_of_week, is_holiday, previous_response_code)

# =======================
# SQL DE INSERCIÓN
# =======================
insert_tx_sql = """
INSERT INTO transactions (
    transaction_id, timestamp, account_id, destination_account, amount, currency,
    transaction_type, channel_code, terminal_id, merchant_id,
    card_number_masked, auth_method, response_code, status,
    processing_time_ms, switch_id, is_suspicious, fraud_pattern
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

insert_feat_sql = """
INSERT INTO transaction_features (
    transaction_id, avg_txn_amount_7d, txn_count_24h, hour_of_day,
    day_of_week, is_holiday, previous_response_code
)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

# =======================
# SIMULACIÓN
# =======================
print(f"ℹ️ Simulación: {TOTAL_TX} transacciones sin etiquetas en BD")
print("🚀 Iniciando simulación...\n")

tx_counter = 0
start_time = time.time()
fraudulentas = []

try:
    for second in range(TOTAL_SECONDS):
        loop_start = time.time()
        for j in range(TX_PER_SECOND):
            idx = tx_counter
            tx = generate_transaction_row(idx)

            # Detección en memoria
            suspicious_flag, reason = detect_fraud(tx)
            if suspicious_flag == 1:
                fraudulentas.append((tx["transaction_id"], tx["amount"], reason))
                print(f"⚠️  {tx['transaction_id']} sospechosa → {reason}")

            # Inserción sin etiquetas
            row = (
                tx["transaction_id"], tx["timestamp"], tx["account_id"], tx["destination_account"],
                tx["amount"], tx["currency"], tx["transaction_type"], tx["channel"],
                tx["terminal_id"], tx["merchant_id"], tx["card_number_masked"],
                tx["auth_method"], tx["response_code"], tx["status"],
                tx["processing_time_ms"], tx["switch_id"], None, None
            )

            feat_row = generate_transaction_features(tx)

            try:
                cursor.execute(insert_tx_sql, row)
                cursor.execute(insert_feat_sql, feat_row)
            except Exception as e:
                print(f"❌ Error insertando TX idx={idx}: {e}")

            tx_counter += 1

        conn.commit()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Enviadas {tx_counter} transacciones")

        elapsed_loop = time.time() - loop_start
        if elapsed_loop < 1.0:
            time.sleep(1.0 - elapsed_loop)

    total_elapsed = time.time() - start_time
    print("\n✅ Simulación completada exitosamente.")
    print(f"🔍 Transacciones generadas: {tx_counter}")
    print(f"⏱ Tiempo total: {total_elapsed:.2f} segundos")

    if fraudulentas:
        print("\n=== 🚨 TRANSACCIONES SOSPECHOSAS DETECTADAS ===")
        for txid, amt, reason in fraudulentas:
            print(f"🔸 {txid} | Monto: {amt} PEN | Motivo: {reason}")
    else:
        print("\n✅ No se detectaron transacciones sospechosas.")

except KeyboardInterrupt:
    print("\n⏹ Simulación interrumpida por el usuario.")
finally:
    cursor.close()
    conn.close()
    print("🔒 Conexión cerrada.")
