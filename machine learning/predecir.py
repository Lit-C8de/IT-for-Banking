import pandas as pd
import numpy as np
import joblib
import os
import random

# =====================================================
# 🧩 CONFIGURACIÓN
# =====================================================
FILE_PATH = "transacciones_nuevas1.csv"
OUTPUT_FULL = "transacciones_evaluadas.csv"
OUTPUT_SUS = "transacciones_sospechosas.csv"

THRESHOLD = 0.0005  # Umbral ajustado de sospecha (puedes bajarlo hasta 0.01)

# =====================================================
# 🚀 CARGA DE MODELOS Y RECURSOS
# =====================================================
print("🚀 Cargando modelo y recursos...")

for f in ("modelo_fraude.pkl", "encoders.pkl", "scaler.pkl"):
    if not os.path.exists(f):
        raise FileNotFoundError(f"No se encontró {f}. Ejecuta el entrenamiento primero.")

model = joblib.load("modelo_fraude.pkl")   # modelo calibrado
encoders = joblib.load("encoders.pkl")
scaler = joblib.load("scaler.pkl")

if not os.path.exists(FILE_PATH):
    raise FileNotFoundError(f"No se encontró {FILE_PATH}")

# =====================================================
# 📥 1) LEER CSV Y VALIDAR
# =====================================================
try:
    df_orig = pd.read_csv(FILE_PATH, encoding="utf-8-sig", sep=None, engine="python")
except Exception:
    df_orig = pd.read_csv(FILE_PATH, encoding="latin1", sep=None, engine="python")

df_orig.columns = df_orig.columns.str.replace('ï»¿', '', regex=False).str.strip()
print(f"✅ Transacciones cargadas: {len(df_orig)} registros")

if "transaction_id" in df_orig.columns:
    before = len(df_orig)
    df_orig = df_orig.drop_duplicates(subset=["transaction_id"])
    if len(df_orig) < before:
        print(f"⚠️ Eliminados {before - len(df_orig)} duplicados por transaction_id")

# =====================================================
# 🧮 2) PREPARAR FEATURES
# =====================================================
df = df_orig.copy()
cols_to_drop = [
    'transaction_id', 'timestamp', 'fraud_pattern',
    'card_number_masked', 'response_code', 'status', 'switch_id'
]
df_features = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')

if "is_suspicious" in df_features.columns:
    df_features = df_features.drop(columns=["is_suspicious"])

df_features = df_features.fillna(0)

# =====================================================
# 🔠 3) APLICAR ENCODERS
# =====================================================
for col, le in encoders.items():
    if col in df_features.columns:
        df_features[col] = df_features[col].astype(str)
        unknown_mask = ~df_features[col].isin(le.classes_)
        if unknown_mask.any():
            if "unknown" not in le.classes_:
                le.classes_ = np.append(le.classes_, "unknown")
            df_features.loc[unknown_mask, col] = "unknown"
        df_features[col] = le.transform(df_features[col])

# =====================================================
# ⚙️ 4) ESCALAR Y PREDECIR
# =====================================================
if hasattr(scaler, "feature_names_in_"):
    expected = list(scaler.feature_names_in_)
else:
    expected = list(df_features.columns)

# Añadir columnas faltantes
for c in expected:
    if c not in df_features.columns:
        df_features[c] = 0

df_features = df_features[expected]

X_scaled = scaler.transform(df_features)

# --- PREDICCIONES ---
prob = model.predict_proba(X_scaled)[:, 1]
pred = (prob >= THRESHOLD).astype(int)

result = df_orig.copy()
result["fraud_probability"] = prob
result["predicted_is_suspicious"] = pred

# =====================================================
# 🚨 5) GENERAR RAZONES DE SOSPECHA
# =====================================================
def reason_generator(row):
    """Genera un motivo según la probabilidad"""
    if row["fraud_probability"] > 0.9:
        return "monto o patrón extremadamente atípico"
    elif row["fraud_probability"] > 0.7:
        return "transacción fuera del comportamiento normal"
    elif row["fraud_probability"] > 0.5:
        return "actividad inusual o repetitiva"
    elif row["fraud_probability"] > 0.3:
        return "transacción en horario o canal poco común"
    elif row["fraud_probability"] > 0.15:
        return "monto moderadamente alto"
    else:
        return "bajo riesgo"

result["is_suspicious"] = np.where(result["fraud_probability"] >= THRESHOLD, 1, None)
result["fraud_pattern"] = result.apply(
    lambda r: reason_generator(r) if r["fraud_probability"] >= THRESHOLD else None,
    axis=1
)

# =====================================================
# 🔍 6) MOSTRAR Y GUARDAR RESULTADOS
# =====================================================
result_sorted = result.sort_values("fraud_probability", ascending=False).reset_index(drop=True)
suspects = result_sorted[result_sorted["fraud_probability"] >= THRESHOLD]

print(f"\n🔎 Resultados:")
print(f"   Total evaluadas: {len(result_sorted)}")
print(f"   Sospechosas: {len(suspects)} (umbral = {THRESHOLD})")

if len(suspects) > 0:
    print("\n⚠️ Transacciones sospechosas detectadas:\n")
    display_cols = ["transaction_id", "account_id", "amount", "channel", "fraud_probability", "fraud_pattern"]
    display_cols = [c for c in display_cols if c in suspects.columns]
    print(suspects[display_cols].to_string(index=False))
else:
    print("\n✅ No se detectaron transacciones sospechosas por encima del umbral.")

# =====================================================
# 💾 7) EXPORTAR CSVs
# =====================================================
result_sorted.to_csv(OUTPUT_FULL, index=False, encoding="utf-8-sig")
suspects.to_csv(OUTPUT_SUS, index=False, encoding="utf-8-sig")

print(f"\n💾 Archivos generados:")
print(f"   - {OUTPUT_FULL}: todas las transacciones evaluadas")
print(f"   - {OUTPUT_SUS}: solo las sospechosas")

print("\n✅ Proceso finalizado correctamente.")
