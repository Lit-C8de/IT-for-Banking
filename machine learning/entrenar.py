import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, roc_curve
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.utils.class_weight import compute_class_weight
from sklearn.calibration import CalibratedClassifierCV
import joblib
import matplotlib.pyplot as plt
import seaborn as sns

# =====================================================
# 1️⃣ CARGAR Y LIMPIAR DATOS
# =====================================================
file_path = "resultados enriquesidos.csv"

df = pd.read_csv(file_path, encoding="latin1", sep=None, engine="python")
print(f"✅ Archivo cargado correctamente. Registros: {len(df)}")

df.columns = df.columns.str.strip().str.lower()

# Buscar columna objetivo
target_col = None
for col in df.columns:
    if "suspicious" in col:
        target_col = col
        break

if not target_col:
    raise ValueError("❌ No se encontró la columna 'is_suspicious' en el dataset.")
else:
    print(f"🎯 Usando columna objetivo: '{target_col}'")

# Eliminar columnas no predictivas
cols_to_drop = [
    'transaction_id', 'timestamp', 'fraud_pattern',
    'card_number_masked', 'response_code', 'status', 'switch_id'
]
df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')

# Rellenar nulos
df = df.fillna(0)

# =====================================================
# 2️⃣ CODIFICAR VARIABLES CATEGÓRICAS
# =====================================================
label_cols = df.select_dtypes(include=['object']).columns
encoders = {}

for col in label_cols:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col].astype(str))
    encoders[col] = le

print(f"🔠 Columnas codificadas: {list(label_cols)}")

# =====================================================
# 3️⃣ SEPARAR FEATURES Y TARGET
# =====================================================
X = df.drop(target_col, axis=1)
y = df[target_col].astype(int)

# Escalar numéricas
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Dividir train/test
X_train, X_test, y_train, y_test = train_test_split(
    X_scaled, y, test_size=0.25, random_state=42, stratify=y
)

# =====================================================
# 4️⃣ ENTRENAR MODELO BALANCEADO Y CALIBRADO
# =====================================================
print("\n🚀 Entrenando modelo Random Forest balanceado y calibrado...")

# Entrenamiento inicial balanceado
base_model = RandomForestClassifier(
    n_estimators=400,
    max_depth=None,
    random_state=42,
    class_weight='balanced',
    n_jobs=-1
)
base_model.fit(X_train, y_train)

# Calibración de probabilidades (mejora predicción probabilística)
model = CalibratedClassifierCV(base_model, cv=3, method='sigmoid')
model.fit(X_train, y_train)

# =====================================================
# 5️⃣ EVALUAR MODELO
# =====================================================
y_pred = model.predict(X_test)
y_prob = model.predict_proba(X_test)[:, 1]

print("\n📊 Matriz de confusión:")
cm = confusion_matrix(y_test, y_pred)
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
plt.xlabel("Predicho")
plt.ylabel("Real")
plt.title("Matriz de Confusión - Detección de Fraude")
plt.show()

print("\n📋 Reporte de clasificación:")
print(classification_report(y_test, y_pred, digits=3))

auc = roc_auc_score(y_test, y_prob)
print(f"\n🔥 AUC-ROC: {auc:.4f}")

# =====================================================
# 6️⃣ CURVA ROC
# =====================================================
fpr, tpr, _ = roc_curve(y_test, y_prob)
plt.figure(figsize=(6,5))
plt.plot(fpr, tpr, label=f"ROC (AUC = {auc:.3f})", color='darkorange')
plt.plot([0,1], [0,1], '--', color='gray')
plt.xlabel("Tasa de Falsos Positivos")
plt.ylabel("Tasa de Verdaderos Positivos")
plt.title("Curva ROC - Modelo de Detección de Fraude")
plt.legend()
plt.show()

# =====================================================
# 7️⃣ IMPORTANCIA DE VARIABLES
# =====================================================
importances = pd.Series(base_model.feature_importances_, index=X.columns)
top_vars = importances.nlargest(15)
top_vars.plot(kind='barh', figsize=(8,6), color='teal')
plt.title("🔎 Variables más importantes en la predicción de fraude")
plt.xlabel("Importancia")
plt.ylabel("Variable")
plt.tight_layout()
plt.show()

# =====================================================
# 8️⃣ GUARDAR MODELO Y TRANSFORMADORES
# =====================================================
joblib.dump(model, "modelo_fraude.pkl")
joblib.dump(encoders, "encoders.pkl")
joblib.dump(scaler, "scaler.pkl")

print("\n💾 Archivos guardados:")
print("   - modelo_fraude.pkl (calibrado)")
print("   - encoders.pkl")
print("   - scaler.pkl")

print("\n✅ Entrenamiento completado con éxito.")
