------------------------------------------------------
-- 🏦 1️⃣ CREACIÓN DE BASE DE DATOS
------------------------------------------------------
CREATE DATABASE bank_transactions;
GO
USE bank_transactions;
GO
------------------------------------------------------
-- 🧱 2️⃣ CREACIÓN DE TABLAS
------------------------------------------------------

-- Tabla de clientes
CREATE TABLE customers (
    customer_id        VARCHAR(20) PRIMARY KEY,
    full_name          VARCHAR(100),
    document_number    VARCHAR(20),
    email              VARCHAR(100),
    phone_number       VARCHAR(20),
    country_code       CHAR(2)
);
GO

-- Cuentas
CREATE TABLE accounts (
    account_id         VARCHAR(30) PRIMARY KEY,
    customer_id        VARCHAR(20),
    account_type       VARCHAR(20),
    balance            DECIMAL(14,2),
    currency           CHAR(3),
    open_date          DATE,
    status             VARCHAR(20),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
GO

-- Comercios
CREATE TABLE merchants (
    merchant_id        VARCHAR(30) PRIMARY KEY,
    merchant_name      VARCHAR(100),
    merchant_category  VARCHAR(50),
    country_code       CHAR(2)
);
GO

-- Terminales
CREATE TABLE terminals (
    terminal_id        VARCHAR(30) PRIMARY KEY,
    terminal_type      VARCHAR(30),
    location_lat       DECIMAL(9,6),
    location_long      DECIMAL(9,6),
    branch_id          VARCHAR(20)
);
GO

-- Switches
CREATE TABLE switches (
    switch_id          VARCHAR(30) PRIMARY KEY,
    switch_name        VARCHAR(50),
    description        VARCHAR(200)
);
GO

-- Canales
CREATE TABLE channels (
    channel_code       VARCHAR(20) PRIMARY KEY,
    channel_name       VARCHAR(50),
    description        VARCHAR(100)
);
GO

-- Transacciones
CREATE TABLE transactions ( 
    transaction_id       VARCHAR(50) PRIMARY KEY,
    timestamp            DATETIME NOT NULL,
    account_id           VARCHAR(30) NOT NULL,
    destination_account  VARCHAR(30),
    amount               DECIMAL(14,2) NOT NULL,
    currency             CHAR(3) NOT NULL,
    transaction_type     VARCHAR(30),
    channel_code         VARCHAR(20),
    terminal_id          VARCHAR(30),
    merchant_id          VARCHAR(30),
    card_number_masked   VARCHAR(20),
    auth_method          VARCHAR(20),
    response_code        VARCHAR(10),
    status               VARCHAR(20),
    processing_time_ms   INT,
    switch_id            VARCHAR(30),
    is_suspicious        BIT,
    FOREIGN KEY (account_id) REFERENCES accounts(account_id),
    FOREIGN KEY (destination_account) REFERENCES accounts(account_id),
    FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id),
    FOREIGN KEY (terminal_id) REFERENCES terminals(terminal_id),
    FOREIGN KEY (channel_code) REFERENCES channels(channel_code),
    FOREIGN KEY (switch_id) REFERENCES switches(switch_id)
);
GO

-- Información extendida del cliente
CREATE TABLE customer_profiles (
    customer_id             VARCHAR(20) PRIMARY KEY,
    customer_segment        VARCHAR(30),
    account_age_days        INT,
    failed_txn_ratio_7d     DECIMAL(5,2),
    avg_time_between_txns   DECIMAL(10,2)
);
GO

-- Información agregada de transacciones recientes
CREATE TABLE transaction_features (
    transaction_id          VARCHAR(50) PRIMARY KEY,
    avg_txn_amount_7d       DECIMAL(14,2),
    txn_count_24h           INT,
    hour_of_day             INT,
    day_of_week             INT,
    is_holiday              BIT,
    previous_response_code  VARCHAR(10),
    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
);
GO

-- Información del comercio
CREATE TABLE merchant_risk (
    merchant_id             VARCHAR(30) PRIMARY KEY,
    merchant_category       VARCHAR(50),
    merchant_risk_score     DECIMAL(5,2),
    FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id)
);
GO

-- Información del dispositivo
CREATE TABLE device_risk (
    terminal_id             VARCHAR(30) PRIMARY KEY,
    geo_distance_last_tx    DECIMAL(10,2),
    device_risk_level       VARCHAR(20),
    country_code            CHAR(2),
    FOREIGN KEY (terminal_id) REFERENCES terminals(terminal_id)
);
GO
------------------------------------------------------
-- 🧱 3 HASHEAR COLUMNAS DE DATOS SENCIBLES
------------------------------------------------------

-- 1️⃣ Asegurarte de estar en el esquema dbo
ALTER SCHEMA dbo TRANSFER customers;
GO

-- 2️⃣ Agregar las columnas hash
ALTER TABLE customers
ADD 
    document_hash VARBINARY(64),
    email_hash VARBINARY(64),
    phone_hash VARBINARY(64),
	full_name_hash VARBINARY(64);
GO

-- 3️⃣ Declarar una sal y actualizar con los valores hasheados
DECLARE @salt VARBINARY(32) = 0xA1B2C3D4E5F60123456789ABCDEF0123;

UPDATE dbo.customers
SET 
    document_hash = HASHBYTES('SHA2_256', @salt + CAST(document_number AS VARBINARY(256))),
    email_hash    = HASHBYTES('SHA2_256', @salt + CAST(email AS VARBINARY(256))),
    phone_hash    = HASHBYTES('SHA2_256', @salt + CAST(phone_number AS VARBINARY(256))),
	full_name_hash    = HASHBYTES('SHA2_256', @salt + CAST(full_name AS VARBINARY(256)));
GO

------------------------------------------------------
-- 🧱 4 POBLAR TABLAS
------------------------------------------------------

------------------------------------------------------
-- 1️⃣ LIMPIEZA DE DATOS EXISTENTES
------------------------------------------------------
DELETE FROM transaction_features;
DELETE FROM customer_profiles;
DELETE FROM device_risk;
DELETE FROM merchant_risk;
DELETE FROM transactions;
DELETE FROM accounts;
DELETE FROM merchants;
DELETE FROM terminals;
DELETE FROM switches;
DELETE FROM channels;
DELETE FROM customers;
GO

------------------------------------------------------
-- 2️⃣ CARGA DE TABLAS MAESTRAS
------------------------------------------------------

-- Canales
INSERT INTO channels (channel_code, channel_name, description) VALUES
('AGT', 'Agente', 'Transacciones por agente autorizado'),
('ATM', 'Cajero Automático', 'Transacciones por cajero automático'),
('APP', 'Aplicación Móvil', 'Transacciones vía app móvil'),
('PLN', 'Plin', 'Transferencias rápidas entre usuarios'),
('WEB', 'Web Banking', 'Portal web del banco');
GO

-- Switches
INSERT INTO switches (switch_id, switch_name, description) VALUES
('SW01', 'SwitchCentral', 'Switch principal de procesamiento'),
('SW02', 'SwitchBackup', 'Switch de respaldo');
GO

-- Comercios (corregido)
WITH gen_merchants AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM sys.objects
)
INSERT INTO merchants (merchant_id, merchant_name, merchant_category, country_code)
SELECT 
    CONCAT('M', RIGHT('0000' + CAST(rn AS VARCHAR), 4)),
    CONCAT('Comercio_', rn),
    CASE 
        WHEN RAND(CHECKSUM(NEWID())) < 0.3 THEN 'Retail'
        WHEN RAND(CHECKSUM(NEWID())) < 0.6 THEN 'Supermercado'
        ELSE 'Restaurante'
    END,
    'PE'
FROM gen_merchants
WHERE rn <= 100;

GO

-- Terminales (corregido)
WITH gen_terminals AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM sys.objects
)
INSERT INTO terminals (terminal_id, terminal_type, location_lat, location_long, branch_id)
SELECT 
    CONCAT('T', RIGHT('0000' + CAST(rn AS VARCHAR), 4)),
    CASE WHEN RAND(CHECKSUM(NEWID())) < 0.5 THEN 'POS' ELSE 'ATM' END,
    ROUND(-12.05 + RAND(CHECKSUM(NEWID())) * 0.1, 6),
    ROUND(-77.05 + RAND(CHECKSUM(NEWID())) * 0.1, 6),
    CONCAT('B', RIGHT('000' + CAST(rn AS VARCHAR), 3))
FROM gen_terminals
WHERE rn <= 100;
GO

------------------------------------------------------
-- 3️⃣ CLIENTES Y CUENTAS
------------------------------------------------------
go

-- Generar 1000 clientes con datos hash y salt
DECLARE @salt VARBINARY(32) = 0xA1B2C3D4E5F60123456789ABCDEF0123;

-- Crear una tabla de números para generar 1000 filas
WITH numbers AS (
    SELECT TOP (1000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects
)
INSERT INTO customers (
    customer_id, full_name, document_number, email, phone_number, country_code,
    document_hash, email_hash, phone_hash, full_name_hash
)
SELECT 
    CONCAT('C', RIGHT('0000' + CAST(n AS VARCHAR), 4)) AS customer_id,
    CONCAT('Cliente_', n) AS full_name,
    CAST(10000000 + n AS VARCHAR(20)) AS document_number,
    CONCAT('cliente', n, '@mail.com') AS email,
    CONCAT('+51', CAST(900000000 + n AS VARCHAR(20))) AS phone_number,
    'PE' AS country_code,
    HASHBYTES('SHA2_256', @salt + CAST(CAST(10000000 + n AS VARCHAR(20)) AS VARBINARY(256))) AS document_hash,
    HASHBYTES('SHA2_256', @salt + CAST(CONCAT('cliente', n, '@mail.com') AS VARBINARY(256))) AS email_hash,
    HASHBYTES('SHA2_256', @salt + CAST(CONCAT('+51', CAST(900000000 + n AS VARCHAR(20))) AS VARBINARY(256))) AS phone_hash,
    HASHBYTES('SHA2_256', @salt + CAST(CONCAT('Cliente_', n) AS VARBINARY(256))) AS full_name_hash
FROM numbers;
GO

-- Crear de 1 a 2 cuentas por cliente
WITH random_accounts AS (
    SELECT 
        c.customer_id,
        v.n AS account_number
    FROM customers c
    CROSS APPLY (
        -- Generar 1 o 2 filas por cliente
        SELECT TOP (ABS(CHECKSUM(NEWID())) % 2 + 1) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
        FROM sys.all_objects
    ) v
)
INSERT INTO accounts (account_id, customer_id, account_type, balance, currency, open_date, status)
SELECT 
    CONCAT('A', RIGHT('000000' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR), 6)) AS account_id,
    r.customer_id,
    CASE WHEN RAND(CHECKSUM(NEWID())) < 0.5 THEN 'Ahorros' ELSE 'Corriente' END AS account_type,
    ROUND(1000 + RAND(CHECKSUM(NEWID())) * 9000, 2) AS balance,
    'PEN' AS currency,
    DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 1500, GETDATE()) AS open_date,
    'Activa' AS status
FROM random_accounts r;
GO

------------------------------------------------------
-- 4️⃣ TRANSACCIONES (10,000, con 2% FRAUDULENTAS)
------------------------------------------------------

-- 1️⃣ Obtener IDs de cuentas existentes
DECLARE @AccountCount INT = (SELECT COUNT(*) FROM accounts);

WITH base_tx AS (
    SELECT TOP 10000
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n,
        RAND(CHECKSUM(NEWID())) AS rand_val
    FROM sys.objects a CROSS JOIN sys.objects b
),
src_dest AS (
    SELECT 
        b.n,
        (SELECT TOP 1 account_id FROM accounts ORDER BY NEWID()) AS acc_src,
        (SELECT TOP 1 account_id FROM accounts ORDER BY NEWID()) AS acc_dest,
        b.rand_val
    FROM base_tx b
)
INSERT INTO transactions (
    transaction_id, timestamp, account_id, destination_account, amount, currency, 
    transaction_type, channel_code, terminal_id, merchant_id,
    card_number_masked, auth_method, response_code, status,
    processing_time_ms, switch_id, is_suspicious
)
SELECT 
    CONCAT('TX', RIGHT('00000' + CAST(n AS VARCHAR), 5)) AS transaction_id,
    DATEADD(SECOND, -n * 10, GETDATE()) AS timestamp,
    acc_src AS account_id,
    acc_dest AS destination_account,
    ROUND(10 + rand_val * 4900, 2) AS amount,
    'PEN' AS currency,
    CASE WHEN rand_val < 0.5 THEN 'Transferencia' ELSE 'Pago' END AS transaction_type,
    (SELECT TOP 1 channel_code FROM channels ORDER BY NEWID()) AS channel_code,
    (SELECT TOP 1 terminal_id FROM terminals ORDER BY NEWID()) AS terminal_id,
    (SELECT TOP 1 merchant_id FROM merchants ORDER BY NEWID()) AS merchant_id,
    CONCAT('**** **** **** ', RIGHT('0000' + CAST(n AS VARCHAR), 4)) AS card_number_masked,
    CASE WHEN rand_val < 0.5 THEN 'PIN' ELSE 'OTP' END AS auth_method,
    '00' AS response_code,
    CASE WHEN n % 50 = 0 THEN 'Rechazada' ELSE 'Aprobada' END AS status,
    CAST(ABS(CHECKSUM(NEWID())) % 900 + 100 AS INT) AS processing_time_ms,
    (SELECT TOP 1 switch_id FROM switches ORDER BY NEWID()) AS switch_id,
    CASE WHEN n <= 200 THEN 1 ELSE 0 END AS is_suspicious  -- 2% fraudulentas
FROM src_dest;
GO

------------------------------------------------------
-- 5️⃣ AGREGAR PATRONES DE FRAUDE
------------------------------------------------------

-- 1️⃣ Crear columna de descripción si no existe
IF COL_LENGTH('transactions', 'fraud_pattern') IS NULL
BEGIN
    ALTER TABLE transactions ADD fraud_pattern VARCHAR(100);
END
GO

-- 2️⃣ Actualizar transacciones fraudulentas según patrón

-- Patrón 1: Monto muy alto
UPDATE t
SET 
    is_suspicious = 1,
    status = 'Fraudulenta',
    fraud_pattern = 'Monto excesivo (>4000)'
FROM transactions t
WHERE amount > 4000;
GO

-- Patrón 2: Transferencia rápida con alto valor
UPDATE t
SET 
    is_suspicious = 1,
    status = 'Fraudulenta',
    fraud_pattern = 'Transferencia rápida y de alto valor'
FROM transactions t
WHERE t.transaction_type = 'Transferencia'
  AND t.amount > 3000
  AND t.processing_time_ms < 200;
GO

-- Patrón 3: Canal sospechoso con monto alto
UPDATE t
SET 
    is_suspicious = 1,
    status = 'Fraudulenta',
    fraud_pattern = 'Canal sospechoso (PLN o APP) con monto alto'
FROM transactions t
WHERE (t.channel_code IN ('PLN', 'APP'))
  AND t.amount > 2500;
GO


------------------------------------------------------
-- 🧩 CORREGIR TRANSACCIONES SOSPECHOSAS INCOMPLETAS
------------------------------------------------------

-- 1️⃣ Actualizar las sospechosas sin patrón definido
UPDATE t
SET 
    fraud_pattern = ISNULL(fraud_pattern, 'Marcada por modelo inicial'),
    status = 'Fraudulenta'
FROM transactions t
WHERE is_suspicious = 1
  AND (fraud_pattern IS NULL OR LTRIM(RTRIM(fraud_pattern)) = '');
GO

-- 2️⃣ (Opcional) Asegurar coherencia total
-- Si el status es Fraudulenta → is_suspicious = 1
UPDATE transactions
SET is_suspicious = 1
WHERE status = 'Fraudulenta';
GO

-- Ver cuántas transacciones están correctamente clasificadas
SELECT 
    status,
    fraud_pattern,
    COUNT(*) AS cantidad
FROM transactions
GROUP BY status, fraud_pattern
ORDER BY status, cantidad DESC;
GO

-- Y revisar si aún hay inconsistencias
SELECT COUNT(*) AS inconsistentes
FROM transactions
WHERE is_suspicious = 1 AND fraud_pattern IS NULL;
GO

------------------------------------------------------
-- 6️⃣ PERFIL DE CLIENTE Y FEATURE ENGINEERING
------------------------------------------------------

-- Crear perfiles de clientes
INSERT INTO customer_profiles (customer_id, customer_segment, account_age_days, failed_txn_ratio_7d, avg_time_between_txns)
SELECT 
    c.customer_id,
    CASE 
        WHEN RAND(CHECKSUM(NEWID())) < 0.3 THEN 'Premium'
        WHEN RAND(CHECKSUM(NEWID())) < 0.7 THEN 'Regular'
        ELSE 'Básico' 
    END AS customer_segment,
    ABS(CHECKSUM(NEWID())) % 1500 AS account_age_days,
    ROUND(RAND(CHECKSUM(NEWID())) * 10, 2) AS failed_txn_ratio_7d,
    ROUND(RAND(CHECKSUM(NEWID())) * 120, 2) AS avg_time_between_txns
FROM customers c;
GO

-- Crear características de transacciones
INSERT INTO transaction_features (transaction_id, avg_txn_amount_7d, txn_count_24h, hour_of_day, day_of_week, is_holiday, previous_response_code)
SELECT 
    t.transaction_id,
    ROUND(t.amount * (0.8 + RAND(CHECKSUM(NEWID())) * 0.4), 2) AS avg_txn_amount_7d,
    CAST(RAND(CHECKSUM(NEWID())) * 10 AS INT) AS txn_count_24h,
    DATEPART(HOUR, t.timestamp) AS hour_of_day,
    DATEPART(WEEKDAY, t.timestamp) AS day_of_week,
    CASE WHEN DATEPART(WEEKDAY, t.timestamp) IN (1,7) THEN 1 ELSE 0 END AS is_holiday,
    '00' AS previous_response_code
FROM transactions t;
GO
-- Poblar merchant_risk con datos simulados coherentes
INSERT INTO merchant_risk (merchant_id, merchant_category, merchant_risk_score)
SELECT 
    m.merchant_id,
    CASE 
        WHEN ABS(CHECKSUM(NEWID())) % 5 = 0 THEN 'Retail'
        WHEN ABS(CHECKSUM(NEWID())) % 5 = 1 THEN 'E-commerce'
        WHEN ABS(CHECKSUM(NEWID())) % 5 = 2 THEN 'Food & Beverage'
        WHEN ABS(CHECKSUM(NEWID())) % 5 = 3 THEN 'Travel'
        ELSE 'Electronics'
    END AS merchant_category,
    CAST(ROUND((RAND(CHECKSUM(NEWID())) * 100), 2) AS DECIMAL(5,2)) AS merchant_risk_score
FROM merchants m
WHERE m.merchant_id NOT IN (SELECT merchant_id FROM merchant_risk);
GO
-- Poblar device_risk con datos simulados coherentes
INSERT INTO device_risk (terminal_id, geo_distance_last_tx, device_risk_level, country_code)
SELECT 
    t.terminal_id,
    CAST(ROUND((RAND(CHECKSUM(NEWID())) * 5000), 2) AS DECIMAL(10,2)) AS geo_distance_last_tx, -- distancia en km
    CASE 
        WHEN ABS(CHECKSUM(NEWID())) % 3 = 0 THEN 'Low'
        WHEN ABS(CHECKSUM(NEWID())) % 3 = 1 THEN 'Medium'
        ELSE 'High'
    END AS device_risk_level,
    CASE 
        WHEN ABS(CHECKSUM(NEWID())) % 5 = 0 THEN 'PE'
        WHEN ABS(CHECKSUM(NEWID())) % 5 = 1 THEN 'US'
        WHEN ABS(CHECKSUM(NEWID())) % 5 = 2 THEN 'BR'
        WHEN ABS(CHECKSUM(NEWID())) % 5 = 3 THEN 'CO'
        ELSE 'MX'
    END AS country_code
FROM terminals t
WHERE t.terminal_id NOT IN (SELECT terminal_id FROM device_risk);
GO

------------------------------------------------------
-- 🧱 4 CONSULTAS COMPROBACION
------------------------------------------------------

-- ==========================================
-- 🔍 CONSULTA: Transacciones enriquecidas
-- Combina transacciones con datos de cliente,
-- cuenta, dispositivo, comercio y features.
-- ==========================================
SELECT top 1500
    -- 🧾 Información base de la transacción
    t.transaction_id,
    t.[timestamp],
    t.account_id,
    t.destination_account,
    t.amount,
    t.currency,
    t.transaction_type,
    t.channel_code        AS channel,
    t.terminal_id,
    t.merchant_id,
    t.card_number_masked,
    t.auth_method,
    t.response_code,
    t.status,
    t.processing_time_ms,
    t.switch_id,
    t.is_suspicious,
    t.fraud_pattern,

    -- 👤 Perfil del cliente
    cp.customer_segment,
    cp.account_age_days,
    cp.failed_txn_ratio_7d,
    cp.avg_time_between_txns,

    -- 📊 Features transaccionales
    tf.avg_txn_amount_7d,
    tf.txn_count_24h,
    tf.hour_of_day,
    tf.day_of_week,
    tf.is_holiday,
    tf.previous_response_code,

    -- 🏪 Riesgo del comercio
    mr.merchant_category,
    mr.merchant_risk_score,

    -- 💻 Riesgo del dispositivo
    dr.geo_distance_last_tx,
    dr.device_risk_level,
    dr.country_code        AS device_country

FROM transactions AS t

-- 🔗 Relación: Transacción → Cuenta
LEFT JOIN accounts AS a
    ON t.account_id = a.account_id

-- 🔗 Relación: Cuenta → Perfil del cliente
LEFT JOIN customer_profiles AS cp
    ON a.customer_id = cp.customer_id

-- 🔗 Relación: Transacción → Features calculadas
LEFT JOIN transaction_features AS tf
    ON t.transaction_id = tf.transaction_id

-- 🔗 Relación: Comercio → Riesgo
LEFT JOIN merchant_risk AS mr
    ON t.merchant_id = mr.merchant_id

-- 🔗 Relación: Dispositivo → Riesgo
LEFT JOIN device_risk AS dr
    ON t.terminal_id = dr.terminal_id
WHERE is_suspicious IS NULL
GO

--VERIFICAR NUEVAS TRANSACCIONES POR CADA EJECUCION DE PYTHON
SELECT TOP 10 *
FROM transactions
ORDER BY timestamp DESC;

SELECT TOP 20 *
FROM transaction_features
ORDER BY transaction_id asc;


--SI NO SE POBLO CON LAS FECHAS LA TRANSACCION
-- Poblar transaction_features con datos derivados y aleatorios
INSERT INTO transaction_features (
    transaction_id,
    avg_txn_amount_7d,
    txn_count_24h,
    hour_of_day,
    day_of_week,
    is_holiday,
    previous_response_code
)
SELECT 
    t.transaction_id,
    CAST(ROUND((RAND(CHECKSUM(NEWID())) * 2000) + 50, 2) AS DECIMAL(10,2)) AS avg_txn_amount_7d,
    ABS(CHECKSUM(NEWID())) % 20 + 1 AS txn_count_24h,
    DATEPART(HOUR, t.[timestamp]) AS hour_of_day,
    DATEPART(WEEKDAY, t.[timestamp]) AS day_of_week,
    CASE 
        WHEN DATEPART(WEEKDAY, t.[timestamp]) IN (1,7) THEN 1 -- fin de semana
        ELSE 0
    END AS is_holiday,
    CASE 
        WHEN ABS(CHECKSUM(NEWID())) % 5 = 0 THEN '00'
        WHEN ABS(CHECKSUM(NEWID())) % 5 = 1 THEN '05'
        WHEN ABS(CHECKSUM(NEWID())) % 5 = 2 THEN '51'
        WHEN ABS(CHECKSUM(NEWID())) % 5 = 3 THEN '91'
        ELSE '12'
    END AS previous_response_code
FROM transactions t
WHERE t.transaction_id NOT IN (SELECT transaction_id FROM transaction_features);
GO
 select * from transactions