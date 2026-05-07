CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    account_id TEXT,
    transaction_date DATE,
    name TEXT,
    merchant_name TEXT,
    amount NUMERIC(12, 2),
    iso_currency_code TEXT,
    category TEXT,
    pending BOOLEAN
);
