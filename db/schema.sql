CREATE TABLE IF NOT EXISTS orders (
    id          SERIAL PRIMARY KEY,
    order_id    UUID NOT NULL UNIQUE,
    user_id     TEXT NOT NULL,
    product     TEXT NOT NULL,
    category    TEXT NOT NULL,
    amount      NUMERIC(10, 2) NOT NULL,
    region      TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'placed',
    created_at  TIMESTAMPtZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_metrics(
    id              SERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    order_count     INTEGER NOT NULL,
    total_gmv       NUMERIC(12, 2) NOT NULL,
    avg_order_value NUMERIC(10, 2) NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS alerts(
    id          SERIAL PRIMARY KEY,
    order_id    UUID NOT NULL,
    alert_type  TEXT NOT NULL,
    reason      TEXT NOT NULL,
    amount      NUMERIC(10, 2),
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS category_stats (
    id          SERIAL PRIMARY KEY,
    category    TEXT NOT NULL,
    region      TEXT NOT NULL,
    order_count INTEGER NOT NULL DEFAULT 0,
    total_gmv   NUMERIC(12, 2) NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_category_stats_category ON category_stats (category, region);


