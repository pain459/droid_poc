CREATE TABLE IF NOT EXISTS system_metrics (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    latency_ms INT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO system_metrics (service_name, status, latency_ms) VALUES
('payment_gateway', 'HEALTHY', 45),
('auth_provider', 'DEGRADED', 1200),
('notification_engine', 'HEALTHY', 12);