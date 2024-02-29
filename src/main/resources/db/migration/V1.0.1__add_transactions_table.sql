CREATE TABLE transactions
(
    id             UUID PRIMARY KEY NOT NULL,
    limit_order_id TEXT                 NOT NULL,
    amount         NUMERIC              NOT NULL,
    created_at     TIMESTAMPTZ          NOT NULL DEFAULT now(),
    CONSTRAINT fk_limit_order
        FOREIGN KEY(limit_order_id)
            REFERENCES limit_order(order_id)
);
