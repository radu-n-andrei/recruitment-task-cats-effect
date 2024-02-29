CREATE TABLE limit_order
(
    order_id            TEXT PRIMARY KEY NOT NULL,
    market              TEXT             NOT NULL,
    total               NUMERIC          NOT NULL,
    filled              NUMERIC          NOT NULL,
    created_at          TIMESTAMPTZ      NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ      NOT NULL DEFAULT now()
);
