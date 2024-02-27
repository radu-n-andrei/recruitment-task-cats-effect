CREATE TABLE limit_order
(
    order_id            TEXT PRIMARY KEY NOT NULL,
    market              TEXT             NOT NULL,
    side                TEXT             NOT NULL,
    price               NUMERIC          NOT NULL,
    total               NUMERIC          NOT NULL,
    filled              NUMERIC          NOT NULL,
    status              TEXT             NOT NULL,
    created_at          TIMESTAMPTZ      NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ      NOT NULL DEFAULT now()
);
