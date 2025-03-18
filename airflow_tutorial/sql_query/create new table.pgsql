CREATE TABLE IF NOT EXISTS orders (
    order_id CHARACTER VARYING,
    date DATE,
    product_name CHARACTER VARYING,
    quantity INTEGER,
    PRIMARY KEY (order_id)
);
