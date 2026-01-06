PRAGMA user_version = 1;
PRAGMA foreign_keys = ON;

CREATE TABLE Configuration (
    id INTEGER PRIMARY KEY,
    label TEXT UNIQUE NOT NULL,
    some_value REAL NOT NULL DEFAULT 100
);

CREATE TABLE Product (
    id INTEGER PRIMARY KEY,
    label TEXT UNIQUE NOT NULL,
    unit TEXT NOT NULL
) STRICT;

CREATE TABLE Process (
    id INTEGER PRIMARY KEY,
    label TEXT UNIQUE NOT NULL
) STRICT;

CREATE TABLE Process_set_products (
    id INTEGER,
    product_id INTEGER,
    FOREIGN KEY(id) REFERENCES Process(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY(product_id) REFERENCES Product(id) ON DELETE SET NULL ON UPDATE CASCADE,
    UNIQUE(id, product_id)
) STRICT;
