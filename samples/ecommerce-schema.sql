-- LatticeQL sample schema: a small e-commerce / team directory dataset.

PRAGMA foreign_keys = ON;

-- ──────────────────────────────────────────────────────────────────────────────
-- Locations
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE locations (
    id      INTEGER PRIMARY KEY,
    city    TEXT    NOT NULL,
    country TEXT    NOT NULL,
    region  TEXT
);

-- ──────────────────────────────────────────────────────────────────────────────
-- Departments
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE departments (
    id          INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL,
    location_id INTEGER REFERENCES locations(id)
);

-- ──────────────────────────────────────────────────────────────────────────────
-- Users
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE users (
    id            INTEGER PRIMARY KEY,
    name          TEXT    NOT NULL,
    email         TEXT    NOT NULL UNIQUE,
    role          TEXT    NOT NULL DEFAULT 'member',
    department_id INTEGER REFERENCES departments(id),
    location_id   INTEGER REFERENCES locations(id)
);

-- ──────────────────────────────────────────────────────────────────────────────
-- Products
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE products (
    id          INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL,
    category    TEXT    NOT NULL,
    price_cents INTEGER NOT NULL,
    sku         TEXT    NOT NULL UNIQUE
);

-- ──────────────────────────────────────────────────────────────────────────────
-- Orders
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE orders (
    id          INTEGER PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id),
    status      TEXT    NOT NULL DEFAULT 'pending',
    total_cents INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT    NOT NULL DEFAULT (date('now'))
);

-- ──────────────────────────────────────────────────────────────────────────────
-- Order items (order ↔ product many-to-many via this join table)
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE order_items (
    id         INTEGER PRIMARY KEY,
    order_id   INTEGER NOT NULL REFERENCES orders(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    quantity   INTEGER NOT NULL DEFAULT 1,
    unit_price INTEGER NOT NULL
);

-- ──────────────────────────────────────────────────────────────────────────────
-- Tags  (free-form labels attached to products)
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE tags (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE product_tags (
    product_id INTEGER NOT NULL REFERENCES products(id),
    tag_id     INTEGER NOT NULL REFERENCES tags(id),
    PRIMARY KEY (product_id, tag_id)
);
