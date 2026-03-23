-- ArborQL sample schema: a small e-commerce / team directory dataset.
-- Run:  sqlite3 sample/sample.db < sample/schema.sql

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

-- ──────────────────────────────────────────────────────────────────────────────
-- Sample data
-- ──────────────────────────────────────────────────────────────────────────────

INSERT INTO locations VALUES
    (1, 'Portland',  'USA', 'Pacific Northwest'),
    (2, 'Seattle',   'USA', 'Pacific Northwest'),
    (3, 'Austin',    'USA', 'Southwest'),
    (4, 'Berlin',    'Germany', 'Central Europe'),
    (5, 'Singapore', 'Singapore', 'Southeast Asia');

INSERT INTO departments VALUES
    (1, 'Engineering',  1),
    (2, 'Marketing',    3),
    (3, 'Design',       4),
    (4, 'Operations',   2),
    (5, 'Sales',        5);

INSERT INTO users VALUES
    (1,  'Alice Nguyen',    'alice@example.com',   'admin',  1, 1),
    (2,  'Rick Sanchez',    'rick@example.com',    'member', 1, 1),
    (3,  'Ricardo Diaz',    'ricardo@example.com', 'member', 1, 2),
    (4,  'Beth Smith',      'beth@example.com',    'member', 2, 3),
    (5,  'Morty Smith',     'morty@example.com',   'member', 2, 3),
    (6,  'Summer Smith',    'summer@example.com',  'member', 3, 4),
    (7,  'Jerry Smith',     'jerry@example.com',   'member', 4, 2),
    (8,  'Diane Nguyen',    'diane@example.com',   'admin',  5, 5),
    (9,  'Todd Chavez',     'todd@example.com',    'member', 5, 5),
    (10, 'Princess Carolyn','pc@example.com',      'admin',  5, 5);

INSERT INTO products VALUES
    (1,  'ArborQL Pro License',   'Software',    9999,  'AQL-PRO-001'),
    (2,  'Mechanical Keyboard',   'Hardware',    14999, 'HW-KBD-001'),
    (3,  'USB-C Hub 7-port',      'Hardware',    4999,  'HW-HUB-001'),
    (4,  'Standing Desk Mat',     'Office',      3499,  'OFF-MAT-001'),
    (5,  'Noise-Cancelling Headphones', 'Hardware', 19999, 'HW-HEAD-001'),
    (6,  'ArborQL Team License',  'Software',    49999, 'AQL-TEAM-001'),
    (7,  'Ergonomic Mouse',       'Hardware',    7999,  'HW-MSE-001'),
    (8,  'Monitor Arm',           'Office',      8999,  'OFF-ARM-001'),
    (9,  'Webcam 4K',             'Hardware',    9999,  'HW-CAM-001'),
    (10, 'Desk Organiser Set',    'Office',      2499,  'OFF-ORG-001');

INSERT INTO orders VALUES
    (1,  1, 'completed', 24998, '2025-01-05'),
    (2,  1, 'completed', 14999, '2025-02-10'),
    (3,  2, 'completed',  9999, '2025-01-15'),
    (4,  2, 'shipped',   22998, '2025-03-01'),
    (5,  3, 'pending',   19999, '2025-03-10'),
    (6,  4, 'completed', 49999, '2025-01-20'),
    (7,  5, 'completed', 12498, '2025-02-14'),
    (8,  6, 'shipped',   17998, '2025-02-28'),
    (9,  8, 'completed', 29998, '2025-01-08'),
    (10, 9, 'pending',    7999, '2025-03-12');

INSERT INTO order_items VALUES
    (1,  1, 1, 1, 9999),
    (2,  1, 7, 1, 7999),
    (3,  1, 4, 2, 3499),
    (4,  2, 2, 1, 14999),
    (5,  3, 1, 1, 9999),
    (6,  4, 5, 1, 19999),
    (7,  4, 3, 1, 4999),
    (8,  5, 5, 1, 19999),
    (9,  6, 6, 1, 49999),
    (10, 7, 7, 1, 7999),
    (11, 7, 10,2, 2499),
    (12, 8, 9, 1, 9999),
    (13, 8, 8, 1, 8999),
    (14, 9, 1, 2, 9999),
    (15, 9, 6, 1, 49999),
    (16, 10,7, 1, 7999);

INSERT INTO tags VALUES
    (1, 'featured'),
    (2, 'bestseller'),
    (3, 'new'),
    (4, 'bundle'),
    (5, 'software'),
    (6, 'ergonomic');

INSERT INTO product_tags VALUES
    (1, 5), (1, 2),
    (2, 2), (2, 6),
    (3, 3),
    (4, 6),
    (5, 1), (5, 2),
    (6, 5), (6, 1),
    (7, 6), (7, 2),
    (8, 6),
    (9, 3),
    (10, 3);
