-- LatticeQL sample schema: a support / CRM dataset.
-- Designed to be used alongside ecommerce — references the same user IDs
-- and product IDs so cross-database exploration works via virtual FKs.

PRAGMA foreign_keys = ON;

-- ──────────────────────────────────────────────────────────────────────────────
-- Agents  (support staff — separate from ecommerce users)
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE agents (
    id       INTEGER PRIMARY KEY,
    name     TEXT    NOT NULL,
    email    TEXT    NOT NULL UNIQUE,
    tier     TEXT    NOT NULL DEFAULT 'L1'  -- L1, L2, L3
);

-- ──────────────────────────────────────────────────────────────────────────────
-- Tickets  (opened by ecommerce users, assigned to agents)
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE tickets (
    id            INTEGER PRIMARY KEY,
    customer_id   INTEGER NOT NULL,  -- references ecommerce.users(id)
    agent_id      INTEGER REFERENCES agents(id),
    product_id    INTEGER,           -- references ecommerce.products(id), nullable
    subject       TEXT    NOT NULL,
    status        TEXT    NOT NULL DEFAULT 'open',  -- open, in_progress, resolved, closed
    priority      TEXT    NOT NULL DEFAULT 'normal', -- low, normal, high, urgent
    created_at    TEXT    NOT NULL DEFAULT (datetime('now')),
    resolved_at   TEXT
);

-- ──────────────────────────────────────────────────────────────────────────────
-- Ticket comments  (conversation thread on a ticket)
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE ticket_comments (
    id         INTEGER PRIMARY KEY,
    ticket_id  INTEGER NOT NULL REFERENCES tickets(id),
    author_type TEXT   NOT NULL,  -- 'agent' or 'customer'
    author_id   INTEGER NOT NULL, -- agent.id or ecommerce user.id
    body       TEXT    NOT NULL,
    created_at TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ──────────────────────────────────────────────────────────────────────────────
-- Knowledge base articles
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE kb_articles (
    id          INTEGER PRIMARY KEY,
    title       TEXT    NOT NULL,
    body        TEXT    NOT NULL,
    product_id  INTEGER,  -- references ecommerce.products(id), nullable
    author_id   INTEGER NOT NULL REFERENCES agents(id),
    published   INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT    NOT NULL DEFAULT (date('now')),
    updated_at  TEXT    NOT NULL DEFAULT (date('now'))
);

-- ──────────────────────────────────────────────────────────────────────────────
-- Ticket ↔ KB article links  (articles suggested for a ticket)
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE ticket_articles (
    ticket_id   INTEGER NOT NULL REFERENCES tickets(id),
    article_id  INTEGER NOT NULL REFERENCES kb_articles(id),
    PRIMARY KEY (ticket_id, article_id)
);

-- ──────────────────────────────────────────────────────────────────────────────
-- Customer satisfaction ratings  (post-resolution feedback)
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE satisfaction_ratings (
    id          INTEGER PRIMARY KEY,
    ticket_id   INTEGER NOT NULL UNIQUE REFERENCES tickets(id),
    customer_id INTEGER NOT NULL,  -- references ecommerce.users(id)
    rating      INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    comment     TEXT,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);
