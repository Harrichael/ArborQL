-- LatticeQL sample schema: a support / CRM dataset.
-- Designed to be used alongside ecommerce.db — references the same user IDs
-- and product IDs so cross-database exploration works via virtual FKs.
--
-- Run:  sqlite3 samples/support.db < samples/support.sql

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

-- ══════════════════════════════════════════════════════════════════════════════
-- Sample data
-- ══════════════════════════════════════════════════════════════════════════════

INSERT INTO agents VALUES
    (1, 'Support Bot',     'bot@support.example.com',    'L1'),
    (2, 'Luna Park',       'luna@support.example.com',   'L2'),
    (3, 'Oscar Martinez',  'oscar@support.example.com',  'L2'),
    (4, 'Yuki Tanaka',     'yuki@support.example.com',   'L3');

-- Tickets — customer_id and product_id reference ecommerce IDs.
INSERT INTO tickets VALUES
    (1,  1, 2, 1,  'License key not arriving',          'resolved', 'high',   '2025-01-06 09:30', '2025-01-06 14:00'),
    (2,  1, 3, 2,  'Keyboard LED stuck on red',         'resolved', 'normal', '2025-02-11 10:15', '2025-02-12 16:00'),
    (3,  2, 2, 1,  'Cannot activate Pro license',       'resolved', 'high',   '2025-01-16 08:00', '2025-01-16 11:30'),
    (4,  2, 4, 5,  'Headphone cushion peeling',         'in_progress','normal','2025-03-02 14:00', NULL),
    (5,  3, NULL, 5, 'Headphones arrived damaged',      'open',     'urgent', '2025-03-11 09:00', NULL),
    (6,  4, 2, 6,  'Team license seat count question',  'resolved', 'low',    '2025-01-21 11:00', '2025-01-21 13:00'),
    (7,  5, 3, 7,  'Ergonomic mouse scroll issue',      'resolved', 'normal', '2025-02-15 16:30', '2025-02-17 10:00'),
    (8,  6, 1, 9,  'Webcam not recognized on Linux',    'in_progress','high',  '2025-03-01 08:45', NULL),
    (9,  8, 4, 1,  'Bulk license discount inquiry',     'resolved', 'low',    '2025-01-09 10:00', '2025-01-10 09:00'),
    (10, 9, 1, 7,  'Mouse double-click defect',         'open',     'normal', '2025-03-13 11:00', NULL);

INSERT INTO ticket_comments VALUES
    (1,  1, 'customer', 1, 'I purchased the Pro license yesterday but never got a key email.',         '2025-01-06 09:30'),
    (2,  1, 'agent',    2, 'Checking your order now. Can you confirm order #1?',                       '2025-01-06 09:45'),
    (3,  1, 'customer', 1, 'Yes, order #1.',                                                           '2025-01-06 10:00'),
    (4,  1, 'agent',    2, 'Found it — the email bounced. Resending to your updated address now.',     '2025-01-06 13:50'),
    (5,  2, 'customer', 1, 'The W key on my keyboard is stuck glowing red and won''t change color.',   '2025-02-11 10:15'),
    (6,  2, 'agent',    3, 'Try a firmware reset: hold Fn+Esc for 5 seconds. Let me know if it helps.','2025-02-11 11:00'),
    (7,  2, 'customer', 1, 'That fixed it, thanks!',                                                   '2025-02-12 15:30'),
    (8,  3, 'customer', 2, 'License activation says "key already used" but I only have one machine.',  '2025-01-16 08:00'),
    (9,  3, 'agent',    2, 'I''ve reset your activation count. Please try again.',                     '2025-01-16 11:00'),
    (10, 4, 'customer', 2, 'The ear cushion on my headphones is peeling after one month.',             '2025-03-02 14:00'),
    (11, 4, 'agent',    4, 'That''s covered under warranty. I''ll send a replacement pair.',           '2025-03-02 15:30'),
    (12, 5, 'customer', 3, 'Box arrived crushed. Left headphone driver is rattling.',                  '2025-03-11 09:00'),
    (13, 6, 'customer', 4, 'How many seats does the Team license include? Can I add more later?',     '2025-01-21 11:00'),
    (14, 6, 'agent',    2, 'Team license covers up to 25 seats. Contact sales for expansion.',         '2025-01-21 12:30'),
    (15, 8, 'customer', 6, 'My 4K webcam shows as "unknown device" on Ubuntu 24.04.',                 '2025-03-01 08:45'),
    (16, 8, 'agent',    1, 'Checking compatibility. What kernel version are you running?',             '2025-03-01 09:00'),
    (17, 10,'customer', 9, 'Mouse registers two clicks for every single click.',                       '2025-03-13 11:00');

INSERT INTO kb_articles VALUES
    (1, 'How to activate your LatticeQL license',    'Step 1: Open Settings > License. Step 2: Paste your key. Step 3: Click Activate.', 1, 2, 1, '2025-01-10', '2025-01-16'),
    (2, 'Keyboard firmware reset guide',              'Hold Fn+Esc for 5 seconds to perform a full firmware reset. All LED settings will return to defaults.', 2, 3, 1, '2025-02-12', '2025-02-12'),
    (3, 'Webcam Linux compatibility',                 'The 4K webcam requires kernel 5.15+ and the uvcvideo module. Install v4l-utils for troubleshooting.', 9, 4, 1, '2025-03-02', '2025-03-02'),
    (4, 'Headphone warranty and replacements',        'All headphones include a 2-year warranty covering manufacturing defects. Contact support for a replacement.', 5, 4, 1, '2025-02-01', '2025-03-02'),
    (5, 'Team license FAQ',                           'The Team license covers up to 25 seats. Volume discounts are available for 50+ seats. Contact sales@example.com.', 6, 2, 1, '2025-01-22', '2025-01-22'),
    (6, 'Mouse double-click troubleshooting',         'If your mouse registers double clicks, try updating firmware via the configuration utility. If the issue persists, request a warranty replacement.', 7, 3, 0, '2025-03-14', '2025-03-14');

INSERT INTO ticket_articles VALUES
    (1, 1),
    (3, 1),
    (2, 2),
    (6, 5),
    (8, 3),
    (4, 4),
    (5, 4);

INSERT INTO satisfaction_ratings VALUES
    (1, 1, 1, 5, 'Quick resolution, great support!',              '2025-01-06 14:30'),
    (2, 2, 1, 4, 'Worked, but took a day.',                       '2025-02-12 16:30'),
    (3, 3, 2, 5, 'Fixed instantly. Thank you!',                   '2025-01-16 12:00'),
    (4, 6, 4, 3, 'Answer was fine but I wanted a self-service option.', '2025-01-21 14:00'),
    (5, 7, 5, 4, 'Replacement mouse works great.',                '2025-02-17 11:00'),
    (6, 9, 8, 5, 'Diane got us a great bulk deal.',               '2025-01-10 10:00');
