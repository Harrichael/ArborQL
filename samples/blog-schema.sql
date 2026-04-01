CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL
);
CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    author_id INTEGER NOT NULL REFERENCES users(id)
);
CREATE TABLE photos (
    id INTEGER PRIMARY KEY,
    url TEXT NOT NULL,
    caption TEXT,
    uploader_id INTEGER NOT NULL REFERENCES users(id)
);
-- Polymorphic: commentable_type/commentable_id → posts or photos
CREATE TABLE comments (
    id INTEGER PRIMARY KEY,
    body TEXT NOT NULL,
    author_id INTEGER NOT NULL REFERENCES users(id),
    commentable_type TEXT NOT NULL,
    commentable_id INTEGER NOT NULL
);
CREATE TABLE tags (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
-- Polymorphic: taggable_type/taggable_id → posts or photos
CREATE TABLE taggings (
    id INTEGER PRIMARY KEY,
    tag_id INTEGER NOT NULL REFERENCES tags(id),
    taggable_type TEXT NOT NULL,
    taggable_id INTEGER NOT NULL
);
-- Polymorphic: likeable_type/likeable_id → posts, photos, or comments
CREATE TABLE likes (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    likeable_type TEXT NOT NULL,
    likeable_id INTEGER NOT NULL
);
