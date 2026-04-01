INSERT INTO users VALUES (1,'Alice','alice@example.com'),(2,'Bob','bob@example.com'),(3,'Carol','carol@example.com');
INSERT INTO posts VALUES (1,'Hello World','My first post',1),(2,'Rust is great','Rust thoughts',2),(3,'TUI apps','Building terminal UIs',1);
INSERT INTO photos VALUES (1,'https://example.com/cat.jpg','My cat',2),(2,'https://example.com/sunset.jpg','Beautiful sunset',3),(3,'https://example.com/code.png','My editor',1);
INSERT INTO tags VALUES (1,'rust'),(2,'programming'),(3,'photography'),(4,'cats'),(5,'nature'),(6,'tui');
INSERT INTO taggings VALUES (1,1,'Post',1),(2,2,'Post',1),(3,1,'Post',2),(4,6,'Post',3),(5,3,'Photo',1),(6,4,'Photo',1),(7,3,'Photo',2),(8,5,'Photo',2);
INSERT INTO comments VALUES (1,'Great post!',2,'Post',1),(2,'I agree!',3,'Post',1),(3,'Love the cat!',1,'Photo',1),(4,'Stunning!',2,'Photo',2),(5,'Rust rocks',1,'Post',2);
INSERT INTO likes VALUES (1,2,'Post',1),(2,3,'Post',1),(3,1,'Photo',1),(4,2,'Comment',1),(5,1,'Post',2);
