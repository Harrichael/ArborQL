## Comments

- Never just narrate the what or how of the code.
- When a comment is warranted, explain the why or motivation, succinctly.
- The bar for adding a comment: large ambiguity, or a design choice that
  would surprise the reader.
- No docstring boilerplate explaining function args, return values, etc.
- Do narrate "gotchas".
- For libraries, consider a file-level extended comment explaining how to
  use the module, with an example.
- In examples, illustrate the API generically. Concrete, evocative names
  are good (`users`, `posts`, `author`). Don't reference specific
  consumers, components, or features elsewhere in this project; the
  example should stand on its own.
- Project-internal references are fine (and often useful) in "why"
  comments; that's where the coupling actually lives.

## Tests

- Refrain from very micro-unit tests; make each test really count.
- Prefer fakes over mocks.
- Lean toward integrated tests written as unit tests.
- Some files may legitimately have few or no tests.

## Rust

- `mod.rs` should only contain `mod` declarations and `pub use`
  re-exports — no logic or implementation.
- Don't add re-exports speculatively in `mod.rs`; wait until a
  caller actually needs it.
