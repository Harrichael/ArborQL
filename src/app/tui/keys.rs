use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

/// Whether a text input buffer is capturing keystrokes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputFocus {
    /// No text buffer — chars are action keys per EntityFocus.
    None,
    /// Minimal nav (j/k/q) bound, everything else → TextInput.
    /// Used by Normal mode — ready to accept query input.
    Idle,
    /// All remaining keys → TextInput. (Query, CommandPalette, forms)
    Text,
    /// All remaining keys → TextInput. (overlay search active)
    Search,
}

/// What kind of UI entity currently has focus.
/// Only consulted when InputFocus is None.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityFocus {
    /// List overlay — standard action keys (x/d=Remove, a, n, /, etc).
    Overlay,
    /// Reorderable list — d=MoveItemDown, u=MoveItemUp.
    Editable,
    /// Yes/no confirmation dialog.
    Confirm,
    /// Any-key dismissal (Error/Info).
    Dismiss,
}

/// Multi-dimensional focus state that determines key→event mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FocusLoci {
    pub input: InputFocus,
    pub entity: EntityFocus,
}

/// A semantically named key event, decoupled from physical key codes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserKeyEvent {
    // ── Global (context-independent) ────────────────────────────────
    Suspend,
    Back,
    Confirm,
    ForceQuit,
    Save,
    ReverseSearch,
    NavigateUp,
    NavigateDown,
    NextField,
    PrevField,

    // ── Idle + Overlay + Editable ───────────────────────────────────
    Quit,

    // ── Overlay + Editable ──────────────────────────────────────────
    StartSearch,
    Remove,
    AddItem,
    InsertBefore,
    InsertAfter,
    Undo,
    ToggleItem,

    // ── Overlay only ────────────────────────────────────────────────
    Redo,
    LoadMore,

    // ── Editable only ───────────────────────────────────────────────
    MoveItemUp,
    MoveItemDown,

    // ── Confirm only ────────────────────────────────────────────────
    ConfirmYes,
    ConfirmNo,

    // ── Text input (Idle fallback, Text, Search) ────────────────────
    /// Raw key event for text editing. The handler inspects the inner
    /// KeyEvent for chars, backspace, delete, cursor movement, etc.
    TextInput(KeyEvent),
}

/// Convert a crossterm `KeyEvent` into a `UserKeyEvent` given the current focus.
///
/// Returns `None` for key combinations that have no mapping in the given focus.
pub fn from_key_event(key: KeyEvent, focus: &FocusLoci) -> Option<UserKeyEvent> {
    // 1. Modifier combos (always)
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        if let KeyCode::Char(c) = key.code {
            return match c {
                'z' => Some(UserKeyEvent::Suspend),
                'c' => Some(UserKeyEvent::ForceQuit),
                's' => Some(UserKeyEvent::Save),
                'r' => Some(UserKeyEvent::ReverseSearch),
                _ => None,
            };
        }
    }

    // 2. Structural keys (always)
    match key.code {
        KeyCode::Esc => return Some(UserKeyEvent::Back),
        KeyCode::Enter => return Some(UserKeyEvent::Confirm),
        KeyCode::Tab => return Some(UserKeyEvent::NextField),
        KeyCode::BackTab => return Some(UserKeyEvent::PrevField),
        KeyCode::Up => return Some(UserKeyEvent::NavigateUp),
        KeyCode::Down => return Some(UserKeyEvent::NavigateDown),
        _ => {}
    }

    // 3. InputFocus short-circuit
    match focus.input {
        InputFocus::Text => {
            return Some(UserKeyEvent::TextInput(key));
        }
        InputFocus::Search => {
            return match key.code {
                KeyCode::Char(' ') => Some(UserKeyEvent::ToggleItem),
                _ => Some(UserKeyEvent::TextInput(key)),
            };
        }
        InputFocus::Idle => {
            return match key.code {
                KeyCode::Char('j') => Some(UserKeyEvent::NavigateDown),
                KeyCode::Char('k') => Some(UserKeyEvent::NavigateUp),
                KeyCode::Char(_)
                | KeyCode::Left
                | KeyCode::Right
                | KeyCode::Backspace
                | KeyCode::Delete => Some(UserKeyEvent::TextInput(key)),
                _ => None,
            };
        }
        InputFocus::None => {} // fall through to EntityFocus
    }

    // 4. EntityFocus char mapping (only when InputFocus::None)
    match focus.entity {
        EntityFocus::Overlay => match key.code {
            KeyCode::Char('j') => Some(UserKeyEvent::NavigateDown),
            KeyCode::Char('k') => Some(UserKeyEvent::NavigateUp),
            KeyCode::Right => Some(UserKeyEvent::NextField),
            KeyCode::Left => Some(UserKeyEvent::PrevField),
            KeyCode::Char('/') => Some(UserKeyEvent::StartSearch),
            KeyCode::Char('x' | 'd') => Some(UserKeyEvent::Remove),
            KeyCode::Char('a') => Some(UserKeyEvent::AddItem),
            KeyCode::Char('i') => Some(UserKeyEvent::InsertBefore),
            KeyCode::Char('o') => Some(UserKeyEvent::InsertAfter),
            KeyCode::Char('z') => Some(UserKeyEvent::Undo),
            KeyCode::Char('y') => Some(UserKeyEvent::Redo),
            KeyCode::Char('n') => Some(UserKeyEvent::LoadMore),
            KeyCode::Char(' ') => Some(UserKeyEvent::ToggleItem),
            _ => None,
        },
        EntityFocus::Editable => match key.code {
            KeyCode::Char('j') => Some(UserKeyEvent::NavigateDown),
            KeyCode::Char('k') => Some(UserKeyEvent::NavigateUp),
            KeyCode::Right => Some(UserKeyEvent::NextField),
            KeyCode::Left => Some(UserKeyEvent::PrevField),
            KeyCode::Char('/') => Some(UserKeyEvent::StartSearch),
            KeyCode::Char('x') => Some(UserKeyEvent::Remove),
            KeyCode::Char('d') => Some(UserKeyEvent::MoveItemDown),
            KeyCode::Char('u') => Some(UserKeyEvent::MoveItemUp),
            KeyCode::Char('a') => Some(UserKeyEvent::AddItem),
            KeyCode::Char('i') => Some(UserKeyEvent::InsertBefore),
            KeyCode::Char('o') => Some(UserKeyEvent::InsertAfter),
            KeyCode::Char('z') => Some(UserKeyEvent::Undo),
            KeyCode::Char('y') => Some(UserKeyEvent::Redo),
            KeyCode::Char(' ') => Some(UserKeyEvent::ToggleItem),
            _ => None,
        },
        EntityFocus::Confirm => match key.code {
            KeyCode::Char('y' | 'Y') => Some(UserKeyEvent::ConfirmYes),
            KeyCode::Char('n' | 'N') => Some(UserKeyEvent::ConfirmNo),
            _ => None,
        },
        EntityFocus::Dismiss => Some(UserKeyEvent::Back),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    fn ctrl(c: char) -> KeyEvent {
        KeyEvent {
            code: KeyCode::Char(c),
            modifiers: KeyModifiers::CONTROL,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    fn loci(input: InputFocus, entity: EntityFocus) -> FocusLoci {
        FocusLoci { input, entity }
    }

    fn event_name(e: &Option<UserKeyEvent>) -> String {
        match e {
            None => "-".to_string(),
            Some(ev) => match ev {
                UserKeyEvent::Suspend => "Suspend".into(),
                UserKeyEvent::Back => "Back".into(),
                UserKeyEvent::Confirm => "Confirm".into(),
                UserKeyEvent::ForceQuit => "FrcQuit".into(),
                UserKeyEvent::Save => "Save".into(),
                UserKeyEvent::ReverseSearch => "RevSrch".into(),
                UserKeyEvent::NavigateUp => "NavUp".into(),
                UserKeyEvent::NavigateDown => "NavDown".into(),
                UserKeyEvent::NextField => "NxtFld".into(),
                UserKeyEvent::PrevField => "PrvFld".into(),
                UserKeyEvent::Quit => "Quit".into(),
                UserKeyEvent::StartSearch => "StartSrch".into(),
                UserKeyEvent::Remove => "Remove".into(),
                UserKeyEvent::AddItem => "AddItem".into(),
                UserKeyEvent::InsertBefore => "InsBfr".into(),
                UserKeyEvent::InsertAfter => "InsAft".into(),
                UserKeyEvent::Undo => "Undo".into(),
                UserKeyEvent::ToggleItem => "TogItem".into(),
                UserKeyEvent::Redo => "Redo".into(),
                UserKeyEvent::LoadMore => "LoadMore".into(),
                UserKeyEvent::MoveItemUp => "MoveUp".into(),
                UserKeyEvent::MoveItemDown => "MoveDn".into(),
                UserKeyEvent::ConfirmYes => "Yes".into(),
                UserKeyEvent::ConfirmNo => "No".into(),
                UserKeyEvent::TextInput(k) => match k.code {
                    KeyCode::Char(' ') => "Text( )".into(),
                    KeyCode::Char(c) => format!("Text({})", c),
                    KeyCode::Backspace => "Text(BS)".into(),
                    KeyCode::Delete => "Text(Del)".into(),
                    KeyCode::Left => "Text(←)".into(),
                    KeyCode::Right => "Text(→)".into(),
                    _ => format!("Text({:?})", k.code),
                },
            },
        }
    }

    fn row_values(key_event: KeyEvent, columns: &[(&str, FocusLoci)]) -> Vec<String> {
        columns.iter()
            .map(|(_, focus)| event_name(&from_key_event(key_event, focus)))
            .collect()
    }

    fn format_row(label: &str, values: &[String], kw: usize, vw: usize) -> String {
        let mut row = format!("{:<kw$}", label);
        for v in values {
            row += &format!("| {:<vw$}", v);
        }
        row.trim_end().to_string()
    }

    /// Normalize a cell value for comparison: Text(x) → Text(_) so all
    /// TextInput variants compare equal regardless of which char they carry.
    fn normalize(v: &str) -> String {
        if v.starts_with("Text(") { "Text(_)".into() } else { v.to_string() }
    }

    /// Collapse consecutive rows with identical (normalized) values into
    /// "a-c" ranges, substituting the range label into Text(...) cells.
    fn collapse_rows(rows: Vec<(char, Vec<String>)>) -> Vec<(String, Vec<String>)> {
        let mut result: Vec<(String, Vec<String>)> = Vec::new();
        let mut i = 0;
        while i < rows.len() {
            let start = i;
            let norm: Vec<String> = rows[i].1.iter().map(|v| normalize(v)).collect();
            while i + 1 < rows.len() {
                let next_norm: Vec<String> = rows[i + 1].1.iter().map(|v| normalize(v)).collect();
                if next_norm != norm { break; }
                i += 1;
            }
            let label = if start == i {
                rows[start].0.to_string()
            } else {
                format!("{}-{}", rows[start].0, rows[i].0)
            };
            let mut values = rows[start].1.clone();
            for v in values.iter_mut() {
                if v.starts_with("Text(") {
                    *v = format!("Text({})", label);
                }
            }
            result.push((label, values));
            i += 1;
        }
        result
    }

    fn input_name(i: InputFocus) -> &'static str {
        match i {
            InputFocus::Idle => "Idle",
            InputFocus::Text => "Text",
            InputFocus::Search => "Search",
            InputFocus::None => "None",
        }
    }

    fn entity_name(e: EntityFocus) -> &'static str {
        match e {
            EntityFocus::Overlay => "Overlay",
            EntityFocus::Editable => "Editable",
            EntityFocus::Confirm => "Confirm",
            EntityFocus::Dismiss => "Dismiss",
        }
    }

    fn build_mapping_table() -> String {
        let all_inputs = [InputFocus::Idle, InputFocus::Text, InputFocus::Search, InputFocus::None];
        let all_entities = [EntityFocus::Overlay, EntityFocus::Editable, EntityFocus::Confirm, EntityFocus::Dismiss];

        // All key events used to determine column grouping
        let special_keys: Vec<(&str, KeyEvent)> = vec![
            ("Ctrl+Z",    ctrl('z')),
            ("Ctrl+C",    ctrl('c')),
            ("Ctrl+S",    ctrl('s')),
            ("Ctrl+R",    ctrl('r')),
            ("Esc",       key(KeyCode::Esc)),
            ("Enter",     key(KeyCode::Enter)),
            ("Tab",       key(KeyCode::Tab)),
            ("BackTab",   key(KeyCode::BackTab)),
            ("Up",        key(KeyCode::Up)),
            ("Down",      key(KeyCode::Down)),
            ("Left",      key(KeyCode::Left)),
            ("Right",     key(KeyCode::Right)),
            ("Ctrl+Left", KeyEvent::new(KeyCode::Left,  KeyModifiers::CONTROL)),
            ("Ctrl+Right",KeyEvent::new(KeyCode::Right, KeyModifiers::CONTROL)),
            ("Backspace", key(KeyCode::Backspace)),
            ("Delete",    key(KeyCode::Delete)),
            ("Space",     key(KeyCode::Char(' '))),
            (":",         key(KeyCode::Char(':'))),
            ("/",         key(KeyCode::Char('/'))),
        ];
        let mut all_key_events: Vec<KeyEvent> = special_keys.iter().map(|(_, ke)| *ke).collect();
        for c in 'a'..='z' { all_key_events.push(key(KeyCode::Char(c))); }
        for c in 'A'..='Z' { all_key_events.push(key(KeyCode::Char(c))); }

        // Determine final columns by grouping EntityFocus values per InputFocus
        struct Col { input: InputFocus, entity_label: String, entity: EntityFocus }
        let mut columns: Vec<Col> = Vec::new();

        for &input in &all_inputs {
            let mut groups: Vec<(Vec<String>, Vec<EntityFocus>)> = Vec::new();
            for &entity in &all_entities {
                let focus = FocusLoci { input, entity };
                let col_values: Vec<String> = all_key_events.iter()
                    .map(|ke| event_name(&from_key_event(*ke, &focus)))
                    .collect();
                if let Some(group) = groups.iter_mut().find(|(v, _)| *v == col_values) {
                    group.1.push(entity);
                } else {
                    groups.push((col_values, vec![entity]));
                }
            }
            for (_, entities) in groups {
                let label = if entities.len() == all_entities.len() {
                    "*".to_string()
                } else {
                    entities.iter().map(|e| entity_name(*e)).collect::<Vec<_>>().join(",")
                };
                columns.push(Col { input, entity_label: label, entity: entities[0] });
            }
        }

        let kw = 10;
        let vw = 10;

        // Helper to compute row values across final columns
        let row_vals = |ke: KeyEvent| -> Vec<String> {
            columns.iter()
                .map(|col| event_name(&from_key_event(ke, &FocusLoci { input: col.input, entity: col.entity })))
                .collect()
        };

        let mut lines: Vec<String> = Vec::new();

        // Header row 1: InputFocus
        let h1: Vec<String> = columns.iter().map(|c| input_name(c.input).to_string()).collect();
        lines.push(format_row("Key", &h1, kw, vw));

        // Header row 2: EntityFocus
        let h2: Vec<String> = columns.iter().map(|c| c.entity_label.clone()).collect();
        lines.push(format_row("", &h2, kw, vw));

        // Separator
        let mut sep = "-".repeat(kw);
        for _ in &columns { sep.push('|'); sep += &"-".repeat(vw + 1); }
        lines.push(sep);

        // Special keys
        for (name, ke) in &special_keys {
            lines.push(format_row(name, &row_vals(*ke), kw, vw));
        }

        // Lowercase a-z (collapsed)
        let lower: Vec<(char, Vec<String>)> = ('a'..='z')
            .map(|c| (c, row_vals(key(KeyCode::Char(c)))))
            .collect();
        for (label, values) in collapse_rows(lower) {
            lines.push(format_row(&label, &values, kw, vw));
        }

        // Uppercase A-Z (collapsed)
        let upper: Vec<(char, Vec<String>)> = ('A'..='Z')
            .map(|c| (c, row_vals(key(KeyCode::Char(c)))))
            .collect();
        for (label, values) in collapse_rows(upper) {
            lines.push(format_row(&label, &values, kw, vw));
        }

        lines.join("\n") + "\n"
    }

    fn assert_table_eq(actual: &str, expected: &str) {
        if actual == expected {
            return;
        }
        let actual_lines: Vec<&str> = actual.lines().collect();
        let expected_lines: Vec<&str> = expected.lines().collect();
        let max = actual_lines.len().max(expected_lines.len());

        let mut diff = String::new();
        diff.push_str("\n\nKey mapping snapshot mismatch:\n\n");

        for i in 0..max {
            let a = actual_lines.get(i).unwrap_or(&"<missing>");
            let e = expected_lines.get(i).unwrap_or(&"<missing>");
            if a == e {
                diff.push_str(&format!("  {}\n", a));
            } else {
                diff.push_str(&format!("- {}\n", e));
                diff.push_str(&format!("+ {}\n", a));
            }
        }
        panic!("{}", diff);
    }

    /// Snapshot test: any change to key mappings will fail this test and
    /// show a readable line-by-line diff of the full mapping table.
    #[test]
    fn key_mapping_snapshot() {
        let table = build_mapping_table();
        let expected = "\
Key       | Idle      | Text      | Search    | None      | None      | None      | None
          | *         | *         | *         | Overlay   | Editable  | Confirm   | Dismiss
----------|-----------|-----------|-----------|-----------|-----------|-----------|-----------
Ctrl+Z    | Suspend   | Suspend   | Suspend   | Suspend   | Suspend   | Suspend   | Suspend
Ctrl+C    | FrcQuit   | FrcQuit   | FrcQuit   | FrcQuit   | FrcQuit   | FrcQuit   | FrcQuit
Ctrl+S    | Save      | Save      | Save      | Save      | Save      | Save      | Save
Ctrl+R    | RevSrch   | RevSrch   | RevSrch   | RevSrch   | RevSrch   | RevSrch   | RevSrch
Esc       | Back      | Back      | Back      | Back      | Back      | Back      | Back
Enter     | Confirm   | Confirm   | Confirm   | Confirm   | Confirm   | Confirm   | Confirm
Tab       | NxtFld    | NxtFld    | NxtFld    | NxtFld    | NxtFld    | NxtFld    | NxtFld
BackTab   | PrvFld    | PrvFld    | PrvFld    | PrvFld    | PrvFld    | PrvFld    | PrvFld
Up        | NavUp     | NavUp     | NavUp     | NavUp     | NavUp     | NavUp     | NavUp
Down      | NavDown   | NavDown   | NavDown   | NavDown   | NavDown   | NavDown   | NavDown
Left      | Text(←)   | Text(←)   | Text(←)   | PrvFld    | PrvFld    | -         | Back
Right     | Text(→)   | Text(→)   | Text(→)   | NxtFld    | NxtFld    | -         | Back
Ctrl+Left | Text(←)   | Text(←)   | Text(←)   | PrvFld    | PrvFld    | -         | Back
Ctrl+Right| Text(→)   | Text(→)   | Text(→)   | NxtFld    | NxtFld    | -         | Back
Backspace | Text(BS)  | Text(BS)  | Text(BS)  | -         | -         | -         | Back
Delete    | Text(Del) | Text(Del) | Text(Del) | -         | -         | -         | Back
Space     | Text( )   | Text( )   | TogItem   | TogItem   | TogItem   | -         | Back
:         | Text(:)   | Text(:)   | Text(:)   | -         | -         | -         | Back
/         | Text(/)   | Text(/)   | Text(/)   | StartSrch | StartSrch | -         | Back
a         | Text(a)   | Text(a)   | Text(a)   | AddItem   | AddItem   | -         | Back
b-c       | Text(b-c) | Text(b-c) | Text(b-c) | -         | -         | -         | Back
d         | Text(d)   | Text(d)   | Text(d)   | Remove    | MoveDn    | -         | Back
e-h       | Text(e-h) | Text(e-h) | Text(e-h) | -         | -         | -         | Back
i         | Text(i)   | Text(i)   | Text(i)   | InsBfr    | InsBfr    | -         | Back
j         | NavDown   | Text(j)   | Text(j)   | NavDown   | NavDown   | -         | Back
k         | NavUp     | Text(k)   | Text(k)   | NavUp     | NavUp     | -         | Back
l-m       | Text(l-m) | Text(l-m) | Text(l-m) | -         | -         | -         | Back
n         | Text(n)   | Text(n)   | Text(n)   | LoadMore  | -         | No        | Back
o         | Text(o)   | Text(o)   | Text(o)   | InsAft    | InsAft    | -         | Back
p-t       | Text(p-t) | Text(p-t) | Text(p-t) | -         | -         | -         | Back
u         | Text(u)   | Text(u)   | Text(u)   | -         | MoveUp    | -         | Back
v-w       | Text(v-w) | Text(v-w) | Text(v-w) | -         | -         | -         | Back
x         | Text(x)   | Text(x)   | Text(x)   | Remove    | Remove    | -         | Back
y         | Text(y)   | Text(y)   | Text(y)   | Redo      | Redo      | Yes       | Back
z         | Text(z)   | Text(z)   | Text(z)   | Undo      | Undo      | -         | Back
A-M       | Text(A-M) | Text(A-M) | Text(A-M) | -         | -         | -         | Back
N         | Text(N)   | Text(N)   | Text(N)   | -         | -         | No        | Back
O-X       | Text(O-X) | Text(O-X) | Text(O-X) | -         | -         | -         | Back
Y         | Text(Y)   | Text(Y)   | Text(Y)   | -         | -         | Yes       | Back
Z         | Text(Z)   | Text(Z)   | Text(Z)   | -         | -         | -         | Back
";
        assert_table_eq(&table, expected);
    }
}
