use std::collections::HashMap;
use crate::schema::TablePath;

/// A candidate for the next token at the current cursor position, used to drive
/// real-time hints as the user types a command.
///
/// Adding a new command type only requires extending [`GrammarState`], [`advance`],
/// and [`valid_completions_for_state`] — `completions_at` needs no changes.
#[derive(Debug, Clone, PartialEq)]
pub enum Completion {
    /// A specific completable token (keyword, table name, column name, or operator).
    Token(String),
    /// The next expected input is a free-form quoted value, e.g. `'Rick'`.
    QuotedValue,
}

// ---------------------------------------------------------------------------
// Grammar state machine (private)
// ---------------------------------------------------------------------------

/// One position in the command grammar, produced by advancing through complete
/// tokens left-to-right. Each variant knows what can legally come next.
#[derive(Clone)]
enum GrammarState {
    /// No tokens yet — expect a table name.
    Initial,
    /// A valid table name was entered. Next: `where`, `to`, or end.
    AfterTable { table: String },
    /// `where` (or `and` after a condition) seen. Next: a column name.
    AfterWhere { table: String },
    /// Column name entered. Next: an operator.
    AfterColumn { table: String },
    /// Operator entered. Next: a quoted (or bare) value.
    AfterOp { table: String },
    /// A complete `col op val` condition was parsed. Next: `and` or end.
    AfterValue { table: String },
    /// `to` keyword seen. Next: the destination table name.
    AfterTo { from: String },
    /// `<from> to <to>` parsed. Next: `via` or end.
    AfterToTable { from: String, to: String },
    /// `via` (or `,` after a via table) seen. Next: a table name.
    AfterVia { from: String, to: String },
    /// A via table was entered. Next: `,` or end.
    AfterViaTable { from: String, to: String },
    /// Invalid token encountered — no valid completions.
    Error,
}

/// Tokenize `input` into complete tokens and a trailing partial token.
///
/// Quoted strings (single or double quotes) are kept together as one token.
/// Commas are emitted as their own `","` token.
/// Returns `(complete_tokens, partial_last_token)`.
fn tokenize_partial(input: &str) -> (Vec<String>, String) {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut quote_char = ' ';

    for ch in input.chars() {
        if in_quote {
            current.push(ch);
            if ch == quote_char {
                in_quote = false;
            }
        } else if ch == '\'' || ch == '"' {
            in_quote = true;
            quote_char = ch;
            current.push(ch);
        } else if ch == ' ' {
            if !current.is_empty() {
                tokens.push(current.clone());
                current.clear();
            }
        } else if ch == ',' {
            if !current.is_empty() {
                tokens.push(current.clone());
                current.clear();
            }
            tokens.push(",".to_string());
        } else {
            current.push(ch);
        }
    }

    (tokens, current)
}

/// Advance the grammar state machine by consuming one complete token.
fn advance(
    state: GrammarState,
    token: &str,
    tables: &[String],
    columns: &HashMap<String, Vec<String>>,
) -> GrammarState {
    let lower = token.to_lowercase();
    match state {
        GrammarState::Initial => {
            if let Some(t) = tables.iter().find(|t| t.to_lowercase() == lower) {
                GrammarState::AfterTable { table: t.clone() }
            } else {
                GrammarState::Error
            }
        }
        GrammarState::AfterTable { table } => match lower.as_str() {
            "where" => GrammarState::AfterWhere { table },
            "to" => GrammarState::AfterTo { from: table },
            _ => GrammarState::Error,
        },
        GrammarState::AfterWhere { ref table } => {
            let cols = columns.get(table).map(|v| v.as_slice()).unwrap_or(&[]);
            if cols.iter().any(|c| c.to_lowercase() == lower) {
                GrammarState::AfterColumn { table: table.clone() }
            } else {
                GrammarState::Error
            }
        }
        GrammarState::AfterColumn { table } => {
            const OPS: &[&str] = &[
                "=", "!=", "<", "<=", ">", ">=", "startswith", "endswith", "contains",
            ];
            if OPS.iter().any(|op| *op == lower.as_str()) {
                GrammarState::AfterOp { table }
            } else {
                GrammarState::Error
            }
        }
        GrammarState::AfterOp { table } => {
            // Any token is accepted as a value.
            GrammarState::AfterValue { table }
        }
        GrammarState::AfterValue { table } => match lower.as_str() {
            "and" => GrammarState::AfterWhere { table },
            _ => GrammarState::Error,
        },
        GrammarState::AfterTo { from } => {
            if let Some(t) = tables.iter().find(|t| t.to_lowercase() == lower) {
                GrammarState::AfterToTable { from, to: t.clone() }
            } else {
                GrammarState::Error
            }
        }
        GrammarState::AfterToTable { from, to } => match lower.as_str() {
            "via" => GrammarState::AfterVia { from, to },
            _ => GrammarState::Error,
        },
        GrammarState::AfterVia { from, to } => {
            if tables.iter().any(|t| t.to_lowercase() == lower) {
                GrammarState::AfterViaTable { from, to }
            } else {
                GrammarState::Error
            }
        }
        GrammarState::AfterViaTable { from, to } => match lower.as_str() {
            "," => GrammarState::AfterVia { from, to },
            _ => GrammarState::Error,
        },
        GrammarState::Error => GrammarState::Error,
    }
}

/// Return the exhaustive set of valid next completions for the given grammar state.
fn valid_completions_for_state(
    state: &GrammarState,
    tables: &[String],
    columns: &HashMap<String, Vec<String>>,
) -> Vec<Completion> {
    match state {
        GrammarState::Initial => tables.iter().map(|t| Completion::Token(t.clone())).collect(),
        GrammarState::AfterTable { .. } => vec![
            Completion::Token("where".to_string()),
            Completion::Token("to".to_string()),
        ],
        GrammarState::AfterWhere { table } => columns
            .get(table)
            .map(|cols| cols.iter().map(|c| Completion::Token(c.clone())).collect())
            .unwrap_or_default(),
        GrammarState::AfterColumn { .. } => [
            "=", "!=", "<", "<=", ">", ">=", "startswith", "endswith", "contains",
        ]
        .iter()
        .map(|op| Completion::Token(op.to_string()))
        .collect(),
        GrammarState::AfterOp { .. } => vec![Completion::QuotedValue],
        GrammarState::AfterValue { .. } => vec![Completion::Token("and".to_string())],
        GrammarState::AfterTo { .. } => {
            tables.iter().map(|t| Completion::Token(t.clone())).collect()
        }
        GrammarState::AfterToTable { .. } => vec![Completion::Token("via".to_string())],
        GrammarState::AfterVia { .. } => {
            tables.iter().map(|t| Completion::Token(t.clone())).collect()
        }
        GrammarState::AfterViaTable { .. } => vec![Completion::Token(",".to_string())],
        GrammarState::Error => vec![],
    }
}

/// Return the valid next completions at the current cursor position in `input`.
///
/// Drives the grammar state machine with the complete tokens already typed, then
/// filters valid next-token candidates by the prefix of the last partial word.
///
/// `tables` is the list of known table names; `columns` maps each table name to
/// its column names. Both are used for context-sensitive suggestions.
pub fn completions_at(
    input: &str,
    tables: &[String],
    columns: &HashMap<String, Vec<String>>,
) -> Vec<Completion> {
    let (tokens, partial) = tokenize_partial(input);

    let mut state = GrammarState::Initial;
    for token in &tokens {
        state = advance(state, token, tables, columns);
    }

    let candidates = valid_completions_for_state(&state, tables, columns);

    if partial.is_empty() {
        candidates
    } else {
        candidates
            .into_iter()
            .filter(|c| match c {
                Completion::Token(s) => s.to_lowercase().starts_with(&partial.to_lowercase()),
                // Show the value placeholder only when the user has opened a quote.
                Completion::QuotedValue => {
                    partial.starts_with('\'') || partial.starts_with('"')
                }
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------

/// A condition operator for filter rules.
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    StartsWith,
    EndsWith,
    Contains,
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::Eq => write!(f, "="),
            Op::Ne => write!(f, "!="),
            Op::Lt => write!(f, "<"),
            Op::Le => write!(f, "<="),
            Op::Gt => write!(f, ">"),
            Op::Ge => write!(f, ">="),
            Op::StartsWith => write!(f, "startswith"),
            Op::EndsWith => write!(f, "endswith"),
            Op::Contains => write!(f, "contains"),
        }
    }
}

/// A single filter condition: `column op value`.
#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    pub column: String,
    pub op: Op,
    pub value: String,
}

impl std::fmt::Display for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} '{}'", self.column, self.op, self.value)
    }
}

/// A rule describing what data to bring into the viewer.
#[derive(Debug, Clone, PartialEq)]
pub enum Rule {
    /// `<table> where <column> <op> <value>` — filter rows from a table.
    Filter {
        table: String,
        conditions: Vec<Condition>,
    },
    /// `<from_table> to <to_table> [via <intermediate>...]` — relationship traversal.
    Relation {
        from_table: String,
        to_table: String,
        /// Explicit via-path supplied by the user (intermediate table names).
        via: Vec<String>,
        /// The resolved path chosen at execution time (auto or manual).
        /// Stored so re-execution uses the exact same path even if new virtual
        /// FKs later make the route ambiguous.
        resolved_path: Option<TablePath>,
    },
}

impl std::fmt::Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Rule::Filter { table, conditions } => {
                if conditions.is_empty() {
                    write!(f, "{}", table)
                } else {
                    let parts: Vec<String> = conditions.iter().map(|c| c.to_string()).collect();
                    write!(f, "{} where {}", table, parts.join(" and "))
                }
            }
            Rule::Relation {
                from_table,
                to_table,
                via,
                resolved_path,
            } => {
                write!(f, "{} to {}", from_table, to_table)?;
                if !via.is_empty() {
                    write!(f, " via {}", via.join(", "))?;
                } else if let Some(path) = resolved_path {
                    let intermediates: Vec<String> = path
                        .steps
                        .iter()
                        .skip(1)
                        .map(|s| s.from_table.clone())
                        .collect();
                    if !intermediates.is_empty() {
                        write!(f, " via {}", intermediates.join(", "))?;
                    }
                }
                Ok(())
            }
        }
    }
}

/// Parse a user-typed command string into a `Rule`.
///
/// Supported syntax:
/// - `<table> where <col> <op> <val> [and <col> <op> <val> ...]`
///   operators: `=`, `!=`, `<`, `<=`, `>`, `>=`, `startswith`, `endswith`, `contains`
/// - `<from> to <to>`
/// - `<from> to <to> via <t1>[, <t2> ...]`
pub fn parse_rule(input: &str) -> Result<Rule, String> {
    let input = input.trim();
    let lower = input.to_lowercase();

    // Check for "X to Y [via ...]" pattern
    if let Some(to_pos) = find_keyword_pos(&lower, " to ") {
        let from_table = input[..to_pos].trim().to_string();
        let rest = &input[to_pos + 4..];
        let (to_table, via) = if let Some(via_pos) = find_keyword_pos(&rest.to_lowercase(), " via ") {
            let to_t = rest[..via_pos].trim().to_string();
            let via_str = rest[via_pos + 5..].trim();
            let via_tables: Vec<String> = via_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            (to_t, via_tables)
        } else {
            (rest.trim().to_string(), vec![])
        };
        if from_table.is_empty() || to_table.is_empty() {
            return Err("'to' rule requires both source and target tables".to_string());
        }
        return Ok(Rule::Relation {
            from_table,
            to_table,
            via,
            resolved_path: None,
        });
    }

    // Check for "X where ..." pattern
    if let Some(where_pos) = find_keyword_pos(&lower, " where ") {
        let table = input[..where_pos].trim().to_string();
        let conditions_str = &input[where_pos + 7..];
        let conditions = parse_conditions(conditions_str)?;
        if table.is_empty() {
            return Err("Filter rule requires a table name".to_string());
        }
        return Ok(Rule::Filter { table, conditions });
    }

    // Plain table name with no conditions - treat as filter with no conditions
    let table = input.to_string();
    if table.is_empty() || table.contains(' ') {
        return Err(format!("Cannot parse rule: '{}'", input));
    }
    Ok(Rule::Filter {
        table,
        conditions: vec![],
    })
}

/// Find the position of `keyword` (case-insensitive substring) in `lower`.
fn find_keyword_pos(lower: &str, keyword: &str) -> Option<usize> {
    lower.find(keyword)
}

/// Parse a chain of conditions joined by " and ".
fn parse_conditions(s: &str) -> Result<Vec<Condition>, String> {
    let mut conditions = Vec::new();
    // Split on " and " (case-insensitive)
    let lower = s.to_lowercase();
    let parts = split_and(&lower, s);
    for part in parts {
        conditions.push(parse_condition(part.trim())?);
    }
    Ok(conditions)
}

/// Split the string on literal " and " keywords, returning original-case slices.
fn split_and<'a>(lower: &'a str, original: &'a str) -> Vec<&'a str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let keyword = " and ";
    let mut search_from = 0;
    while let Some(pos) = lower[search_from..].find(keyword) {
        let abs = search_from + pos;
        parts.push(&original[start..abs]);
        start = abs + keyword.len();
        search_from = start;
    }
    parts.push(&original[start..]);
    parts
}

/// Parse a single condition like `name startswith 'Rick'`.
fn parse_condition(s: &str) -> Result<Condition, String> {
    let lower = s.to_lowercase();

    // Try multi-word operators first (longest match)
    let two_word_ops = [
        (" startswith ", Op::StartsWith),
        (" endswith ", Op::EndsWith),
        (" contains ", Op::Contains),
    ];
    for (kw, op) in &two_word_ops {
        if let Some(pos) = lower.find(kw) {
            let column = s[..pos].trim().to_string();
            let raw_val = s[pos + kw.len()..].trim();
            let value = strip_quotes(raw_val);
            return Ok(Condition { column, op: op.clone(), value });
        }
    }

    // Symbol operators
    let symbol_ops = [
        ("!=", Op::Ne),
        ("<=", Op::Le),
        (">=", Op::Ge),
        ("<", Op::Lt),
        (">", Op::Gt),
        ("=", Op::Eq),
    ];
    for (sym, op) in &symbol_ops {
        if let Some(pos) = s.find(sym) {
            let column = s[..pos].trim().to_string();
            let raw_val = s[pos + sym.len()..].trim();
            let value = strip_quotes(raw_val);
            return Ok(Condition { column, op: op.clone(), value });
        }
    }

    Err(format!("Cannot parse condition: '{}'", s))
}

fn strip_quotes(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\''))
        || (s.starts_with('"') && s.ends_with('"'))
    {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// Build a SQL WHERE clause from conditions (for filter rules).
pub fn conditions_to_sql(conditions: &[Condition]) -> String {
    if conditions.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = conditions
        .iter()
        .map(|c| {
            let escaped = c.value.replace('\'', "''");
            match &c.op {
                Op::Eq => format!("{} = '{}'", c.column, escaped),
                Op::Ne => format!("{} != '{}'", c.column, escaped),
                Op::Lt => format!("{} < '{}'", c.column, escaped),
                Op::Le => format!("{} <= '{}'", c.column, escaped),
                Op::Gt => format!("{} > '{}'", c.column, escaped),
                Op::Ge => format!("{} >= '{}'", c.column, escaped),
                Op::StartsWith => format!("{} LIKE '{}%'", c.column, escaped),
                Op::EndsWith => format!("{} LIKE '%{}'", c.column, escaped),
                Op::Contains => format!("{} LIKE '%{}%'", c.column, escaped),
            }
        })
        .collect();
    parts.join(" AND ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filter_no_conditions() {
        let rule = parse_rule("users").unwrap();
        assert_eq!(
            rule,
            Rule::Filter {
                table: "users".to_string(),
                conditions: vec![]
            }
        );
    }

    #[test]
    fn test_parse_filter_startswith() {
        let rule = parse_rule("users where name startswith 'Rick'").unwrap();
        assert_eq!(
            rule,
            Rule::Filter {
                table: "users".to_string(),
                conditions: vec![Condition {
                    column: "name".to_string(),
                    op: Op::StartsWith,
                    value: "Rick".to_string()
                }]
            }
        );
    }

    #[test]
    fn test_parse_filter_multiple_conditions() {
        let rule = parse_rule("orders where status = 'open' and amount > '100'").unwrap();
        if let Rule::Filter { conditions, .. } = rule {
            assert_eq!(conditions.len(), 2);
            assert_eq!(conditions[0].op, Op::Eq);
            assert_eq!(conditions[1].op, Op::Gt);
        } else {
            panic!("Expected Filter rule");
        }
    }

    #[test]
    fn test_parse_relation_simple() {
        let rule = parse_rule("user to location").unwrap();
        assert_eq!(
            rule,
            Rule::Relation {
                from_table: "user".to_string(),
                to_table: "location".to_string(),
                via: vec![],
                resolved_path: None,
            }
        );
    }

    #[test]
    fn test_parse_relation_via() {
        let rule = parse_rule("user to location via location_assignments").unwrap();
        assert_eq!(
            rule,
            Rule::Relation {
                from_table: "user".to_string(),
                to_table: "location".to_string(),
                via: vec!["location_assignments".to_string()],
                resolved_path: None,
            }
        );
    }

    #[test]
    fn test_conditions_to_sql() {
        let conds = vec![Condition {
            column: "name".to_string(),
            op: Op::StartsWith,
            value: "Rick".to_string(),
        }];
        let sql = conditions_to_sql(&conds);
        assert_eq!(sql, "name LIKE 'Rick%'");
    }

    #[test]
    fn test_rule_display() {
        let r = Rule::Relation {
            from_table: "user".to_string(),
            to_table: "location".to_string(),
            via: vec!["location_assignments".to_string()],
            resolved_path: None,
        };
        assert_eq!(r.to_string(), "user to location via location_assignments");
    }

    // ---------------------------------------------------------------------------
    // completions_at tests
    // ---------------------------------------------------------------------------

    fn tables() -> Vec<String> {
        vec!["users".to_string(), "orders".to_string(), "products".to_string()]
    }

    fn columns() -> HashMap<String, Vec<String>> {
        let mut m = HashMap::new();
        m.insert(
            "users".to_string(),
            vec!["id".to_string(), "name".to_string(), "email".to_string()],
        );
        m.insert(
            "orders".to_string(),
            vec!["id".to_string(), "user_id".to_string(), "total".to_string()],
        );
        m
    }

    #[test]
    fn test_completions_initial_all_tables() {
        let c = completions_at("", &tables(), &columns());
        assert!(c.contains(&Completion::Token("users".to_string())));
        assert!(c.contains(&Completion::Token("orders".to_string())));
        assert!(c.contains(&Completion::Token("products".to_string())));
    }

    #[test]
    fn test_completions_initial_prefix_filter() {
        let c = completions_at("us", &tables(), &columns());
        assert_eq!(c, vec![Completion::Token("users".to_string())]);
    }

    #[test]
    fn test_completions_after_table() {
        let c = completions_at("users ", &tables(), &columns());
        assert!(c.contains(&Completion::Token("where".to_string())));
        assert!(c.contains(&Completion::Token("to".to_string())));
    }

    #[test]
    fn test_completions_after_table_partial_where() {
        let c = completions_at("users wh", &tables(), &columns());
        assert_eq!(c, vec![Completion::Token("where".to_string())]);
    }

    #[test]
    fn test_completions_after_where_shows_columns() {
        let c = completions_at("users where ", &tables(), &columns());
        assert!(c.contains(&Completion::Token("name".to_string())));
        assert!(c.contains(&Completion::Token("email".to_string())));
    }

    #[test]
    fn test_completions_after_where_partial_column() {
        let c = completions_at("users where na", &tables(), &columns());
        assert_eq!(c, vec![Completion::Token("name".to_string())]);
    }

    #[test]
    fn test_completions_after_column_shows_operators() {
        let c = completions_at("users where name ", &tables(), &columns());
        let tokens: Vec<_> = c.iter().filter_map(|x| if let Completion::Token(s) = x { Some(s.as_str()) } else { None }).collect();
        assert!(tokens.contains(&"="));
        assert!(tokens.contains(&"startswith"));
        assert!(tokens.contains(&"contains"));
    }

    #[test]
    fn test_completions_after_column_partial_op() {
        let c = completions_at("users where name starts", &tables(), &columns());
        assert_eq!(c, vec![Completion::Token("startswith".to_string())]);
    }

    #[test]
    fn test_completions_after_op_shows_value_placeholder() {
        let c = completions_at("users where name = ", &tables(), &columns());
        assert_eq!(c, vec![Completion::QuotedValue]);
    }

    #[test]
    fn test_completions_after_op_partial_quote_shows_placeholder() {
        let c = completions_at("users where name = '", &tables(), &columns());
        assert_eq!(c, vec![Completion::QuotedValue]);
    }

    #[test]
    fn test_completions_after_value_shows_and() {
        let c = completions_at("users where name = 'Rick' ", &tables(), &columns());
        assert_eq!(c, vec![Completion::Token("and".to_string())]);
    }

    #[test]
    fn test_completions_after_and_shows_columns() {
        let c = completions_at("users where name = 'Rick' and ", &tables(), &columns());
        assert!(c.contains(&Completion::Token("email".to_string())));
    }

    #[test]
    fn test_completions_after_to_shows_tables() {
        let c = completions_at("users to ", &tables(), &columns());
        assert!(c.contains(&Completion::Token("orders".to_string())));
        assert!(c.contains(&Completion::Token("products".to_string())));
    }

    #[test]
    fn test_completions_after_to_table_shows_via() {
        let c = completions_at("users to orders ", &tables(), &columns());
        assert_eq!(c, vec![Completion::Token("via".to_string())]);
    }

    #[test]
    fn test_completions_after_via_shows_tables() {
        let c = completions_at("users to orders via ", &tables(), &columns());
        assert!(c.contains(&Completion::Token("products".to_string())));
    }

    #[test]
    fn test_completions_after_via_table_shows_comma() {
        let c = completions_at("users to orders via products ", &tables(), &columns());
        assert_eq!(c, vec![Completion::Token(",".to_string())]);
    }

    #[test]
    fn test_completions_after_comma_in_via_shows_tables() {
        let c = completions_at("users to orders via products,", &tables(), &columns());
        assert!(c.contains(&Completion::Token("users".to_string())));
    }

    #[test]
    fn test_completions_error_state_empty() {
        let c = completions_at("users where nonsense gobbledygook ", &tables(), &columns());
        assert!(c.is_empty());
    }

    #[test]
    fn test_completions_case_insensitive_table() {
        let c = completions_at("USERS wh", &tables(), &columns());
        assert_eq!(c, vec![Completion::Token("where".to_string())]);
    }
}
