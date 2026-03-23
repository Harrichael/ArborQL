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
        /// Explicit via-path, if already selected by the user.
        via: Vec<String>,
    },
}

impl std::fmt::Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Rule::Filter { table, conditions } => {
                write!(f, "{} where ", table)?;
                let parts: Vec<String> = conditions.iter().map(|c| c.to_string()).collect();
                write!(f, "{}", parts.join(" and "))
            }
            Rule::Relation {
                from_table,
                to_table,
                via,
            } => {
                write!(f, "{} to {}", from_table, to_table)?;
                if !via.is_empty() {
                    write!(f, " via {}", via.join(", "))?;
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
        };
        assert_eq!(r.to_string(), "user to location via location_assignments");
    }
}
