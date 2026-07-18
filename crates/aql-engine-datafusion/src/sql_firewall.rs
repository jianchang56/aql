use super::*;

/// Scalar value accepted for a named SQL parameter.
#[derive(Clone, Debug, PartialEq)]
pub enum SqlParameter {
    /// SQL `NULL`.
    Null,
    /// Boolean value.
    Bool(bool),
    /// Signed 64-bit integer.
    Int64(i64),
    /// Finite 64-bit floating-point value. Non-finite values are rejected.
    Float64(f64),
    /// UTF-8 text escaped as a SQL string literal by sqlparser.
    Text(String),
}

/// Binds every named `:parameter` placeholder to a scalar SQL literal.
///
/// Binding operates on the parsed AST, not string interpolation. Missing,
/// positional, or unused parameters are rejected.
///
/// # Examples
///
/// ```
/// use std::collections::BTreeMap;
/// use aql_engine_datafusion::{SqlParameter, bind_sql_parameters};
///
/// let sql = bind_sql_parameters(
///     "SELECT session_id FROM sessions WHERE agent_id = :agent",
///     &BTreeMap::from([("agent".to_string(), SqlParameter::Text("codex".into()))]),
/// ).unwrap();
/// assert!(sql.contains("'codex'"));
/// ```
pub fn bind_sql_parameters(
    sql: &str,
    parameters: &BTreeMap<String, SqlParameter>,
) -> std::result::Result<String, QueryError> {
    use sqlparser::ast::{ValueWithSpan, VisitMut, VisitorMut};

    if sql.len() > MAX_SQL_BYTES {
        return Err(sql_rejected(
            "parse",
            "query exceeds the fixed length limit",
        ));
    }

    struct Binder<'a> {
        parameters: &'a BTreeMap<String, SqlParameter>,
        used: BTreeSet<String>,
    }

    impl VisitorMut for Binder<'_> {
        type Break = Box<QueryError>;

        fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<Self::Break> {
            let Value::Placeholder(placeholder) = &value.value else {
                return ControlFlow::Continue(());
            };
            let Some(name) = placeholder.strip_prefix(':').map(str::to_string) else {
                return ControlFlow::Break(Box::new(sql_rejected(
                    "parameters",
                    "only named :parameter placeholders are supported",
                )));
            };
            let Some(parameter) = self.parameters.get(&name) else {
                return ControlFlow::Break(Box::new(sql_rejected(
                    "parameters",
                    "query contains an unbound parameter",
                )));
            };
            value.value = match parameter {
                SqlParameter::Null => Value::Null,
                SqlParameter::Bool(value) => Value::Boolean(*value),
                SqlParameter::Int64(value) => Value::Number(value.to_string(), false),
                SqlParameter::Float64(value) => {
                    if !value.is_finite() {
                        return ControlFlow::Break(Box::new(sql_rejected(
                            "parameters",
                            "float parameters must be finite",
                        )));
                    }
                    Value::Number(value.to_string(), false)
                }
                SqlParameter::Text(value) => Value::SingleQuotedString(value.clone()),
            };
            self.used.insert(name);
            ControlFlow::Continue(())
        }
    }

    let mut statements = Parser::parse_sql(&GenericDialect, sql)
        .map_err(|_| sql_rejected("parse", "query is not valid SQL"))?;
    if statements.len() != 1 {
        return Err(sql_rejected(
            "parse",
            "exactly one read-only query is required",
        ));
    }
    let mut statement = statements
        .pop()
        .ok_or_else(|| sql_rejected("parse", "exactly one read-only query is required"))?;
    let mut binder = Binder {
        parameters,
        used: BTreeSet::new(),
    };
    if let ControlFlow::Break(error) = VisitMut::visit(&mut statement, &mut binder) {
        return Err(*error);
    }
    if binder.used.len() != parameters.len() {
        return Err(sql_rejected(
            "parameters",
            "one or more supplied parameters are unused",
        ));
    }
    Ok(statement.to_string())
}

/// Sanitized query validation, authorization, and execution failures.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// The query violated a fixed validation or lifecycle contract.
    #[error("SQL rejected at stage {stage}: {reason}")]
    SqlRejected {
        /// Stable validation stage.
        stage: &'static str,
        /// Stable, non-sensitive rejection reason.
        reason: &'static str,
    },
    /// The query references a canonical field without its required grant.
    #[error("query references a field that requires --access {0}")]
    AccessDenied(&'static str),
    /// DataFusion failed after AQL validation and authorization.
    #[error("query engine execution failed")]
    Engine(
        #[from]
        #[source]
        DataFusionError,
    ),
}

/// Parsed, single-statement SQL that passed AQL's read-only firewall.
#[derive(Clone, Debug)]
pub struct ValidatedSql {
    statement: Statement,
}

impl ValidatedSql {
    /// Returns the parser-normalized SQL representation.
    #[must_use]
    pub fn normalized_sql(&self) -> String {
        self.statement.to_string()
    }
}

/// Parses and validates exactly one read-only canonical SELECT or CTE query.
///
/// The firewall rejects writes, external tables, catalog qualification, table
/// functions, non-allowlisted functions, unsafe wildcards, and excessive query
/// complexity. Safe wildcards are expanded before the value is returned.
///
/// # Examples
///
/// ```
/// use aql_engine_datafusion::validate_read_only_sql;
///
/// assert!(validate_read_only_sql("SELECT session_id FROM sessions").is_ok());
/// assert!(validate_read_only_sql("DELETE FROM sessions").is_err());
/// ```
pub fn validate_read_only_sql(sql: &str) -> std::result::Result<ValidatedSql, QueryError> {
    if sql.len() > MAX_SQL_BYTES {
        return Err(sql_rejected(
            "parse",
            "query exceeds the fixed length limit",
        ));
    }
    let mut statements = Parser::parse_sql(&GenericDialect, sql)
        .map_err(|_| sql_rejected("parse", "query is not valid SQL"))?;
    if statements.len() != 1 {
        return Err(sql_rejected(
            "parse",
            "exactly one read-only query is required",
        ));
    }
    let mut statement = statements
        .pop()
        .ok_or_else(|| sql_rejected("parse", "exactly one read-only query is required"))?;
    let Statement::Query(query) = &statement else {
        return Err(sql_rejected("allowlist", "only SELECT queries are allowed"));
    };

    let mut ctes = BTreeSet::new();
    collect_ctes(query, &mut ctes)?;
    let mut visitor = ReadOnlyVisitor::new(ctes);
    if let ControlFlow::Break(error) = statement.visit(&mut visitor) {
        return Err(*error);
    }
    let Statement::Query(query) = &mut statement else {
        return Err(sql_rejected("allowlist", "only SELECT queries are allowed"));
    };
    rewrite_safe_wildcards(query)?;
    let mut remaining_wildcard = RemainingWildcardVisitor;
    if let ControlFlow::Break(error) = statement.visit(&mut remaining_wildcard) {
        return Err(*error);
    }
    Ok(ValidatedSql { statement })
}

struct RemainingWildcardVisitor;

impl Visitor for RemainingWildcardVisitor {
    type Break = Box<QueryError>;

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
        if select.projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
            )
        }) {
            ControlFlow::Break(Box::new(sql_rejected(
                "wildcard",
                "wildcard scope could not be safely resolved",
            )))
        } else {
            ControlFlow::Continue(())
        }
    }
}

#[derive(Clone)]
struct OutputColumn {
    name: String,
    access: AccessClass,
}

#[derive(Clone)]
struct RelationColumns {
    qualifier: String,
    columns: Vec<OutputColumn>,
}

fn rewrite_safe_wildcards(query: &mut Query) -> std::result::Result<Vec<OutputColumn>, QueryError> {
    rewrite_query_with_ctes(query, &BTreeMap::new())
}

fn rewrite_query_with_ctes(
    query: &mut Query,
    inherited_ctes: &BTreeMap<String, Vec<OutputColumn>>,
) -> std::result::Result<Vec<OutputColumn>, QueryError> {
    let mut ctes = inherited_ctes.clone();
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            let mut columns = rewrite_query_with_ctes(&mut cte.query, &ctes)?;
            if !cte.alias.columns.is_empty() {
                if cte.alias.columns.len() != columns.len() {
                    return Err(sql_rejected(
                        "wildcard",
                        "CTE column aliases do not match the query output",
                    ));
                }
                for (column, alias) in columns.iter_mut().zip(&cte.alias.columns) {
                    column.name.clone_from(&alias.name.value);
                }
            }
            ctes.insert(cte.alias.name.value.to_ascii_lowercase(), columns);
        }
    }
    rewrite_set_expr(&mut query.body, &ctes)
}

fn rewrite_set_expr(
    expression: &mut SetExpr,
    ctes: &BTreeMap<String, Vec<OutputColumn>>,
) -> std::result::Result<Vec<OutputColumn>, QueryError> {
    match expression {
        SetExpr::Select(select) => rewrite_select(select, ctes),
        SetExpr::Query(query) => rewrite_query_with_ctes(query, ctes),
        SetExpr::SetOperation { left, right, .. } => {
            let left_columns = rewrite_set_expr(left, ctes)?;
            let right_columns = rewrite_set_expr(right, ctes)?;
            if left_columns.len() != right_columns.len() {
                return Err(sql_rejected(
                    "wildcard",
                    "set operation outputs have different widths",
                ));
            }
            Ok(left_columns
                .into_iter()
                .zip(right_columns)
                .map(|(left, right)| OutputColumn {
                    name: left.name,
                    access: stricter_access(left.access, right.access),
                })
                .collect())
        }
        _ => Err(sql_rejected(
            "allowlist",
            "query bodies must be SELECT expressions",
        )),
    }
}

fn rewrite_select(
    select: &mut Select,
    ctes: &BTreeMap<String, Vec<OutputColumn>>,
) -> std::result::Result<Vec<OutputColumn>, QueryError> {
    let mut relations = Vec::new();
    for table in &mut select.from {
        relations.push(relation_columns(&mut table.relation, ctes)?);
        for join in &mut table.joins {
            relations.push(relation_columns(&mut join.relation, ctes)?);
        }
    }

    let mut rewritten = Vec::new();
    let mut outputs = Vec::new();
    for item in std::mem::take(&mut select.projection) {
        match item {
            SelectItem::Wildcard(options) => {
                if !wildcard_options_empty(&options) {
                    return Err(sql_rejected(
                        "wildcard",
                        "wildcard modifiers are not supported",
                    ));
                }
                for relation in &relations {
                    expand_safe_relation(relation, &mut rewritten, &mut outputs);
                }
            }
            SelectItem::QualifiedWildcard(kind, options) => {
                if !wildcard_options_empty(&options) {
                    return Err(sql_rejected(
                        "wildcard",
                        "wildcard modifiers are not supported",
                    ));
                }
                let SelectItemQualifiedWildcardKind::ObjectName(name) = kind else {
                    return Err(sql_rejected(
                        "wildcard",
                        "expression wildcards are not supported",
                    ));
                };
                let qualifier = single_name(&name)?;
                let relation = relations
                    .iter()
                    .find(|relation| relation.qualifier.eq_ignore_ascii_case(&qualifier))
                    .ok_or_else(|| sql_rejected("wildcard", "wildcard qualifier is unknown"))?;
                expand_safe_relation(relation, &mut rewritten, &mut outputs);
            }
            SelectItem::ExprWithAliases { .. } => {
                return Err(sql_rejected(
                    "allowlist",
                    "multi-alias projection expressions are not supported",
                ));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let access = expression_access(&expr, &relations);
                outputs.push(OutputColumn {
                    name: alias.value.clone(),
                    access,
                });
                rewritten.push(SelectItem::ExprWithAlias { expr, alias });
            }
            SelectItem::UnnamedExpr(expr) => {
                let access = expression_access(&expr, &relations);
                outputs.push(OutputColumn {
                    name: expression_name(&expr),
                    access,
                });
                rewritten.push(SelectItem::UnnamedExpr(expr));
            }
        }
    }
    if rewritten.len() > MAX_EXPRESSIONS {
        return Err(sql_rejected(
            "allowlist",
            "query has too many projection expressions",
        ));
    }
    select.projection = rewritten;
    Ok(outputs)
}

fn relation_columns(
    factor: &mut TableFactor,
    ctes: &BTreeMap<String, Vec<OutputColumn>>,
) -> std::result::Result<RelationColumns, QueryError> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let table_name = single_name(name)?;
            let columns = QUERY_SCHEMAS
                .iter()
                .find(|schema| schema.name.eq_ignore_ascii_case(&table_name))
                .map(|schema| {
                    schema
                        .columns
                        .iter()
                        .map(|column| OutputColumn {
                            name: column.name.to_string(),
                            access: column.access,
                        })
                        .collect()
                })
                .or_else(|| ctes.get(&table_name.to_ascii_lowercase()).cloned())
                .ok_or_else(|| sql_rejected("wildcard", "relation schema is unavailable"))?;
            Ok(RelationColumns {
                qualifier: alias
                    .as_ref()
                    .map_or(table_name, |alias| alias.name.value.clone()),
                columns,
            })
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let columns = rewrite_query_with_ctes(subquery, ctes)?;
            let qualifier = alias
                .as_ref()
                .ok_or_else(|| sql_rejected("wildcard", "derived tables require an alias"))?
                .name
                .value
                .clone();
            Ok(RelationColumns { qualifier, columns })
        }
        _ => Err(sql_rejected(
            "wildcard",
            "unsupported relation in wildcard scope",
        )),
    }
}

fn expand_safe_relation(
    relation: &RelationColumns,
    projection: &mut Vec<SelectItem>,
    outputs: &mut Vec<OutputColumn>,
) {
    for column in relation
        .columns
        .iter()
        .filter(|column| column.access == AccessClass::Safe)
    {
        projection.push(SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(vec![
            Ident::new(relation.qualifier.clone()),
            Ident::new(column.name.clone()),
        ])));
        outputs.push(column.clone());
    }
}

fn wildcard_options_empty(options: &sqlparser::ast::WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_none()
        && options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_replace.is_none()
        && options.opt_rename.is_none()
        && options.opt_alias.is_none()
}

fn single_name(name: &ObjectName) -> std::result::Result<String, QueryError> {
    if name.0.len() != 1 {
        return Err(sql_rejected(
            "allowlist",
            "qualified object names are not allowed",
        ));
    }
    name.0[0]
        .as_ident()
        .map(|ident| ident.value.clone())
        .ok_or_else(|| sql_rejected("allowlist", "dynamic object names are not allowed"))
}

fn expression_name(expr: &SqlExpr) -> String {
    match expr {
        SqlExpr::Identifier(identifier) => identifier.value.clone(),
        SqlExpr::CompoundIdentifier(identifiers) => identifiers
            .last()
            .map_or_else(|| expr.to_string(), |identifier| identifier.value.clone()),
        _ => expr.to_string(),
    }
}

fn expression_access(expr: &SqlExpr, relations: &[RelationColumns]) -> AccessClass {
    struct AccessVisitor<'a> {
        relations: &'a [RelationColumns],
        access: AccessClass,
    }

    impl Visitor for AccessVisitor<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
            let found = match expr {
                SqlExpr::Identifier(identifier) => self
                    .relations
                    .iter()
                    .flat_map(|relation| &relation.columns)
                    .filter(|column| column.name.eq_ignore_ascii_case(&identifier.value))
                    .map(|column| column.access)
                    .reduce(stricter_access),
                SqlExpr::CompoundIdentifier(identifiers) if identifiers.len() == 2 => {
                    self.relations
                        .iter()
                        .find(|relation| {
                            relation
                                .qualifier
                                .eq_ignore_ascii_case(&identifiers[0].value)
                        })
                        .and_then(|relation| {
                            relation.columns.iter().find(|column| {
                                column.name.eq_ignore_ascii_case(&identifiers[1].value)
                            })
                        })
                        .map(|column| column.access)
                }
                _ => None,
            };
            if let Some(found) = found {
                self.access = stricter_access(self.access, found);
            }
            ControlFlow::Continue(())
        }
    }

    let mut visitor = AccessVisitor {
        relations,
        access: AccessClass::Safe,
    };
    let _ = expr.visit(&mut visitor);
    visitor.access
}

fn stricter_access(left: AccessClass, right: AccessClass) -> AccessClass {
    if access_rank(left) >= access_rank(right) {
        left
    } else {
        right
    }
}

const fn access_rank(access: AccessClass) -> u8 {
    match access {
        AccessClass::Safe => 0,
        AccessClass::Path => 1,
        AccessClass::Content => 2,
        AccessClass::ToolInput => 3,
        AccessClass::ToolOutput => 4,
        AccessClass::Secret => 5,
    }
}

pub(super) fn sql_rejected(stage: &'static str, reason: &'static str) -> QueryError {
    QueryError::SqlRejected { stage, reason }
}

fn collect_ctes(
    query: &Query,
    names: &mut BTreeSet<String>,
) -> std::result::Result<(), QueryError> {
    if let Some(with) = &query.with {
        if with.recursive {
            return Err(sql_rejected(
                "allowlist",
                "recursive CTEs are not supported",
            ));
        }
        if names.len().saturating_add(with.cte_tables.len()) > MAX_CTES {
            return Err(sql_rejected("complexity", "query has too many CTEs"));
        }
        for cte in &with.cte_tables {
            names.insert(cte.alias.name.value.to_ascii_lowercase());
            collect_ctes(&cte.query, names)?;
        }
    }
    Ok(())
}

struct ReadOnlyVisitor {
    ctes: BTreeSet<String>,
    query_depth: usize,
    expression_depth: usize,
    expressions: usize,
    joins: usize,
}

impl ReadOnlyVisitor {
    fn new(ctes: BTreeSet<String>) -> Self {
        Self {
            ctes,
            query_depth: 0,
            expression_depth: 0,
            expressions: 0,
            joins: 0,
        }
    }

    fn reject(reason: &'static str) -> ControlFlow<Box<QueryError>> {
        ControlFlow::Break(Box::new(sql_rejected("allowlist", reason)))
    }
}

impl Visitor for ReadOnlyVisitor {
    type Break = Box<QueryError>;

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        if matches!(statement, Statement::Query(_)) {
            ControlFlow::Continue(())
        } else {
            Self::reject("embedded write or control statements are not allowed")
        }
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        self.query_depth += 1;
        if self.query_depth > MAX_QUERY_DEPTH {
            return Self::reject("query nesting exceeds the fixed limit");
        }
        if query.with.as_ref().is_some_and(|with| with.recursive) {
            return Self::reject("recursive CTEs are not supported");
        }
        if !query.locks.is_empty()
            || query.for_clause.is_some()
            || query.settings.is_some()
            || query.format_clause.is_some()
            || !query.pipe_operators.is_empty()
        {
            return Self::reject("query control and format clauses are not allowed");
        }
        let order_by_extension = query.order_by.as_ref().is_some_and(|order_by| {
            order_by.interpolate.is_some()
                || match &order_by.kind {
                    OrderByKind::Expressions(exprs) => {
                        exprs.iter().any(|expr| expr.with_fill.is_some())
                    }
                    OrderByKind::All(_) => false,
                }
        });
        let limit_by_extension = matches!(
            &query.limit_clause,
            Some(LimitClause::LimitOffset { limit_by, .. }) if !limit_by.is_empty()
        );
        if order_by_extension || limit_by_extension {
            return Self::reject("dialect-specific ORDER BY and LIMIT clauses are not allowed");
        }
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.query_depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
        self.joins += select
            .from
            .iter()
            .map(|item| item.joins.len())
            .sum::<usize>()
            + select.from.len().saturating_sub(1);
        if self.joins > MAX_JOINS {
            return Self::reject("query has too many joins");
        }
        if select.projection.len() > MAX_EXPRESSIONS {
            return Self::reject("query has too many projection expressions");
        }
        if select.into.is_some()
            || !select.optimizer_hints.is_empty()
            || select.select_modifiers.is_some()
            || select.top.is_some()
            || select.exclude.is_some()
            || !select.lateral_views.is_empty()
            || select.prewhere.is_some()
            || !select.connect_by.is_empty()
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || select.value_table_mode.is_some()
            || !select.named_window.is_empty()
            || select.qualify.is_some()
        {
            return Self::reject(
                "dialect-specific or write-capable SELECT clauses are not allowed",
            );
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        if relation.0.len() != 1 {
            return Self::reject("catalog and schema-qualified tables are not allowed");
        }
        let Some(name) = relation.0[0].as_ident() else {
            return Self::reject("dynamic table names are not allowed");
        };
        let normalized = name.value.to_ascii_lowercase();
        if QUERY_TABLE_NAMES.contains(&normalized.as_str()) || self.ctes.contains(&normalized) {
            ControlFlow::Continue(())
        } else {
            Self::reject("only canonical AQL tables and query CTEs are allowed")
        }
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
        match factor {
            TableFactor::Table {
                args,
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
                ..
            } if args.is_none()
                && with_hints.is_empty()
                && version.is_none()
                && !with_ordinality
                && partitions.is_empty()
                && json_path.is_none()
                && sample.is_none()
                && index_hints.is_empty() =>
            {
                ControlFlow::Continue(())
            }
            TableFactor::Derived { lateral: false, .. } => ControlFlow::Continue(()),
            _ => Self::reject("table functions and special table sources are not allowed"),
        }
    }

    fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
        self.expressions += 1;
        self.expression_depth += 1;
        if self.expressions > MAX_EXPRESSIONS || self.expression_depth > MAX_QUERY_DEPTH {
            return Self::reject("query expression complexity exceeds the fixed limit");
        }
        if let SqlExpr::Function(function) = expr {
            if function.name.0.len() != 1 {
                return Self::reject("qualified functions are not allowed");
            }
            let Some(name) = function.name.0[0].as_ident() else {
                return Self::reject("dynamic functions are not allowed");
            };
            let normalized = name.value.to_ascii_lowercase();
            if !ALLOWED_FUNCTIONS.contains(&normalized.as_str()) {
                return Self::reject("function is not in the AQL allowlist");
            }
            if normalized == "redact" && !valid_redact_arguments(&function.args) {
                return Self::reject("REDACT requires a fixed supported policy");
            }
            if normalized == "mask_path" && !valid_mask_path_arguments(&function.args) {
                return Self::reject("MASK_PATH requires a fixed depth from 1 to 16");
            }
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, _expr: &SqlExpr) -> ControlFlow<Self::Break> {
        self.expression_depth -= 1;
        ControlFlow::Continue(())
    }
}

fn positional_function_args(arguments: &FunctionArguments) -> Option<Vec<&SqlExpr>> {
    let FunctionArguments::List(list) = arguments else {
        return None;
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return None;
    }
    list.args
        .iter()
        .map(|argument| match argument {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => Some(expression),
            _ => None,
        })
        .collect()
}

fn valid_redact_arguments(arguments: &FunctionArguments) -> bool {
    let Some(arguments) = positional_function_args(arguments) else {
        return false;
    };
    match arguments.as_slice() {
        [_] => true,
        [_, SqlExpr::Value(value)] => matches!(
            value.value,
            Value::SingleQuotedString(ref policy)
                if matches!(policy.as_str(), "placeholder" | "hash" | "last4")
        ),
        _ => false,
    }
}

fn valid_mask_path_arguments(arguments: &FunctionArguments) -> bool {
    let Some(arguments) = positional_function_args(arguments) else {
        return false;
    };
    match arguments.as_slice() {
        [_] => true,
        [_, SqlExpr::Value(value)] => match &value.value {
            Value::Number(depth, _) => depth
                .to_string()
                .parse::<i64>()
                .is_ok_and(|depth| (1..=16).contains(&depth)),
            _ => false,
        },
        _ => false,
    }
}
