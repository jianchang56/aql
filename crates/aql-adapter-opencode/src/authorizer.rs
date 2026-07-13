use super::*;

pub(super) struct AuthorizerPolicy {
    reads: BTreeMap<&'static str, BTreeSet<&'static str>>,
    schema_pragmas: BTreeSet<&'static str>,
}

impl AuthorizerPolicy {
    pub(super) fn schema() -> Self {
        let mut reads = BTreeMap::new();
        reads.insert("migration", BTreeSet::from(["id"]));
        reads.insert(
            "sqlite_master",
            BTreeSet::from(["type", "name", "tbl_name", "rootpage", "sql"]),
        );
        reads.insert(
            "sqlite_schema",
            BTreeSet::from(["type", "name", "tbl_name", "rootpage", "sql"]),
        );
        Self {
            reads,
            schema_pragmas: BTreeSet::from(["session", "message", "part"]),
        }
    }

    pub(super) fn table(
        table: &'static str,
        columns: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        Self {
            reads: BTreeMap::from([(table, columns.into_iter().collect())]),
            schema_pragmas: BTreeSet::new(),
        }
    }

    pub(super) fn message_parts() -> Self {
        Self {
            reads: BTreeMap::from([
                (
                    "message",
                    BTreeSet::from(["id", "session_id", "time_created", "data"]),
                ),
                (
                    "part",
                    BTreeSet::from([
                        "id",
                        "message_id",
                        "session_id",
                        "time_created",
                        "time_updated",
                        "data",
                    ]),
                ),
            ]),
            schema_pragmas: BTreeSet::new(),
        }
    }

    pub(super) fn messages_only() -> Self {
        Self::table("message", ["id", "session_id", "time_created", "data"])
    }

    pub(super) fn allows(&self, context: AuthContext<'_>) -> bool {
        if context.database_name.is_some_and(|name| name != "main") || context.accessor.is_some() {
            return false;
        }
        match context.action {
            AuthAction::Select => true,
            AuthAction::Read {
                table_name,
                column_name,
            } => self
                .reads
                .get(table_name)
                .is_some_and(|columns| columns.contains(column_name)),
            AuthAction::Pragma {
                pragma_name,
                pragma_value,
            } => {
                pragma_name.eq_ignore_ascii_case("table_info")
                    && pragma_value.is_some_and(|table| self.schema_pragmas.contains(table))
            }
            AuthAction::Transaction { .. } => true,
            AuthAction::Function { function_name } => function_name.eq_ignore_ascii_case("length"),
            _ => false,
        }
    }
}
