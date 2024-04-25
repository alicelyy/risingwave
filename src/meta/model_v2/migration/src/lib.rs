#![allow(clippy::enum_variant_names)]

pub use sea_orm_migration::prelude::*;

mod m20230908_072257_init;
mod m20231008_020431_hummock;
mod m20240304_074901_subscription;
mod m20240410_082733_with_version_column_migration;
mod m20240410_154406_session_params;
mod m20240417_062305_subscription_internal_table_name;
mod m20240418_142249_function_runtime;
mod m20240425_053608_add_rate_limit_to_source_catalog;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20230908_072257_init::Migration),
            Box::new(m20231008_020431_hummock::Migration),
            Box::new(m20240304_074901_subscription::Migration),
            Box::new(m20240410_082733_with_version_column_migration::Migration),
            Box::new(m20240410_154406_session_params::Migration),
            Box::new(m20240417_062305_subscription_internal_table_name::Migration),
            Box::new(m20240418_142249_function_runtime::Migration),
            Box::new(m20240425_053608_add_rate_limit_to_source_catalog::Migration),
        ]
macro_rules! drop_tables {
    ($manager:expr, $( $table:ident ),+) => {
        $(
            $manager
                .drop_table(
                    sea_orm_migration::prelude::Table::drop()
                        .table($table::Table)
                        .if_exists()
                        .cascade()
                        .to_owned(),
                )
                .await?;
        )+
    };
}
