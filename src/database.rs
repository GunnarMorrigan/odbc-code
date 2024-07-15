use std::fmt::Debug;

use anyhow::{anyhow, bail};
use lazy_static::lazy_static;
use odbc_api::{
    buffers::{AnyBuffer, ColumnarBuffer, Item, NullableSlice, TextColumnView},
    sys::{AttrConnectionPooling, AttrCpMatch},
    Connection, ConnectionOptions, Environment, IntoParameter,
};
use tracing::{debug, error};

use crate::DataStruct;

lazy_static! {
    pub static ref ENV: Environment = unsafe {
        Environment::set_connection_pooling(AttrConnectionPooling::DriverAware).unwrap();
        let mut env = Environment::new().unwrap();
        env.set_connection_pooling_matching(AttrCpMatch::Strict)
            .unwrap();
        env
    };
}


pub struct Database<'a> {
    conn: Connection<'a>,
    _db_url: String,
}

impl Database<'_> {
    pub fn new(db_url: &str) -> Result<Self, anyhow::Error> {
        let conn = ENV.connect_with_connection_string(
            db_url,
            ConnectionOptions {
                login_timeout_sec: Some(2),
                packet_size: None,
            },
        );
        match conn {
            Ok(conn) => {
                conn.set_autocommit(false)?;
                Ok(Database {
                    conn,
                    _db_url: db_url.to_string(),
                })
            }
            Err(err) => {
                error!("Could not establish connection to DB. Error: {}", err);
                bail!("Could not establish connection to DB. Error: {}", err);
            }
        }
    }

    pub fn store_single_entry_all_data<'a>(
        &self,
        data: &DataStruct,
    ) -> Result<(), odbc_api::Error> {
        self.store_sync_pulse(&data)?;
        // self.store_another_thing(&signal_monitor_id, &date_time, &thing)?;
        // self.and_store_another_thing_(&signal_monitor_id, &date_time, &another_thing)?;

        Ok(())
    }

    pub fn commit(&self) -> Result<(), odbc_api::Error> {
        self.conn.commit()
    }

    pub fn rollback(&self) -> Result<(), odbc_api::Error> {
        self.conn.rollback()
    }
}

impl Database<'_> {
    pub fn store_sync_pulse(
        &self,
        data: &DataStruct,
    ) -> Result<(), odbc_api::Error> {
        let res = self.conn.execute(
            "INSERT INTO [dbo].[Table] (
                [Id]
                ,[Field0]
                ,[Field1]
            ) VALUES (?, ?, ?)",
            (
                &data.id.into_parameter(),
                &data.field0.into_parameter(),
                &data.field1.into_parameter(),
            ),
        );

        match res {
            Err(err) => Err(err),
            Ok(Some(_)) => {
                debug!("Inserted signal monitor config into the DB");
                Ok(())
            }
            Ok(None) => {
                // warn!("Not sure why the DB can return an Ok(None). Will this happen often?");
                Ok(())
            }
        }
    }
}

impl Debug for Database<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish()
    }
}

#[allow(unused)]
fn column_error_message(name: &str) -> anyhow::Error {
    error!("{} is missing an entry", name);
    anyhow!("{} is missing an entry", name)
}

// #[allow(unused)]
// fn sql_timestamp_to_offset_date_time(time: &Timestamp) -> Result<OffsetDateTime, anyhow::Error> {
//     let month = time::Month::try_from(time.month as u8)?;
//     let date = Date::from_calendar_date(time.year as i32, month, time.day as u8)?;
//     let time = time::Time::from_hms(time.hour as u8, time.minute as u8, time.second as u8)?;
//     Ok(date.with_time(time).assume_utc())
// }

#[allow(unused)]
fn required_column<'a, T: Item>(
    columns: &'a ColumnarBuffer<AnyBuffer>,
    column_index: usize,
    name: &str,
) -> anyhow::Result<&'a [T]> {
    columns
        .column(column_index)
        .as_slice::<T>()
        .ok_or(anyhow::anyhow!("Need {} column", name))
}

#[allow(unused)]
fn text_column<'a>(
    columns: &'a ColumnarBuffer<AnyBuffer>,
    column_index: usize,
    name: &str,
) -> anyhow::Result<TextColumnView<'a, u8>> {
    columns
        .column(column_index)
        .as_text_view()
        .ok_or(anyhow::anyhow!("Need {} column", name))
}

#[allow(unused)]
fn required_nullable_column<'a, T: Item>(
    columns: &'a ColumnarBuffer<AnyBuffer>,
    column_index: usize,
    name: &str,
) -> anyhow::Result<NullableSlice<'a, T>> {
    columns
        .column(column_index)
        .as_nullable_slice::<T>()
        .ok_or(anyhow::anyhow!("Need {} column", name))
}

// fn time_to_string(time: &OffsetDateTime) -> String {
//     let time = time.to_offset(offset!(UTC));
//     format!(
//         "{}-{}-{} {}:{}:{}",
//         time.year(),
//         time.month() as u16,
//         time.day(),
//         time.hour(),
//         time.minute(),
//         time.second()
//     )
// }