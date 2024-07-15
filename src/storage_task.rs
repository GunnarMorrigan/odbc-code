use tokio::runtime::Handle;
use tracing::{error, info, instrument};

use crate::{ database::Database, DataStruct};

pub struct StorageTask<'a> {
    db: Database<'a>,
    _db_url: Box<str>,
    receiver: tokio::sync::mpsc::Receiver<DataStruct>,
    tokio_handle: Handle,
}

impl StorageTask<'_> {
    pub fn new<'a>(
        db_url: Box<str>,
        tokio_handle: Handle,
        receiver: tokio::sync::mpsc::Receiver<DataStruct>,
    ) -> Result<Self, anyhow::Error> {
        let db = Database::new(db_url.as_ref())?;


        Ok(Self {
            db,
            _db_url: db_url,
            receiver,
            tokio_handle,
        })
    }

    fn inner_run(&mut self) {
        let mut i = 0u32;
        while let Some(data) = self.receiver.blocking_recv() {
            if i % 500 == 0 {
                info!(
                    "Received {i} entries, channel still contains {}",
                    self.receiver.len()
                );
                i = 0;
            }
            i += 1;

            match self.handle_single_result(data) {
                Ok(_) => {
                    // Metrics not included right now
                    // self.success_counter.add(1, &[]);
                }
                Err(err) => {
                    error!("Error handling single result: {err}");
                    // Metrics not included right now
                    // self.failed_counter.add(1, &[]);
                }
            }
        }
    }

    fn handle_single_result(
        &mut self,
        data: DataStruct,
    ) -> Result<(), anyhow::Error> {

        match self.db.store_single_entry_all_data(
            &data
        ) {
            Ok(_) => {
                info!("Stored single entry into DB");
            }
            Err(err) => {
                error!("Error storing entry: {err}");
                // As you can see auto commit is off. Before when auto commit was on. I rarely had issues with the connection.
                // At least not any that would panic.
                // The below line would crash with the provided panic.
                self.db = Database::new(self._db_url.as_ref())?;
                if let Err(err) = self.db.store_single_entry_all_data(&data) {
                    error!("Again error storing entry: {err}");
                    self.db.rollback()?;
                    return Err(err.into());
                }
            }
        }

        // Here i used to upload some data to cloud storage and then commit on success but this will also do.
        self.db.commit()?;

        Ok(())
    }
}

impl StorageTask<'static> {
    pub fn run(mut self) -> std::thread::JoinHandle<Self> {
        info!("Starting db storage task");
        std::thread::Builder::new()
            .name("db_storage_task".to_string())
            .spawn(move || {
                self.inner_run();
                self
            })
            .unwrap()
    }
}
