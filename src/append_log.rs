#![deny(missing_docs)]

//! An on-disk compactable, indexed key-value log implementation.

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use failure::{Error, Fail};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// The Result type used by all functions in the AppendLog.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Fail, Debug)]
#[fail(display = "Path provided is not a file.")]
/// Error when the path passed in is not a valid log file.
pub struct InvalidLogFileError;

/// Commands that can be issued into the AppendLog.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogCommand {
    /// Set a value into the log, this will udate the index.
    Set,
    /// Remove a value from the log. This value will be immediately removed from the index and removed from the file on compaction.
    Remove,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LogEntry {
    cmd: LogCommand,
    key: Box<[u8]>,
    val: Option<Box<[u8]>>,
}

impl LogEntry {
    fn new(cmd: LogCommand, key: &[u8], val: Option<&[u8]>) -> LogEntry {
        let key = Box::from(key);
        let val = match val {
            Some(s) => Some(Box::from(s)),
            None => None,
        };

        LogEntry { cmd, key, val }
    }
}

/// An AppendOnly, indexed log.
///
/// Using LogCommand's byte-slices can be appended into the log and addressed by the key that was used to add them.
pub struct AppendLog {
    inner: RefCell<InnerAppendLog>,
}

impl AppendLog {
    /// Loads a log file from the given path.
    pub fn load(path: &Path) -> Result<AppendLog> {
        Ok(AppendLog {
            inner: RefCell::new(InnerAppendLog::load(path)?),
        })
    }

    /// Compacts the log into the new path, closing out the old one.
    /// Log entries can continue to be written to the AppendLog.
    pub fn compact(&mut self, path: &Path) -> Result<()> {
        let new_log = self.inner.get_mut().compact(path)?;
        self.inner.replace(new_log);
        Ok(())
    }

    /// Flush the logs to their storage backend.
    pub fn flush(&mut self) -> Result<()> {
        self.inner.get_mut().flush()
    }

    /// Append the given LogCommand to the log.
    pub fn append(&mut self, cmd: LogCommand, key: &[u8], val: Option<&[u8]>) -> Result<()> {
        self.inner.borrow_mut().append(cmd, key, val)
    }

    /// Returns true iff the value is currently in the index.
    /// i.e. it has been added and not removed.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.inner.borrow_mut().contains(key)
    }

    /// Fetches the value from the index.
    pub fn fetch_by_key(&self, key: &[u8]) -> Result<Option<Box<[u8]>>> {
        self.inner.borrow_mut().fetch_by_key(key)
    }

    /// Return the total length of the log - this is the total number of commands in the log.
    /// The length of the index and log should be equal only immediately after compaction.
    pub fn len(&self) -> usize {
        self.inner.borrow().len()
    }

    /// Returns true iff there are zero log entries.
    pub fn is_empty(&self) -> bool {
        self.inner.borrow().is_empty()
    }

    /// Returns the length of the index.
    pub fn index_len(&self) -> usize {
        self.inner.borrow().index_len()
    }
}

struct InnerAppendLog {
    /// The index mapping all of the active entries in the Log.
    index: HashMap<Box<[u8]>, u64>,
    /// The file descriptor that is used for reading the entries from the log file.
    log_file_read: File,
    /// The file descriptor that is used to append the log entries.
    log_file_write: File,
    /// The number of LogEntry entries in the log.
    entry_count: usize,
}

impl InnerAppendLog {
    /// Creates a new, empty log.
    // pub fn new() -> Log {
    //     Log {
    //         index: HashMap::new(),
    //     }
    // }

    /// Loads a Log from a file on disk, and builds the index.
    fn load(path: &Path) -> Result<InnerAppendLog> {
        if !path.is_file() || !path.exists() {
            return Err(Error::from(InvalidLogFileError {}));
        }

        let mut log = InnerAppendLog {
            index: HashMap::new(),
            log_file_read: OpenOptions::new()
                .read(true)
                .write(false)
                .create(false)
                .open(path)?,
            log_file_write: OpenOptions::new()
                .read(true)
                .append(true)
                .create(false)
                .open(path)?,
            entry_count: 0,
        };
        log.build_index()?;
        Ok(log)
    }

    /// Compacts the current Log to the new path specified.
    ///
    /// It is still possible to write to this log.
    fn compact(&mut self, path: &Path) -> Result<InnerAppendLog> {
        if path.exists() {
            // We don't want to clobber anything when we compact.
            return Err(Error::from(InvalidLogFileError {}));
        }

        eprintln!("Compacting into file: {:?}", path);

        // Create a new log as the compaction target.
        let write_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        let mut log = InnerAppendLog {
            index: HashMap::new(),
            log_file_read: OpenOptions::new().read(true).write(false).open(path)?,
            log_file_write: write_file,
            entry_count: 0,
        };

        for (k, _) in self.index.clone().into_iter() {
            match self.fetch_by_key(&k)? {
                Some(bytes) => {
                    log.append(LogCommand::Set, &k, Some(bytes.as_ref()))?;
                }
                None => {
                    // This "should not occur" as the index tracks what is added and removed
                    // but in the event where we get back a None from fetch_by_key then we
                    // drop it here on compact.
                }
            }
        }

        log.build_index()?;
        Ok(log)
    }

    /// Flushes any buffered LogEntries to disk.
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    /// Appends the LogEntry to the Log and updates the index as required.
    ///
    /// If the command is LogCommand::Remove then the key should be None.
    fn append(&mut self, cmd: LogCommand, key: &[u8], val: Option<&[u8]>) -> Result<()> {
        let entry = LogEntry::new(cmd.clone(), key, val);

        // Append the file to the log.
        let offset = self.log_file_write.seek(SeekFrom::Current(0))?;
        let mut w = BufWriter::new(&self.log_file_write);
        let entry_encoded = bincode::serialize(&entry)?;
        w.write_u32::<BigEndian>(entry_encoded.len() as u32)?;
        w.write_all(&entry_encoded)?;

        self.entry_count += 1;

        // Now update the index.
        match cmd {
            LogCommand::Set => {
                self.index.insert(entry.key, offset);
            }
            LogCommand::Remove => {
                self.index.remove(&entry.key);
            }
        }

        Ok(())
    }

    /// Returns true if the provided key resides in the index.
    fn contains(&self, key: &[u8]) -> bool {
        self.index.contains_key(key)
    }

    /// Returns a given LogEntry referenced by the key String, or None if it does not exist.
    fn fetch_by_key(&mut self, key: &[u8]) -> Result<Option<Box<[u8]>>> {
        let offset = match self.index.get(key) {
            Some(o) => *o,
            None => return Ok(None),
        };

        self.log_file_read.seek(SeekFrom::Start(offset))?;
        let mut reader = BufReader::new(&self.log_file_read);

        let len = reader.read_u32::<BigEndian>()?;
        let mut entry_data: Vec<u8> = vec![0u8; len as usize];
        reader.read_exact(entry_data.as_mut_slice())?;
        let entry: LogEntry = bincode::deserialize(&entry_data)?;

        Ok(entry.val)
    }

    /// The current length of the log in LogEntries.
    fn len(&self) -> usize {
        self.entry_count
    }

    /// Returns true if this is an empty log.
    fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// The number of entries in the index.
    ///
    /// This is the number of entries that are addressable from the current state of the log.
    fn index_len(&self) -> usize {
        self.index.len()
    }

    /// Constructs the index for the append log.
    ///
    /// This traverses the entire file and indexes the values that are in there.
    /// Mapping from the key of LogEntry to the offset within the file that the key refers to.
    ///
    /// This requires parsing all LogEntries to build the index, so duplicate keys may be parsed
    /// if the log has not been compacted.
    fn build_index(&mut self) -> Result<()> {
        // Seek to the start of the file for indexing.
        self.log_file_write.seek(SeekFrom::Start(0))?;

        let mut reader = BufReader::new(&self.log_file_write);
        let mut read_count = 0;
        loop {
            if read_count >= self.log_file_write.metadata()?.len() {
                break;
            }
            // This is the offset we will store for this entry.
            let entry_offset = read_count;
            let len = reader.read_u32::<BigEndian>()?;
            read_count += 4;
            let mut entry_data: Vec<u8> = vec![0u8; len as usize];

            reader.read_exact(entry_data.as_mut_slice())?;
            read_count += u64::from(len);

            // Deserialize the entry and update the index.
            let entry: LogEntry = bincode::deserialize(&entry_data)?;
            self.entry_count += 1;

            match entry.cmd {
                LogCommand::Set => {
                    self.index.insert(entry.key, entry_offset);
                }
                LogCommand::Remove => {
                    self.index.remove(&entry.key);
                }
            }
        }

        eprintln!("Index built with {} entries:", self.index.len());
        Ok(())
    }
}

impl Drop for InnerAppendLog {
    fn drop(&mut self) {
        match self.flush() {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error when dropping Log on flush(): {}", e);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use filepath::FilePath;
    use std::path::PathBuf;

    fn create_empty_temp_file() -> PathBuf {
        let f = tempfile::tempfile().unwrap();
        {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(f.path().unwrap().as_path())
                .unwrap();
        }
        f.path().unwrap()
    }

    #[test]
    fn log_load_empty_file() {
        let p = create_empty_temp_file();
        InnerAppendLog::load(p.as_path()).unwrap();
    }

    #[test]
    fn log_write_and_read() {
        let p = create_empty_temp_file();

        {
            let mut log = InnerAppendLog::load(p.as_path()).unwrap();
            log.append(LogCommand::Set, b"aaaa", Some(b"1111")).unwrap();
            log.append(LogCommand::Set, b"bbbb", Some(b"2222")).unwrap();
            log.append(LogCommand::Set, b"cccc", Some(b"3333")).unwrap();
            log.append(LogCommand::Set, b"dddd", Some(b"4444")).unwrap();

            assert_eq!(
                log.fetch_by_key(b"aaaa").unwrap().unwrap().as_ref(),
                b"1111"
            );
            assert_eq!(
                log.fetch_by_key(b"bbbb").unwrap().unwrap().as_ref(),
                b"2222"
            );
            assert_eq!(
                log.fetch_by_key(b"cccc").unwrap().unwrap().as_ref(),
                b"3333"
            );
            assert_eq!(
                log.fetch_by_key(b"dddd").unwrap().unwrap().as_ref(),
                b"4444"
            );

            log.append(LogCommand::Remove, b"aaaa", None).unwrap();
            assert_eq!(log.fetch_by_key(b"aaaa").unwrap(), None);
        }

        {
            let mut log = InnerAppendLog::load(p.as_path()).unwrap();

            assert_eq!(log.fetch_by_key(b"aaaa").unwrap(), None);
            assert_eq!(
                log.fetch_by_key(b"bbbb").unwrap().unwrap().as_ref(),
                b"2222"
            );
            assert_eq!(
                log.fetch_by_key(b"cccc").unwrap().unwrap().as_ref(),
                b"3333"
            );
            assert_eq!(
                log.fetch_by_key(b"dddd").unwrap().unwrap().as_ref(),
                b"4444"
            );
        }
    }
}
