use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::BufRead;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

/// Command represents an operation on the database (Set or Remove).
///
/// ### Why Store Commands Instead of Raw Data?
///
/// When working with in-memory tables (MemTable), a key design decision is whether to store
/// raw data directly or to store commands (like Set and Remove). This choice impacts how
/// smoothly and consistently we can transition data between in-memory tables and on-disk
/// storage (SSTable).
///
/// #### The Problem
///
/// Consider a scenario where a memory table reaches its capacity and needs to be persisted
/// to disk. This process can be simple if we temporarily stop all write operations. However,
/// if we want to allow read/write operations to continue uninterrupted, we face a challenge.
///
/// A common solution is to create a new memory table, say `B`, while persisting the old table,
/// `A`, to disk. At this point, `A` becomes read-only, and all new write operations are directed
/// to `B`. The challenge arises when you need to handle operations across these tables:
///
/// - **Get Request**: when reading a value, you first check `B`, then `A`, and finally the SSTable
///   on disk if necessary.
/// - **Set Request**: new data is written directly to `B`.
/// - **Remove (RM) Request**: if a key to be deleted exists only in `A`, which is now read-only,
///   you cannot directly remove it from `A`. However, by storing the RM command in `B`, the system
///   can effectively register the deletion, ensuring that even though `A` is read-only, the key is
///   considered deleted when checking `B`.
///
/// #### The Solution
///
/// By storing commands like Set and Remove in the memory table instead of just raw data, we can
/// ensure that operations are handled correctly, even when transitioning between memory tables.
/// This approach allows us to maintain consistency and support ongoing read/write operations
/// without interruption.
#[derive(Serialize, Deserialize, Debug)]
enum Command {
    /// Set a key-value pair.
    Set(String, String),
    /// Remove a key from the database.
    Remove(String),
}

/// The memory table is an in-memory, ordered key-value store.
///
/// It will be flushed to disk as an SSTable when it exceeds a certain size.
struct MemTable {
    /// The data stored in the MemTable.
    data: BTreeMap<String, Command>,
}

impl MemTable {
    /// Create a new memory table.
    fn new() -> Self {
        MemTable {
            data: BTreeMap::new(),
        }
    }

    /// Set a key-value pair in the memory table.
    fn set(&mut self, key: String, value: String) {
        self.data.insert(key.clone(), Command::Set(key, value));
    }

    /// Remove a key from the memory table.
    fn remove(&mut self, key: String) {
        self.data.insert(key.clone(), Command::Remove(key));
    }

    /// Get a command from the memory table.
    fn get(&self, key: &str) -> Option<&Command> {
        self.data.get(key)
    }

    /// Flush the memory table to disk as an SSTable.
    fn flush_to_sstable(&self, path: &str) -> io::Result<()> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        for command in self.data.values() {
            let serialized = serde_json::to_string(command)?;
            writeln!(writer, "{}", serialized)?;
        }
        Ok(())
    }
}

/// `SSTable` represents a simple, immutable, on-disk key-value store.
///
/// This structure is primarily used for reading data that has been flushed from
/// the in-memory `MemTable` to disk. It helps in retrieving the most recent
/// state of a key, whether it was set or removed.
///
/// ### Why SSTable?
///
/// When the `MemTable` (the in-memory store) reaches its size limit, its contents
/// are flushed to disk in the form of an SSTable. SSTables are immutable, meaning
/// once they are written, they don't change. This immutability ensures consistency
/// and allows for efficient on-disk storage and retrieval.
///
/// The SSTable stores commands (`Set` or `Remove`) rather than raw data values.
/// As long as we have the latest command for a key, we can determine the current
/// state of that key—whether it holds a value or has been removed.
struct SSTable {
    /// Internal storage for the SSTable, using a BTreeMap to keep keys sorted.
    data: BTreeMap<String, Command>,
}

impl SSTable {
    /// Creates a new SSTable by loading data from a file.
    ///
    /// This function reads the file line by line, deserializing each line
    /// into a `Command` and then storing it in the BTreeMap.
    fn from_file(path: &str) -> io::Result<Self> {
        // Open the file at the given path.
        let file = File::open(path)?;

        // Wrap the file in a buffered reader for efficient reading.
        let reader = BufReader::new(file);

        // Initialize an empty BTreeMap to store the commands.
        let mut data = BTreeMap::new();

        // Read the file line by line.
        for line in reader.lines() {
            // Handle any potential errors from reading a line.
            let line = line?;

            // Deserialize the JSON string into a Command.
            let command: Command = serde_json::from_str(&line)?;

            // Insert the command into the BTreeMap.
            // The key is extracted depending on the type of command.
            match &command {
                // For `Set` commands, the key is stored along with the command.
                Command::Set(key, _) | Command::Remove(key) => {
                    data.insert(key.clone(), command);
                }
            }
        }

        // Return the populated SSTable.
        Ok(SSTable { data })
    }

    /// Retrieves a command from the SSTable for a given key.
    fn get(&self, key: &str) -> Option<&Command> {
        // Look up the key in the BTreeMap and return the associated command if found.
        self.data.get(key)
    }
}

/// `LSMDatabase` is a key-value store that manages in-memory and on-disk storage using
/// the Log-Structured Merge-Tree (LSM-Tree) model.
///
/// ### Components:
/// - **MemTable**: the in-memory table where all new write operations (set and remove)
///   are initially stored. It serves as the first layer of data storage.
/// - **SSTables**: a series of immutable, on-disk tables where the data from the MemTable
///   is persisted once the MemTable reaches a certain size. These SSTables are used for
///   reading data when it is no longer in the MemTable.
/// - **memtable_threshold**: this defines the maximum size the MemTable can reach before
///   it is flushed to disk and converted into a new SSTable. This threshold helps in
///   managing memory usage and ensuring efficient persistence.
/// - **next_sstable_id**: a counter used to assign unique IDs to each new SSTable as it
///   is created. This helps in organizing and identifying SSTables over time.
///
/// ### How it Works:
/// The LSM-Tree model is designed to efficiently handle both write and read operations.
/// - **Writes**: new entries are first written to the MemTable. When the MemTable reaches
///   its threshold size, it is flushed to disk as an SSTable, ensuring that the in-memory
///   data is persisted and the memory is freed up for new data.
/// - **Reads**: data retrieval happens by first checking the MemTable (the most recent
///   data), then moving through the SSTables in reverse chronological order until the
///   desired key is found or all sources have been exhausted.
///
/// This structure ensures that writes are fast and that reads are efficient even as the
/// database grows.
struct LSMDatabase {
    /// The in-memory table where all new writes are stored.
    memtable: MemTable,
    /// A vector of SSTables representing the on-disk storage.
    sstables: Vec<SSTable>,
    /// The size threshold that triggers the flushing of the memory table to disk.
    memtable_threshold: usize,
    /// Counter for assigning unique IDs to new SSTables.
    next_sstable_id: u32,
}

impl LSMDatabase {
    /// Creates a new instance of `LSMDatabase`.
    ///
    /// This function initializes a new LSM-based database with an empty `MemTable`,
    /// an empty list of `SSTables`, and sets the threshold for the `MemTable` size
    /// before it gets flushed to disk.
    fn new(memtable_threshold: usize) -> Self {
        LSMDatabase {
            memtable: MemTable::new(), // Initialize an empty MemTable.
            sstables: Vec::new(),      // Start with an empty list of SSTables.
            memtable_threshold,        // Set the flush threshold for the MemTable.
            next_sstable_id: 0,        // Start the SSTable ID counter at 0.
        }
    }

    /// Sets a key-value pair in the database.
    ///
    /// This function inserts or updates the specified key-value pair in the `MemTable`.
    /// If the `MemTable` exceeds its size threshold, it will automatically be flushed
    /// to disk, and a new `SSTable` will be created.
    fn set(&mut self, key: String, value: String) -> io::Result<()> {
        // Insert or update the key-value pair in the MemTable.
        self.memtable.set(key, value);

        // Check if the MemTable has exceeded its size threshold.
        if self.memtable.data.len() > self.memtable_threshold {
            // If it has, flush the MemTable to disk as a new SSTable.
            self.flush_memtable()?;
        }
        Ok(())
    }

    /// Removes a key from the database.
    ///
    /// This function marks a key for deletion by storing a `Remove` command in the `MemTable`.
    /// If the `MemTable` exceeds its size threshold, it will automatically be flushed to disk,
    /// and a new `SSTable` will be created.
    fn remove(&mut self, key: String) -> io::Result<()> {
        // Mark the key as removed in the MemTable.
        self.memtable.remove(key);

        // Check if the MemTable has exceeded its size threshold.
        if self.memtable.data.len() > self.memtable_threshold {
            // If it has, flush the MemTable to disk as a new SSTable.
            self.flush_memtable()?;
        }
        Ok(())
    }

    /// Retrieves a value associated with a key from the database.
    ///
    /// This function first checks the `MemTable` for the key. If the key is not found
    /// in the `MemTable`, it then checks each `SSTable` in reverse order (from the
    /// most recent to the oldest) to find the key. If found, the function executes the
    /// associated command to return the current value or deletion status.
    fn get(&self, key: &str) -> Option<String> {
        // First, check the MemTable for the key.
        if let Some(command) = self.memtable.get(key) {
            // If found, execute the command to determine the value or removal.
            return self.execute_command(command);
        }

        // If not found in the MemTable, check each SSTable in reverse order.
        for sstable in &self.sstables {
            if let Some(command) = sstable.get(key) {
                // If found, execute the command to determine the value or removal.
                return self.execute_command(command);
            }
        }

        // Return None if the key is not found in either MemTable or SSTables.
        None
    }

    /// Executes a command to determine the current value or deletion status of a key.
    ///
    /// This function interprets a `Command` to either return the associated value (for a `Set`
    /// command) or `None` (for a `Remove` command).
    fn execute_command(&self, command: &Command) -> Option<String> {
        match command {
            Command::Set(_, value) => Some(value.clone()), // Return the value if it's a Set command.
            Command::Remove(_) => None,                    // Return None if it's a Remove command.
        }
    }

    /// Flushes the current `MemTable` to disk as a new `SSTable`.
    ///
    /// This function writes the contents of the `MemTable` to a new log file on disk,
    /// creates a corresponding `SSTable` from the file, and then clears the `MemTable`.
    ///
    /// # Returns
    ///
    /// An `io::Result<()>` indicating the success or failure of the operation.
    fn flush_memtable(&mut self) -> io::Result<()> {
        // Generate a unique file name for the new SSTable.
        let sstable_path = format!("sstable_{}.log", self.next_sstable_id);
        // Increment the SSTable ID counter for the next SSTable.
        self.next_sstable_id += 1;

        // Write the MemTable data to the new SSTable log file.
        self.memtable.flush_to_sstable(&sstable_path)?;

        // Load the new SSTable from the file.
        let sstable = SSTable::from_file(&sstable_path)?;
        // Add the new SSTable to the list of SSTables.
        self.sstables.push(sstable);

        // Clear the MemTable to free up memory for new operations.
        self.memtable = MemTable::new();

        Ok(())
    }
}

fn main() -> io::Result<()> {
    // Create a new LSM database instance with a MemTable threshold of 3 entries.
    //
    // This means that when the MemTable holds more than 3 entries, it will automatically
    // be flushed to disk as a new SSTable.
    let mut db = LSMDatabase::new(3);

    // Insert the key-value pair ("key1", "value1") into the database.
    //
    // This operation will store the pair in the MemTable.
    db.set("key1".to_string(), "value1".to_string())?;

    // Insert the key-value pair ("key2", "value2") into the database.
    //
    // The MemTable now contains two entries.
    db.set("key2".to_string(), "value2".to_string())?;

    // Insert the key-value pair ("key3", "value3") into the database.
    //
    // The MemTable now contains three entries, reaching the threshold.
    db.set("key3".to_string(), "value3".to_string())?;

    // Insert the key-value pair ("key4", "value4") into the database.
    //
    // This causes the MemTable to exceed its threshold of 3 entries.
    // As a result, the MemTable is flushed to disk as a new SSTable,
    // and the MemTable is reset to store this new entry.
    db.set("key4".to_string(), "value4".to_string())?;

    // Retrieve the value associated with "key1" from the database.
    //
    // Since "key1" was part of the first three entries, it has been flushed to disk.
    // The system will search the SSTables and return the value if found.
    if let Some(value) = db.get("key1") {
        println!("Found key1: {}", value);
    } else {
        println!("Key1 not found");
    }

    // Retrieve the value associated with "key3" from the database.
    //
    // Similar to "key1", "key3" was flushed to disk, so it will be retrieved from an SSTable.
    if let Some(value) = db.get("key3") {
        println!("Found key3: {}", value);
    } else {
        println!("Key3 not found");
    }

    // Retrieve the value associated with "key4" from the database.
    //
    // "Key4" was added after the flush, so it is still in the current MemTable.
    // The system will return the value from the MemTable.
    if let Some(value) = db.get("key4") {
        println!("Found key4: {}", value);
    } else {
        println!("Key4 not found");
    }

    // Return an `Ok` result, indicating the program executed successfully.
    Ok(())
}
