#include "heap_storage.h"
#include <cstring>

/**
 * @class SlottedPage
 * 
 * Implements a block in a database using the slotted page structure.
 */

/**
 * Constructs a new SlottedPage
 * @param block the base block
 * @param block_id the block's id
 * @param is_new whether or not the block is new
 */
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        num_records = 0;
        end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(num_records, end_free);
    }
}

/**
 * Add a new block
 * @param data data to be added
 * @return ID of the new block
 * @throws DbBlockNoRoomError if not enough room
 */
RecordID SlottedPage::add(const Dbt *data) {
    // Check if there's enough room to add data
    if (has_room(data->get_size())) {
        num_records++;
        RecordID id = num_records;
        u_int16_t size = data->get_size();
        end_free -= size;
        u_int16_t loc = end_free + 1;
        put_header();
        put_header(id, size, loc);
        memcpy(address(loc), data->get_data(), size);
        return id;
    }
    // Throw error if not enough room
    else {
        throw DbBlockNoRoomError("Not enough room to add new record");
    }
}

/**
 * Gets record from block based on ID
 * @param record_id record's ID
 * @return record, or nullptr if there's nothing there
 */
Dbt *SlottedPage::get(RecordID record_id) {
    u_int16_t size, loc;
    get_header(size, loc, record_id);
    if (loc == 0)
        return nullptr;
    return new Dbt(address(loc), size);
}

/**
 * Update record with new data
 * @param record_id record's ID
 * @param data new data for record
 * @throws DbBlockNoRoomError if not enough room
 */
void SlottedPage::put(RecordID record_id, const Dbt &data) {
    u_int16_t old_size, loc;
    get_header(old_size, loc, record_id);
    u_int16_t new_size = (u_int16_t)data.get_size();
    if (new_size > old_size) {
        u_int16_t diff = new_size - old_size;
        if (has_room(diff)) {
            slide(loc, loc - diff);
            memcpy(address(loc - diff), data.get_data(), new_size);
        }
        else {
            throw DbBlockNoRoomError("Not enough room for new record");
        }
    }
    else {
        memcpy(address(loc), data.get_data(), new_size);
        slide(loc + new_size, loc + old_size);
    }
    get_header(old_size, loc, record_id);
    put_header(record_id, new_size, loc);
}

/**
 * Deletes a record
 * @param record_id record's ID
 */
void SlottedPage::del(RecordID record_id) {
    u_int16_t size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);
    slide(loc, loc + size);
}

/**
 * All IDs with data
 * @return vector of RecordId
 */
RecordIDs *SlottedPage::ids(void) {
    RecordIDs *all = new RecordIDs();
    u_int16_t size, loc;
    for (int i = 1; i < num_records; i++) {
        get_header(size, loc, i);
        if (loc != 0)
            all->push_back(i);
    }
    return all;
}

/**
 * Get size and location based on record ID
 * @param size size of data
 * @param loc location of data
 * @param id record's ID
 */
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
    size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}

/**
 * Add size and location based on record ID
 * @param id record's ID
 * @param size size of data
 * @param loc location of data
 */
void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc) {
    if (id == 0) {
        size = num_records;
        loc = end_free;
    }
    else {
        put_n(4 * id, size);
        put_n(4 * id + 2, loc);
    }
}

/**
 * Check how much space is in the block
 * @param size size of data
 * @return true if there's room, false if no room
 */
bool SlottedPage::has_room(u_int16_t size) {
    u_int16_t space = end_free - 4 * (num_records + 1);
    if (space >= size)
        return true;
    return false;
}

/**
 * Slide records to adjust for smaller and larger records
 * @param start location to begin slide
 * @param end location to end slide
 */
void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    int move = end - start;
    if (move == 0)
        return;
    
    void *new_loc = address(end_free + 1 + move);
    void *old_loc = address(end_free + 1);
    int data_size = start - (end_free + 1);
    memmove(new_loc, old_loc, data_size);

    RecordIDs *ids = SlottedPage::ids();
    for (auto const &id : *ids) {
        u_int16_t size, loc;
        get_header(size, loc, id);
        if (loc <= start) {
            loc += move;
            put_header(id, size, loc);
        }
    }
    delete ids;
    end_free += move;
    put_header();
}

/**
 * Get integer at given offset
 * @param offset 
 * @return integer
 */
u_int16_t SlottedPage::get_n(u_int16_t offset) {
    return *(u_int16_t *)address(offset);
}

/**
 * Put given integer at given offset
 * @param offset
 * @param n
 */
void SlottedPage::put_n(u_int16_t offset, u_int16_t n) {
    *(u_int16_t *)address(offset) = n;
}

/**
 * Create a pointer for the offset
 * @param offset
 */
void *SlottedPage::address(u_int16_t offset) {
    return (void *) ((char *) this->block.get_data() + offset);
}

// fields: name, dbfilename, last, closed, Db db

// get: get a block from the database file (via the buffer manager, 
// presumably) for a given block id. The client code can then read 
// or modify the block via the DbBlock interface.

// get_new: create a new empty block and add it to the database 
// file. Returns the new block to be modified by the client via 
// the DbBlock interface.

// put: write a block to the file. Presumably the client has 
// made modifications in the block that he would like to save. 
// Typically, it's up to the buffer manager exactly when the 
// block is actually written out to disk.

// block_ids: iterate through all the block ids in the file.


/**
 * @class HeapFile
 *
 * Implements a database file using a heap structure
 */


/** 
 * Creates a database file
 */
void HeapFile::create(void) {
    std::cout << std::endl << std::endl << "In create" << std::endl;

    // open and use DB_CREATE to create the database. DB_EXCL throws an error if the database already exists
    // returns 0 if it opened successfully, nonzero otherwise
    int errorCode = db.open(NULL, (const char*)(&dbfilename), NULL, DB_RECNO, DB_CREATE | DB_EXCL| DB_TRUNCATE, 0644);

    if(errorCode != 0)
        std::cout << "File creation failed!" << std::endl;
    else
        std::cout << std::endl << "Created" << std::endl;
}

/**
 * Drops a database file
 */
void HeapFile::drop(void) {
    std::cout << std::endl << std::endl << "In drop" << std::endl;

    if(!closed){
        std::cout << std::endl << "The file to be dropped is open" << std::endl;
        close();
    }
    
    db.remove((const char*)(&dbfilename), NULL, 0); // remove the file
    std::cout << std::endl << "Dropped" << std::endl;
}

/**
 * Opens a database file
 */
void HeapFile::open(void) {
    std::cout << std::endl << std::endl << "In open" << std::endl;

    if(closed){
        std::cout << std::endl << "The file to be opened is closed" << std::endl;
        db.open(NULL, (const char *)(&dbfilename), NULL, DB_RECNO, 0, 0644);
        closed = false;
    }

    std::cout << std::endl << "opened" << std::endl;
}

/**
 * Closes a database file
 */
void HeapFile::close(void) {
    std::cout << std::endl << std::endl << "In close" << std::endl;

    if(!closed){
        std::cout << std::endl << std::endl << "File to be closed is open" << std::endl;
        db.close(0);
        closed = true;
    }
    std::cout << std::endl << std::endl << "Closed" << std::endl;
}

/**
 * Create a new empty block and add it to the database
 * @return the new block to be modified by the client via the DbBlock
 * interface
 *
 * This method was copied from Prof. Guardia
 */
SlottedPage *HeapFile::get_new(void) {
    // Dbt emptyDbt = Dbt(); // empty Dbt for the key in the key/data pair
    // Dbt emptyDbtRef = &emptyDbt;
    // SlottedPage* newBlock = new SlottedPage(&emptyDbt, 0, true); // empty SlottedPage block
    // Dbt* newData = new Dbt(newBlock, sizeof(newBlock)); // Dbt to hold the new block
    // db.put(NULL, emptyDbt, newData, 0);
    // return newBlock;

    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

/**
 * Gets the block based on the ID
 * @param block_id the ID of the block to get
 * @return a new SlottedPage with the data
 */
SlottedPage *HeapFile::get(BlockID block_id) {
    BlockID& blockIdByReference = block_id; // reference version of block_id to pass to SlottedPage constructor
    Dbt* key = new Dbt(&block_id, sizeof(block_id)); // the key is the block ID, wrap it in a Dbt
    Dbt data = Dbt(); // the Dbt to hold the data, BerkeleyDB will fill it with data
    db.get(0, key, &data, 0);
    return new SlottedPage(data, block_id, false); // use the data and block id to fill a SlottedPage
}

/** 
  * Write a block to the file
  * @param block the block to be written
  */
void HeapFile::put(DbBlock *block) {
    BlockID id = block->get_block_id();
    Dbt* key = new Dbt(&id, sizeof(id)); // key is block id; wrap it in a Dbt
    Dbt* dataToWrite = block->get_block(); // get the Dbt that holds the block for the DbBlock
    db.put(nullptr, key, dataToWrite, 0);
}

/**
 * Get all block IDs
 * @return a vector of block IDs
 */
BlockIDs *HeapFile::block_ids() {
    BlockIDs* blockIds = new BlockIDs();
    Dbt key;
    Dbt data;
    // SlottedPage block = SlottedPage(Dbt(), 0, true); // this will hold all the blocks

    // Do the blockIds start from 0 or 1? Refresh on pre-increment
    for(int blockId=1; blockId <= last; blockId++){
        // Do I need the following 3 lines?
        key = Dbt(&blockId, sizeof(blockId));
        data = Dbt();
        db.get(0, &key, &data, 0);
        // block = SlottedPage(data, blockId); // make a SlottedPage from the Dbt containing the block of data
        blockIds->push_back(blockId); // add the SlottedPage to the list of all the BlockIDs
    }
    return blockIds;
}

/* Attributes of HeapTable:
        HeapFile file;
   Attributes of DBRelation, which HeapTable inherits from
        Identifier table_name;
        ColumnNames column_names;
        ColumnAttributes column_attributes;
   Attributes/methods of ColumnAttribute, which holds datatype and other info for a column
        DataType data_type;
        virtual DataType get_data_type() { return data_type; }
        virtual void set_data_type(DataType data_type) { this->data_type = data_type; }
*/

// ALL METHODS FOR HEAPTABLE:
// constructors: takes the name of the relation, the columns (in order), and all the column attributes (e.g., it's data type, any constraints, is it allowed to be null, etc.) It's not the job of DbRelation to track all this information. That's done by the schema storage.
// create: corresponds to the SQL command CREATE TABLE. At minimum, it presumably sets up the DbFile and calls its create method.
// create_if_not_exists: corresponds to the SQL command CREATE TABLE IF NOT EXISTS. Whereas create will throw an exception if the table already exists, this method will just open the table if it already exists.
// open: opens the table for insert, update, delete, select, and project methods
// close: closes the table, temporarily disabling insert, update, delete, select, and project methods.
// drop: corresponds to the SQL command DROP TABLE. Deletes the underlying DbFile.
// insert: corresponds to the SQL command INSERT INTO TABLE. Takes a proposed row and adds it to the table. 
//         This is the method that determines the block to write it to and marshals the data and writes it to the block. 
//         It is also responsible for handling any constraints, applying defaults, etc.
// update: corresponds to the SQL command UPDATE. Like insert, but only applies specific field changes, keeping other fields as they were before. Same logic as insert for constraints, defaults, etc. The client needs to first obtain a handle to the row that is meant to be updated either from insert or from select.
// delete: corresponds to the SQL command DELETE FROM. Deletes a row for a given row handle (obtained from insert or select).
// select: corresponds to the SQL query SELECT * FROM...WHERE. Returns handles to the matching rows.
// project: extracts specific fields from a row handle (a projection).

void HeapFile::db_open(uint flags) {

}

/**
 * @class HeapTable
 * Implements a table in the database
 */

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : 
            DbRelation(table_name, column_names, column_attributes), file(table_name) {

}

/**
 * Creates a new table, equivalent to SQL CREATE TABLE
 */
void HeapTable::create() {
    // create a DbFile with the filename
    file.create(); // this will throw an exception if the file already exists
}

/**
 * Create a new table, if it doesn't exist, equivalent to SQL CREATE TABLE IF
 * NOT EXISTS
 */
void HeapTable::create_if_not_exists() {
    Db db(_DB_ENV, 0); // create a DB to create the file

    // create a file with the same name as the table, but don't throw an exception
    db.open(NULL, (const char*)(&table_name), NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644);
}

/**
 * Drop a table, equivalent to SQL DROP TABLE
 */
void HeapTable::drop() {
    file.drop();
}

/**
 * Open the table for insert, update, delete, select methods
 */
void HeapTable::open() {
    file.open();
}

/**
 * Close the table, disables insert, update, delete, select methods
 */
void HeapTable::close() {
    file.close();
}

/**
 * Insert into the table, equivalent to SQL INSERT INTO TABLE
 * @param row the row of data to be inserted
 * @return a Handle to where the row was inserted
 */
Handle HeapTable::insert(const ValueDict *row) {
    open();
    ValueDict *new_row = validate(row);
    Handle handle = append(new_row);
    delete new_row;
    return handle;
}

// not yet implemented
void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    throw DbRelationError("Update not implemented");
}

// also not yet implemented
void HeapTable::del(const Handle handle) {
    throw DbRelationError("Delete not implemented");
}

/** 
 * Select data from table, equivalent to SQL SELECT * FROM
 * @return Handles to the matching rows
 */
Handles *HeapTable::select() {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

// not yet implemented
Handles *HeapTable::select(const ValueDict *where) {
    throw DbRelationError("Select where not implemented");
}

/**
 * Extracts specific fields from a row Handle
 * @param handle the handle of the row
 * @return a ValueDict of the row's data
 */
ValueDict *HeapTable::project(Handle handle) {
   return project(handle, &column_names);
}

/**
 * Extracts specific fields from a row Handle and column names
 * @param handle the handle of the row
 * @param column_names the names of the columns to project
 * @return a ValueDict of the row's data
 */
ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {
    throw DbRelationError("Project not implemented");
}

/**
 * Checks if given row as valid column types
 * @param row
 * @return new row ValueDict
 * @throws DbRelationError if invalid data
 */
ValueDict *HeapTable::validate(const ValueDict *row) {
    ValueDict *new_row = new ValueDict();
    for (const auto& name : column_names) {
        ValueDict::const_iterator entry = row->find(name);
        if (entry == row->end())
            throw DbRelationError("Incorrect data type");
        Value new_value = entry->second;
        (*new_row)[name] = new_value;
    } 
    return new_row;
}

/**
 * Appends a row to a table
 * @param row the row to be appended
 * @return a Handle to the new row
 */
Handle HeapTable::append(const ValueDict *row) {
    Dbt* new_row = marshal(row);
    RecordID id;
    SlottedPage *block = file.get(file.get_last_block_id());
    try {
        id = block->add(new_row);
    }
    catch (DbBlockNoRoomError &e){
        block = file.get_new();
        id = block->add(new_row);
    }
    return Handle(file.get_last_block_id(), id);
}

/**
 * Marshal the column names in a given row
 * @param row the row to be marshaled
 * @return a Dbt of the marshaled data
 */
Dbt *HeapTable::marshal(const ValueDict *row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u_int16_t size = value.s.length();
            *(u_int16_t*) (bytes + offset) = size;
            offset += sizeof(u_int16_t);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
    return new Dbt();
}

/**
 * Unmarshal column names from given Dbt
 * @param data the Dbt to unmarshal
 * @return a ValueDict of the unmarshaled data
 */
ValueDict *HeapTable::unmarshal(Dbt *data) {
    ValueDict *dict = new ValueDict();
    Value value;
    char *bytes = (char *) data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column : column_names) {
        ColumnAttribute ca = column_attributes[col_num++];
        value.data_type = ca.get_data_type();
        if (value.data_type == ColumnAttribute::DataType::INT) {
            value.n = *(int32_t *) (bytes + offset);
            offset += sizeof(int32_t);
        }
        else if (value.data_type == ColumnAttribute::DataType::TEXT) {
            uint size = *(int32_t *) (bytes + offset);
            offset += sizeof(uint);
            char buffer[DbBlock::BLOCK_SZ];
            memcpy(buffer, bytes + offset, size);
            buffer[size] = '\0';
            value.s = std::string(buffer); 
            offset += size;
        }
        else {
            throw DbRelationError("Data type not supported");
        }
        (*dict)[column] = value;
    }

    return dict;
}

// bool test_heap_storage(){};
    // test function -- returns true if all tests pass



bool test_heap_storage() {
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    //table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    //std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    //ValueDict *result = table.project((*handles)[0]);
    //std::cout << "project ok" << std::endl;
    //Value value = (*result)["a"];
    //if (value.n != 12)
    //	return false;
    //value = (*result)["b"];
    //if (value.s != "Hello!")
	//	return false;
    table.drop();

    return true;
}