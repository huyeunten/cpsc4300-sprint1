#include "heap_storage.h"
#include <cstring>

/*
    Slotted page documentation to add later
*/

/**
 * Constructs a new SlottedPage
 * @param block
 * @param block_id 
 * @param is_new
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
 * @param data
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
 * @param record_id
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
 * @param record_id
 * @param data
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
 * @param record_id
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
 * Get size and location for record id
 * @param size
 * @param loc
 * @param id
 */
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
    size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}

/**
 * Add size and location for record id
 * @param id
 * @param size
 * @param loc
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
 * @param size
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
 * @param start
 * @param end
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

void HeapFile::drop(void) {
    std::cout << std::endl << std::endl << "In drop" << std::endl;

    if(!closed){
        std::cout << std::endl << "The file to be dropped is open" << std::endl;
        close();
    }
    
    db.remove((const char*)(&dbfilename), NULL, 0); // remove the file
    std::cout << std::endl << "Dropped" << std::endl;
}

void HeapFile::open(void) {
    std::cout << std::endl << std::endl << "In open" << std::endl;

    if(closed){
        std::cout << std::endl << "The file to be opened is closed" << std::endl;
        db.open(NULL, (const char *)(&dbfilename), NULL, DB_RECNO, 0, 0644);
        closed = false;
    }

    std::cout << std::endl << "opened" << std::endl;
}

void HeapFile::close(void) {
    std::cout << std::endl << std::endl << "In close" << std::endl;

    if(!closed){
        std::cout << std::endl << std::endl << "File to be closed is open" << std::endl;
        db.close(0);
        closed = true;
    }
    std::cout << std::endl << std::endl << "Closed" << std::endl;
}

// get_new: create a new empty block and add it to the database 
// file. Returns the new block to be modified by the client via 
// the DbBlock interface.
// This method was copied from Prof. Guardia
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

SlottedPage *HeapFile::get(BlockID block_id) {
    BlockID& blockIdByReference = block_id; // reference version of block_id to pass to SlottedPage constructor
    Dbt* key = new Dbt(&block_id, sizeof(block_id)); // the key is the block ID, wrap it in a Dbt
    Dbt data = Dbt(); // the Dbt to hold the data, BerkeleyDB will fill it with data
    db.get(0, key, &data, 0);
    return new SlottedPage(data, block_id, false); // use the data and block id to fill a SlottedPage
}

// put: write a block to the file. Presumably the client has 
// made modifications in the block that he would like to save. 
// Typically, it's up to the buffer manager exactly when the 
// block is actually written out to disk.
void HeapFile::put(DbBlock *block) {
    BlockID id = block->get_block_id();
    Dbt* key = new Dbt(&id, sizeof(id)); // key is block id; wrap it in a Dbt
    Dbt* dataToWrite = block->get_block(); // get the Dbt that holds the block for the DbBlock
    db.put(nullptr, key, dataToWrite, 0);
}

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

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : 
            DbRelation(table_name, column_names, column_attributes), file(table_name) {

}

void HeapTable::create() {
    // create a DbFile with the filename
    file.create(); // this will throw an exception if the file already exists
}

void HeapTable::create_if_not_exists() {
    Db db(_DB_ENV, 0); // create a DB to create the file

    // create a file with the same name as the table, but don't throw an exception
    db.open(NULL, (const char*)(&table_name), NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644);
}

void HeapTable::drop() {
    file.drop();
}

void HeapTable::open() {
    file.open();
}

void HeapTable::close() {
    file.close();
}

// Handle HeapTable::insert(const ValueDict *row) {
//     return new Handle();
// }

// void HeapTable::update(const Handle handle, const ValueDict *new_values) {

// }

// void HeapTable::del(const Handle handle) {

// }

// Handles *HeapTable::select() {
//     return new Handle();
// }

// Handles *HeapTable::select(const ValueDict *where) {
//     return new Handle();
// }

// ValueDict *HeapTable::project(Handle handle) {
//     return new ValueDict();
// }

// ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {
//     return new ValueDict();
// }

// ValueDict *HeapTable::validate(const ValueDict *row) {
//     return new ValueDict();
// }

// Handle HeapTable::append(const ValueDict *row) {
//     return new Handle();
// }

// Dbt *HeapTable::marshal(const ValueDict *row) {
//     return new Dbt();
// }

// ValueDict *HeapTable::unmarshal(Dbt *data) {
//     return new ValueDict();
// }

// bool test_heap_storage(){};
    // test function -- returns true if all tests pass



// bool test_heap_storage() {
// 	ColumnNames column_names;
// 	column_names.push_back("a");
// 	column_names.push_back("b");
// 	ColumnAttributes column_attributes;
// 	ColumnAttribute ca(ColumnAttribute::INT);
// 	column_attributes.push_back(ca);
// 	ca.set_data_type(ColumnAttribute::TEXT);
// 	column_attributes.push_back(ca);
//     HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
//     table1.create();
//     std::cout << "create ok" << std::endl;
//     table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
//     std::cout << "drop ok" << std::endl;

//     HeapTable table("_test_data_cpp", column_names, column_attributes);
//     table.create_if_not_exists();
//     std::cout << "create_if_not_exsts ok" << std::endl;

//     ValueDict row;
//     row["a"] = Value(12);
//     row["b"] = Value("Hello!");
//     std::cout << "try insert" << std::endl;
//     table.insert(&row);
//     std::cout << "insert ok" << std::endl;
//     Handles* handles = table.select();
//     std::cout << "select ok " << handles->size() << std::endl;
//     ValueDict *result = table.project((*handles)[0]);
//     std::cout << "project ok" << std::endl;
//     Value value = (*result)["a"];
//     if (value.n != 12)
//     	return false;
//     value = (*result)["b"];
//     if (value.s != "Hello!")
// 		return false;
//     table.drop();

//     return true;
// }