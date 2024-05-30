#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "dberror.h"
#include "expr.h"
#include "tables.h"
#include "record_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"

SM_FileHandle fh;                 // file handle
SM_PageHandle ph;                 // pointer to memory storing the page data
int totalNumPages;                // start from 1
int currentPageNum = 0;           // start from 0
int slots = 99999;                // 100 records per page
int recordSize = PAGE_SIZE / 100; // 4 bytes for each record
int numSlotsPerPage;              // 100 slots per page (100 records per page)

// table and manager
typedef struct RM_TableInfo
{
    RM_TableData *rel;
    int numTuples;
    Record *records;
    int *slotsBitMap;
    int numSlotsPerPage;
    int totalNumPages;
    int currentPageNum;
} RM_TableInfo;

// handling records in a table
RM_TableInfo *tables[10];
int tableIndex = -1;
int currentActiveTableIndex = -1;
RM_TableData *currentActiveTable = NULL;
int TABLE_INFO_PAGE_NUM = 0;

// Define the file name and the no table ref
char *filename = "database.bin";
char *NO_TABLE = "Deleted Table";

RC initRecordManager(void *mgmtData)
{
    // Initialize the table index
    tableIndex = 0;
    // Initialize the current active table index
    currentActiveTableIndex = tableIndex;

    // Initialize the tables array
    for (int i = 0; i < 10; i++)
    {
        tables[i] = NULL; // Set all the table pointers to NULL initially
    }

    // Initialize the storage manager
    initStorageManager();
    // Initialize the file handle
    createPageFile(filename);
    openPageFile(filename, &fh);
    // Initialize the page handle
    ph = (SM_PageHandle)malloc(PAGE_SIZE);
    // Initialize the total number of pages
    totalNumPages = fh.totalNumPages;
    // Initialize the current page number
    currentPageNum = 0;
    // Initialize the number of slots per page
    numSlotsPerPage = PAGE_SIZE / recordSize;

    // Initialize the buffer manager
    initBufferPool(&fh, filename, 3, RS_FIFO, NULL);
    // Pin the first page
    pinPage(&fh, &ph, TABLE_INFO_PAGE_NUM);

    // Return OK status code if initialization is successful
    return RC_OK;
}

RC shutdownRecordManager()
{
    int i; // Loop counter
    // Reset the table info
    for (i = 0; i < 10; i++)
    {
        // Free the memory allocated for the table info
        if (tables[i] != NULL)
        {
            free(tables[i]->rel);         // Free the memory allocated for the table info
            free(tables[i]->records);     // Free the memory allocated for the records
            free(tables[i]->slotsBitMap); // Free the memory allocated for the slots bit map
            free(tables[i]);              // Free the memory allocated for the table info
        }
    }

    shutdownBufferPool(&fh);

    // Return OK status code if shutdown is successful
    return RC_OK;
}

RC createTable(char *name, Schema *schema)
{
    // Pin the first page
    pinPage(&fh, &ph, TABLE_INFO_PAGE_NUM);

    // Check if the table already exists
    int i;
    for (i = 0; i < 10; i++)
    {
        if (tables[i] != NULL && strcmp(tables[i]->rel->name, name) == 0)
        {
            // If the table already exists, return an error code
            return RC_TABLE_ALREADY_EXISTS;
        }
    }

    // Create the table
    RM_TableData *rel = (RM_TableData *)malloc(sizeof(RM_TableData));
    rel->name = name;
    rel->schema = schema;
    rel->mgmtData = NULL;

    // Create the table info
    tables[currentActiveTableIndex] = (RM_TableInfo *)malloc(sizeof(RM_TableInfo));
    tables[currentActiveTableIndex]->rel = rel;
    tables[currentActiveTableIndex]->numTuples = 0;
    tables[currentActiveTableIndex]->records = (Record *)malloc(sizeof(Record) * slots);
    tables[currentActiveTableIndex]->slotsBitMap = (int *)malloc(sizeof(int) * slots);
    // Initialize the slots bit map
    for (int i = 0; i < slots; i++)
    {
        tables[currentActiveTableIndex]->slotsBitMap[i] = 0;
    }
    // Initialize the table info
    tables[currentActiveTableIndex]->numSlotsPerPage = slots;
    tables[currentActiveTableIndex]->totalNumPages = 1;
    tables[currentActiveTableIndex]->currentPageNum = 0;

    tableIndex++; // Increment the table index

    // Write the table info to the first page
    // memcpy(ph, serializeTableInfo(rel), sizeof(RM_TableData));
    markDirty(&fh, &ph);
    unpinPage(&fh, &ph);

    // Return OK status code if table creation is successful
    return RC_OK;
}

RC openTable(RM_TableData *rel, char *name)
{
    // Pin the first page
    pinPage(&fh, &ph, TABLE_INFO_PAGE_NUM);

    // Check if the table exists
    int i;
    for (i = 0; i < 10; i++)
    {
        // If the table exists, set the current active table to the table
        if (tables[i] != NULL && strcmp(tables[i]->rel->name, name) == 0)
        {
            // Set the current active table
            currentActiveTable = tables[i]->rel;
            currentActiveTableIndex = i;
        }
    }

    // Return an error code if the table does not exist
    if (currentActiveTable == NULL)
    {
        return RC_TABLE_NOT_FOUND;
    }

    // Set the table info
    rel->name = currentActiveTable->name;
    rel->schema = currentActiveTable->schema;
    rel->mgmtData = currentActiveTable->mgmtData;

    return RC_OK;
}

RC closeTable(RM_TableData *rel)
{
    currentActiveTable = NULL;
    unpinPage(&fh, &ph);
    return RC_OK;
}

RC deleteTable(char *name)
{
    // Pin the first page
    pinPage(&fh, &ph, TABLE_INFO_PAGE_NUM);

    // Reset the table info
    int i;
    for (i = 0; i < 10; i++)
    {
        if (tables[i] != NULL && strcmp(tables[i]->rel->name, name) == 0)
        {
            tables[i]->rel->name = NO_TABLE;
            tables[i]->rel->schema = NULL;
            tables[i]->rel->mgmtData = NULL;
            tables[i]->numTuples = 0;
            tables[i]->records = NULL;
            tables[i]->slotsBitMap = NULL;
            tables[i]->numSlotsPerPage = 0;
            tables[i]->totalNumPages = 0;
            tables[i]->currentPageNum = 0;

            tables[i] = NULL;
        }
    }

    // Write the table info to the first page
    // memcpy(ph, serializeTableInfo(rel), sizeof(RM_TableData));
    markDirty(&fh, &ph);
    unpinPage(&fh, &ph);

    // Return OK status code if table deletion is successful
    return RC_OK;
}

int getNumTuples(RM_TableData *rel)
{
    // Data fom the first page is already in memory, for faster access
    return tables[currentActiveTableIndex]->numTuples;
}

// handling records in a table
RC insertRecord(RM_TableData *rel, Record *record)
{
    // If there is no current active table, return an error code
    if (currentActiveTable == NULL)
    {
        return RC_TABLE_NOT_FOUND;
    }

    // If the current page is full, create a new page
    if (tables[currentActiveTableIndex]->numTuples == slots)
    {
        pinPage(&fh, &ph, tables[currentActiveTableIndex]->totalNumPages);
        tables[currentActiveTableIndex]->totalNumPages++;
        tables[currentActiveTableIndex]->currentPageNum++;
        tables[currentActiveTableIndex]->numTuples = 0;
        unpinPage(&fh, &ph);

        // return RC_RM_NO_MORE_TUPLES;
    }

    // Insert the record into the table
    int i;
    for (i = 0; i < slots; i++)
    {
        // Check if the slot is empty
        if (tables[currentActiveTableIndex]->slotsBitMap[i] == 0)
        {
            tables[currentActiveTableIndex]->slotsBitMap[i] = 1;               // Set the slot to occupied
            record->id.page = tables[currentActiveTableIndex]->currentPageNum; // Set the page number
            record->id.slot = i;                                               // Set the slot number
            tables[currentActiveTableIndex]->records[i] = *record;             // Insert the record into the table
            tables[currentActiveTableIndex]->numTuples++;                      // Increment the number of tuples in the table
            return RC_OK;                                                      // Return OK status code if insertion is successful
        }
    }

    // Return an error code if there is no empty slot
    return RC_RM_NO_MORE_TUPLES;
}

RC deleteRecord(RM_TableData *rel, RID id)
{
    // Pin the page containing the record
    pinPage(&fh, &ph, id.page);

    // Check if the table exists
    if (currentActiveTable == NULL)
    {
        unpinPage(&fh, &ph);
        return RC_TABLE_NOT_FOUND;
    }

    // Check if the slot is empty
    if (tables[currentActiveTableIndex]->slotsBitMap[id.slot] == 0)
    {
        unpinPage(&fh, &ph);
        return RC_RM_NO_MORE_TUPLES;
    }

    // Delete the record from the table
    tables[currentActiveTableIndex]->slotsBitMap[id.slot] = 0;
    tables[currentActiveTableIndex]->numTuples--;

    // Write the table info to the first page
    // memcpy(ph, serializeTableInfo(rel), sizeof(RM_TableData));
    markDirty(&fh, &ph);
    unpinPage(&fh, &ph);

    // Return OK status code if deletion is successful
    return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record)
{
    // Pin the page containing the record
    pinPage(&fh, &ph, record->id.page);

    // Check if the table exists
    if (currentActiveTable == NULL)
    {
        unpinPage(&fh, &ph);
        return RC_TABLE_NOT_FOUND;
    }

    // Check if the slot is empty
    if (tables[currentActiveTableIndex]->slotsBitMap[record->id.slot] == 0)
    {
        unpinPage(&fh, &ph);
        return RC_RM_NO_MORE_TUPLES;
    }

    // Update the record in the table
    tables[currentActiveTableIndex]->records[record->id.slot] = *record;

    // Write the table info to the first page
    // memcpy(ph, serializeTableInfo(rel), sizeof(RM_TableData));
    markDirty(&fh, &ph);
    unpinPage(&fh, &ph);

    // Return OK status code if update is successful
    return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    // Pin the page containing the record
    pinPage(&fh, &ph, id.page);

    // Check if the table exists
    if (currentActiveTable == NULL)
    {
        unpinPage(&fh, &ph);
        return RC_TABLE_NOT_FOUND;
    }

    // Check if the slot is empty
    if (tables[currentActiveTableIndex]->slotsBitMap[id.slot] == 0)
    {
        unpinPage(&fh, &ph);
        return RC_RM_NO_MORE_TUPLES;
    }

    // Get the record from the table
    *record = tables[currentActiveTableIndex]->records[id.slot];

    // Write the table info to the first page
    unpinPage(&fh, &ph);

    // Return OK status code if retrieval is successful
    return RC_OK;
}

// scans
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    // Pin the first page
    pinPage(&fh, &ph, TABLE_INFO_PAGE_NUM);

    // Check if the table exists
    if (currentActiveTable == NULL)
    {
        return RC_TABLE_NOT_FOUND;
    }

    // Initialize the scan handle
    scan->rel = rel;
    scan->mgmtData = cond; // Store the scan condition for later use
    scan->scanCounter = 0; // Reset the scan index to start from the beginning of the table

    // Return OK status code if scan is successful
    return RC_OK;
}

RC next(RM_ScanHandle *scan, Record *record)
{
    // Check if the table exists
    if (currentActiveTable == NULL || scan->mgmtData == NULL)
    {
        return RC_TABLE_NOT_FOUND;
    }

    // Get the scan condition
    Expr *cond = (Expr *)scan->mgmtData;
    RM_TableData *rel = scan->rel;
    Schema *schema = rel->schema;
    Operator *op = NULL;
    Record *records = tables[currentActiveTableIndex]->records;

    // Scan the table for the next tuple
    while (scan->scanCounter < slots)
    {
        // Check if there is a condition to evaluate
        if (cond != NULL && tables[currentActiveTableIndex]->slotsBitMap[scan->scanCounter] == 1)
        {
            // Evaluate the expression to determine if the current tuple satisfies the condition
            Value *result = NULL;
            // Evaluate the expression to determine if the current tuple satisfies the condition
            evalExpr(&records[scan->scanCounter], schema, cond, &result);

            // Check if the current tuple satisfies the condition
            if (result->v.boolV == FALSE)
            {
                freeVal(result);     // Free the memory allocated for the result value
                scan->scanCounter++; // Increment the scan index
                continue;            // Continue to the next tuple
            }
            freeVal(result); // Free the memory allocated for the result value

            // Retrieve the tuple and set it in the `record` parameter
            getRecord(rel, (RID){.page = records[scan->scanCounter].id.page, .slot = records[scan->scanCounter].id.slot}, record);
            // Increment the scan index
            scan->scanCounter++;

            // Return OK status code if the tuple satisfies the condition
            return RC_OK;
        }

        //
        scan->scanCounter++;
    }

    // No more tuples to return
    return RC_RM_NO_MORE_TUPLES;
}

RC closeScan(RM_ScanHandle *scan)
{
    // Clear any resources or cleanup here if needed
    scan->rel = NULL;
    scan->mgmtData = NULL;
    scan->scanCounter = 0;

    return RC_OK;
}

// dealing with schemas
int getRecordSize(Schema *schema)
{
    // Calculate the record size
    int size = 0;
    int i;
    for (i = 0; i < schema->numAttr; i++)
    {
        if (schema->dataTypes[i] == DT_INT)
        {
            size += sizeof(int);
        }
        else if (schema->dataTypes[i] == DT_FLOAT)
        {
            size += sizeof(float);
        }
        else if (schema->dataTypes[i] == DT_BOOL)
        {
            size += sizeof(bool);
        }
        else if (schema->dataTypes[i] == DT_STRING)
        {
            size += schema->typeLength[i];
        }
    }

    return size;
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    // Allocate memory for the schema
    Schema *schema = (Schema *)malloc(sizeof(Schema)); // Allocate memory for the schema
    schema->numAttr = numAttr;                         // Set the number of attributes
    schema->attrNames = attrNames;                     // Set the attribute names
    schema->dataTypes = dataTypes;                     // Set the attribute data types
    schema->typeLength = typeLength;                   // Set the attribute type lengths
    schema->keyAttrs = keys;                           // Set the key attributes
    schema->keySize = keySize;                         // Set the key size

    // Return the schema
    return schema;
}

RC freeSchema(Schema *schema)
{
    // Free the memory allocated for the schema
    free(schema);
    // Return OK status code if schema deallocation is successful
    return RC_OK;
}

// dealing with records and attribute values
RC createRecord(Record **record, Schema *schema)
{
    // Allocate memory for the record
    *record = (Record *)malloc(sizeof(Record));
    // Allocate memory for the record's data field
    (*record)->data = (char *)malloc(getRecordSize(schema));
    // Set the page number to current page number
    (*record)->id.page = currentPageNum;
    // Return OK status code if record creation is successful
    return RC_OK;
}

RC freeRecord(Record *record)
{
    // Free the memory allocated for the record
    free(record->data);
    free(record);
    // Return OK status code if record deallocation is successful
    return RC_OK;
}

int getAttributeOffset(Schema *schema, int attrNum)
{
    // Calculate the attribute offset
    int offset = 0;
    // Loop through the attributes
    for (int i = 0; i < attrNum; i++)
    {
        switch (schema->dataTypes[i])
        {
        case DT_INT:
            offset += sizeof(int);
            break;
        case DT_STRING:
            offset += schema->typeLength[i];
            break;
        case DT_FLOAT:
            offset += sizeof(float);
            break;
        case DT_BOOL:
            offset += sizeof(bool);
            break;
        }
    }
    // Return the attribute offset
    return offset;
}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    // Check if the attribute number is valid
    if (attrNum < 0 || attrNum >= schema->numAttr)
    {
        return NULL;
    }

    // Allocate memory for the attribute value
    *value = (Value *)malloc(sizeof(Value));

    // Get the attribute offset in the record
    int offset = getAttributeOffset(schema, attrNum);

    // Copy the attribute value from the record's data field to the new value
    switch (schema->dataTypes[attrNum])
    {
    case DT_INT:
        (*value)->dt = DT_INT;
        memcpy(&((*value)->v.intV), record->data + offset, sizeof(int));
        break;
    case DT_STRING:
        (*value)->dt = DT_STRING;
        (*value)->v.stringV = (char *)malloc(schema->typeLength[attrNum] + 1);
        memcpy((*value)->v.stringV, record->data + offset, schema->typeLength[attrNum]);
        (*value)->v.stringV[schema->typeLength[attrNum]] = '\0'; // Null-terminate the string
        break;
    case DT_FLOAT:
        (*value)->dt = DT_FLOAT;
        memcpy(&((*value)->v.floatV), record->data + offset, sizeof(float));
        break;
    case DT_BOOL:
        (*value)->dt = DT_BOOL;
        memcpy(&((*value)->v.boolV), record->data + offset, sizeof(bool));
        break;
    }

    // Return OK status code if attribute retrieval is successful
    return RC_OK;
}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    // Check if the attribute number is valid
    if (attrNum < 0 || attrNum >= schema->numAttr)
    {
        return RC_ERROR;
    }

    // Check if the value's data type matches the attribute's data type
    if (schema->dataTypes[attrNum] != value->dt)
    {
        return RC_ERROR;
    }

    // Get the attribute offset in the record
    int offset = getAttributeOffset(schema, attrNum);

    // Copy the attribute value from the value to the record's data field
    switch (value->dt)
    {
    case DT_INT:
        memcpy(record->data + offset, &(value->v.intV), sizeof(int));
        break;
    case DT_STRING:
        memcpy(record->data + offset, value->v.stringV, schema->typeLength[attrNum]);
        break;
    case DT_FLOAT:
        memcpy(record->data + offset, &(value->v.floatV), sizeof(float));
        break;
    case DT_BOOL:
        memcpy(record->data + offset, &(value->v.boolV), sizeof(bool));
        break;
    }

    // Return OK status code if attribute setting is successful
    return RC_OK;
}
