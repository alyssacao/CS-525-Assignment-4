#include "dberror.h"
#include "expr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

// This is custom data structure defined for making the use of Record Manager.
typedef struct RecordManager
{
	// Buffer Manager's PageHandle for using Buffer Manager to access Page files
	BM_PageHandle pageHandle;	// Buffer Manager PageHandle 
	// Buffer Manager's Buffer Pool for using Buffer Manager	
	BM_BufferPool bufferPool;
	// Record ID	
	RID recordID;
	// This variable defines the condition for scanning the records in the table
	Expr *condition;
	// This variable stores the total number of tuples in the table
	int tuplesCount;
	// This variable stores the location of first free page which has empty slots in table
	int freePage;
	// This variable stores the count of the number of records scanned
	int scanCount;
} RecordManager;

const int MAX_NUMBER_OF_PAGES = 100;
const int ATTRIBUTE_SIZE = 15; // Size of the name of the attribute
RecordManager *recordManager;

// Initializing global variables

SM_FileHandle fHandle;
int numRecordsPerPage;
BM_BufferPool *BM;
Schema *SCHEMA = NULL;

// Structure for managing scanning operations
typedef struct RMScan
{
    int count;      
    RID recordID;   
    Expr *cond;     
} RMScan;

RMScan scanManagementData;

// Structure for managing a table in the database
typedef struct RMTableManagement
{
    BM_BufferPool *bm;      
    int recordLastPageRead; 
    int numRecords;       
    int maxnumSlot;       
    int slot_len;       
} RMTableManagement;

RMTableManagement rmTableMgmt;

// Pointer to page metadata (if used)
void *pointerToPageMetaData;

int getNumTuples(RM_TableData *rel) {
    // Cast the management data to RMTableManagement type and access the numRecords field
    RMTableManagement *mgmtdata = (RMTableManagement *)rel->mgmtData;
    return mgmtdata->numRecords;
}

RC insertRecord(RM_TableData *rel, Record *record)
{
    RMTableManagement *mgmtdata = rel->mgmtData;
    BM_PageHandle *page = (BM_PageHandle*)malloc(sizeof(BM_PageHandle));
    char *rec_str = (char*)malloc(mgmtdata->slot_len);
    int page_num = 1; // Assuming pages are numbered starting from 1
    int slot = 0;
    bool slotFound = false;

    // Iterate through pages until a free slot is found
    while (!slotFound) {
        pinPage(mgmtdata->bm, page, page_num);
        char *data = page->data;

        // Check each slot in the page for availability
        for (slot = 0; slot < mgmtdata->maxnumSlot; ++slot) {
            if (data[slot * mgmtdata->slot_len] == '\0') { // Assuming '\0' indicates a free slot
                slotFound = true;
                break;
            }
        }

        unpinPage(mgmtdata->bm, page);

        // If no free slot is found in the current page, move to the next page
        if (!slotFound) {
            ++page_num;
        }
    }

    // Assign page number and slot number to record ID
    record->id.page = page_num;
    record->id.slot = slot;

    // Copy content of record data to a string variable
    memcpy(rec_str, record->data, mgmtdata->slot_len);

    // Pin the page for modification
    pinPage(mgmtdata->bm, page, page_num);

    // Copy string data to the page
    memcpy(page->data + (slot * mgmtdata->slot_len), rec_str, mgmtdata->slot_len);

    // Mark the page as dirty and unpin it
    markDirty(mgmtdata->bm, page);
    unpinPage(mgmtdata->bm, page);
    forcePage(mgmtdata->bm, page);

    // Increment the count of records
    mgmtdata->numRecords++;
    rel->mgmtData = mgmtdata;

    // Clean up allocated memory
    free(rec_str);
    free(page);
    return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id)
{
    RMTableManagement *mgmtdata = rel->mgmtData;
    BM_PageHandle pageHandle;
    
    // Pin the page for modification
    pinPage(mgmtdata->bm, &pageHandle, id.page);

    // Calculate the offset for the record in the page
    int offset = id.slot * mgmtdata->slot_len;

    // Set the bytes to '-' in the record's slot to indicate deletion
    memset(pageHandle.data + offset, '-', mgmtdata->slot_len);

    // Mark the page as dirty and unpin it
    markDirty(mgmtdata->bm, &pageHandle);
    unpinPage(mgmtdata->bm, &pageHandle);
    forcePage(mgmtdata->bm, &pageHandle);

    // Decrement the record count by 1
    mgmtdata->numRecords--;

    // Update the table management data
    rel->mgmtData = mgmtdata;

    return RC_OK;
}

// Define a separate function for marking dirty, unpinning, and forcing the page
void markDirtyUnpinAndForce(BM_BufferPool *bm, BM_PageHandle *page) 
{
    markDirty(bm, page);
    unpinPage(bm, page);
    forcePage(bm, page);
}

RC updateRecord(RM_TableData *rel, Record *record) {
    // Use the global RMTableManagement structure
    RMTableManagement *mgmtdata = &rmTableMgmt;
    BM_PageHandle pageHandle;

    // Directly use record ID to access page and slot
    int page_num = record->id.page;
    int slot = record->id.slot;

    // Pin the page where the record is located for modification
    pinPage(mgmtdata->bm, &pageHandle, page_num);

    // Directly copy record data to the correct slot in the page
    memcpy(pageHandle.data + (slot * mgmtdata->slot_len), record->data, mgmtdata->slot_len);

    // Mark the page as dirty to indicate it has been modified
    markDirty(mgmtdata->bm, &pageHandle);

    // Unpin the page after modifications are done
    unpinPage(mgmtdata->bm, &pageHandle);

    // Force the page to write back to disk ensuring data persistence
    forcePage(mgmtdata->bm, &pageHandle);

    return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record) {
    RMTableManagement *mgmtdata = rel->mgmtData;
    BM_PageHandle pageHandle;

    // Pin the page containing the record
    pinPage(mgmtdata->bm, &pageHandle, id.page);

    // Calculate the offset for the record in the page
    int offset = id.slot * mgmtdata->slot_len;

    // Copy the record data from the page to the record's data field
    record->data = (char *)malloc(mgmtdata->slot_len);
    memcpy(record->data, pageHandle.data + offset, mgmtdata->slot_len);

    // Unpin the page after copying the data
    unpinPage(mgmtdata->bm, &pageHandle);

    // Check if the record is valid (not deleted or empty)
    int recordNum = (id.page - 1) * mgmtdata->maxnumSlot + id.slot + 1;
    if (recordNum > mgmtdata->numRecords || record->data[0] == '\0') {
        free(record->data);
        record->data = NULL;
        return (recordNum > mgmtdata->numRecords) ? RC_ERROR : RC_RM_RECORD_DELETED;
    }

    // Assign the record ID to the record
    record->id = id;

    return RC_OK;
}

// Alyssa's part
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	// Checking if scan condition (test expression) is present
	if (cond == NULL)
	{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}

	// Open the table in memory
	openTable(rel, "ScanTable");

    RecordManager *scanManager;
	RecordManager *tableManager;

	// Allocating some memory to the scanManager
    	scanManager = (RecordManager*) malloc(sizeof(RecordManager));
    	
	// Setting the scan's meta data to our meta data
    	scan->mgmtData = scanManager;
    	
	// 1 to start scan from the first page
    	scanManager->recordID.page = 1;
    	
	// 0 to start scan from the first slot	
	scanManager->recordID.slot = 0;
	
	// 0 because this just initializing the scan. No records have been scanned yet    	
	scanManager->scanCount = 0;

	// Setting the scan condition
    	scanManager->condition = cond;
    	
	// Setting the our meta data to the table's meta data
    	tableManager = rel->mgmtData;

	// Setting the tuple count
    	tableManager->tuplesCount = ATTRIBUTE_SIZE;

	// Setting the scan's table i.e. the table which has to be scanned using the specified condition
    	scan->rel= rel;

	return RC_OK;
}
extern RC next (RM_ScanHandle *scan, Record *record)
{
	// Initiliazing scan data
	RecordManager *scanManager = scan->mgmtData;
	RecordManager *tableManager = scan->rel->mgmtData;
    	Schema *schema = scan->rel->schema;
	
	// Checking if scan condition (test expression) is present
	if (scanManager->condition == NULL)
	{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}

	Value *result = (Value *) malloc(sizeof(Value));
   
	char *data;
   	
	// Getting record size of the schema
	int recordSize = getRecordSize(schema);

	// Calculating Total number of slots
	int totalSlots = PAGE_SIZE / recordSize;

	// Getting Scan Count
	int scanCount = scanManager->scanCount;

	// Getting tuples count of the table
	int tuplesCount = tableManager->tuplesCount;

	// Checking if the table contains tuples. If the tables doesn't have tuple, then return respective message code
	if (tuplesCount == 0)
		return RC_RM_NO_MORE_TUPLES;

	// Iterate through the tuples
	while(scanCount <= tuplesCount)
	{  
		// If all the tuples have been scanned, execute this block
		if (scanCount <= 0)
		{
			// printf("INSIDE If scanCount <= 0 \n");
			// Set PAGE and SLOT to first position
			scanManager->recordID.page = 1;
			scanManager->recordID.slot = 0;
		}
		else
		{
			// printf("INSIDE Else scanCount <= 0 \n");
			scanManager->recordID.slot++;

			// If all the slots have been scanned execute this block
			if(scanManager->recordID.slot >= totalSlots)
			{
				scanManager->recordID.slot = 0;
				scanManager->recordID.page++;
			}
		}

		// Pinning the page i.e. putting the page in buffer pool
		pinPage(&tableManager->bufferPool, &scanManager->pageHandle, scanManager->recordID.page);
			
		// Retrieving the data of the page			
		data = scanManager->pageHandle.data;

		// Calulate the data location from record's slot and record size
		data = data + (scanManager->recordID.slot * recordSize);
		
		// Set the record's slot and page to scan manager's slot and page
		record->id.page = scanManager->recordID.page;
		record->id.slot = scanManager->recordID.slot;

		// Intialize the record data's first location
		char *dataPointer = record->data;

		// '-' is used for Tombstone mechanism.
		*dataPointer = '-';
		
		memcpy(++dataPointer, data + 1, recordSize - 1);

		// Increment scan count because we have scanned one record
		scanManager->scanCount++;
		scanCount++;

		// Test the record for the specified condition (test expression)
		evalExpr(record, schema, scanManager->condition, &result); 

		// v.boolV is TRUE if the record satisfies the condition
		if(result->v.boolV == TRUE)
		{
			// Unpin the page i.e. remove it from the buffer pool.
			unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);
			// Return SUCCESS			
			return RC_OK;
		}
	}
	
	// Unpin the page i.e. remove it from the buffer pool.
	unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);
	
	// Reset the Scan Manager's values
	scanManager->recordID.page = 1;
	scanManager->recordID.slot = 0;
	scanManager->scanCount = 0;
	
	// None of the tuple satisfy the condition and there are no more tuples to scan
	return RC_RM_NO_MORE_TUPLES;
}

// This function closes the scan operation.
extern RC closeScan (RM_ScanHandle *scan)
{
	RecordManager *scanManager = scan->mgmtData;
	RecordManager *recordManager = scan->rel->mgmtData;

	// Check if scan was incomplete
	if(scanManager->scanCount > 0)
	{
		// Unpin the page i.e. remove it from the buffer pool.
		unpinPage(&recordManager->bufferPool, &scanManager->pageHandle);
		
		// Reset the Scan Manager's values
		scanManager->scanCount = 0;
		scanManager->recordID.page = 1;
		scanManager->recordID.slot = 0;
	}
	
	// De-allocate all the memory space allocated to the scans's meta data (our custom structure)
    	scan->mgmtData = NULL;
    	free(scan->mgmtData);  
	
	return RC_OK;
}

// This function returns the record size of the schema referenced by "schema"
extern int getRecordSize (Schema *schema)
{
	int size = 0, i; // offset set to zero
	
	// Iterating through all the attributes in the schema
	for(i = 0; i < schema->numAttr; i++)
	{
		switch(schema->dataTypes[i])
		{
			// Switch depending on DATA TYPE of the ATTRIBUTE
			case DT_STRING:
				// If attribute is STRING then size = typeLength (Defined Length of STRING)
				size = size + schema->typeLength[i];
				break;
			case DT_INT:
				// If attribute is INTEGER, then add size of INT
				size = size + sizeof(int);
				break;
			case DT_FLOAT:
				// If attribite is FLOAT, then add size of FLOAT
				size = size + sizeof(float);
				break;
			case DT_BOOL:
				// If attribite is BOOLEAN, then add size of BOOLEAN
				size = size + sizeof(bool);
				break;
		}
	}
	return ++size;
}

// This function creates a new schema
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	// Allocate memory space to schema
	Schema *schema = (Schema *) malloc(sizeof(Schema));
	// Set the Number of Attributes in the new schema	
	schema->numAttr = numAttr;
	// Set the Attribute Names in the new schema
	schema->attrNames = attrNames;
	// Set the Data Type of the Attributes in the new schema
	schema->dataTypes = dataTypes;
	// Set the Type Length of the Attributes i.e. STRING size  in the new schema
	schema->typeLength = typeLength;
	// Set the Key Size  in the new schema
	schema->keySize = keySize;
	// Set the Key Attributes  in the new schema
	schema->keyAttrs = keys;

	return schema; 
}

// This function removes a schema from memory and de-allocates all the memory space allocated to the schema.
extern RC freeSchema (Schema *schema)
{
	// De-allocating memory space occupied by 'schema'
	free(schema);
	return RC_OK;
}

extern RC initRecordManager(void *mgmtData) {
    SCHEMA = (Schema *)calloc(1, sizeof(Schema));
    return SCHEMA ? RC_OK : RC_MEM_ALLOC_ERROR;
}

extern RC shutdownRecordManager() {
    free(SCHEMA);
    SCHEMA = NULL;
    return RC_OK;
}

extern RC createTable(char *name, Schema *schema) {
    SM_FileHandle fh;
    if (createPageFile(name) != RC_OK)
        return RC_WRITE_FAILED;
    if (openPageFile(name, &fh) != RC_OK)
        return RC_FILE_NOT_FOUND;

    RMTableManagement *rm = (RMTableManagement *)malloc(sizeof(RMTableManagement));
    rm->bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
    initBufferPool(rm->bm, name, 100, RS_FIFO, NULL);
    rm->numRecords = 0;
    rm->recordLastPageRead = 1;
    rm->maxnumSlot = PAGE_SIZE / getRecordSize(schema);
    rm->slot_len = getRecordSize(schema);
    memcpy(SCHEMA, schema, sizeof(Schema));

    char *pageData = serializeSchema(schema);
    ensureCapacity(1, &fh);
    writeBlock(0, &fh, pageData);
    closePageFile(&fh);
    free(pageData);

    rmTableMgmt = *rm;
    free(rm);
    return RC_OK;
}

extern RC openTable(RM_TableData *rel, char *name) {
    if (access(name, F_OK) != 0)
        return RC_FILE_NOT_FOUND;
    rel->name = name;
    rel->schema = SCHEMA;
    rel->mgmtData = malloc(sizeof(SM_FileHandle));
    return RC_OK;
}

extern RC closeTable(RM_TableData *rel) {
    if (access(rel->name, F_OK) != 0)
        return RC_FILE_NOT_FOUND;
    free(rel->mgmtData);
    rel->mgmtData = NULL;
    return RC_OK;
}

extern RC deleteTable(char *name) {
    if (access(name, F_OK) != 0)
        return RC_FILE_NOT_FOUND;
    destroyPageFile(name);
    return RC_OK;
}

RC createRecord(Record **record, Schema *schema) {
    // Validate the schema
    if (schema == NULL) {
        RC_message = "Schema is NULL.";
        return RC_SCHEMA_NOT_FOUND;
    }

    // Allocate memory for the Record structure and initialize its data pointer to NULL
    *record = (Record *)calloc(1, sizeof(Record));
    if (*record == NULL) {
        RC_message = "Memory allocation error for Record.";
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    // Calculate the total size required for the record's data based on the schema
    int totalSize = 0;
    for (int i = 0; i < schema->numAttr; i++) {
        totalSize += (schema->dataTypes[i] == DT_STRING) ? schema->typeLength[i] : sizeof(int);
    }

    // Allocate memory for the record's data and initialize it to zero
    (*record)->data = (char *)calloc(totalSize, sizeof(char));
    if ((*record)->data == NULL) {
        free(*record);  // Free the record structure if data allocation fails
        RC_message = "Memory allocation error for Record data.";
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    return RC_OK;
}

RC freeRecord(Record *record) {
    // Check if the record is valid
    if (record == NULL) {
        RC_message = "The record pointer passed is null.";
        return RC_NULL_POINTER;
    }

    // Check if the record's data is valid before freeing
    if (record->data != NULL) {
        free(record->data);
        record->data = NULL;  // Set the data pointer to NULL after freeing
    }

    // Free the memory allocated for the record structure
    free(record);

    return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    // Check for NULL schema or record pointers
    if (schema == NULL || record == NULL) {
        RC_message = "The Schema or record pointer passed is null.";
        return RC_NULL_POINTER;
    }

    // Check the validity of the attribute number
    if (attrNum < 0 || attrNum >= schema->numAttr) {
        RC_message = "Invalid attribute number.";
        return RC_INVALID_ATTRIBUTE_NUMBER;
    }

    // Allocate memory for the Value structure
    *value = (Value *)malloc(sizeof(Value));
    if (*value == NULL) {
        RC_message = "Memory allocation error for Value.";
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    // Calculate the offset for the attribute
    int offset = 0;
    for (int i = 0; i < attrNum; i++) {
        offset += (schema->dataTypes[i] == DT_STRING) ? schema->typeLength[i] : sizeof(int);
    }

    // Get the attribute value based on its data type
    char *attrData = record->data + offset;
    switch (schema->dataTypes[attrNum]) {
        case DT_INT:
            memcpy(&((*value)->v.intV), attrData, sizeof(int));
            (*value)->dt = DT_INT;
            break;
        case DT_STRING:
            (*value)->v.stringV = strndup(attrData, schema->typeLength[attrNum]);
            (*value)->dt = DT_STRING;
            break;
        case DT_FLOAT:
            memcpy(&((*value)->v.floatV), attrData, sizeof(float));
            (*value)->dt = DT_FLOAT;
            break;
        case DT_BOOL:
            memcpy(&((*value)->v.boolV), attrData, sizeof(bool));
            (*value)->dt = DT_BOOL;
            break;
        default:
            free(*value);
            RC_message = "Unsupported data type.";
            return RC_UNSUPPORTED_DATATYPE;
    }

    return RC_OK;
}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    // Check for NULL schema or record pointers
    if (schema == NULL || record == NULL) {
        RC_message = "The Schema or record pointer passed is null.";
        return RC_NULL_POINTER;
    }

    // Check the validity of the attribute number
    if (attrNum < 0 || attrNum >= schema->numAttr) {
        RC_message = "Invalid attribute number.";
        return RC_INVALID_ATTRIBUTE_NUMBER;
    }

    // Calculate the offset for the attribute
    int offset = 0;
    for (int i = 0; i < attrNum; i++) {
        offset += (schema->dataTypes[i] == DT_STRING) ? schema->typeLength[i] : sizeof(int);
    }

    // Set the attribute value in the record based on its data type
    char *attrData = record->data + offset;
    switch (value->dt) {
        case DT_INT:
            memcpy(attrData, &(value->v.intV), sizeof(int));
            break;
        case DT_STRING:
            strncpy(attrData, value->v.stringV, schema->typeLength[attrNum]);
            attrData[schema->typeLength[attrNum] - 1] = '\0';  // Ensure null-termination
            break;
        case DT_FLOAT:
            memcpy(attrData, &(value->v.floatV), sizeof(float));
            break;
        case DT_BOOL:
            memcpy(attrData, &(value->v.boolV), sizeof(bool));
            break;
        default:
            RC_message = "Unsupported data type.";
            return RC_UNSUPPORTED_DATATYPE;
    }

    return RC_OK;
}
