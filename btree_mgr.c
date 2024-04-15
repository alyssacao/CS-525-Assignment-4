#include "string.h"
#include "stdlib.h"
#include "unistd.h"
#include "test_helper.h"
#include "dberror.h"
#include "expr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "btree_mgr.h"
#include <stdio.h>
#include <stdlib.h>

//Initializing global variables
typedef struct Node
{
    RID *id;
    int *key;
    struct Node **next;
} Node;

Node *root;

//Structure to store metadata for B-tree
typedef struct BTreeManager {
    int numKeysPerNode;
    int numEntries;
    int numNodes;
    BM_PageHandle *pageHandle;
    BM_BufferPool *bufferPool;
    Node *root;
    BTreeHandle btreehandle;
} BTreeManager;

BTreeManager *btreeManager;

//Structure to store metadata for B-tree scan
typedef struct BTreeScan {
    int indexNum;
    struct Node *scan;
} BTreeScan;

BTreeScan *btreeScan;

// Convert node value to string
char * NodeValuetoString(Node *node)
{
    // Allocate memory for temporary string and result string
    char *temp = malloc(sizeof(char)*(12));
    char *result = malloc(sizeof(char)*(btreeManager->numKeysPerNode*9));
    
    int i = 0;

    while (i < btreeManager->numKeysPerNode) {
        // Construct string manually for each key and id pair
        sprintf(temp, "%d %d %d,", node->key[i], node->id[i].page, node->id[i].slot);

        // Copy to result string
        strcat(result, temp);

        // Move to the next key and id pair
        i++;
    }
    
    // Free memory for temporary string and result
    free(temp);
    free(result);

    return result;  
}

// Persistently store a string in a page within the buffer pool
RC persistStringtoPage(BM_BufferPool *pool, BM_PageHandle *handle, char *data)
{
    // Pin the page to be modified
    pinPage(pool, handle, 0);

    // Copy data into page's data field
    sprintf(handle->data, "%s", data);

    //Mark the page as dirty to be write back
    markDirty(pool, handle);

    // Unpin page and make sure page is written back to disk
    unpinPage(pool, handle);
    forcePage(pool, handle);

    return RC_OK;
}

// create BTreesNode index and initialize the defaults for management
RC createBtree(char *idxId, DataType keyType, int n) {
    // Handle the error condition 
    RC errorCheck = (idxId == NULL) ? (RC_message = "idxId was NULL", RC_ERROR) :
                                   (createPageFile(idxId) != RC_OK) ? RC_ERROR :
                                   ((btreeManager = (BTreeManager *)malloc(sizeof(BTreeManager))) == NULL) ? RC_ERROR : RC_OK;

    // Early return based on error condition
    if (errorCheck != RC_OK) {
        return errorCheck;
    }

    // Set values for btreeManager
    btreeManager->numKeysPerNode = n;
    btreeManager->numEntries = btreeManager->numNodes = 0;
    btreeManager->root = root;
    btreeManager->btreehandle.keyType = keyType;
    btreeManager->btreehandle.idxId = idxId;

    // Allocate and set buffers
    btreeManager->bufferPool = (BM_BufferPool *)MAKE_POOL();
    btreeManager->pageHandle = (BM_PageHandle *)MAKE_PAGE_HANDLE();

    return RC_OK;
}


// open B-tree index and initialize the root node
RC openBtree (BTreeHandle **tree, char *idxId)
{
	// Initialize a Buffer Pool for the b-tree manager
	initBufferPool(btreeManager->bufferPool, idxId, 100, RS_FIFO, NULL);

    // Allocate memory for BTreeHandle
    *tree = (BTreeHandle *) malloc(sizeof(BTreeHandle));
	(*tree)->mgmtData = btreeManager;

    // Allocate memory for the root node and its components
    root = (Node *)malloc(sizeof(Node));
    root->next = malloc(sizeof(Node) * (btreeManager->numKeysPerNode + 1));
    root->id = malloc(sizeof(int) * btreeManager->numKeysPerNode);
    root->key = malloc(sizeof(int) * btreeManager->numKeysPerNode);

    // Initialize default values for root node
    for (int i = 0; i < btreeManager->numKeysPerNode + 1; i++) {
        root->next[i] = NULL;
    }
    for (int i = 0; i < btreeManager->numKeysPerNode; i++) {
        root->id[i].page = root->id[i].slot = -1;
        root->key[i] = 0;
    }
    btreeManager->numNodes++; // Increment number of nodes

    // Serialize root node to string
    char* NodeValue = NodeValuetoString(root);

    //Writing root node to page 0 in buffer pool
    btreeManager->pageHandle->data = NULL;
    btreeManager->pageHandle->pageNum = 0;
    persistStringtoPage(btreeManager->bufferPool, btreeManager->pageHandle,NodeValue);
    
    // Free memory  
    free(NodeValue);

    return RC_OK;
}

//Close B-tree index by freeing allocated memory
RC closeBtree (BTreeHandle *tree)
{
	BTreeManager *btreeMgmt = (BTreeManager*) tree->mgmtData;
   
	// Shutdown the buffer pool.
	shutdownBufferPool(btreeMgmt->bufferPool);

    // Free memory for BTreeManager and root node
	free(btreeMgmt);
    free(root);
	return RC_OK;
}

//Delete file created for B-tree
RC deleteBtree (char *idxId)
{
    // Check if idxId is NULL, if not, destroy the page file
    return (idxId == NULL) ? RC_ERROR : (destroyPageFile(idxId), RC_OK);
}
