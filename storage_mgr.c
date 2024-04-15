#include "storage_mgr.h"
#include "string.h"
#include "stdio.h"
#include "dberror.h"
#include "unistd.h"
#include "stdlib.h"

typedef FILE file_datatype;
typedef char* char_ptr;

file_datatype *filePtr = NULL;

#define F_OK 0

// Manipulating Page Files Implementation //
// Initializing Global variables
// Storage Manager Setup and filepointer is empty
FILE *fileptr;
typedef char *char_ptr;
void initStorageManager()     // Roshan
{
  fileptr = NULL;
}

//here we are going to create a "create page file"
RC createPageFile(char *fileName)     //Roshan
{
//here we can open a page file in write phase
  fileptr = fopen(fileName, "w+");

  // here we check if the file could be opened or not
  if (fileptr == NULL)
  {
    return RC_FILE_NOT_FOUND;
  }

  // "Create a memory block of PAGE_SIZE and initialize it to contain null characters ('\0')."
  int s = sizeof(char);
  char_ptr chWriteMemory = (char_ptr)malloc(PAGE_SIZE * sizeof(char));
  
  //An alternative method to using malloc with a cast is to use malloc without a cast.
  // char_ptr chWriteMemory = malloc(PAGE_SIZE * s);
  // memset(chWriteMemory,'\0', PAGE_SIZE);
  // // write the block content to file
  // fwrite(chWriteMemory, s , PAGE_SIZE, fileptr);
  // // free allocated memory
  // free(chWriteMemory);
  // // file close
  // fclose(fileptr);
  // return RC_OK;
  /*    if(chWriteMemory == NULL){
    fclose(fileptr);
    return RC_WRITE_FAILED;
  }   */
  // char *chWriteMemory = malloc(PAGE_SIZE * s);
  

  // here By using the memset function, we can allocate null (\0) memory to the file if it exists.
  memset(chWriteMemory, '\0', PAGE_SIZE);
  // We are saving the block content to a file.
  fwrite(chWriteMemory, s, PAGE_SIZE, fileptr);
  // we are making the block free which we allocted menory after usage
  free(chWriteMemory);
  // we are closing the file after everything is completed.
  fclose(fileptr);
  return RC_OK;
}

//here we are creating a "opening a page file"
RC openPageFile(char *fileName, SM_FileHandle *fHandle)       //Roshan
{
  // we are Opening the file in read-write mode without truncating the file
  fileptr = fopen(fileName, "r+");
  if (fileptr == NULL)
  {
    // RC_message ='File Not Found\n';
    return RC_FILE_NOT_FOUND;
  }
  else
  {
    // To open an existing file, initialize the file handle and set mgmtInfo to filepointer descriptor once again.
    fHandle->fileName = fileName;
    fHandle->totalNumPages = 0;
    fHandle->mgmtInfo = fileptr;

    //Finding the file's end to determine the entire number of pages
    fseek(fileptr, 0, SEEK_END);
    long totalfileSize = ftell(fileptr);
    fHandle->totalNumPages = (int)(totalfileSize / PAGE_SIZE);
    // Adjust the file pointer's location to the file's beginning.
    fseek(fileptr, 0, SEEK_SET);
    // Since the file has just been opened, position 0 is assigned to the present page position.
    fHandle->curPagePos = 0;
    // RC_message = "Opened the file successfully";
    return RC_OK;
  }
}

// here we are gonig to create " close page file".
extern RC closePageFile(SM_FileHandle *fHandle)         //Roshan
{
  RC rc = RC_OK;
// we are opening the file in the formate of read-write.
  fileptr = fopen(fHandle->fileName, "rw+");
//Verify whether the file may be opened.
  if (fileptr == NULL)
  {

    return RC_FILE_NOT_FOUND; //If there was a problem opening the file, it will return a code for the error.
  
  }
// here we are closing the file
  int cF = fclose(fileptr);
  
// we are checking that file is closed properly and successfully
  if (cF == 0)
  {
    rc = RC_OK;   // if the file closes we return the code 
  }
  else
  {
    rc = RC_FILE_NOT_FOUND; // if the file dont closed properly we return this 
  }
  // here we are assinging the null because to avoid the accidental use of file after closing.
  fileptr = NULL;
  return rc;
}

// here we are going to create" destroy page file".
extern RC destroyPageFile(char *fileName)             //Roshan
{
  RC rc;

  // here If the file is open we will close it.
  if (fileptr != NULL)
  {
    fclose(fileptr);
    fileptr = NULL; //To signal that the file is closed, set fileptr to NULL.
  }
  // The file can be removed from the directory using the remove function.
  int removeFile = remove(fileName);
//Check if the file was removed successfully
  if (removeFile == 0)
    rc = RC_OK; //If the removal of the file was successful, return a success code.
  else
  {
    rc = RC_FILE_NOT_FOUND; //If the file removal attempt fails, return an error code.
  }

  return rc;
}





// Function to read the current block
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)             // Tim
{
    // Cast mgmtInfo to FILE* for file operations
    FILE* filePtr = (FILE*)fHandle->mgmtInfo;

    if (!filePtr) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Seek to the start of the desired page
    if (fseek(filePtr, pageNum * PAGE_SIZE, SEEK_SET) != 0) {
        return RC_READ_NON_EXISTING_PAGE; // Error during seek
    }

    // Read the page into the provided memory page
    size_t bytesRead = fread(memPage, sizeof(char), PAGE_SIZE, filePtr);
    if (bytesRead < PAGE_SIZE) {
        // Handle partial read or read error
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Update the current page position
    fHandle->curPagePos = pageNum;

    return RC_OK;
}

// Function to get the current block position
RC getBlockPos(SM_FileHandle *fHandle)         // Tim
{
    if (!fHandle) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    return fHandle->curPagePos;
}

// Function to read the first block
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)    // Tim
{
    return readBlock(0, fHandle, memPage);
}

// Function to read the previous block relative to the current position
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)          // Tim
{
    if (!fHandle || fHandle->curPagePos <= 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)    // Alyssa
{
    // Check if file handle is initialized
    if (fHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;
    // Check if file exists
    if (fopen(fHandle->fileName,"r") == NULL)
        return RC_FILE_NOT_FOUND;
    // Check if pageNum is our of bounds
    if (pageNum >= fHandle->totalNumPages)
        return RC_WRITE_FAILED;
    FILE *file;
    file = fopen(fHandle->fileName, "r+");
    if (file == NULL)
        return RC_FILE_NOT_FOUND;
    fseek(file, pageNum*PAGE_SIZE, SEEK_SET);
    if (fwrite(memPage, PAGE_SIZE, 1, file) != 1)
        // Close the file if write failed
    {
        fclose(file);
        return RC_WRITE_FAILED;
    }
    // Reopen and update file handle info
    file = fopen(fHandle-> fileName, "r");
    fseek(file, (pageNum+1)*PAGE_SIZE, SEEK_SET);
    // Update file handle metadata
    fHandle->mgmtInfo = file;
    fHandle->curPagePos = ftell(fHandle->mgmtInfo) /PAGE_SIZE;
    fHandle->totalNumPages = ftell(file) / PAGE_SIZE;
    return RC_OK;
} 
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)      // Alyssa
{
      // Check if file handle is initialized
    if (fHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;
    // Check if the file exists
    FILE *fileCheck = fopen(fHandle->fileName, "r");
    if (fileCheck == NULL)
        return RC_FILE_NOT_FOUND;
    fclose(fileCheck);
    // Check if current page position is out of bounds
    if (fHandle->curPagePos > fHandle->totalNumPages)
        return RC_WRITE_FAILED;
    FILE *file = fopen(fHandle->fileName, "r+");
    if (file == NULL)
        return RC_FILE_NOT_FOUND;
    // Seek to the current page in the file
    fseek(file, fHandle->curPagePos * PAGE_SIZE, SEEK_SET);
    // Write the page data to the file
    if (fwrite(memPage, PAGE_SIZE, 1, file) !=1)
    {
        fclose(file);
        return RC_WRITE_FAILED;
    }
    //Update the file handle info
    fclose(file);
    file = fopen(fHandle->fileName, "r");
    fHandle->curPagePos = ftell(fHandle->mgmtInfo) / PAGE_SIZE;
    fseek(file, fHandle->curPagePos * PAGE_SIZE, SEEK_SET);
    // Update file handle metadata
    fHandle->mgmtInfo = file;
    return RC_OK;
}
RC appendEmptyBlock (SM_FileHandle *fHandle)   // Alyssa
{
    int i;
    if (fHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;
    FILE *fileCheck = fopen(fHandle->fileName, "r");
    if (fileCheck == NULL)
        return RC_FILE_NOT_FOUND;
    fclose(fileCheck);
    FILE *file = fopen(fHandle->fileName,"r+");
    if (file == NULL)
        return RC_FILE_NOT_FOUND;
    fseek(file, 0, SEEK_END);
    for (i=0; i<PAGE_SIZE; i++){
        fwrite ("\0", 1, 1, file);
        fseek(file, 0, SEEK_END);
    }
    fHandle->mgmtInfo =file;
    fHandle->totalNumPages = ftell(file) / PAGE_SIZE;
    return RC_OK;
}
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)     // Alyssa
{
    if (fHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;
    FILE *fileCheck = fopen(fHandle->fileName, "r");
    if (fileCheck == NULL)
        return RC_FILE_NOT_FOUND;
    fclose(fileCheck);
    if (fHandle->totalNumPages < numberOfPages) {
        while (fHandle->totalNumPages != numberOfPages) {
           appendEmptyBlock(fHandle);
        }
    }
    return RC_OK;
}











// Read the current block pointed to by the file handle
RC readCurrentBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage) //Uday
{
    // Check if the file handle is valid
    if (fileHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT; // File handle not initialized

    // Get the current block position
    int currentBlock = getBlockPos(fileHandle);

    // Move the file pointer to the current block
    fseek(fileHandle->mgmtInfo, (currentBlock * PAGE_SIZE), SEEK_SET);

    // Read the current block into memPage
    int bytesRead = fread(memPage, sizeof(char), PAGE_SIZE, fileHandle->mgmtInfo);

    // Update the current page position
    fileHandle->curPagePos = currentBlock;

    // Check if reading was successful
    if (bytesRead < 0 || bytesRead > PAGE_SIZE)
        return RC_READ_NON_EXISTING_PAGE; // Unable to read the current block

    return RC_OK; // Reading successful
}

// Read the next block of the file
RC readNextBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage) //Uday
{
    // Check if the file handle is valid
    if (fileHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT; // File handle not initialized

    // Get the position of the next block
    int nextBlock = fileHandle->curPagePos + 1;

    // Check if the next block exists
    if (nextBlock >= fileHandle->totalNumPages)
        return RC_READ_NON_EXISTING_PAGE; // Next block does not exist

    // Move the file pointer to the next block
    fseek(fileHandle->mgmtInfo, (nextBlock * PAGE_SIZE), SEEK_SET);

    // Read the next block into memPage
    int bytesRead = fread(memPage, sizeof(char), PAGE_SIZE, fileHandle->mgmtInfo);

    // Update the current page position
    fileHandle->curPagePos = nextBlock;

    // Check if reading was successful
    if (bytesRead < 0 || bytesRead > PAGE_SIZE)
        return RC_READ_NON_EXISTING_PAGE; // Unable to read the next block

    return RC_OK; // Reading successful
}

// Read the last block of the file
RC readLastBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage) //Uday
{
    // Check if the file handle is valid
    if (fileHandle == NULL)
        return RC_FILE_NOT_FOUND; // File not found

    // Get the position of the last block
    int lastBlock = fileHandle->totalNumPages - 1;

    // Move the file pointer to the last block
    fseek(fileHandle->mgmtInfo, (lastBlock * PAGE_SIZE), SEEK_SET);

    // Read the last block into memPage
    int bytesRead = fread(memPage, sizeof(char), PAGE_SIZE, fileHandle->mgmtInfo);

    // Update the current page position
    fileHandle->curPagePos = lastBlock;

    // Check if reading was successful
    if (bytesRead < 0 || bytesRead > PAGE_SIZE)
        return RC_READ_NON_EXISTING_PAGE; // Unable to read the last block

    return RC_OK; // Reading successful
}

