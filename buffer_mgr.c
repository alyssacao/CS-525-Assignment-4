#include<stdlib.h>
#include<stdio.h>
#include<string.h>
#include<math.h>

#include"buffer_mgr.h"
#include"storage_mgr.h"
#include"buffer_mgr_stat.h"
#include"test_helper.h"
#include"dt.h"
#include"dberror.h"


#define RC_ERROR_PINNED_PAGE 9
#define RC_ERROR_FRAME_PINNED 10
#define RC_THIS_FRAME_PINNED 11

/*The two data structures used for defining and managing page frames*/
// BM_PageFrame structure
typedef struct BM_PageFrame{
    // sequence number field used in FIFO algorithm
	int seqNum;
    // the localtion of this frame in mgmtData
    int localNum;
	// pageNum field indicates the page number of the page
	int pageNum;
	// isDirty field verifies if the page is in use or not
	int isDirty;
	// fixCount field determines if the client is handling the page, value zero indicates the page is free, value non-zero indicates the page is currently in use	
	int fixCount;
	// 	timeUsage field is used in LRU algorithm
	int timeUsage;
	// the data contained in this page			
	char *data;
	// pointer to previous and next page frame
	struct BM_PageFrame *previous;	
	struct BM_PageFrame *next;
}bmPageFrame;

// BM_managePageFrame structure
typedef struct BM_managePageFrame{
	// total number of page frames in the buffer
	int pageNum;
	// sequence number field is used in FIFO algorithm
	int seqNum;
	// the number of current pages
	int currentNum;
    // timeUsage field is used in LRU algorithm
    int timeUsage;
    // readIO field is used to count the number of reads that has been done to the page
    int readIO;
    // writeIO field is used to count the number of writes that has been done to the page
    int writeIO;
	// the head and tail of the frame
	bmPageFrame *head; 
	bmPageFrame *tail;

}bmManagePageFrame;

/* createBufferFrame function is used to create new buffer frame */
RC createBufferFrame(bmManagePageFrame *mgmtData){
    bmPageFrame *frame = (bmPageFrame *)malloc(sizeof(bmPageFrame));
    frame->seqNum = mgmtData->seqNum;
    mgmtData->seqNum++;
    frame->isDirty = 0;
    frame->fixCount = 0;
    frame->timeUsage = mgmtData->timeUsage;
    mgmtData->timeUsage++;
    frame->pageNum = -1;
    frame->data = (char *)calloc(PAGE_SIZE,sizeof(char));

    if (mgmtData->head->next->next == NULL)
    {
        mgmtData->head->next = frame;
        frame->previous = mgmtData->head;
        mgmtData->tail->previous = frame;
        frame->next = mgmtData->tail;
        frame->localNum = 0;
    }else
    {
        frame->previous = mgmtData->tail->previous;
        mgmtData->tail->previous->next = frame;
        frame->next = mgmtData->tail;
        mgmtData->tail->previous = frame;
        frame->localNum = frame->previous->localNum+1;
    }
    mgmtData->currentNum++;
    return RC_OK;
}

bmPageFrame *replaceBufferFrame(bmManagePageFrame *mgmtData, bmPageFrame *inputFrame){
    bmPageFrame *frame = (bmPageFrame *)malloc(sizeof(bmPageFrame));
    frame->seqNum = mgmtData->seqNum;
    mgmtData->seqNum++;
    frame->isDirty = 0;
    frame->fixCount = 0;
    frame->timeUsage = mgmtData->timeUsage;
    mgmtData->timeUsage++;
    frame->pageNum = -1;
    frame->data = (char *)calloc(PAGE_SIZE,sizeof(char));
    //frame insertion
    frame->previous = inputFrame->previous;
    frame->next = inputFrame->next;
    frame->next->previous = frame;
    frame->previous->next = frame;
    frame->localNum = inputFrame->localNum;
    mgmtData->currentNum++;
    return frame;
}

int containBufferFrame(bmManagePageFrame *mgmtData, const PageNumber pageNum){
    bmPageFrame *frame = mgmtData->head;
    while (frame->next != NULL)
    {
        if (frame->pageNum == pageNum)
        {
            return 1;
        }
        frame = frame->next;
    }
    return 0;
}

/*Functions of the buffer pool. The initBufferPool() is used to build a buffer pool for an existing page file, 
whereas shutdownBufferPool() is to stop the buffer pool and forceFlushPool() is used to force the buffer manager to flush all dirty pages to disk*/

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages,
    ReplacementStrategy strategy, void *stratData){

    //checking whether the input is valid
    SM_FileHandle *fHandle = (SM_FileHandle *) malloc(sizeof(SM_FileHandle));
    if (0>numPages || openPageFile ((char *)pageFileName, fHandle) != RC_OK)
    {
        return RC_FILE_NOT_FOUND;
    }

    //loading the values of mgmtData
    bmManagePageFrame *mgmtData = (bmManagePageFrame *)malloc(sizeof(bmManagePageFrame));
    bmPageFrame *head = (bmPageFrame *)malloc(sizeof(bmPageFrame));
    head->pageNum = -1;
    bmPageFrame *tail = (bmPageFrame *)malloc(sizeof(bmPageFrame));
    tail->pageNum = -1;
    mgmtData->head = head;
    mgmtData->tail = tail;
    mgmtData->seqNum = 0;
    mgmtData->timeUsage = 0;
    mgmtData->currentNum = 0;
    mgmtData->readIO = 0;
    mgmtData->writeIO = 0;
    mgmtData->head->next = mgmtData->tail;
    mgmtData->tail->previous = mgmtData->head;
    mgmtData->head->previous = NULL;
    mgmtData->tail->next = NULL;
    mgmtData->pageNum = numPages;
    for (int i = 0; i < numPages; i++)
    {
        // create frames
        createBufferFrame(mgmtData);
    }
    //adding the values of fields pointed by bm pointer
    bm->numPages = numPages;
    bm->pageFile = (char *)pageFileName;
    bm->strategy = strategy;
    bm->mgmtData = mgmtData;

    //closing the page file
    closePageFile(fHandle);

    return RC_OK;
}


RC shutdownBufferPool(BM_BufferPool *const bm){
    //checking whether the input is valid
    if (bm == NULL || bm->mgmtData == NULL)
    {
        return RC_OK;
    }
    //loading the values of mgmtData
    bmManagePageFrame *mgmtData = (bmManagePageFrame *)bm->mgmtData;
    
    //writing the dirty pages to disk
    forceFlushPool(bm);
    
    //freeing all the frames held by bm pointer
    bmPageFrame *frame = mgmtData->head;
    while (frame->next != NULL)
    {
        if (frame->fixCount != 0 )
        {
            return RC_ERROR_PINNED_PAGE;
        }
        frame = frame->next;
        free(frame->previous);
    }
    //freeing other attributes to avoid memory leak
    free(mgmtData->tail);
    free(mgmtData);

    bm->numPages = 0;
    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm){
    //checking whether the input is valid
    if (bm == NULL || bm->mgmtData == NULL)
    {
        return RC_OK;
    }
    //loading the mgmtData
    bmManagePageFrame *mgmtData = (bmManagePageFrame *)bm->mgmtData; 

    //looping until all frames that are eligible are written back
    bmPageFrame *frame = mgmtData->head;
    while (frame->next != NULL)
    {
        if (frame->isDirty == 1 && frame->fixCount == 0)
        {
            //opening the old page file
            SM_FileHandle *fHandle = (SM_FileHandle *) malloc(sizeof(SM_FileHandle));
            if(openPageFile(bm->pageFile,fHandle) != RC_OK){
                return RC_FILE_NOT_FOUND;
            }
            //write the current frame to block
            if(writeBlock(frame->pageNum,fHandle,frame->data) != RC_OK){
                mgmtData->writeIO++;
                closePageFile(fHandle);
                return RC_FILE_NOT_FOUND;
            }
            mgmtData->writeIO++;
            frame->isDirty = 0;
            closePageFile(fHandle);
        }
        //move to next frame
        frame = frame->next;
    }
    return RC_OK;
}

/*Page Management Functions. These functions are used to pin pages, unpin pages, mark pages as
dirty, and force a page back to disk.*/
// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    //checking whether the input is vaild
    if (bm == NULL || page == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }
    //loading the mgmtData
    bmManagePageFrame *mgmtData = (bmManagePageFrame *)bm->mgmtData;
    bmPageFrame *frame = mgmtData->head;
    while (frame->next != NULL)
    {
        if (frame->pageNum == page->pageNum)
        {
            frame->isDirty = 1;
            return RC_OK;
        }
        frame = frame->next;
        
    }
    return RC_FILE_NOT_FOUND;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
    //checking whether the input is vaild
    if (bm == NULL || page == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }
    //loading the mgmtData
    bmManagePageFrame *mgmtData = (bmManagePageFrame *)bm->mgmtData;
    bmPageFrame *frame = mgmtData->head;
    while (frame->next != NULL)
    {
        if (frame->pageNum == page->pageNum)
        {
            frame->fixCount--;
            return RC_OK;
        }
        frame = frame->next;
        
    }
    return RC_OK;
}
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
    //checking whether the input is vaild
    if (bm == NULL || page == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }
    //loading the mgmtData
    bmManagePageFrame *mgmtData = (bmManagePageFrame *)bm->mgmtData;
    bmPageFrame *frame = mgmtData->head;
    //opening the old page file
    SM_FileHandle *fHandle = (SM_FileHandle *) malloc(sizeof(SM_FileHandle));
    if(openPageFile(bm->pageFile,fHandle) != RC_OK){
        return RC_FILE_NOT_FOUND;
    }
    while (frame->next != NULL)
    {
        if (frame->pageNum == page->pageNum && frame->isDirty == 1)
        {
            //writeing this page frame to block
            if(writeBlock(frame->pageNum,fHandle,frame->data) != RC_OK){
                mgmtData->writeIO++;
                closePageFile(fHandle);
                return RC_FILE_NOT_FOUND;
            }else{
                mgmtData->writeIO++;
                frame->isDirty = 0;
                closePageFile(fHandle);
                return RC_OK;
            }
        }
        //moving to next page frame
        frame = frame->next;
    }
    closePageFile(fHandle);
    return RC_OK;
}

/*------Implementing Page Replacement Strategies------*/
RC algoFIFO(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
    //checking whether the input is vaild
    if (bm == NULL || pageNum < 0)
    {
        return RC_FILE_NOT_FOUND;
    }
    //loading mgmtData
    bmManagePageFrame *mgmtData = (bmManagePageFrame *)bm->mgmtData;
    bmPageFrame *frame = mgmtData->head->next;
    //checking if this page is in pool
    if (containBufferFrame(mgmtData,pageNum))
    {
        while (frame->pageNum != pageNum)
        {
            frame = frame->next;
        }
        page->pageNum = pageNum;
        page->data = frame->data;
        frame->fixCount++;
        frame->timeUsage = mgmtData->timeUsage;
        mgmtData->timeUsage++;
        return RC_OK;
    }
    //find the value of mini sequence
    int minSeqNum = pow(2,31) - 1;
    while (frame != mgmtData->tail){
        if(frame->seqNum < minSeqNum && frame->fixCount == 0){
            minSeqNum = frame->seqNum;
        }
        frame = frame->next;
    }
    //replacing the pointer
    frame = mgmtData->head->next;
    //if not in the pool, delete
    while (frame->next != NULL)
    {
        //delete the first non-pinned page frame
        if (frame->seqNum == minSeqNum )
        {
            if (frame->isDirty)
            {
                SM_FileHandle *fHandle = (SM_FileHandle *) malloc(sizeof(SM_FileHandle));
                if(openPageFile(bm->pageFile,fHandle) != RC_OK){
                    return RC_FILE_NOT_FOUND;
                }
                //writing this page frame to block
                ensureCapacity(pageNum,fHandle);
                if(writeBlock(frame->pageNum,fHandle,frame->data) != RC_OK){
                    mgmtData->writeIO++;
                    closePageFile(fHandle);
                    return RC_FILE_NOT_FOUND;
                }else{
                    mgmtData->writeIO++;
                    frame->isDirty = 0;
                    closePageFile(fHandle);
                }
            }
            frame = replaceBufferFrame(mgmtData,frame);
            break;
        }
        frame = frame->next;
    }
    if (frame == mgmtData->tail)
    {
        return RC_ERROR_FRAME_PINNED;
    }
    //opening the page file 
    SM_FileHandle *fHandle = (SM_FileHandle *) malloc(sizeof(SM_FileHandle));
    openPageFile(bm->pageFile,fHandle);

    //setting new page frame's attributes
    fHandle->curPagePos = pageNum;
    readBlock(pageNum,fHandle,frame->data);
    mgmtData->readIO++;
    closePageFile(fHandle);
    frame->fixCount = 1;
    frame->isDirty = 0;
    frame->localNum = pageNum;
    frame->pageNum = pageNum;
    frame->timeUsage = mgmtData->timeUsage;
    mgmtData->timeUsage++;
    // set page's attributes
    page->pageNum = pageNum;
    page->data = frame->data;
    return RC_OK;
}
//Before using the page, move it to the tail. The most recent page utilized should then be in the head.
RC algoLRU(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
    //checking whether the input is vaild
    if (bm == NULL || pageNum < 0)
    {
        return RC_FILE_NOT_FOUND;
    }
    //loading the mgmtData
    bmManagePageFrame *mgmtData = (bmManagePageFrame *)bm->mgmtData;
    bmPageFrame *frame = mgmtData->head->next;
    //checking if this page is in the buffer pool
    if (containBufferFrame(mgmtData,pageNum))
    {
        while (frame->pageNum != pageNum)
        {
            frame = frame->next;
        }
        page->pageNum = pageNum;
        page->data = frame->data;
        frame->fixCount++;
        frame->timeUsage = mgmtData->timeUsage;
        mgmtData->timeUsage++;
        return RC_OK;
    }
    //finding the minimum used value
    int minUsedNum = pow(2,31) - 1;
    while (frame != mgmtData->tail){
        if(frame->timeUsage < minUsedNum && frame->fixCount == 0){
            minUsedNum = frame->timeUsage;
        }
        frame = frame->next;
    }
    //replacing the pointer
    frame = mgmtData->head->next;
    //if not in the buffer pool, delete
    while (frame->next != NULL)
    {
        // delete the first non-pinned frame
        if (frame->timeUsage == minUsedNum )
        {
            if (frame->isDirty)
            {
                SM_FileHandle *fHandle = (SM_FileHandle *) malloc(sizeof(SM_FileHandle));
                if(openPageFile(bm->pageFile,fHandle) != RC_OK){
                    return RC_FILE_NOT_FOUND;
                }
                // write this frame to block
                ensureCapacity(pageNum,fHandle);
                if(writeBlock(frame->pageNum,fHandle,frame->data) != RC_OK){
                    mgmtData->writeIO++;
                    closePageFile(fHandle);
                    return RC_FILE_NOT_FOUND;
                }else{
                    mgmtData->writeIO++;
                    frame->isDirty = 0;
                    closePageFile(fHandle);
                }
            }
            frame = replaceBufferFrame(mgmtData,frame);
            break;
        }
        frame = frame->next;
    }
    if (frame == mgmtData->tail)
    {
        return RC_ERROR_FRAME_PINNED;
    }
    //open page file
    SM_FileHandle *fHandle = (SM_FileHandle *) malloc(sizeof(SM_FileHandle));
    openPageFile(bm->pageFile,fHandle);
    
    fHandle->curPagePos = pageNum;
    readBlock(pageNum,fHandle,frame->data);
    mgmtData->readIO++;
    closePageFile(fHandle);
    frame->fixCount = 1;
    frame->isDirty = 0;
    frame->localNum = pageNum;
    frame->pageNum = pageNum;
    frame->timeUsage = mgmtData->timeUsage;
    mgmtData->timeUsage++;
    // set page's attributes
    page->pageNum = pageNum;
    page->data = frame->data;
    return RC_OK;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
   return (bm->strategy == RS_FIFO) ? algoFIFO(bm, page, pageNum) : algoLRU(bm, page, pageNum);
}


/*Statistics functions give statistics regarding a buffer pool's contents. 
These functions are used internally by the print debug procedures described below to collect data about a pool.*/

/*------Implementing Statistics Interfaces------*/
PageNumber *getFrameContents (BM_BufferPool *const bm){
    //loading the mgmtData
    bmManagePageFrame *mgmtData = (bmManagePageFrame *)bm->mgmtData;
    bmPageFrame *frame = mgmtData->head;
    PageNumber *frameData =(PageNumber *)malloc(sizeof(PageNumber) * mgmtData->currentNum);
    for (int i = 0; i < mgmtData->pageNum; i++)
    {
        frame = frame->next;
        frameData[i] = frame->pageNum;
    }
    return frameData;
}
bool *getDirtyFlags (BM_BufferPool *const bm){
    //loading the mgmtData
    bmManagePageFrame *mgmtData = (bmManagePageFrame *)bm->mgmtData;
    bmPageFrame *frame = mgmtData->head;
    bool *frameData =(bool *)malloc(sizeof(bool) * mgmtData->currentNum);
    for (int i = 0; i < mgmtData->pageNum; i++)
    {
        frame = frame->next;
        if (frame->isDirty == 1)
        {
            frameData[i] = 1;
        }else frameData[i] = 0;
        
    }
    return frameData;
}
int *getFixCounts (BM_BufferPool *const bm){
    //loading the mgmtData
    bmManagePageFrame *mgmtData = (bmManagePageFrame *)bm->mgmtData;
    bmPageFrame *frame = mgmtData->head;
    int *frameData =(int *)malloc(sizeof(int) * mgmtData->currentNum);
    for (int i = 0; i < mgmtData->pageNum; i++)
    {
        frame = frame->next;
        frameData[i] = frame->fixCount;
    }
    return frameData;
}
int getNumReadIO (BM_BufferPool *const bm){
    //checking whether the input is vaild
    if (bm == NULL)
    {
        return 0;
    }
    //loading the mgmtData
    bmManagePageFrame *mgmtData = (bmManagePageFrame *)bm->mgmtData;
    return mgmtData->readIO;
}
int getNumWriteIO (BM_BufferPool *const bm){
    //checking whether the input is vaild
    if (bm == NULL)
    {
        return 0;
    }
    //loading the mgmtData
    bmManagePageFrame *mgmtData = (bmManagePageFrame *)bm->mgmtData;
    return mgmtData->writeIO;
}
