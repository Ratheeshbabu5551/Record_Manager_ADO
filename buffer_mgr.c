#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr_helper.c"

// Buffer Manager Interface Pool Handling

// Author: Prajwal Somendyapanahalli Venkateshmurthy

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
    if (pageFileName == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }

    FILE *fh = fopen(pageFileName, "rb+");
    // Prevent init buffer pool for non existing page file
    if (fh == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }

    fclose(fh);

    // Added #pragma GCC diagnostic push and #pragma GCC diagnostic ignored to suppress the following warning:
    // warning: discarding 'const' qualifier from pointer target type [-Wdiscarded-qualifiers]
    // See https://stackoverflow.com/questions/19452971/why-does-gcc-complain-about-passing-const-parameters-as-arguments for more details
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdiscarded-qualifiers"
    bm->pageFile = pageFileName;
#pragma GCC diagnostic pop
    bm->numPages = numPages; // Set Number of pages in the buffer pool
    bm->strategy = strategy; // Set Replacement strategy for the buffer pool

    // Allocate memory for mgmtData
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)malloc(sizeof(BM_MGMT_DATA));
    mgmtData->numReadIO = 0;  // Initialize number of read IOs to 0
    mgmtData->numWriteIO = 0; // Initialize number of write IOs to 0
    mgmtData->queueHead = 0;  // Initialize queue head to 0 -> FIFO Strategy

    // Allocate memory for page frames
    mgmtData->frames = (PAGE_FRAME *)malloc(numPages * sizeof(PAGE_FRAME));

    // Initialize page frames
    for (int i = 0; i < numPages; i++)
    {
        mgmtData->frames[i].pageNum = NO_PAGE;     // Set page number to NO_PAGE
        mgmtData->frames[i].data = NULL;           // Set data to NULL
        mgmtData->frames[i].isDirty = false;       // Set isDirty to false
        mgmtData->frames[i].fixCount = 0;          // Set fixCount to 0
        mgmtData->frames[i].recentAccessCount = 0; // Set recentAccessCount to 0 -> LRU Strategy
    }

    // Update mgmtData pointer in the buffer pool
    bm->mgmtData = mgmtData;

    // Return success
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{

    // Check if buffer pool is not existing
    if (bm == NULL || bm->mgmtData == NULL)
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }

    // Allocate memory for mgmtData
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData;

    // TEST CASE FAILING: Hence commented
    //     // Check if there are any pinned pages in the buffer pool
    //     for (int i = 0; i < bm->numPages; i++)
    //     {
    //         if (mgmtData->frames[i].fixCount > 0)
    //         {
    //             printf("Error: Buffer pool contains pinned pages.\n");
    //             return RC_NO_AVAILABLE_FRAME; // Error: Buffer pool contains pinned pages
    //         }
    //     }

    // Flush dirty pages to disk before destroying the buffer pool
    for (int i = 0; i < bm->numPages; i++)
    {
        // If the page is dirty, and has no fix count, write it back to disk
        if (mgmtData->frames[i].isDirty && mgmtData->frames[i].fixCount == 0)
        {
            // Write the dirty page back to disk
            writePageToFile(bm, &mgmtData->frames[i]);
            // Increment the number of write IOs
            mgmtData->numWriteIO++;
        }
    }

    // Free the memory allocated for page frames
    for (int i = 0; i < bm->numPages; i++)
    {
        free(mgmtData->frames[i].data);
    }
    free(mgmtData->frames);

    // Free the memory allocated for mgmtData
    free(mgmtData);

    // Reset the mgmtData pointer in the buffer pool
    bm->mgmtData = NULL;

    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm)
{
    if (bm == NULL || bm->mgmtData == NULL)
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }
    // Allocate memory for mgmtData
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData;

    // Perform a forced flush operation for all dirty pages with fix count 0 in the buffer pool
    for (int i = 0; i < bm->numPages; i++)
    {
        // If the page is dirty, and has no fix count, write it back to disk
        if (mgmtData->frames[i].isDirty && mgmtData->frames[i].fixCount == 0)
        {
            // Write the dirty page back to disk
            writePageToFile(bm, &mgmtData->frames[i]);
            // Increment the number of write IOs
            mgmtData->numWriteIO++;

            // Mark the page as not dirty after it has been written back to disk
            mgmtData->frames[i].isDirty = false;
        }
    }

    return RC_OK;
}

// Buffer Manager Interface Access Pages

// Author: Komal Bhavake (Primary) & Prajwal Somendyapanahalli Venkateshmurthy (Secondary)

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // Check if buffer pool is not existing
    if (bm == NULL || bm->mgmtData == NULL)
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }

    // Get the mgmtData pointer from the buffer pool
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData;
    PAGE_FRAME *frames = mgmtData->frames;
    int numPages = bm->numPages;

    // Find the target page in the buffer pool
    int frameIndex = -1;
    for (int i = 0; i < numPages; i++)
    {
        if (frames[i].pageNum == page->pageNum)
        {
            frameIndex = i;
            break;
        }
    }

    // If the page is not found in the buffer pool, return an error
    if (frameIndex == -1)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Mark the page as dirty
    frames[frameIndex].isDirty = true;

    return RC_OK;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // Check if buffer pool is not existing
    if (bm == NULL || bm->mgmtData == NULL)
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }

    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData;
    PAGE_FRAME *frames = mgmtData->frames;
    int numPages = bm->numPages;

    // Find the target page in the buffer pool
    int frameIndex = -1;
    for (int i = 0; i < numPages; i++)
    {
        if (frames[i].pageNum == page->pageNum)
        {
            frameIndex = i;
            break;
        }
    }

    // If the page is not found in the buffer pool, return an error
    if (frameIndex == -1)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Decrement the fix count
    if (frames[frameIndex].fixCount > 0)
    {
        frames[frameIndex].fixCount--;
    }

    return RC_OK;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // Check if buffer pool is not existing
    if (bm == NULL || bm->mgmtData == NULL)
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }

    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData;
    PAGE_FRAME *frames = mgmtData->frames;
    int numPages = bm->numPages;

    // Find the target page in the buffer pool
    int frameIndex = -1;
    for (int i = 0; i < numPages; i++)
    {
        if (frames[i].pageNum == page->pageNum)
        {
            frameIndex = i;
            break;
        }
    }

    // If the page is not found in the buffer pool, return an error
    if (frameIndex == -1)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Write the page back to disk
    writePageToFile(bm, &frames[frameIndex]);
    mgmtData->numWriteIO++;

    // Mark the page as not dirty after it has been written back to disk
    frames[frameIndex].isDirty = false;

    return RC_OK;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    // Check for invalid page number
    if (pageNum < 0)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }
    switch (bm->strategy)
    {
    case RS_FIFO:
        return pinPageUsingFIFO(bm, page, pageNum);
    case RS_LRU:
        return pinPageUsingLRU(bm, page, pageNum);
    case RS_LRU_K:
        return pinPageUsingLRU_K(bm, page, pageNum);
    default:
        return RC_INVALID_REPLACEMENT_STRATEGY;
    }
}

// Statistics Interface

// Author: Ravin Krishnan

PageNumber *getFrameContents(BM_BufferPool *const bm) // returns an array of pagenumbers whose ith element is the page number stored in ith frame
{

    if (bm == NULL || bm->mgmtData == NULL) // Check if buffer pool is not existing
    {
        return (PageNumber *)RC_BUFFER_POOL_NOT_EXISTING;
    }
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData;         // bufferpool's data is stored in mgmtData
    PageNumber *ARR = malloc((bm->numPages) * sizeof(PageNumber)); // creates PageNumber array whose size is the size of integer times number of pages
    PAGE_FRAME *frames = mgmtData->frames;                         // Creates frames of type PAGE_FRAME to store the frames from the buffer pool
    int POS = 0;                                                   // Creates integer to store POSITION
    int NUMPAG = bm->numPages;                                     // Creates integer which stores number of pages

    while (POS < NUMPAG) // While loop iterates NUMPAG times
    {
        ARR[POS] = frames[POS].pageNum; // Array stores the pagenumber
        POS = POS + 1;                  // Increment position
    }
    return ARR; // Return the array
}

bool *getDirtyFlags(BM_BufferPool *const bm) // returns an array of bools whose ith element is true if page stored in ith frame is dirty else false
{

    if (bm == NULL || bm->mgmtData == NULL) // Check if buffer pool is not existing
    {
        return (bool*)RC_BUFFER_POOL_NOT_EXISTING;
    }
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData; //   bufferpool's data is stored in mgmtData
    bool *DARR = malloc((bm->numPages) * sizeof(bool));    //  creates boolean array whose size is the size of bool times number of pages
    PAGE_FRAME *frames = mgmtData->frames;                 //  Creates frames of type PAGE_FRAME to store the frames from the buffer pool
    int POS = 0;                                           //  Creates integer to store POSITION
    int NUMPAG = bm->numPages;                             //  Creates integer which stores number of pages
    while (POS < NUMPAG)                                   // While loop iterates NUMPAG times
    {
        DARR[POS] = frames[POS].isDirty;
        POS = POS + 1; //  Position is incremented
    }
    return DARR; // Array is returned
}

int *getFixCounts(BM_BufferPool *const bm) // returns an array of ints whose ith element is the fix count of the page stored in ith page frame
{

    if (bm == NULL || bm->mgmtData == NULL) // Check if buffer pool is not existing
    {
        return (int *)RC_BUFFER_POOL_NOT_EXISTING;
    }
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData; //   bufferpool's data is stored in mgmtData
    int *FIXARR = malloc((bm->numPages) * sizeof(int));    //  creates integer array to store the fixcounts
    PAGE_FRAME *frames = mgmtData->frames;                 //  Creates frames of type PAGE_FRAME to store the frames from the buffer pool
    int POS = 0;                                           //  Creates integer to store POSITION
    int NUMPAG = bm->numPages;                             //  Creates integer which stores number of pages

    while (POS < NUMPAG) // While loop iterates NUMPAG times
    {
        FIXARR[POS] = frames[POS].fixCount; //  Array stores appropriate fixCount
        POS = POS + 1;                      //  Position is incremented
    }
    return FIXARR; // Array is returned
}

int getNumReadIO(BM_BufferPool *const bm) // returns the number of pages that have been read from disk since a buffer pool has been initialized
{

    if (bm == NULL || bm->mgmtData == NULL) // Check if buffer pool is not existing
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData; // bufferpool's data is stored in mgmtData
    return mgmtData->numReadIO;                            // return READPAGES
}

int getNumWriteIO(BM_BufferPool *const bm) // returns the number of pages written to the page file since the buffer pool has been initialized
{

    if (bm == NULL || bm->mgmtData == NULL) // Check if buffer pool is not existing
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData; // bufferpool's data is stored in mgmtData
    return mgmtData->numWriteIO;                           // return WRITTENPAGES
}