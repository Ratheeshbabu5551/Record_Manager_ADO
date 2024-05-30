/*
 * buffer_mgr_helper.c
 * --------------------
 * Contains helper functions for buffer pool, manager and replacement strategies implementation
 * Author: Prajwal Somendyapanahalli Venkateshmurthy
 */

#include <limits.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"

extern char *getPageFromFile(BM_BufferPool *const bm, const PageNumber pageNum)
{
    SM_FileHandle fileHandle;

    // Open the page file
    if (openPageFile(bm->pageFile, &fileHandle) != RC_OK)
    {
        printf("Error opening page file for reading.\n");
        return NULL;
    }

    // Allocate memory for the page data
    char *pageData = (char *)malloc(PAGE_SIZE);

    // Read the page from the file
    if (readBlock(pageNum, &fileHandle, pageData) != RC_OK)
    {
        printf("Error reading page from file.\n");
        free(pageData);
        return NULL;
    }

    // Close the page file
    if (closePageFile(&fileHandle) != RC_OK)
    {
        printf("Error closing page file after reading.\n");
    }

    return pageData;
}

extern int findPage_LRU(const PAGE_FRAME *frames, int numFrames, PageNumber targetPageNum)
{
    int frameIndex = -1;                  // Set the frame index to -1 if the page is not found in the buffer pool
    int leastRecentAccessCount = INT_MAX; // Set the least recent access count to the maximum integer value

    // Find the target page in the buffer pool
    for (int i = 0; i < numFrames; i++)
    {
        // If the page is found in the buffer pool, update the frame index
        if (frames[i].pageNum == targetPageNum || frames[i].pageNum == NO_PAGE)
        {
            // Update the frame index
            if (frames[i].recentAccessCount < leastRecentAccessCount)
            {
                // Update the least recent access count
                leastRecentAccessCount = frames[i].recentAccessCount;
                frameIndex = i;
            }
        }
    }

    return frameIndex;
}

extern int findLRUVictim(const PAGE_FRAME *frames, int numFrames)
{
    int victimIndex = -1;                 // Set the victim index to -1 if no victim is found
    int leastRecentAccessCount = INT_MAX; // Set the least recent access count to the maximum integer value

    for (int i = 0; i < numFrames; i++)
    {
        // If the frame is not pinned, and has the least recent access count, update the victim index
        if (frames[i].fixCount == 0 && frames[i].recentAccessCount < leastRecentAccessCount)
        {
            // Update the least recent access count
            leastRecentAccessCount = frames[i].recentAccessCount;
            victimIndex = i;
        }
    }

    return victimIndex;
}

extern void writePageToFile(BM_BufferPool *const bm, const PAGE_FRAME *frame)
{
    SM_FileHandle fileHandle;

    // Open the page file
    if (openPageFile(bm->pageFile, &fileHandle) != RC_OK)
    {
        printf("Error opening page file for writing.\n");
        return;
    }

    // Write the page to the file
    if (writeBlock(frame->pageNum, &fileHandle, frame->data) != RC_OK)
    {
        printf("Error writing page to file.\n");
    }

    // Close the page file
    if (closePageFile(&fileHandle) != RC_OK)
    {
        printf("Error closing page file after writing.\n");
    }
}

extern void updateLRUList(PAGE_FRAME *frames, int numFrames, int accessedFrameIndex)
{
    // Update the accessed frame's accessCount to the current highest count
    int highestAccessCount = 0;
    // Find the highest access count
    for (int i = 0; i < numFrames; i++)
    {
        // If the frame is not pinned, and has the least recent access count, update the victim index
        if (frames[i].recentAccessCount > highestAccessCount)
        {
            // Update the least recent access count
            highestAccessCount = frames[i].recentAccessCount;
        }
    }
    frames[accessedFrameIndex].recentAccessCount = highestAccessCount + 1;
}

extern int findPage_FIFO(PAGE_FRAME *frames, int numPages, PageNumber pageNum)
{
    // Find the target page in the buffer pool
    int frameIndex = -1;
    // Find the target page in the buffer pool
    for (int i = 0; i < numPages; i++)
    {
        // If the page is found in the buffer pool, update the frame index
        if (frames[i].pageNum == pageNum || frames[i].pageNum == NO_PAGE)
        {
            // Update the frame index
            frameIndex = i;
            break;
        }
    }

    return frameIndex;
}

extern int findVictimPage_FIFO(BM_MGMT_DATA *const mgmtData, PAGE_FRAME *frames, int numPages)
{
    // Find the victim page using FIFO strategy
    int frameIndex = NO_PAGE;
    // Find the victim page using FIFO strategy
    int i = (mgmtData->queueHead + 1) % numPages;

    // Find the first frame with fix count 0 -> Kind of a Round Robin approach
    while (mgmtData->queueHead != i)
    {
        if (frames[i].fixCount == 0)
        {
            frameIndex = i;
            break;
        }
        i = (i + 1) % numPages;
    }

    return frameIndex;
}

// FIFO Replacement Strategy
extern RC pinPageUsingFIFO(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    // Check if buffer pool is not existing
    if (bm == NULL || bm->mgmtData == NULL)
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }

    // Get the management data from the buffer pool
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData;
    PAGE_FRAME *frames = mgmtData->frames;
    int numPages = bm->numPages;

    // Find an available frame using FIFO strategy
    int frameIndex = findPage_FIFO(frames, numPages, pageNum);

    if (frameIndex == -1)
    {
        frameIndex = findVictimPage_FIFO(mgmtData, frames, numPages); // Find a victim page using FIFO strategy
        if (frameIndex == -1)
        {
            // printf("Error: No available frame.\n");
            getFrameContents(bm);
            return RC_NO_AVAILABLE_FRAME;
        }

        // Write the victim page back to disk if dirty
        if (frames[frameIndex].isDirty)
        {
            BM_PageHandle pageHandle;
            pageHandle.pageNum = frames[frameIndex].pageNum;
            pageHandle.data = frames[frameIndex].data;
            forcePage(bm, &pageHandle);
        }
    }

    // Update read count if the page is not already in the buffer pool
    if (frames[frameIndex].pageNum != pageNum)
    {
        mgmtData->numReadIO++;
    }

    // Update the page handle with the pinned page information
    mgmtData->queueHead = (frameIndex) % numPages;

    // Get the page from the file
    frames[frameIndex].data = getPageFromFile(bm, pageNum);

    // Update the page handle with the pinned page information
    page->pageNum = pageNum;
    page->data = frames[frameIndex].data;

    // Update the frame with the new page information
    frames[frameIndex].pageNum = pageNum;
    frames[frameIndex].isDirty = false;
    frames[frameIndex].fixCount++;

    return RC_OK;
}

// LRU Replacement Strategy
extern RC pinPageUsingLRU(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    // Check if buffer pool is not existing
    if (bm == NULL)
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }

    // Get the management data from the buffer pool
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)(bm->mgmtData);
    PAGE_FRAME *frames = mgmtData->frames;

    // Find the target page in the buffer pool
    int frameIndex = findPage_LRU(mgmtData->frames, bm->numPages, pageNum);

    // If the page is found in the buffer pool
    if (frameIndex != -1)
    {
        // Update page handle with existing page information
        frames[frameIndex].data = getPageFromFile(bm, pageNum);

        // Update the page handle with the pinned page information
        page->pageNum = pageNum;
        page->data = frames[frameIndex].data;

        // Update the frame with the new page information
        frames[frameIndex].pageNum = pageNum;
        frames[frameIndex].isDirty = false;
        // Increment fix count
        frames[frameIndex].fixCount++;
        // Move the page to the front of the LRU list (update accessCount)
        updateLRUList(mgmtData->frames, bm->numPages, frameIndex);

        // Increment read count if the page is not already in the buffer pool
        mgmtData->numReadIO++;

        return RC_OK;
    }

    // If the page is not found in the buffer pool
    // Find a victim frame using the LRU strategy
    frameIndex = findLRUVictim(mgmtData->frames, bm->numPages);

    // If a victim frame is found
    if (frameIndex != -1)
    {
        // Write back the victim page if dirty
        if (mgmtData->frames[frameIndex].isDirty)
        {
            // Write the dirty page back to disk
            writePageToFile(bm, &mgmtData->frames[frameIndex]);
            mgmtData->numWriteIO++;
        }

        // Update the victim frame with the new page information
        mgmtData->frames[frameIndex].pageNum = pageNum;
        mgmtData->frames[frameIndex].isDirty = false;
        mgmtData->frames[frameIndex].fixCount = 1;
        mgmtData->frames[frameIndex].data = getPageFromFile(bm, pageNum);
        // mgmtData->numReadIO++;

        // Update page handle with the new page information
        page->pageNum = pageNum;
        page->data = mgmtData->frames[frameIndex].data;

        // Move the page to the front of the LRU list (update accessCount)
        updateLRUList(mgmtData->frames, bm->numPages, frameIndex);

        return RC_OK;
    }

    return RC_NO_AVAILABLE_FRAME;
}

extern RC pinPageUsingLRU_K(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    // Check if buffer pool is not existing
    if (bm == NULL)
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }

    // Get the management data from the buffer pool
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)(bm->mgmtData);
    PAGE_FRAME *frames = mgmtData->frames;

    // Find the target page in the buffer pool
    int frameIndex = findPage_LRU(mgmtData->frames, bm->numPages, pageNum);

    // If the page is found in the buffer pool
    if (frameIndex != -1)
    {
        // Update page handle with existing page information
        frames[frameIndex].data = getPageFromFile(bm, pageNum);

        // Update the page handle with the pinned page information
        page->pageNum = pageNum;
        page->data = frames[frameIndex].data;

        // Update the frame with the new page information
        frames[frameIndex].pageNum = pageNum;
        frames[frameIndex].isDirty = false;
        // Increment fix count
        frames[frameIndex].fixCount++;
        // Move the page to the front of the LRU list (update accessCount)
        updateLRUList(mgmtData->frames, bm->numPages, frameIndex);

        mgmtData->numReadIO++;

        return RC_OK;
    }

    // If the page is not found in the buffer pool
    // Find a victim frame using the LRU strategy
    frameIndex = findLRUVictim(mgmtData->frames, bm->numPages);

    // If a victim frame is found
    if (frameIndex != -1)
    {
        // Write back the victim page if dirty
        if (mgmtData->frames[frameIndex].isDirty)
        {
            writePageToFile(bm, &mgmtData->frames[frameIndex]);
            mgmtData->numWriteIO++;
        }

        // Update the victim frame with the new page information
        mgmtData->frames[frameIndex].pageNum = pageNum;
        mgmtData->frames[frameIndex].isDirty = false;
        mgmtData->frames[frameIndex].fixCount = 1;
        mgmtData->frames[frameIndex].data = getPageFromFile(bm, pageNum);

        // Update page handle with the new page information
        page->pageNum = pageNum;
        page->data = mgmtData->frames[frameIndex].data;

        // Move the page to the front of the LRU list (update accessCount)
        updateLRUList(mgmtData->frames, bm->numPages, frameIndex);

        return RC_OK;
    }

    return RC_NO_AVAILABLE_FRAME;
}