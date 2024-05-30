#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
// #include "helper.c"

SM_FileHandle *fileHandle;

/* manipulating page files */
void initStorageManager(void)
{
    // Initialize the fileHandle attributes to default values
    // resetHandle(fileHandle);
}

RC createPageFile(char *fileName)
{
    // Create a file pointer and open the file in write in binary mode
    FILE *file = fopen(fileName, "wb");
    // If the file is non existent, return RC_FILE_NOT_FOUND
    if (file == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }

    // Create a new page of size PAGE_SIZE
    char *emptyPage = malloc(PAGE_SIZE * sizeof(char));
    // If the page is not created, return RC_WRITE_FAILED
    if (emptyPage == NULL)
    {
        // Defensive Check: Close the file to avoid memory leaks
        fclose(file);
        return RC_WRITE_FAILED;
    }
    // Fill the page with 0's as it is a new page
    memset(emptyPage, 0, PAGE_SIZE);
    // Write the page to the file
    size_t writeSize = fwrite(emptyPage, sizeof(char), PAGE_SIZE, file);
    // If the write is not successful, return RC_WRITE_FAILED
    if (writeSize < PAGE_SIZE)
    {
        // Defensive Check: Close the file and free the page to avoid memory leaks
        fclose(file);
        free(emptyPage);
        return RC_WRITE_FAILED;
    }

    // Close the file and free the page to avoid memory leaks
    fclose(file);
    free(emptyPage);

    // Return RC_OK if the file is created successfully
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    // If the fileHandle is not initialized, return RC_FILE_HANDLE_NOT_INIT
    if (fHandle == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    //  Open the file in read and write(+) in binary mode
    fHandle->mgmtInfo = fopen(fileName, "rb+"); // Storing File pointer in mgmtInfo
    // If the file is non existent, return RC_FILE_NOT_FOUND
    if (fHandle->mgmtInfo == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }

    // Assign the fileHandle attributes to the values of the file
    fHandle->fileName = fileName;

    fseek(fHandle->mgmtInfo, 0, SEEK_END); // Move the file pointer to the end of the file
    long fileSize = ftell(fHandle->mgmtInfo);

    fHandle->totalNumPages = fileSize / PAGE_SIZE; // The total number of pages is the size of the file divided by the page size
    fHandle->curPagePos = 0;                       // The current page position is set to the beginning of the file

    // Return RC_OK if the file is opened successfully
    return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle)
{
    // If the fileHandle or its fileName is not initialized, return RC_FILE_HANDLE_NOT_INIT
    if (fHandle == NULL || fHandle->fileName == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    fclose(fHandle->mgmtInfo); // Close the fileHandle

    // Return RC_OK if the file is closed successfully
    return RC_OK;
}

RC destroyPageFile(char *fileName)
{
    // Remove the file from the disk
    int destroyStatus = remove(fileName);
    // If the file is non existent, return RC_FILE_NOT_FOUND
    if (destroyStatus != 0)
    {
        return RC_FILE_NOT_FOUND;
    }

    // Return RC_OK if the file is destroyed successfully
    return RC_OK;
}

/* reading blocks from disc */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    FILE *fp = fopen(fHandle->fileName, "r+");
    size_t status = fseek(fp, pageNum * PAGE_SIZE, SEEK_SET);
    if (status != 0)
    {
        // return read error
        return RC_READ_NON_EXISTING_PAGE;
    }
    fread(memPage, sizeof(char), PAGE_SIZE, fp);
    fHandle->curPagePos = ftell(fp);
    fclose(fp);
    return RC_OK;
}

int getBlockPos(SM_FileHandle *fHandle)
{
    return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock((fHandle->curPagePos) - 1, fHandle, memPage);
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock((fHandle->curPagePos) + 1, fHandle, memPage);
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

// Write page to a disk using absolute position
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    FILE *file = fopen(fHandle->fileName, "r+");
    RC rc;
    if (file == NULL)
        rc = RC_FILE_NOT_FOUND;
    if (fHandle == NULL)
        rc = RC_FILE_HANDLE_NOT_INIT;
    if (pageNum > fHandle->totalNumPages || pageNum < 0)
        rc = RC_WRITE_FAILED;
    if (fseek(file, (pageNum)*PAGE_SIZE, SEEK_SET) == 0)
    {
        fwrite(memPage, 1, PAGE_SIZE, file);
        fHandle->curPagePos = pageNum;
        rc = RC_OK;
    }
    else if (fseek(file, (pageNum)*PAGE_SIZE, SEEK_SET) != 0)
        rc = RC_WRITE_FAILED;

    fclose(file);
    return rc;
}

// Write page to a disk using current position
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC rc, cp;
    if (fHandle == NULL)
        rc = RC_FILE_HANDLE_NOT_INIT;

    else
        cp = getBlockPos(fHandle);
    rc = writeBlock(cp, fHandle, memPage);

    return rc;
}

// Increase number of pages in file by one
RC appendEmptyBlock(SM_FileHandle *fHandle)
{
    if (fHandle == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    else
    {
        FILE *file = fopen(fHandle->fileName, "r+");
        fseek(file, 0, SEEK_END);
        fHandle->totalNumPages = fHandle->totalNumPages + 1;
        char *c = (char *)calloc(PAGE_SIZE, sizeof(char));
        fwrite(c, sizeof(char), PAGE_SIZE, file);
        fclose(file);
        return RC_OK;
    }
}

// Ensuring file has appropriate number of pages
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
    RC retcod; // CREATES RETCOD OF TYPE RETURN CODE(RC)

    if (fHandle != NULL) // CHECKS IF FILEHANDLE IS INITITALIZED OR NOT
    {
        if (numberOfPages > fHandle->totalNumPages) // CHECKS IF NUMBER OF PAGES IS GREATER THAN FILE'S PAGES
        {
            while ((numberOfPages - (fHandle->totalNumPages)) > 0) // USES WHILE LOOP TO APPEND CORRECT NUMBER OF TIMES
                appendEmptyBlock(fHandle);                         // APPEND EMPTY BLOCKS/NEW PAGES UNTIL THE DIFFERENCE IS NULLIFIED
        }
        retcod = RC_OK; // RC_OK IS THE RETURN CODE FOR SUCCESSFUL METHOD CALL
    }
    else
    {
        retcod = RC_FILE_HANDLE_NOT_INIT; // RC_FILE_HANDLE_NOT_INIT IF THE RETURN CODE IF FILE HANDLE IS NOT INITIALIZED
    }

    return retcod; // RETURNS RETURN CODE
}
