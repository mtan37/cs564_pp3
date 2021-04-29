/***
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_exists_exception.h"
#include "exceptions/bad_scan_param_exception.h"

//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
    // Add your code below. Please do not remove this line.
	
	// determine the specified index file name
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName = idxStr.str();

	// initialization
	this->bufMgr = bufMgrIn;
	this->attributeType = attributeType;
	this->attrByteOffset = attrByteOffset;
	this->scanExecuting = false;	

	try {
		// check if the specified index file exist
		file = new BlobFile(outIndexName, true);
		
		// specified index file did not exist
		// and is created successfully
		this->headerPageNum = file->getFirstPageNo();
		Page * metaPage;
		bufMgr->readPage(file, headerPageNum, metaPage);
		IndexMetaInfo * metaInfo = (IndexMetaInfo *) metaPage;
		metaInfo->attrType = attrType;
		metaInfo->attrByteOffset = attrByteOffset;
		strcpy(metaInfo->relationName, relationName.c_str());

		// set index file root page number
		this->rootPageNum = metaInfo->rootPageNo;
		bufMgr->unPinPage(file, headerPageNum, false);

		// create a root page
		Page * rootPage;
		bufMgr->allocPage(file, rootPageNum, rootPage);
		bufMgr->unPinPage(file, rootPageNum, true);

		// scan the relation
		FileScan * fscan = new FileScan(relationName, bufMgr);
		try {
			RecordId rid;
			while (true) {
				fscan->scanNext(rid);
				
				// scan record
				std::string rstr = fscan->getRecord();
				const char * record = rstr.c_str();

				// scan and construct key
				int key = *((int *)(record + attrByteOffset));
				insertEntry(&key, rid);
			}
		}
		catch (EndOfFileException e) {
			bufMgr->flushFile(file);
		}

		// erase the fscan object
		delete fscan;
	} catch (FileExistsException & e) {
		// open the index file
		file = new BlobFile(outIndexName, false);
	}	

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    // Add your code below. Please do not remove this line.
	bufMgr->clearBufStats();
	bufMgr->flushFile(file);
	delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    // Add your code below. Please do not remove this line.
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    // Add your code below. Please do not remove this line.
    scanExecuting = true;
    
    if (lowOpParm != GT && lowOpParm !=GTE){
        throw BadOpcodesException();
    } else if(highOpParm != LT && highOpParm != LTE){
        throw BadOpcodesException();
    }
    
    lowOp = lowOpParm;
    highOp = highOpParm;
    
    switch (attributeType){
    
        case INTEGER: {
            int lowVal = *(int *)lowValParm;
            int highVal = *(int *)highValParm;
            startScanInt(lowVal, lowOpParm, highVal, highOpParm);            
        break; }
    
        case DOUBLE:{
        std::cout << "Scan is not started: "
            << "Index data type DOUBLE is not implemented" << std::endl;
        scanExecuting = false;
        return; }
    
        case STRING:{
        std::cout << "Scan is not started: "
            << "Index data type STRING is not implemented" << std::endl;
        scanExecuting = false;
        return; }
    
        default:{
        std::cout << "Scan is not started: "
            << "Unkonwn index data type." << std::endl;
        scanExecuting = false;
        return; }
    
    }    

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScanInt
// -----------------------------------------------------------------------------

void BTreeIndex::startScanInt(const int lowVal, const Operator lowOp, const int highVal, const Operator highOp){
    
    if (lowVal > highVal)
        throw BadScanrangeException();
    else if (lowVal == highVal && (lowOp != GTE || highOp != LTE))
        throw BadScanrangeException();
    
    Page rootPage = file->readPage(rootPageNum);
    NonLeafNodeInt* currNode = (NonLeafNodeInt *)&rootPage;
    //traverse to the non-leaf a level above leaf node
    PageId nextPageId = 0;
    LeafNodeInt * leafNode = NULL;

    while (true){
        for (int i = 0; i < currNode->length; i++){
           
            if ((currNode->keyArray)[i] >= lowVal){
                nextPageId = (currNode->pageNoArray)[i];
                break;
            }
        
        }    
        
        if ((currNode->keyArray)[currNode->length - 1] < lowVal){
            nextPageId = (currNode->pageNoArray)[currNode->length];
        }

        if (currNode->level != 1){
            Page pageNode = file->readPage(nextPageId);
            currNode = (NonLeafNodeInt *)&pageNode;
            continue;
        } else {
            //leaf node located
            Page pageNode = file->readPage(nextPageId);
            currentPageData = &pageNode; 
            currentPageNum = nextPageId; 
            leafNode = (LeafNodeInt *)currentPageData;    
            break;
        }

    }
 
    if (leafNode == NULL){
        throw BadScanParamException();
    }

    //find the first record that is equal or greater than the low val
    for (int i = 0; i < leafNode->length; i++){

        if (lowOp == GTE && (leafNode->keyArray)[i] >= lowVal){
            nextEntry = i;
            return;
        }
        else if (lowOp == GT && (leafNode->keyArray)[i] > lowVal){
            nextEntry = i;
            return;
        }

    }
    
    //no matching entry in the range found, end the scan
    throw BadScanParamException();
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
    // Add your code below. Please do not remove this line.
    //load the next record entry
    Page currPage = file->readPage(currentPageNum);
    LeafNodeInt *currNode = (LeafNodeInt *)&currPage;
    RecordId currRid = (currNode->ridArray)[nextEntry];
    
    Page metaPage = file->readPage(headerPageNum);
    IndexMetaInfo *metaData = (IndexMetaInfo *)&metaPage;
    FileScan *scanner = new FileScan(metaData->relationName, bufMgr);
    
    //check if the loaded record satisfy the range condition 
    scanner->scanNext(currRid);
    std::string currRecord = scanner->getRecord();
    int keyVal = *(int *) currRecord.c_str() + attrByteOffset;
    
    //key value validation
    switch (lowOp){
        case GT:{
            if (keyVal <= lowValInt)
                throw IndexScanCompletedException();
        }
        case GTE: {
            if (keyVal < lowValInt)
                throw IndexScanCompletedException();
        }
        default:{
            throw BadScanrangeException();
        }
    }

    switch (highOp){
        case LT:{
            if (keyVal >= highValInt)
                throw IndexScanCompletedException();
        }
        case LTE:{
            if (keyVal > highValInt)
                throw IndexScanCompletedException();
        }
        default:{
            throw BadScanrangeException();
        }
    }   
 
    //move the cursor forward
    if (nextEntry + 1 >= currNode->length){
        currentPageNum = currNode->rightSibPageNo;
        nextEntry = 0;
    } else {
        nextEntry++;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
    // Add your code below. Please do not remove this line.
	if (!scanExecuting) 
		throw ScanNotInitializedException();
	scanExecuting = false;
	bufMgr->unPinPage(file, currentPageNum, false);
}

}
