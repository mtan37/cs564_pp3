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
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    // Add your code below. Please do not remove this line.
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
        //TODO throw exeception, something wrong happened
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
    //TODO
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
    // Add your code below. Please do not remove this line.
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
    // Add your code below. Please do not remove this line.
}

}
