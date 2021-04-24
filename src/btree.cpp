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
	// my part to work on

	int keyVal = *(int*)key;
	//int targetPageNum = rid.page_number;
	//int targetSlotNum = rid.slot_number;
	
	Page* rootPage = &(file->readPage(rootPageNum));
	NonLeafNodeInt* rootNode = (NonLeafNodeInt*)rootPage;

	//iterate children to find appropriate leafnode (needs to be written recursive to handle multiple layers)
	// something like while(level != 1) 
	LeafNodeInt* targetNode;
	for (int i = 0; i < sizeof(rootNode->pageNoArray); i++) {
		NonLeafNodeInt* childNode = (NonLeafNodeInt*)(&(file->readPage(rootNode->pageNoArray[i])));
		for (int j = 0; j < sizeof(childNode->keyArray); j++) {
			if (childNode->keyArray[j] == keyVal) {
				targetNode = (LeafNodeInt*)(&(file->readPage(childNode->keyArray[j])));
				goto loopend;
			}
		}
	}
	loopend:

	//insert record
	//full node
	if (targetNode->length == INTARRAYLEAFSIZE) {

		//split

	} else { //not full node, no need to split
		
		//find where to insert new key
		for (int i = 0; i < targetNode->length - 1; i++) {
			if (targetNode->keyArray[i] <= keyVal && targetNode->keyArray[i + 1] >= keyVal) {
				
				//shift existing keys upwards
				for (int j = i; j < targetNode->length - 1; j++) {
					targetNode->keyArray[i + 1] = targetNode->keyArray[j];
					targetNode->ridArray[i + 1] = targetNode->ridArray[j];
				}
				targetNode->keyArray[i] = keyVal;
				targetNode->ridArray[i] = rid;
			}
		}
		loopend2:
		targetNode->keyArray[targetNode->length] = keyVal;
		targetNode->ridArray[targetNode->length] = rid;
		targetNode->length++;
	}

	//handle rebalancing



	//test stuff below here

	//rootPageNum; //PageID
	//headerPageNum; //PageID

	//Page headerPage = file->readPage(headerPageNum);

	//BlobFile indexFile = ;

	//IndexMetaInfo metaInfo = (IndexMetaInfo)headerPage;
	//FileScan fs(metaInfo.relationName, bufMgr);
	//RecordId nextRecord;
	//fs.scanNext(nextRecord);
	
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
