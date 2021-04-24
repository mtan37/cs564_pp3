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
	int targetPageNum = rid.page_number;
	int targetSlotNum = rid.slot_number;
	
	Page* rootPage = &(file->readPage(rootPageNum));
	NonLeafNodeInt* rootNode = (NonLeafNodeInt*)rootPage;

	//iterate children to find appropriate leafnode (needs to be written recursive to handle multiple layers)
	LeafNodeInt* targetNode;
	outer:for (int i = 0; i < sizeof(rootNode->pageNoArray); i++) {
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
	if (sizeof(targetNode->keyArray) == INTARRAYLEAFSIZE) {

	} else { //not full node

	}

	//handle rebalancing



	//test

	rootPageNum; //PageID
	headerPageNum; //PageID

	Page headerPage = file->readPage(headerPageNum);

	//BlobFile indexFile = ;

	IndexMetaInfo metaInfo = (IndexMetaInfo)headerPage;
	FileScan fs(metaInfo.relationName, bufMgr);
	RecordId nextRecord;
	fs.scanNext(nextRecord);
	
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
