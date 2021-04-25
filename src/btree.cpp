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

	//
	// setup for insertion
	//

	int keyVal = *(int*)key;	
	Page* rootPage = &(file->readPage(rootPageNum));
	NonLeafNodeInt* rootNode = (NonLeafNodeInt*)rootPage;

	//
	// find appropriate leafnode
	//

	//iterate children to find appropriate leafnode (needs to be written recursive to handle multiple layers)
	// something like while(level != 1) 
	LeafNodeInt* targetNode;
	for (int i = 0; i < sizeof(rootNode->pageNoArray); i++) {
		NonLeafNodeInt* childNode = (NonLeafNodeInt*)(&(file->readPage(rootNode->pageNoArray[i])));
		for (int j = 0; j < sizeof(childNode->keyArray); j++) {
			if (childNode->keyArray[j] == keyVal) {
				targetNode = (LeafNodeInt*)(&(file->readPage(childNode->keyArray[j])));
				goto foundleafnode;
			}
		}
	}
	foundleafnode:

	//
	// insert record
	//

	//check if key belongs in the beginning
	if (targetNode->keyArray[0] >= keyVal) {

		//shift existing keys upwards
		for (int i = 0; i < targetNode->length - 1; i++) {
			targetNode->keyArray[i + 1] = targetNode->keyArray[i];
			targetNode->ridArray[i + 1] = targetNode->ridArray[i];
		}

		//insert key at start
		targetNode->keyArray[0] = keyVal;
		targetNode->ridArray[0] = rid;
		goto finishedinsert;
	}

	//check if key belongs between existing key
	for (int i = 0; i < targetNode->length - 1; i++) {

		//perform the insert if the values align
		if (targetNode->keyArray[i] <= keyVal && targetNode->keyArray[i + 1] >= keyVal) {
			
			//shift existing keys upwards
			for (int j = i; j < targetNode->length - 1; j++) {
				targetNode->keyArray[i + 1] = targetNode->keyArray[j];
				targetNode->ridArray[i + 1] = targetNode->ridArray[j];
			}

			//fill in new value in the gap
			targetNode->keyArray[i] = keyVal;
			targetNode->ridArray[i] = rid;

			goto finishedinsert;
		}
	}

	//got to the end without finding a spot to insert, so key belongs at the end of the node
	targetNode->keyArray[targetNode->length] = keyVal;
	targetNode->ridArray[targetNode->length] = rid;
	targetNode->length++;

	//all three cases will end up concluding here
	finishedinsert:

	//check if node has been filled up after insert
	if (targetNode->length == INTARRAYLEAFSIZE) {

		//split + rebalance

	}
	
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
