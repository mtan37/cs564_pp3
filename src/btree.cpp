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
// BTreeIndex:belongsBefore -- helper method for insert
// -----------------------------------------------------------------------------

bool BTreeIndex::belongsBefore(int key, int sortedKeyList[]) {
	return sortedKeyList[0] >= key;
}

// -----------------------------------------------------------------------------
// BTreeIndex:belongsAfter -- helper method for insert
// -----------------------------------------------------------------------------

bool BTreeIndex::belongsAfter(int key, int sortedKeyList[], int keyListLength) {
	return sortedKeyList[keyListLength - 1] <= key;
}

// -----------------------------------------------------------------------------
// BTreeIndex:belongsInRange -- helper method for insert
// -----------------------------------------------------------------------------

int BTreeIndex::belongsInRange(int key, int sortedKeyList[], int keyListLength) {
	
	if (belongsBefore(key, sortedKeyList) || belongsAfter(key, sortedKeyList, keyListLength)) {
		return -1;
	}

	for (int i = 0; i < keyListLength; i++) {
		if (sortedKeyList[i] <= key && sortedKeyList[i + 1] >= key) {
			return i;
		}
	}

	return -1;
}

// -----------------------------------------------------------------------------
// BTreeIndex:insertInNode -- helper method for insert
// -----------------------------------------------------------------------------

void BTreeIndex::insertInNode(LeafNodeInt* nodeRef, int key, RecordId rid) {

	//check if key belongs in the beginning
	if (belongsBefore(key, nodeRef->keyArray)) {

		//shift existing keys upwards
		for (int i = 0; i < nodeRef->length - 1; i++) {
			nodeRef->keyArray[i + 1] = nodeRef->keyArray[i];
			nodeRef->ridArray[i + 1] = nodeRef->ridArray[i];
		}

		//insert key at first index
		nodeRef->keyArray[0] = key;
		nodeRef->ridArray[0] = rid;
		nodeRef->length++;
		return;
	}

	//check if key belongs between existing keys
	int index = belongsInRange(key, nodeRef->keyArray, nodeRef->length);
	if (index != -1) {

		//shift existing keys upwards
		for (int j = index; j < nodeRef->length - 1; j++) {
			nodeRef->keyArray[index + 1] = nodeRef->keyArray[j];
			nodeRef->ridArray[index + 1] = nodeRef->ridArray[j];
		}

		//fill in new value in the gap
		nodeRef->keyArray[index] = key;
		nodeRef->ridArray[index] = rid;
		nodeRef->length++;
		return;
	}

	//got to the end without finding a spot to insert, so key belongs at the end of the node
	nodeRef->keyArray[nodeRef->length] = key;
	nodeRef->ridArray[nodeRef->length] = rid;
	nodeRef->length++;
}

// -----------------------------------------------------------------------------
// BTreeIndex:splitBetweenNodes -- helper method for insert
// -----------------------------------------------------------------------------

void BTreeIndex::splitBetweenNodes(LeafNodeInt* fullNode, LeafNodeInt* newNode, int newKey, RecordId newRid) {

	//split keys between two nodes
	for (int i = 0; i < INTARRAYLEAFSIZE / 2; i++) {
		
		//move keys to new node
		newNode->keyArray[i] = fullNode->keyArray[i];
		newNode->ridArray[i] = fullNode->ridArray[i];
		newNode->length++;
		
		//fill missing space with remaining keys
		fullNode->keyArray[i] = fullNode->keyArray[(INTARRAYLEAFSIZE / 2) + i];
		fullNode->ridArray[i] = fullNode->ridArray[(INTARRAYLEAFSIZE / 2) + i];
		fullNode->length--;
	}

	//insert the new value into the newly created node
	insertInNode(newNode, newKey, newRid);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    // Add your code below. Please do not remove this line.

	//TODO handle edge cases where the tree hasn't yet been filled up (need to check instructions to see expectations)

	//
	// SETUP FOR INSERTION
	//

	int keyVal = *(int*)key;	
	Page* rootPage = &(file->readPage(rootPageNum));
	NonLeafNodeInt* rootNode = (NonLeafNodeInt*)rootPage;

	//
	// TRAVERSE FROM ROOT TO FIND LEAF NODE
	//

	NonLeafNodeInt* nextNode = rootNode;

	//go until reaching a level 1 node (the NonLeafNode above a LeafNode)
	continuewhile:
	while (nextNode->level != 1) {

		//leftmost case (key falls before beginning of key list)
		if (belongsBefore(keyVal, nextNode->keyArray)) {
			nextNode = (NonLeafNodeInt*)(&(file->readPage(nextNode->pageNoArray[0])));
			goto continuewhile;
		}

		//middle case
		int index = belongsInRange(keyVal, nextNode->keyArray, nextNode->length);
		if (index != -1) {
			nextNode = (NonLeafNodeInt*)(&(file->readPage(nextNode->pageNoArray[index])));
			goto continuewhile;
		}

		//rightmost case (key falls at the end of the key list)
		nextNode = (NonLeafNodeInt*)(&(file->readPage(nextNode->pageNoArray[nextNode->length - 1])));
		goto continuewhile;
	}

	//get LeafNode (targetNode) from level 1 NonLeafNode (nextNode)

	LeafNodeInt* targetNode;

	//leftmost case (key falls before beginning of key list)
	if (belongsBefore(keyVal, nextNode->keyArray)) {
		targetNode = (LeafNodeInt*)(&(file->readPage(nextNode->pageNoArray[0])));
		goto foundleafnode;
	}

	//middle case
	int index = belongsInRange(keyVal, nextNode->keyArray, nextNode->length);
	if (index != -1) {
		targetNode = (LeafNodeInt*)(&(file->readPage(nextNode->pageNoArray[index])));
		goto foundleafnode;
	}

	//rightmost case (key falls at the end of the key list)
	targetNode = (LeafNodeInt*)(&(file->readPage(nextNode->pageNoArray[nextNode->length - 1])));

	foundleafnode:

	//
	// INSERT RECORD INTO LEAF NODE
	//

	//case where no need to rebalance
	if (targetNode->length < INTARRAYLEAFSIZE) {

		insertInNode(targetNode, keyVal, rid);

		finishedinsert:

		return; //all done :)

	} else if (targetNode->length == INTARRAYLEAFSIZE) {
		//
		// REBALANCE AND MERGE
		//

		PageId newLeafId;
		Page* newLeaf = &(file->allocatePage(newLeafId));
		LeafNodeInt* newLeafNode = (LeafNodeInt*)newLeaf;

		Page* curLeafPage = (Page*)targetNode;
		int curPageNo = curLeafPage->page_number();

		int midLeftIndex = (INTARRAYLEAFSIZE / 2) - 1;
		int midRightIndex = (INTARRAYLEAFSIZE) / 2;

		//key falls on left half, newLeaf is splitting off to the left
		if (keyVal < midLeftIndex) {

			//readjust pointers
			//TODO need to set left page rightNo to newLeaf
			newLeafNode->rightSibPageNo = curPageNo;

			//TODO push up key
			int midKey = targetNode->keyArray[midLeftIndex];

			splitBetweenNodes(targetNode, newLeafNode, keyVal, rid);

			goto postsplit;
		}

		//key falls on right half, newLeaf is splitting off to the right
		if (keyVal > midRightIndex) {
			
			//readjust pointers
			int oldRightNo = targetNode->rightSibPageNo;
			targetNode->rightSibPageNo = newLeafId;
			newLeafNode->rightSibPageNo = oldRightNo;

			//TODO push up key
			int midKey = targetNode->keyArray[midRightIndex];

			splitBetweenNodes(targetNode, newLeafNode, keyVal, rid);

			goto postsplit;
		}

		//key falls in the center, choosing to split newLeaf to the left 
		if (keyVal >= midLeftIndex && keyVal <= midRightIndex) {

			//readjust pointers
			//TODO need to set left page rightNo to newLeaf
			newLeafNode->rightSibPageNo = curPageNo;

			//TODO push up key
			int midKey = keyVal;

			splitBetweenNodes(targetNode, newLeafNode, keyVal, rid);

			goto postsplit;
		}

		postsplit:

		//TODO handle recursive non-leaf rebalancing

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
