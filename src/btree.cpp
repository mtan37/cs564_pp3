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
		// open the index file - TODO
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
	if (!scanExecuting) 
		throw ScanNotInitializedException();
	scanExecuting = false;
	bufMgr->unPinPage(file, currentPageNum, false);
}

}
