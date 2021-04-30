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
	this->attributeType = attrType;
	this->attrByteOffset = attrByteOffset;
	this->scanExecuting = false;	

	try {
		// check if the specified index file exist
		file = new BlobFile(outIndexName, true);
		
		// specified index file did not exist
		// and is created successfully
		Page *metaPage;
		bufMgr->allocPage(file, headerPageNum, metaPage);
		IndexMetaInfo * metaInfo = (IndexMetaInfo *) metaPage;
		metaInfo->attrType = attrType;
		metaInfo->attrByteOffset = attrByteOffset;
		strncpy(metaInfo->relationName, relationName.c_str(), 20);

		// create a root page
		Page *rootPage;
		bufMgr->allocPage(file, rootPageNum, rootPage);
		LeafNodeInt *rootNode = (LeafNodeInt *)rootPage;
        rootNode->length = 0;
        rootNode->rightSibPageNo = Page::INVALID_NUMBER;
        isRootLeaf = true;

        bufMgr->unPinPage(file, rootPageNum, true);
        rootPage = NULL;
        rootNode = NULL;
		
        // set index file root page number
        metaInfo->rootPageNo = this->rootPageNum;
		bufMgr->unPinPage(file, headerPageNum, true);
        metaPage = NULL;
        metaInfo = NULL;

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
		catch (EndOfFileException &e) {
			bufMgr->flushFile(file);
		}

		// erase the fscan object
		delete fscan;
	} catch (FileExistsException & e) {
		// open the index file
        // pull varible from File
		file = new BlobFile(outIndexName, false);
        this->headerPageNum = file->getFirstPageNo();
		Page *metaPage;
		bufMgr->allocPage(file, headerPageNum, metaPage);
		IndexMetaInfo * metaInfo = (IndexMetaInfo *) metaPage;
		this->attributeType = metaInfo->attrType;
		this->attrByteOffset = metaInfo->attrByteOffset;
	    this->rootPageNum = metaInfo->rootPageNo; 
		bufMgr->unPinPage(file, headerPageNum, true);
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
// BTreeIndex::splitNonLeafNodeWithNewKey
// -----------------------------------------------------------------------------
void BTreeIndex::splitNonLeafNodeWithNewKey(PageId nfId, PageId &newNodeNumL, PageId &newNodeNumR, int &newKey){
    
    Page *page = NULL;
    bufMgr->readPage(file, nfId, page);
    NonLeafNodeInt *nf = (NonLeafNodeInt*) page;
    
    if (nf->length < INTARRAYNONLEAFSIZE){
        // do nothing, the node is not full yet
        std::cout << "split function called but nothing was done: The node is not full" << std::endl;
        return;
    }

    Page *newPageR;
    newKey = nf->keyArray[INTARRAYNONLEAFSIZE/2];
    bufMgr->allocPage(file, newNodeNumR, newPageR); 
    NonLeafNodeInt *newNodeR = (NonLeafNodeInt *)newPageR;
    
    // copy into right node
    // note that the pushed up key is not kept
    int newNodeLength = 0;
    for (int i = INTARRAYNONLEAFSIZE/2 + 1; i < INTARRAYNONLEAFSIZE; i++){
        newNodeR->keyArray[newNodeLength] = nf->keyArray[i]; 
        newNodeR->pageNoArray[newNodeLength] = nf->pageNoArray[i]; 
        newNodeLength = newNodeLength + 1;
    }
    newNodeR->pageNoArray[newNodeLength] = nf->pageNoArray[INTARRAYNONLEAFSIZE];
    newNodeR->length = newNodeLength;   
    newNodeR->level = nf->level;
    
    nf->length = INTARRAYNONLEAFSIZE/2;   
    
    bufMgr->unPinPage(file, nfId, true); 
    bufMgr->unPinPage(file, newNodeNumR, true);
    newPageR = NULL;
    newNodeR = NULL; 
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNodeWithNewKey
// -----------------------------------------------------------------------------
void BTreeIndex::splitLeafNodeWithNewKey(const PageId pageId,PageId &newNodeNumL, PageId &newNodeNumR, int &newKey){
    
    Page *page = NULL;
    bufMgr->readPage(file, pageId, page);
    LeafNodeInt *leaf = (LeafNodeInt*) page;
    
    if (leaf->length < INTARRAYLEAFSIZE){
        // do nothing, the node is not full yet
        std::cout << "split function called but nothing was done: The node is not full" << std::endl;
        return;
    }

    Page *newPageR = NULL;
    newNodeNumR = Page::INVALID_NUMBER; 
    newKey = leaf->keyArray[INTARRAYLEAFSIZE/2];
    
    bufMgr->allocPage(file, newNodeNumR, newPageR); 
    LeafNodeInt *newNodeR = (LeafNodeInt *)newPageR;
    
    // copy the key array from the original node into the new node
    // copy into right node
    int newNodeLength = 0;
    for (int i = INTARRAYLEAFSIZE/2; i < INTARRAYLEAFSIZE; i++){
        newNodeR->keyArray[newNodeLength] = leaf->keyArray[i]; 
        newNodeR->ridArray[newNodeLength] = leaf->ridArray[i]; 
        newNodeLength = newNodeLength + 1;
    }
    newNodeR->rightSibPageNo = leaf->rightSibPageNo;
    newNodeR->length = newNodeLength;   

    // change the original node to left node
    leaf->rightSibPageNo = newNodeNumR;
    leaf->length = INTARRAYLEAFSIZE/2; 

    // unpin the new pages
    bufMgr->unPinPage(file, newNodeNumR, true);
    newPageR = NULL;
    newNodeR = NULL; 
    
    newNodeNumL = pageId;
    bufMgr->unPinPage(file, pageId, true);
}   

// -----------------------------------------------------------------------------
// BTreeIndex::insertNewKeyInNonLeaf
// -----------------------------------------------------------------------------
void BTreeIndex::insertNewKeyInNonLeaf(NonLeafNodeInt* node, const int key, const PageId pageNumL, const PageId pageNumR){
    if (node->length >= INTARRAYNONLEAFSIZE){
        // do nothing, the node is full!
        std::cout << "non-leaf insertoin function was called but nothing was done: The node is full" << std::endl;
        return;
    }

    // find the appropriate new index for key
    int newKeyIndex = node->length;
    for (int i = 0; i < node->length; i++){
        if (node->keyArray[i] >= key){
            newKeyIndex = i;
            break;
        }
    }
    
    // shift the keys on and from the right of the new index up
    for (int i = node->length - 1 ; i >= newKeyIndex; i--){
        node->keyArray[i + 1] = node->keyArray[i];
        node->pageNoArray[i + 2] = node->pageNoArray[i+1];
    }
    
    node->keyArray[newKeyIndex] = key;
    node->pageNoArray[newKeyIndex] = pageNumL;
    node->pageNoArray[newKeyIndex + 1] = pageNumR;// overide the original page
    node->length = node->length + 1;

}

// -----------------------------------------------------------------------------
// BTreeIndex::insertNewRInLeaf
// -----------------------------------------------------------------------------
void BTreeIndex::insertNewRInLeaf(LeafNodeInt* leaf, const int key, const RecordId rid){
    if (leaf->length >= INTARRAYLEAFSIZE){
        // do nothing, the node is full!
        std::cout << "leaf insertoin function was called but nothing was done: The node is full" << std::endl;
        return;
    }

    // find the appropriate new index for key
    int newKeyIndex = leaf->length;
    for (int i = 0; i < leaf->length; i++){
        if (leaf->keyArray[i] >= key){
            newKeyIndex = i;
            break;
        }
    }        
    // shift the keys on and from the right of the new index up
    for (int i = leaf->length - 1 ; i >= newKeyIndex; i--){
        leaf->keyArray[i + 1] = leaf->keyArray[i];
        leaf->ridArray[i + 1] = leaf->ridArray[i];
    }
    
    leaf->keyArray[newKeyIndex] = key;
    leaf->ridArray[newKeyIndex] = rid;
    leaf->length = leaf->length + 1;
}   

// -----------------------------------------------------------------------------
// BTreeIndex::insertToNonLeaf
// -----------------------------------------------------------------------------
void BTreeIndex::insertToNonLeaf(PageId pageId, int keyVal, const RecordId rid,bool &hasNewKey, PageId &newPageNumL, PageId &newPageNumR, int &newKey)
{
        Page* page;
        PageId insertPageId = Page::INVALID_NUMBER;
        bufMgr->readPage(file, pageId, page);
        NonLeafNodeInt* node = (NonLeafNodeInt*)page;
        hasNewKey = false;
        
        // if this non-leaf is full, split
        if (node->length >= INTARRAYNONLEAFSIZE) {
            bufMgr->unPinPage(file, pageId, true);
            hasNewKey = true;
            node = NULL;
            page = NULL;       
            
            splitNonLeafNodeWithNewKey(pageId, newPageNumL, newPageNumR, newKey);             
            
            if (newKey < keyVal){
                bufMgr->readPage(file, newPageNumR, page);
                pageId = newPageNumR;
                node = (NonLeafNodeInt*)page;
            } else {
                bufMgr->readPage(file, newPageNumL, page);
                pageId = newPageNumL;
                node = (NonLeafNodeInt*)page;
            }
        
        }

        // traverse non-leaf node to find next page id
        if (node->keyArray[node->length -1] < keyVal){
            insertPageId = node->pageNoArray[node->length];
        } else {
            for (int i = 0; i < node->length; i++){
                if (node->keyArray[i] > keyVal){
                    insertPageId = node->pageNoArray[i];
                    break;
                }
            }
        }
    
        bool hasPropagatedKey = false;
        int propagatedKey = false;
        PageId propagatedPageL = Page::INVALID_NUMBER;
        PageId propagatedPageR = Page::INVALID_NUMBER;
        if (node->level == 1){// next level is a leaf        
            insertToLeaf(insertPageId, keyVal, rid, hasPropagatedKey, propagatedPageL, propagatedPageR, propagatedKey);
        } else {
            insertToNonLeaf(insertPageId, keyVal, rid, hasPropagatedKey, propagatedPageL, propagatedPageR, propagatedKey);
        }

        // add propagated new key to the node
        if (hasPropagatedKey) {
            insertNewKeyInNonLeaf(node, propagatedKey, propagatedPageL, propagatedPageR);
        }
        
        bufMgr->unPinPage(file, pageId, true);
        node = NULL;
        page = NULL;       
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertToLeaf
// -----------------------------------------------------------------------------
void BTreeIndex::insertToLeaf(PageId pageId, int keyVal, const RecordId rid, bool &hasNewKey, PageId &newPageNumL, PageId &newPageNumR, int &newKey)
{
        Page* page;
        PageId insertPageId = pageId;
        bufMgr->readPage(file, insertPageId, page);
        LeafNodeInt* leaf = (LeafNodeInt*)page;
        hasNewKey = false;
        
        // if this is a full leaf, split
        if (leaf->length >= INTARRAYLEAFSIZE){
            
            bufMgr->unPinPage(file, pageId, true);
            page = NULL;
            leaf = NULL;           
 
            splitLeafNodeWithNewKey(pageId, newPageNumL, newPageNumR, newKey);
            hasNewKey = true; 
            //bufMgr->disposePage(file, pageId);// delete the old page

            if (keyVal < newKey){
                bufMgr->readPage(file, newPageNumL, page);
                insertPageId = newPageNumL;
                leaf = (LeafNodeInt*)page;
            } else {
                bufMgr->readPage(file, newPageNumR, page);
                insertPageId = newPageNumR;
                leaf = (LeafNodeInt*)page;
            }

        }
    
        insertNewRInLeaf(leaf, keyVal, rid);
        bufMgr->unPinPage(file, insertPageId, true);
        page = NULL;
        leaf = NULL;           
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    int keyVal = *(int*)key;	
 
    // root node is a leaf node
    if (isRootLeaf){
        Page* rootPage;
        bufMgr->readPage(file, rootPageNum, rootPage);
        LeafNodeInt* rootNode = (LeafNodeInt*)rootPage;
        // if root node is an empty leaf
        if (rootNode->length == 0) {
            rootNode->keyArray[0] = keyVal;
            rootNode->ridArray[0] = rid;
            rootNode->length = 1;
            // done
            bufMgr->unPinPage(file, rootPageNum, true);
            rootPage = NULL;
            rootNode = NULL;
            return; 
        }
        bufMgr->unPinPage(file, rootPageNum, true);
        rootPage = NULL;
        rootNode = NULL;
    }    
  
    PageId newPageNumL = Page::INVALID_NUMBER;
    PageId newPageNumR = Page::INVALID_NUMBER;
    int newKey = 0;
    bool hasNewKey = false;
    if (isRootLeaf){
        insertToLeaf(rootPageNum, keyVal, rid, hasNewKey, newPageNumL, newPageNumR, newKey); 
        if (hasNewKey){
                isRootLeaf = false;
                PageId newPageNumL = rootPageNum;
                // create new root page with a new root number
                Page* page;
                rootPageNum = Page::INVALID_NUMBER;
                bufMgr->allocPage(file, rootPageNum, page);
                 
                // reset current root to nonLeafInt
                NonLeafNodeInt *newNode = (NonLeafNodeInt *)page;   
                newNode->length = 1;
                newNode->level = 1;
                newNode->keyArray[0] = newKey;
                newNode->pageNoArray[0] = newPageNumL;
                newNode->pageNoArray[1] = newPageNumR;
                bufMgr->unPinPage(file, rootPageNum, true);
                page = NULL;
                newNode = NULL;
        }

    } else {
        insertToNonLeaf(rootPageNum, keyVal, rid, hasNewKey, newPageNumL, newPageNumR, newKey);  
        if (hasNewKey){
            Page* page;
            rootPageNum = Page::INVALID_NUMBER;
            bufMgr->allocPage(file, rootPageNum, page);
            NonLeafNodeInt *rootNode = (NonLeafNodeInt *)page;   
            rootNode->length = 1;
            rootNode->keyArray[0] = newKey;
            rootNode->pageNoArray[0] = newPageNumL;
            rootNode->pageNoArray[1] = newPageNumR;
            bufMgr->unPinPage(file, rootPageNum, true);
            page = NULL;
            rootNode = NULL;
        }
    }

    // update metapage
    Page *metaPage;
    bufMgr->allocPage(file, headerPageNum, metaPage);
    IndexMetaInfo * metaInfo = (IndexMetaInfo *) metaPage;
    metaInfo->rootPageNo = rootPageNum; 
    bufMgr->unPinPage(file, headerPageNum, true);
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
            lowValInt = *(int *)lowValParm;
            highValInt = *(int *)highValParm;
            startScanInt();            
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
// BTreeIndex::scanLeaf
// -----------------------------------------------------------------------------
void BTreeIndex::scanLeaf(PageId leafId, int &keyIndex, bool &found){
    if (leafId == Page::INVALID_NUMBER){
        found = false;
        return;
    }
    
    Page *page = NULL;
    bufMgr->readPage(file, leafId, page);
    LeafNodeInt *leaf = (LeafNodeInt*) page;
    found = false;
    int searchVal = lowValInt;
    if (lowOp == GT){
        searchVal = lowValInt + 1;
    }

    // return the first value that is equal to or greater than the search value
    for (int i = 0; i < leaf->length; i++){
        if ((leaf->keyArray)[i] >= searchVal){
            keyIndex = i;
            found = true;         
            break;
        }
    }
     
    bufMgr->unPinPage(file, leafId, false);
    page = NULL;
    leaf = NULL;
}
// -----------------------------------------------------------------------------
// BTreeIndex::scanRecursiveHelper
// -----------------------------------------------------------------------------

void BTreeIndex::scanRecursiveHelper(PageId pageId, PageId &leaf){
    Page *page = NULL;
    bufMgr->readPage(file, pageId, page);
    NonLeafNodeInt *node = (NonLeafNodeInt *)page;
    PageId nextLevel = Page::INVALID_NUMBER;
    int searchVal = lowValInt;
    
    if (lowOp == GT){
        searchVal = lowValInt + 1;
    }

    for (int i = 0; i < node->length; i++){
        if ((node->keyArray)[i] > searchVal){
            nextLevel = node->pageNoArray[i];
            break;
        }
    }

    if (nextLevel == Page::INVALID_NUMBER){
        nextLevel = node->pageNoArray[node->length - 1];
    }

    int level = node->level;
    bufMgr->unPinPage(file, pageId, false);
    node = NULL;
    page = NULL;
    
    if (level == 1){ //next level is a leaf
        leaf = nextLevel;
    } else { //recursive case
        scanRecursiveHelper(nextLevel, leaf);
    }
    
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScanInt
// -----------------------------------------------------------------------------

void BTreeIndex::startScanInt(){
    
    if (lowValInt > highValInt)
        throw BadScanrangeException();
    else if (lowValInt == highValInt && (lowOp != GTE || highOp != LTE))
        throw BadScanrangeException();
    
    PageId leaf;
    bool found = false;
    
    // if the root is leaf, just scan it 
    if (isRootLeaf){
        scanLeaf(rootPageNum, nextEntry, found); 
        currentPageNum = rootPageNum;        
    } else {
        scanRecursiveHelper(rootPageNum, leaf);
        scanLeaf(leaf, nextEntry, found);
        currentPageNum = leaf;         
    }

    if (!found) {
        scanExecuting = false;
        std::cout << "Scan finished: "
            << "No matching value found within the range " << 
            lowValInt << "to " << highValInt << std::endl;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
    // Add your code below. Please do not remove this line.
	if (!scanExecuting) 
		throw ScanNotInitializedException();
    
    if (currentPageNum == Page::INVALID_NUMBER){
        throw IndexScanCompletedException();
    }   

    //load the next record entry
    Page *currPage;
    bufMgr->readPage(file, currentPageNum, currPage);
    LeafNodeInt *currNode = (LeafNodeInt *)currPage;
    int keyVal = (currNode->keyArray)[nextEntry];
    
    //key value validation
    switch (lowOp){
        case GT:{
            if (keyVal <= lowValInt){
                bufMgr->unPinPage(file, currentPageNum, false);   
                throw IndexScanCompletedException();
            }
            break;
        }
        case GTE: {
            if (keyVal < lowValInt){
                bufMgr->unPinPage(file, currentPageNum, false);   
                throw IndexScanCompletedException();
            }
            break;
        }
        default:{
            throw BadScanrangeException();
        }
    }

    switch (highOp){
        case LT:{
            if (keyVal >= highValInt){
                bufMgr->unPinPage(file, currentPageNum, false);   
                throw IndexScanCompletedException();
            }
            break;
        }
        case LTE:{
            if (keyVal > highValInt){
                bufMgr->unPinPage(file, currentPageNum, false);   
                throw IndexScanCompletedException();
            }
            break;
        }
        default:{
            throw BadScanrangeException();
        }
    }   

    outRid = (currNode->ridArray)[nextEntry]; 

    //move the cursor forward
    nextEntry++;
    PageId nextPageId = currNode->rightSibPageNo;
    int currNodeLeng = currNode->length;
    bufMgr->unPinPage(file, currentPageNum, false);   
    currPage = NULL;
    currNode = NULL;
    
    if (nextEntry >= currNodeLeng){
        currentPageNum = nextPageId;
        nextEntry = 0;
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
}

}
