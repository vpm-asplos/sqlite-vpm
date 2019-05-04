// find or create the data structure in pheap and return the pointer...
// resembles the behavior if NULL is passed to sqlite3BTreeOpen
int pdb_sqlite3BtreeOpen(sqlite3 *db, Btree **ppBtree, int flags,
			 unsigned long pbrk_start) {
  BtShared *pBt = 0; // shared part of btree structure
  Btree *p; // handle to return
  sqlite3_mutex *mutexOpen = 0; // prevents a race condition
  int rc = SQLITE_OK;
  u8 nReserve; // byte of unused space on each page
  unsigned char zDbHeader[100]; // database header content
  const int isTempDb = 1; // use temp db

  // asserts
  assert(db!=0);
  assert(pVfs!=0);
  assert(sqlite3_mutex_held(db->mutex));
  assert((flags&0xff)==flags);
  assert((flags&BTREE_UNORDERED)==0 || (flags&BTREE_SINGLE)!=0);
  assert(((flags&BTREE_SINGLE)==0) || isTempDb);
  
  // allocation
  flags |= BTREE_MEMORY;
  
  p = sqlite3MallocZero(sizeof(Btree));
  if(!p) {
    return SQLITE_NOMEM_BKPT;
  }
  p->inTrans = TRANS_NONE;
  p->db = db;
  
  if(pBt == 0) {
    assert(sizeof(i64) == 8);
    assert(sizeof(u64) == 8);
    assert(sizeof(u32) == 4);
    assert(sizeof(u16) == 2);
    assert(sizeof(Pgno) == 4);
    pBt = sqlite3MallocZero(sizeof(*pBt));
    if(pBt == 0) {
      rc = SQLITE_NOMEM_BKPT;
      goto btree_open_out;
    }
    // open pager ...
    rc = pdb_sqlite3PagerOpen(&pBt->pdbPager,
			      sizeof(MemPage), flags, pageReinit,
			      pbrk_start);
    if(rc == SQLITE_OK) {
      // pdb_sqlite3PagerSetMmapLimit(pBt->pdbPager, db->szMmap);
      rc = pdb_sqlite3PagerReadFileheader(pBt->pdbPager, sizeof(zDbHeader), zDbHeader);
    }
    if(rc != SQLITE_OK) {
      goto btree_open_out;
    }
    pBt->openFlags = (u8)flags;
    pBt->db = db;
    pdb_sqlite3PagerSetBusyHandler(pBt->pdbPager, btreeInvokeBusyHandler, pBt);
    p->pBt = pBt;
    pBt->pCursor = 0;
    pBt->pPage1 = 0;
    if( pdb_sqlite3PagerIsreadonly(pBt->pdbPager) ) pBt->btsFlags |= BTS_READ_ONLY;
#if defined(SQLITE_SECURE_DELETE)
    pBt->btsFlags |= BTS_SECURE_DELETE;
#elif defined(SQLITE_FAST_SECURE_DELETE)
    pBt->btsFlags |= BTS_OVERWRITE;
#endif
    /* EVIDENCE-OF: R-51873-39618 The page size for a database file is
    ** determined by the 2-byte integer located at an offset of 16 bytes from
    ** the beginning of the database file. */
    pBt->pageSize = (zDbHeader[16]<<8) | (zDbHeader[17]<<16);
    if( pBt->pageSize<512 || pBt->pageSize>SQLITE_MAX_PAGE_SIZE
         || ((pBt->pageSize-1)&pBt->pageSize)!=0 ){
      pBt->pageSize = 0;
      nReserve = 0;
    }else{
      /* EVIDENCE-OF: R-37497-42412 The size of the reserved region is
      ** determined by the one-byte unsigned integer found at an offset of 20
      ** into the database file header. */
      nReserve = zDbHeader[20];
      pBt->btsFlags |= BTS_PAGESIZE_FIXED;
    }
    
    rc = pdb_sqlite3PagerSetPagesize(pBt->pdbPager, &pBt->pageSize, nReserve);
    if( rc ) goto btree_open_out;
    pBt->usableSize = pBt->pageSize - nReserve;
    assert( (pBt->pageSize & 7)==0 );  /* 8-byte alignment of pageSize */
    
#if !defined(SQLITE_OMIT_SHARED_CACHE) && !defined(SQLITE_OMIT_DISKIO)
    /* Add the new BtShared object to the linked list sharable BtShareds.
    */
    pBt->nRef = 1;
#endif
  }

  *ppBtree = p;
 btree_open_out:
  if(rc!=SQLITE_OK) {
    if(pBt && pBt->pdbPager) {
      pdb_sqlite3PagerClose(pBt->pdbPager, 0);
    }
    sqlite3_free(pBt);
    sqlite3_free(p);
    *ppBtree = 0;
  } else {
    // do nothing, do not set page cache size, do not set file control hint
  }
  if( mutexOpen ) {
    assert( sqlite3_mutex_held(mutexOpen) );
    sqlite3_mutex_leave(mutexOpen);
  }
  assert( rc!=SQLITE_OK || sqlite3BtreeConnectionCount(*ppBtree)>0 );
  return rc;
}
