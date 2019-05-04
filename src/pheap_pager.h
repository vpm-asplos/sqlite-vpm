#include "pheap_flag.h"
long pdetach();
#define DEBUG_PRINT_FUNC 1
static void pdb_pager_write_changecounter(PgHdr *pPg);
#define PHEAPMEMHDRSIZE 2048

typedef struct PghdrCache PghdrCache;
struct PghdrCache
{
    PgHdr *hdr;
    struct PghdrCache *next;
    Pgno pgno;
};
PghdrCache *store = 0x2a0006200000;

typedef struct freeNode
{
    u64 addr;
    struct freeNode *next;
} freeNode;
freeNode *Buffer_freeList = NULL;

static u64 pdb_getFreeBuffer(int pageSize)
{
    if (Buffer_freeList == NULL)
    {
        return NULL;
    }
    else
    {
        if (Buffer_freeList->next)
        {
            freeNode *next = Buffer_freeList->next;
            u64 ret = Buffer_freeList->addr;
            sqlite3_free(Buffer_freeList);
            Buffer_freeList = next;
            return ret;
        }
        else
        {
            u64 ret = Buffer_freeList->addr;
            Buffer_freeList->addr += pageSize;
            return ret;
        }
    }
}

static void pdb_addFreeBuffer(u64 addr)
{
    if (Buffer_freeList == NULL)
    {
        return;
    }
    else
    {
        freeNode *newNode = sqlite3Malloc(sizeof(freeNode));
        newNode->next = Buffer_freeList;
        newNode->addr = addr;
        Buffer_freeList = newNode;
        return;
    }
}

static int pheap_read(void *addr, void *dest, int size)
{
    // copy memory from addr to dest
    memcpy(dest, addr, size);
    return 0;
}

static int pheap_write(void *dest, void *addr, int size)
{
    memcpy(dest, addr, size);
    return 0;
}

typedef u32 Pgno;
typedef struct PdbPager PdbPager;
struct PdbPager
{
    u8 exclusiveMode;
    u8 journalMode;
    u8 useJournal;
    u8 noLock;
    u8 readOnly;
    u8 eState; // Pager state (OPEN, READER, WRITER_LOCKED..)
    u8 eLock;  // Current lock held on database file
    u8 changeCountDone;
    u8 setMaster;
    u8 doNotSpill;
    u8 subjInMemory;
    u8 bUseFetch;
    Pgno dbSize;                 // Number of pages in the database
    Pgno dbPheapSize;            // size of pheap persisted
    Pgno dbOrigSize;             // dbSize before current transaction
    int errCode;                 // One of several kind of errors
    int nRec;                    // Pages journalled since last j-header written
    u32 cksumInit;               // Quasi-random value added to every checksum;
    Bitvec *pInJournal;          // One bit for each page in the database ptr
    unsigned long dbPtr;         // start ptr for database
    unsigned long dbOff;         // database cursor -> maps to the file size!!
    unsigned long jPtr;          // start ptr for journal
    i64 journalOff;              // current write offset in the journal file, journal cursor
    i64 journalHdr;              // Byte offset to previous journal header
    i64 journalSizeLimit;        // Size limit for persistent journal files
    sqlite3_backup *pBackup;     // Pointer to list of ongoing backup processes
    u32 iDataVersion;            // Changes whenever database content changes
    char dbPtrVers[16];          // Changes whenever database ptr changes
    u16 nExtra;                  // Add this many bytes to each in-memory page
    i16 nReserve;                // Number of unused bytes at the end of each page
    u32 sectorSize;              // Sector size during rollback
    int pageSize;                // Number of bytes in one page
    Pgno mxPgno;                 // Maximum allowed size of the database
    int (*xBusyHandler)(void *); // Function to call when busy
    void *pBusyHandlerArg;       // Context argument for xBusyHandler
    int aStat[4];                // total cache hits misses, writes spills
    void (*xReiniter)(DbPage *); // Call this routine when reloading pages
    char *pTmpSpace;             // TmpSpace
    // PCache *pPache; // We do not need page cache
    // pheap descriptors
    char *pheapName; // Name of the pheap
    unsigned long pbrk_start;
    unsigned long pbrk_current;
    unsigned long pWal; // Write-ahead log used by "journal_mode=wal"
};

static u32 pdb_pager_cksum(PdbPager *pPager, const u8 *aData)
{
    u32 cksum = pPager->cksumInit;  /* Checksum value to return */
    int i = pPager->pageSize - 200; /* Loop counter */
    while (i > 0)
    {
        cksum += aData[i];
        i -= 200;
    }
    return cksum;
}

int pdb_sqlite3PagerMaxPageCount(PdbPager *pPager, int mxPage)
{
    if (mxPage > 0)
    {
        pPager->mxPgno = mxPage;
    }
    assert(pPager->eState != PAGER_OPEN);     /* Called only by OP_MaxPgcnt */
    assert(pPager->mxPgno >= pPager->dbSize); /* OP_MaxPgcnt enforces this */
    return pPager->mxPgno;
}

static int pheap_emptyjournal(PdbPager *pPager)
{
    unsigned long journalsize = pPager->journalOff - pPager->jPtr;
    memset((void *)pPager->jPtr, 0, journalsize); // set journal to zero
    pPager->journalOff = 0;
    return SQLITE_OK;
}

void pdb_sqlite3PagerUnrefNotNull(DbPage *pPg)
{
    assert(pPg != 0);
    // no pcache release, no release map page, just return...
    return;
}

int pdb_sqlite3PagerIswritable(DbPage *pPg)
{
    return pPg->flags & PGHDR_WRITEABLE;
}

void pdb_sqlite3PagerUnref(DbPage *pPg)
{
    if (pPg)
        pdb_sqlite3PagerUnrefNotNull(pPg);
}

void pdb_sqlite3PagerRekey(DbPage *pPg, Pgno iNew, u16 flags)
{
    pPg->flags = flags;
    // no need to move pCache;
}

u32 pdb_sqlite3PagerDataVersion(PdbPager *pPager)
{
    assert(pPager->eState > PAGER_OPEN);
    return pPager->iDataVersion;
}

static int pdb_readDbPage(PgHdr *pPg)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_readDbPage: %p\n", pPg);
#endif
    PdbPager *pPager = pPg->pdbPager;
    int rc = SQLITE_OK;
    i64 iOffset = (pPg->pgno - 1) * (i64)pPager->pageSize;
    // read 4K from dbPtr+iOffset to pPg->pData
    // printf("Now reading from: %p, to: %p\n", (void*)pPg->pdbPager->dbPtr, (void*)pPg->pData);
    rc = pheap_read((void *)(pPg->pdbPager->dbPtr + iOffset), pPg->pData, pPg->pdbPager->pageSize);
    if (pPg->pgno == 1)
    {
        if (rc)
        { // if the read is unsuccessful, reset dbFileVers to a valid number
            printf("read unsuccessful. exit now.");
            exit(1);
        }
        else
        {
            // the 24th in the pPg->pData is the dbPtrVers (following the page format) ...
            // readin to the pPager->dbPtrVers field
            u8 *dbPtrVers = &((u8 *)pPg->pData)[24];
            memcpy(&pPager->dbPtrVers, dbPtrVers, sizeof(pPager->dbPtrVers));
        }
    }
    PAGER_INCR(sqlite3_pager_readdb_count);
    PAGER_INCR(pdbPager->nRead);
    // printf("FETCH pPager page %d hash(%08x)\n",
    // 	 pPg->pgno, pager_pagehash(pPg));
    return rc;
}

static SQLITE_NOINLINE int pdb_pagerAddPageToRollbackJournal(PgHdr *pPg)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_pagerAddPageToRollbackJournal: %p\n", pPg);
#endif
    PdbPager *pdbPager = pPg->pdbPager;
    int rc;
    u32 cksum;
    char *pData2;
    i64 iOff = pdbPager->journalOff;
    /* We should never write to the journal file the page that
  ** contains the database locks.  The following assert verifies
  ** that we do not. */
    assert(pPg->pgno != PAGER_MJ_PGNO(pdbPager));
    assert(pdbPager->journalHdr <= pPager->journalOff);
    CODEC2(pdbPager, pPg->pData, pPg->pgno, 7, return SQLITE_NOMEM_BKPT, pData2);
    cksum = pdb_pager_cksum(pdbPager, (u8 *)pData2);
    pPg->flags |= PGHDR_NEED_SYNC;
    rc = pheap_write((void *)(pdbPager->jPtr + iOff), &pPg->pgno, sizeof(pPg->pgno)); // write 32 bits
    rc = pheap_write((void *)(pdbPager->jPtr + iOff + 4), pData2, pdbPager->pageSize);
    rc = pheap_write((void *)(pdbPager->jPtr + iOff + 4 + pdbPager->pageSize), &cksum, sizeof(cksum));
    pdbPager->journalOff += 8 + pdbPager->pageSize; // journalOff is increased!!
    pdbPager->nRec++;
    rc = sqlite3BitvecSet(pdbPager->pInJournal, pPg->pgno);
    // rc |= addToSavepointBitvecs(pPager, pPg->pgno);
    return rc;
}

int pdb_sqlite3PagerSync(PdbPager *pPager, const char *zMaster)
{
    return SQLITE_OK;
}

static int pdb_syncJournal(PdbPager *pPager, int newHdr)
{
    // printf("SyncJournal is no-op! \n");
    return SQLITE_OK;
}

static int pdb_pager_write(PgHdr *pPg)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_pager_write: %p, page size: %d, pData: %p, pExtra: %p, pgno: %d\n",
           pPg, pPg->pdbPager->pageSize, pPg->pData, pPg->pExtra, pPg->pgno);
#endif
    PdbPager *pdbPager = pPg->pdbPager;
    int rc = SQLITE_OK;
    // no need to open journal...
    // no need to mark pcache dirty...
    if (!(pPg->flags & PGHDR_WRITEABLE))
    {
        u64 cache = pdb_getFreeBuffer(pdbPager->pageSize);
        if (!cache)
        {
            return SQLITE_NOMEM_BKPT;
        }
        memcpy(cache, pPg->pData, pdbPager->pageSize);
#if DEBUG_PRINT_FUNC
        printf("    Copied %d from %p to %p\n", pdbPager->pageSize, pPg->pData, pdbPager->jPtr + pPg->pgno * pdbPager->pageSize);
#endif
        pPg->pData = cache;
        pdbPager->readOnly = 0;
        *(pPg->aData_ptr) = cache;
    }
    pPg->flags |= PGHDR_WRITEABLE;
    /*
    if (pdbPager->pInJournal != 0 && sqlite3BitvecTestNotNull(pdbPager->pInJournal, pPg->pgno) == 0)
    {
        if (pPg->pgno <= pdbPager->dbOrigSize)
        {
            //vpm-anonymous:
            // rc = pdb_pagerAddPageToRollbackJournal(pPg);
            // if (rc != SQLITE_OK)
            // {
            //     return rc;
            // }
            memcpy(pdbPager->jPtr + pPg->pgno * pdbPager->pageSize, pPg->pData, pdbPager->pageSize);
#if DEBUG_PRINT_FUNC
            printf("    Copied from %p to %p\n",pPg->pData, pdbPager->jPtr + pPg->pgno * pdbPager->pageSize);
#endif            
            pPg->pData = pdbPager->jPtr + pPg->pgno * pdbPager->pageSize;

        }
        else
        {
            if (pdbPager->eState != PAGER_WRITER_DBMOD)
            {
                pPg->flags |= PGHDR_NEED_SYNC;
            }
        }
    }
*/
    /* if( pPager->nSavepoint>0 ){ */
    /*   rc = subjournalPageIfRequired(pPg); */
    /* } */

    if (pdbPager->dbSize < pPg->pgno)
    {
        pdbPager->dbSize = pPg->pgno;
    }
    return rc;
}

// mark a data page writable
int pdb_sqlite3PagerWrite(PgHdr *pPg)
{
#if DEBUG_PRINT_FUNC
    //printf("calling pdb_sqlite3PagerWrite: %p\n",pPg);
#endif
    PdbPager *pdbPager = pPg->pdbPager;
    assert(pdbPager);
    assert((pPg->flags & PGHDR_MMAP) == 0);
    assert(pdbPager->eState >= PAGER_WRITER_LOCKED);
    assert(assert_pager_state(pdbPager));
    if ((pPg->flags & PGHDR_WRITEABLE) != 0 && pdbPager->dbSize >= pPg->pgno)
    {
        // if( pPager->nSavepoint ) return subjournalPageIfRequired(pPg);
        return SQLITE_OK;
    }
    else if (pdbPager->errCode)
    {
        return pdbPager->errCode;
    }
    else
    {
        return pdb_pager_write(pPg);
    }
}

static u32 pdb_pager_pagehash(PgHdr *pPage)
{
    PdbPager *pPager = pPage->pdbPager;
    return pager_datahash(pPager->pageSize, (unsigned char *)(pPager->pData));
}

void *pdb_sqlite3PagerTempSpace(PdbPager *pdbPager)
{
    return pdbPager->pTmpSpace;
}

void pdb_sqlite3PagerDontWrite(PgHdr *pPg)
{
    PdbPager *pPager = pPg->pdbPager;
    // nSavepoint is always 0. we don't have any save points
    if ((pPg->flags & PGHDR_DIRTY))
    {
        pPg->flags |= PGHDR_DONT_WRITE;
        pPg->flags &= ~PGHDR_WRITEABLE;
        pager_set_pagehash(pPage);
    }
}

int pdb_sqlite3PagerPageRefcount(DbPage *pPage)
{
    // no nothing because we don't count pcache
    return 0;
}

Schema *pdb_sqlite3SchemaGet(sqlite3 *db, Btree *pBt)
{
    Schema *p;
    if (pBt)
    {
        p = (Schema *)sqlite3BtreeSchema(pBt, sizeof(Schema), sqlite3SchemaClear);
    }
    else
    {
        p = (Schema *)sqlite3DbMallocZero(0, sizeof(Schema));
    }
    if (!p)
    {
        sqlite3OomFault(db);
    }
    else if (0 == p->file_format)
    { // Schema Format Version for this file
        sqlite3HashInit(&p->tblHash);
        sqlite3HashInit(&p->idxHash);
        sqlite3HashInit(&p->trigHash);
        sqlite3HashInit(&p->fkeyHash);
        p->enc = SQLITE_UTF8;
    }
    return p;
}

int pdb_sqlite3PagerGet(PdbPager *, Pgno, DbPage **, int);

int pdb_sqlite3PagerMovepage(PdbPager *pPager, DbPage *pPg, Pgno pgno, int isCommit)
{
    DbPage *nPage; /* Page to copy pPg to */
    int rc;        /* Return code */
#if DEBUG_PRINT_FUNC
    printf("calling pdb_sqlite3PagerMovepage: %p\n", pPg);
#endif
    /* In order to be able to rollback, an in-memory database must journal
  ** the page we are moving from.
  */
    rc = pdb_sqlite3PagerWrite(pPg);
    if (rc)
        return rc;

    rc = pdb_sqlite3PagerGet(pPager, pgno, &nPage, 0);
    if (rc)
        return rc;

    memcpy(nPage->pData, pPg->pData, pPager->pageSize); // data is in store; written to db on commit
    pdb_sqlite3PagerUnref(nPage);

    return rc;
}

void pdb_sqlite3PagerTruncateImage(PdbPager *pPager, Pgno noPage)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_sqlite3PagerTruncateImage: %p, %d\n", pPager, noPage);
#endif
    assert(pPager->dbSize >= nPage);
    assert(pPager->eState >= PAGER_WRITER_CACHEMOD);
    pPager->dbSize = noPage;
    return;
}

static SQLITE_NOINLINE int pdb_pagerOpenSavepoint(PdbPager *pPager, int nSavepoint)
{
    printf("Stub! not support opensavepoint!");
    exit(1);
}

static int pdb_pager_error(PdbPager *pPager, int rc)
{
    int rc2 = rc & 0xff;
    assert(rc == SQLITE_OK || !MEMDB);
    assert(
        pPager->errCode == SQLITE_FULL ||
        pPager->errCode == SQLITE_OK ||
        (pPager->errCode & 0xff) == SQLITE_IOERR);
    if (rc2 == SQLITE_FULL || rc2 == SQLITE_IOERR)
    {
        pPager->errCode = rc;
        pPager->eState = PAGER_ERROR;
        // setGetterMethod(pPager); we don't setgetterMethod
    }
    return rc;
}

// we do not support playback at this point.
static int pdb_pager_playback(PdbPager *pPager, int isHot)
{
    printf("We do not support playback at this moment!");
    exit(1);
    return SQLITE_OK;
}

int pdb_pagerUserWal(PdbPager *pPager)
{
    if (pPager->pWal == 0)
        return 0;
    return 1;
}

static int pdb_pagerUnlockDb(PdbPager *pPager, int eLock)
{
    return SQLITE_OK;
}

static int pdb_pager_end_transaction(PdbPager *pPager, int hasMaster, int bCommit)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_pager_end_transaction: %p, size: %d\n", pPager, pPager->dbSize);
    printf("0x2a0002200000: %s\n", 0x2a0002200000);
#endif
    int rc = SQLITE_OK;
    int rc2 = SQLITE_OK;
    assert(assert_pager_state(pPager));
    assert(pPager->eState != PAGER_ERROR);
    if (pPager->eState < PAGER_WRITER_LOCKED && pPager->eLock < RESERVED_LOCK)
    {
#if DEBUG_PRINT_FUNC
        printf("    Direct return\n");
#endif
        return SQLITE_OK;
    }
    // Finalize Journal
#if DEBUG_PRINT_FUNC
    printf("    Finalize Journal, %d, &d\n", pPager->eState, pPager->eLock);
#endif
    if (pPager->journalOff == 0)
    {
        rc = SQLITE_OK;
    }
    else
    {
        // we can either truncate(set offset to zero) or persist(invalidate the journal header, first 28 bytes)
        rc = pheap_emptyjournal(pPager); // no need to sync
    }
    rc2 = pdb_pagerUnlockDb(pPager, SHARED_LOCK);
    sqlite3BitvecDestroy(pPager->pInJournal);
    pPager->pInJournal = 0;
    pPager->nRec = 0;
    // Noneed to clean pcache
    pPager->changeCountDone = 0;
    pPager->eState = PAGER_READER;
    pPager->setMaster = 0;
    return (rc == SQLITE_OK ? rc2 : rc);
}

static int pdb_sqlite3PagerRollback(PdbPager *pPager)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_sqlite3PagerRollback: %p\n", pPager);
#endif
    int rc = SQLITE_OK;
    // printf("ROLLBACK pdbPager...\n");
    // exit(1);
    //
    if (pPager->eState == PAGER_ERROR)
        return pPager->errCode;
    if (pPager->eState <= PAGER_READER)
        return SQLITE_OK;
    if (pPager->eState == PAGER_WRITER_LOCKED)
    {
        //roll back:
        pPager->dbSize = pPager->dbOrigSize;
        for (int i = 0; i < PHEAPMEMHDRSIZE; i++)
        {
            PgHdr *pList = store[i].hdr;
            Pgno pgno = store[i].pgno;
            PghdrCache *pgc = NULL;
            PghdrCache *pgc_next = store[i].next;
            store[i].hdr = NULL;
            store[i].pgno = 0;
            store[i].next = NULL;
            if (pgno == 0 || pList == NULL)
            {
                store[i].pgno = 0;
                continue;
            }
#if DEBUG_PRINT_FUNC
            printf("    R: %dchecking page: %d, dbsize: %d, pData: %p, jPtr: %p\n",
                   i, pgno, pPager->dbSize, pList->pData, pPager->jPtr);
#endif
        find_next:
            if (pList->pdbPager == pPager)
            {
                if (pgno <= pPager->dbSize && pgno >= 0)
                {
                    i64 offset = (pgno - 1) * (i64)pPager->pageSize;
                    char *pData = pList->pData;
                    // if (pgno == 1)
                    //     pdb_pager_write_changecounter(pList);
                    //vpm-anonymous:
    #if DEBUG_PRINT_FUNC
                    if (pgno == 1)
                    {
                        printf("        R: page1: %d\n      %d\n", *(u32 *)(pData + 28), *(u32 *)(pPager->jPtr + pgno * pPager->pageSize + 28));
                    }
    #endif
                    //             if(pList->flags & PGHDR_WRITEABLE){
                    // #if DEBUG_PRINT_FUNC
                    //                 printf("        R: calling pswap: %p <-> %p for pager #%d\n",pData, pPager->jPtr + pgno * pPager->pageSize,pgno);
                    // #endif
                    //                 rc = pswap(pData, pPager->jPtr + pgno * pPager->pageSize, pPager->pageSize);
                    // #if DEBUG_PRINT_FUNC
                    //                 printf("        R: returned: %d\n",rc);
                    // #endif
                    //                 rc = SQLITE_OK;
                    //             }
                    //rc = pheap_write((void *)(pPager->dbPtr + offset), pData, pPager->pageSize);
                    // if (rc == 0)
                    // {
                    //     pPager->dbOff = offset + pPager->pageSize; // we use the page end address, not starting address
                    // }
                    if (pgno == 1)
                    {
                        //printf("        after: R: page1: %s\n      %s\n",pData,pPager->jPtr + pgno * pPager->pageSize);
                        pdb_pager_write_changecounter(pList);
                        memcpy(&pPager->dbPtrVers, &pData[24], sizeof(pPager->dbPtrVers));
                        //printf("        after: R: page1: %s\n      %s\n",pData,pPager->jPtr + pgno * pPager->pageSize);
                        //*(u32*)(pData + 28) = pPager->dbSize;
                        //printf("      writing %d to @%p \n", pPager->dbSize, pData + 28);
                    }
                    if (pgno > pPager->dbPheapSize)
                    {
                        pPager->dbPheapSize = pgno;
                    }
                    pPager->aStat[PAGER_STAT_WRITE]++;
                }
                if(pList->pExtra){
                    sqlite3_free(pList->pExtra);
                }
                sqlite3_free(pList);
                if (pgc != NULL)
                {
                    sqlite3_free(pgc);
                }
            }
            else
            {
                if (store[i].hdr != NULL)
                {
                    pgc->next = store[i].next;
                    store[i].next = pgc;
                }
                else
                {
                    store[i].hdr = pList;
                    store[i].pgno = pgno;
                    if (pgc)
                        sqlite3_free(pgc);
                }
            }
            if (pgc_next)
            {
                pgc = pgc_next;
                pgc_next = pgc->next;
                pgno = pgc->pgno;
                pList = pgc->hdr;
                goto find_next;
            }
            store[i].hdr = NULL;
            store[i].pgno = 0;
            store[i].next = NULL;
        }
        pPager->eState = PAGER_READER;
        int eState = pPager->eState;
        rc = pdb_pager_end_transaction(pPager, 0, 0);
        //printf("    end_transaction return %d\n", rc);
        // if (eState > PAGER_WRITER_LOCKED)
        // {
        //     printf("    pager still writer locked!\n")
        //     pPager->errCode = SQLITE_ABORT;
        //     pPager->eState = PAGER_ERROR;
        //     return rc;
        // }
    }
    else
    {
        rc = pdb_pager_playback(pPager, 0);
    }
    assert(pPager->eState == PAGER_READER || rc != SQLITE_OK);
    assert(rc == SQLITE_OK || rc == SQLITE_FULL || rc == SQLITE_CORRUPT || rc == SQLITE_NOMEM || (rc & 0xFF) == SQLITE_IOERR || rc == SQLITE_CANTOPEN);

    return pdb_pager_error(pPager, rc);
}

static void pdb_pager_unlock(PdbPager *pPager);

static void pdb_pagerUnlockAndRollback(PdbPager *pPager)
{
    if (pPager->eState != PAGER_ERROR && pPager->eState != PAGER_OPEN)
    {
        if (pPager->eState >= PAGER_WRITER_LOCKED)
        {
            sqlite3BeginBenignMalloc();
            pdb_sqlite3PagerRollback(pPager);
            sqlite3EndBenignMalloc();
        }
        else if (!pPager->exclusiveMode)
        {
            pdb_pager_end_transaction(pPager, 0, 0);
        }
    }
    pdb_pager_unlock(pPager);
}

static void pdb_pagerUnlockIfUnused(PdbPager *pdbPager)
{
    pdb_pagerUnlockAndRollback(pdbPager);
}

static void pdb_pager_reset(PdbPager *pPager)
{
    pPager->iDataVersion++;
    // No need for backup restart or pcache clear
}

int pdb_sqlite3PagerClose(PdbPager *pPager, sqlite3 *db)
{
    // no Tmp space, no mmap
    sqlite3BeginBenignMalloc();
    pPager->exclusiveMode = 0;
    pdb_pager_reset(pPager);
    pdb_pagerUnlockAndRollback(pPager);
    sqlite3EndBenignMalloc();
    sqlite3_free(pPager);
    return SQLITE_OK;
}

// First 8 bytes in metaZone: dbsize
// passes in: pbt->pagesize (stored in zDbHeader) (Must-have)
// check against:
// nByte: == Database file size
// pPager->pageSize
int pdb_sqlite3PagerSetPagesize(PdbPager *pPager, u32 *pPageSize, int nReserve)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_sqlite3PagerSetPagesize: %p, %d, pbrk start: %p\n",
           pPager, *pPageSize, pPager->pbrk_start);
#endif
    int rc = SQLITE_OK;
    u32 pageSize = *pPageSize;
    i64 nByte = 0;
    pheap_read((void *)pPager->pbrk_start, &nByte, 8); // 8-bytes size
    //printf("    nbyte: %d\n",nByte);
    assert(pageSize == 0 || (pageSize >= 512 && pageSize <= SQLITE_MAX_PAGE_SIZE));
    // dbSize == 0 means it is in initialization stage
    if (pPager->dbSize == 0 && pageSize && pageSize != (u32)pPager->pageSize)
    {
        char *pNew = NULL;
        pNew = (char *)sqlite3_malloc(pageSize);
        if (!pNew)
            rc = SQLITE_NOMEM_BKPT;
        if (rc == SQLITE_OK)
        {
            sqlite3_free(pPager->pTmpSpace);
            pPager->pTmpSpace = pNew;
            pPager->dbSize = (Pgno)((nByte + pageSize - 1) / pageSize); // round up
            pPager->pageSize = pageSize;
        }
        else
        {
            sqlite3_free(pNew);
        }
    }
    // write back to *pPageSize (pBt->pageSize)
    // o/w use pPager->pageSize to overwrite pBt->pageSize...
#if DEBUG_PRINT_FUNC
    printf("    page size: %d, nByte: %d\n", pPager->pageSize, nByte);
#endif
    *pPageSize = pPager->pageSize;
    if (rc == SQLITE_OK)
    {
        // nReserve should be zero at this point...
        if (nReserve < 0)
            nReserve = pPager->nReserve;
        assert(nReserve >= 0 && nReserve < 1000);
        pPager->nReserve = (i16)nReserve;
    }
    return rc;
}

// read amt bytes from pager to pBuf with offset to cursor
int pdb_sqlite3PHeapRead(unsigned long cursor, void *pBuf, int amt, i64 offset)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_sqlite3PHeapRead: %p\n", pBuf);
#endif
    size_t i = 0;
    char *buf = (char *)pBuf;
    for (int i = 0; i < amt; i++)
    {
        buf[i] = *((char *)(cursor + offset + i));
    }
    return SQLITE_OK;
}

int pdb_sqlite3PHeapWrite(unsigned long cursor, void *pBuf, int amt, i64 offset)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_sqlite3PHeapWrite: %p\n", pBuf);
#endif
    size_t i = 0;
    char *buf = (char *)pBuf;
    for (int i = 0; i < amt; i++)
    {
        *(((char *)cursor) + offset + i) = *buf;
    }
    return SQLITE_OK;
}

static int pdb_addToSavepointBitvecs(PdbPager *pPager, Pgno pgno)
{
    printf("Stub! Do not support addtosavepointbitvecs!");
    exit(1);
}

int pdb_sqlite3PagerOpen(PdbPager **ppPager,
                         int nExtra,
                         int flags,
                         void (*xReinit)(DbPage *), unsigned long pbrk_start)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_sqlite3PagerOpen: %p\n", ppPager);
#endif
    PdbPager *pPager = 0;
    int rc = SQLITE_OK;
    int readOnly = 0;
    int journalFileSize = 0;
    // int memJM = 1; // in-memory journal
    int memDb = 1; // in-memory PHeap
    u32 szPageDflt = SQLITE_DEFAULT_PAGE_SIZE;
    int useJournal = (flags & PAGER_OMIT_JOURNAL) == 0; /* False to omit journal */

    // journalFileSize = ROUND8(sqlite3JournalSize(pVfs));
    // **ppPager = 0; // set output variable to NULL in case an error occurs...

    /*
   * Allocate memory for the Pager structure, 
   * database pointer
   * journal pointer
   */
    // -journal\000 and -wal\000
    pPager = sqlite3MallocZero(ROUND8(sizeof(*pPager)));
    if (!pPager)
    {
        return SQLITE_NOMEM_BKPT;
    }
    // printf("Allocating pager: %p, pbrk_start: %p \n", pPager, pbrk_start);
    pPager->pbrk_start = pbrk_start;
    pPager->dbPtr = pbrk_start + DB_SZ;
    pPager->dbOff = 0;
    pPager->journalOff = 0;

    /*
   * Now we start to access pheap region. Choose a default page size.
   * The default page size is SQLITE_DEFAULT_PAGE_SIZE
   * This is to set szPageDflt
   */
    if (!readOnly)
    {
        pPager->sectorSize = 4096;
        assert(SQLITE_DEFAULT_PAGE_SIZE <= SQLITE_MAX_DEFAULT_PAGE_SIZE);
        if (szPageDflt < pPager->sectorSize)
        {
            if (pPager->sectorSize > SQLITE_MAX_DEFAULT_PAGE_SIZE)
            {
                szPageDflt = SQLITE_MAX_DEFAULT_PAGE_SIZE;
            }
            else
            {
                szPageDflt = (u32)pPager->sectorSize;
            }
        }
    }
    // pPager->eLock = EXCLUSIVE_LOCK;
    pPager->noLock = 1;

    // set page size
    if (rc == SQLITE_OK)
    {
        rc = pdb_sqlite3PagerSetPagesize(pPager, &szPageDflt, -1);
    }
    else
    {
        sqlite3_free(pPager);
        return rc;
    }
    pPager->jPtr = (u64)store + PHEAPMEMHDRSIZE * sizeof(PghdrCache) + pPager->pageSize;
    pPager->jPtr = pPager->jPtr / pPager->pageSize * pPager->pageSize;
    pPager->useJournal = (u8)useJournal;
    pPager->mxPgno = SQLITE_MAX_PAGE_COUNT;
    pPager->readOnly = (u8)readOnly;
    pPager->nExtra = (u16)nExtra;
    pPager->journalSizeLimit = SQLITE_DEFAULT_JOURNAL_SIZE_LIMIT;
    pPager->xReiniter = xReinit;
    // finished initializing pPager!

    if (Buffer_freeList == NULL)
    {
        Buffer_freeList = sqlite3Malloc(sizeof(freeNode));
        Buffer_freeList->addr = pPager->jPtr;
        Buffer_freeList->next = NULL;
    }

    *ppPager = pPager;
    return SQLITE_OK;
}

/* Read the first N bytes from the beginning of the file to the memory pDest points to */
int pdb_sqlite3PagerReadFileheader(PdbPager *pPager, int N, unsigned char *pDest)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_sqlite3PagerReadFileheader: %p\n", pPager);
#endif
    int rc = SQLITE_OK;
    memset(pDest, 0, N);
    assert(pPager->pheapName);
    /* This routine is only called by btree immediately after creating
  ** the Pager object.  There has not been an opportunity to transition
  ** to WAL mode yet.
  */
    assert(!pagerUseWal(pPager));
    rc = pdb_sqlite3PHeapRead(pPager->pbrk_start, pDest, N, 0);
    return rc;
}

int pdb_sqlite3PagerSetBusyHandler(PdbPager *pPager,
                                   int (*xBusyHandler)(void *),
                                   void *pBusyHandlerArg)
{
    void **ap;
    pPager->xBusyHandler = xBusyHandler;
    pPager->pBusyHandlerArg = pBusyHandlerArg;
    ap = (void **)&pPager->xBusyHandler;
    assert(((int (*)(void *))(ap[0])) == xBusyHandler);
    assert(ap[1] == pBusyHandlerArg);
    return SQLITE_OK;
}

int pdb_sqlite3PagerIsreadonly(PdbPager *pPager)
{
    return pPager->readOnly;
}

int pdb_sqlite3PagerSharedLock(PdbPager *pPager)
{
    // printf("You opened pdb shared lock!\n");
    return SQLITE_OK;
}

// We don't have fd
// int pdb_sqlite3PagerFile() { }

// We don't have page cache either.
// int pdb_sqlitePagerSetCachesize() { }
// ================== needed by create ==============
// void pdb_sqlite3PagerShared

void *pdb_sqlite3PagerGetData(DbPage *pPg)
{
    assert(pPg->nRef > 0 || pPg->pPager->memDb);
    return pPg->pData;
}
void *pdb_sqlite3PagerGetExtra(DbPage *pPg)
{
    return pPg->pExtra;
}

// get a page from pheap, similar to getPageNormal
int pdb_sqlite3PagerGet(PdbPager *pdbPager,
                        Pgno pgno,       // Page number to fetch
                        DbPage **ppPage, // Write a pointer to the page here
                        int flags)
{
    int rc = SQLITE_OK;
    PgHdr *pPg;
    u8 noContent;
#if DEBUG_PRINT_FUNC
    //printf("calling pdb_sqlite3PagerGet %d: %p\n",pgno,pdbPager);
#endif
    // printf("Calling pager get with pgno: %d, db heap size: %d\n",
    // pgno, pdbPager->dbOff);
    assert(pdbPager->errCode == SQLITE_OK);
    assert(pdbPager->eState >= PAGER_READER);
    assert(assert_pager_state(pPage));
    assert(pPager->hasHeldSharedLock == 1);

    if (pgno <= 0)
        return SQLITE_CORRUPT_BKPT;

    //Xu's implementation
    // int findcache = -1;
    // int findempty = -1;
    // for (int i = 0; i < PHEAPMEMHDRSIZE; i++)
    // {
    //     if (store[i].pgno == pgno)
    //     {
    //         findcache = i;
    //     }
    //     else if (findempty == -1 && store[i].pgno == 0)
    //     {
    //         findempty = i;
    //     }
    // }
    // if (findcache != -1)
    
    
    
    // {
    //     // fast return
    //     pPg = store[findcache].hdr;
    //     *ppPage = pPg;
    //     return SQLITE_OK;
    // }
    // else
    // {
    //     pPg = sqlite3_malloc(sizeof(PgHdr));
    //     store[findempty].pgno = pgno;
    //     store[findempty].hdr = pPg;
    //     store_cnt++;
    //     memset(pPg, 0, sizeof(PgHdr));
    // }

    //vpm-anonymous
    u32 h_value = pgno % PHEAPMEMHDRSIZE;
    if (store[h_value].hdr != NULL)
    {
        if (store[h_value].pgno == pgno && store[h_value].hdr->pdbPager == pdbPager)
        {
            *ppPage = store[h_value].hdr;
            return SQLITE_OK;
        }
        else if (store[h_value].next != NULL)
        {
            PghdrCache *t = store[h_value].next;
            while (t != NULL)
            {
                if (t->pgno == pgno && t->hdr->pdbPager == pdbPager)
                {
                    *ppPage = t->hdr;
                    return SQLITE_OK;
                }
                t = t->next;
            }
        }
        //create a new one
        pPg = sqlite3_malloc(sizeof(PgHdr));
        PghdrCache *t = sqlite3_malloc(sizeof(PghdrCache));
        t->hdr = pPg;
        t->pgno = pgno;
        t->next = store[h_value].next;
        store[h_value].next = t;
        memset(pPg, 0, sizeof(PgHdr));
    }
    else
    {
        //create a new one
        pPg = sqlite3_malloc(sizeof(PgHdr));
        store[h_value].hdr = pPg;
        store[h_value].pgno = pgno;
        store[h_value].next = NULL;
        memset(pPg, 0, sizeof(PgHdr));
    }

    *ppPage = pPg; // DbPage and PgHdr is the same thing
    // Create a new page and initialize its content
    pPg->pPager = NULL; // we don't have the pPager when use pdbPager
    pPg->pgno = pgno;
    pPg->pdbPager = pdbPager;
    pPg->pExtra = sqlite3_malloc(pdbPager->nExtra); // allocate space for pExtra
    //vpm-anonymous:
    //pPg->pData = sqlite3_malloc(pdbPager->pageSize);
    pPg->pData = pdbPager->dbPtr + (pgno - 1) * (i64)pdbPager->pageSize;
    memset(pPg->pExtra, 0, pdbPager->nExtra); // set MemPage to zero
    pPg->flags = 0;
    //memset(pPg->pData, 0, pdbPager->pageSize); // set page to zero
#if DEBUG_PRINT_FUNC
    //printf("finishing pdb_sqlite3PagerGet: %p, pData: %p\n",pdbPager, pPg->pData);
#endif
    noContent = ((flags & PAGER_GET_NOCONTENT) != 0);
    // in pheap we don't have cache so always miss the cache
    if (pgno > PAGER_MAX_PGNO || pgno == PAGER_MJ_PGNO(pdbPager))
    {
        rc = SQLITE_CORRUPT_BKPT;
        goto pager_acquire_err;
    }
    pPg->pdbPager = pdbPager;
    if (pdbPager->dbSize < pgno || noContent)
    {
        if (pgno > pdbPager->mxPgno)
        {
            rc = SQLITE_FULL;
            goto pager_acquire_err;
        }

        // alloc space for pData
    }
    else
    {
        // pdbPager->aStat[PAGER_STAT_MISS]++;
        // rc = pdb_readDbPage(pPg);
        // if (rc != SQLITE_OK)
        // {
        //     goto pager_acquire_err;
        // }
        pager_set_pagehash(pPg);
    }
    return SQLITE_OK;
pager_acquire_err:
    assert(rc != SQLITE_OK);
    pdb_pagerUnlockIfUnused(pdbPager);
    *ppPage = 0;
    return rc;
}

void pdb_sqlite3PagerResetLockTimeout(PdbPager *pdbPager)
{
    return;
}

// This func is called as part of transition from PAGER_OPEN to PAGER_READER
// To determine the size of db file in pages
static int pdb_sqlite3PagerPagecount(PdbPager *pdbPager, Pgno *pnPage)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_sqlite3PagerPagecount: %p, %d\n", pdbPager, pdbPager->dbSize);
#endif
    *pnPage = (int)pdbPager->dbSize;
    return SQLITE_OK;
    Pgno nPage;
    assert(pdbPager->eState == PAGER_OPEN);
    assert(pdbPager->eLock >= SHARED_LOCK);
    nPage = 0; // WAL is not open the size is 0
    // get pdbPager file size
    pheap_read((void *)pdbPager->pbrk_start, &pdbPager->dbOff, sizeof(pdbPager->dbOff));
    int n = pdbPager->dbOff;
    // printf("pdbPager: %p, Pagecount db off: %d\n", pdbPager, pdbPager->dbOff);
    nPage = (Pgno)((n + pdbPager->pageSize - 1) / (pdbPager->pageSize));
    if (nPage > pdbPager->mxPgno)
    {
        pdbPager->mxPgno = (Pgno)nPage;
    }
    *pnPage = nPage;
    return SQLITE_OK;
}

void pdb_sqlite3PagerSetFlags(PdbPager *pdbPager, unsigned pgFlags)
{
    unsigned level = pgFlags & PAGER_SYNCHRONOUS_MASK;
    if (pgFlags & PAGER_CACHESPILL)
    {
        pdbPager->doNotSpill &= ~SPILLFLAG_OFF;
    }
    else
    {
        pdbPager->doNotSpill |= SPILLFLAG_OFF;
    }
}

void pdb_sqlite3PagerUnrefPageOne(DbPage *pPg)
{
    assert(pPg != 0);
    assert(pPg->pgno == 1);
    assert((pPg->flags & PGHDR_MMAP) == 0);
    // pdb_sqlite3ResetLockTimeout(pdbPager);
    // release page 1 in page cache...
    // pdb_pagerUnlockIfUnused(pdbPager);
    // do nothing since we don't have page cache ...
    return;
}

/* Acquire a page if it is already in the memory cache. Do not read from disk 
* Since we don't have pcache, just return NULL. */
DbPage *pdb_sqlite3PagerLookup()
{
    return NULL;
}

static int pdb_pagerLockDb(PdbPager *pPager, int eLock)
{
    return SQLITE_OK; // we don't lock...
}

static void pdb_pager_unlock(PdbPager *pPager)
{
    int rc = SQLITE_OK;
    assert(pPager->eState == PAGER_READER || pPager->eState == PAGER_OPEN || pPager->eState == PAGER_ERROR);
    pPager->pInJournal = 0;
    rc = pdb_pagerUnlockDb(pPager, NO_LOCK);
    pPager->journalOff = 0;
    pPager->journalHdr = 0;
    pPager->setMaster = 0;
    return;
}

static int pdb_pager_wait_on_lock(PdbPager *pPager, int locktype)
{
    assert((pPager->eLock >= locktype) || (pPager->eLock == NO_LOCK && locktype == SHARED_LOCK) || (pPager->eLock == RESERVED_LOCK && locktype == EXCLUSIVE_LOCK));
    int rc = 0;
    do
    {
        rc = pdb_pagerLockDb(pPager, locktype);
    } while (rc == SQLITE_BUSY && pPager->xBusyHandler(pPager->pBusyHandlerArg));
    return rc;
}

/* Begin a write-transaction on the specified pager object
* If a write-transaction has already been opened, this function is no-op. */
int pdb_sqlite3PagerBegin(PdbPager *pPager, int exFlag, int subjInMemory)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_sqlite3PagerBegin: %p\n", pPager);
#endif
    int rc = SQLITE_OK;
    if (pPager->errCode)
        return pPager->errCode;
    assert(pPager->eState >= PAGER_READER && pPager->eState < PAGER_ERROR);
    pPager->subjInMemory = (u8)subjInMemory;
    if (ALWAYS(pPager->eState == PAGER_READER))
    {
        rc = pdb_pagerLockDb(pPager, RESERVED_LOCK);
        if (rc == SQLITE_OK && exFlag)
        {
            rc = pdb_pager_wait_on_lock(pPager, EXCLUSIVE_LOCK);
        }
    }
    if (rc == SQLITE_OK)
    {
        // transition to WRITER_LOCKED state
        pPager->eState = PAGER_WRITER_LOCKED;
        pPager->dbPheapSize = pPager->dbSize;
        pPager->dbOrigSize = pPager->dbSize;
        pPager->journalOff = 0;
        pPager->dbOff = 0;
    }
    assert(rc == SQLITE_OK || pPager->eState == PAGER_READER);
    assert(rc != SQLITE_OK || pPager->eState == PAGER_WRITER_LOCKED);
    assert(assert_pager_state(pPager));
    return rc;
}

void pdb_sqlite3PagerRef(DbPage *pPg)
{
    //printf("[sqlite3PageRef] We do not provide page cache\n");
    return;
}

int pdb_sqlite3PagerSavepoint(PdbPager *pPager, int op, int iSavepoint)
{
    printf("[PagerSavepoint] We do not support savepoint at this point.. pagersavepoint\n");
    exit(1);
}

int pdb_sqlite3PagerOpenSavepoint(PdbPager *pPaber, int nSavepoint)
{
    printf("We do not support savepoint at this point... open pagersavepoint\n");
    exit(1);
}

// always flush during the commit
static int pdb_pagerFlushOnCommit(PdbPager *pPager, int bCommit)
{
    return 1;
}

static void pdb_pager_write_changecounter(PgHdr *pPg)
{
    u32 change_counter;
    change_counter = sqlite3Get4byte((u8 *)pPg->pdbPager->dbPtrVers) + 1;
    put32bits(((char *)pPg->pData) + 24, change_counter);

    put32bits(((char *)pPg->pData) + 92, change_counter);
    put32bits(((char *)pPg->pData) + 96, change_counter);
}

// static void pdb_pager_write_changecounter(PgHdr *p)
static int pdb_pager_incr_changecounter(PdbPager *pPager, int isDirectMode)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_pager_incr_changecounter: %p\n", pPager);
#endif
    int rc = SQLITE_OK;
    assert(pPager->eState == PAGER_WRITER_CACHEMOD || pPager->eState == PAGER_WRITER_DBMOD);
    assert(assert_pager_state(pPager));
    if (!pPager->changeCountDone && ALWAYS(pPager->dbSize > 0))
    {
        PgHdr *pPgHdr;
        rc = pdb_sqlite3PagerGet(pPager, 1, &pPgHdr, 0);
        assert(pPgHdr == 0 || rc == SQLITE_OK);
        if (ALWAYS(rc == SQLITE_OK))
        {
            rc = pdb_sqlite3PagerWrite(pPgHdr);
        }
        if (rc == SQLITE_OK)
        {
            pdb_pager_write_changecounter(pPgHdr);
            if (isDirectMode)
            { // write page content to pheap if page 1
                /*const void *zBuf = pPgHdr->pData;
	assert(pPager->dbFileSize > 0);
        // CODEC2(pPager, pPgHdr->pData, 1, 6, rc=SQLITE_NOMEM_BKPT, zBuf);
	if(rc == SQLITE_OK) {
	  // write zBuf to 
	  rc = pheap_write((void*)pPager->dbPtr, zBuf, pPager->pageSize);
	  pPager->aStat[PAGER_STAT_WRITE]++;
	}
	if(rc == SQLITE_OK) {
	  // update the pager's copy of the change-counter.
	  const void *pCopy = (const void*)&((const char *)zBuf)[24];
	  memcpy(&pPager->dbPtrVers, pCopy, sizeof(pPager->dbPtrVers));
	  pPager->changeCountDone = 1;
	  }*/
                printf("You should not enter direct mode!\n");
                exit(1);
            }
            else
            {
                pPager->changeCountDone = 1;
            }
        }
        pdb_sqlite3PagerUnref(pPgHdr);
    }
    return rc;
}

static int pdb_pager_truncate(PdbPager *pPager, Pgno nPage)
{
    printf("Stub! You should not truncate the database!\n");
    exit(1);
}

#ifdef SQLITE_CHECK_PAGES
static void pdb_pager_set_pagehash(PgHdr *pPage)
{
    pPage->pageHash = pdb_pager_pagehash(pPage);
}
#else
#define pdb_pager_datahash(X, Y) 0
#define pdb_pager_pagehash(X) 0
#define pdb_pager_set_pagehash(X)
#define CHECK_PAGE(x)
#endif

// write all the pages in the header to the pmem!
static int pdb_pager_write_pagelist(PdbPager *pPager)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_pager_write_pagelist: %p\n", pPager);
#endif
    int rc = SQLITE_OK;
    char *pData;
    assert(!pagerUseWal(pPager));
    assert(pPager->eLock == EXCLUSIVE_LOCK);
    u64 ptr1[100], ptr2[100];
    u32 pswap_count = 0;
    for (int i = 0; i < PHEAPMEMHDRSIZE; i++)
    {
        PgHdr *pList = store[i].hdr;
        Pgno pgno = store[i].pgno;
        PghdrCache *pgc = NULL;
        PghdrCache *pgc_next = store[i].next;
        store[i].hdr = NULL;
        store[i].pgno = 0;
        store[i].next = NULL;
        if (pgno == 0 || pList == NULL)
        {
            continue;
        }
#if DEBUG_PRINT_FUNC
        printf("    checking page: %d, dbsize: %d, pData: %p, jPtr: %p\n",
               pgno, pPager->dbSize, pList->pData, pPager->jPtr);
#endif
    find_next:
        if (pList->pdbPager == pPager)
        {
            pData = pList->pData;
            if (pgno <= pPager->dbSize)
            {
                if (pgno == 1)
                {
                    pdb_pager_write_changecounter(pList);
                    memcpy(&pPager->dbPtrVers, &pData[24], sizeof(pPager->dbPtrVers));
                }
                if (pgno > pPager->dbPheapSize)
                {
                    pPager->dbPheapSize = pgno;
                }
                pPager->aStat[PAGER_STAT_WRITE]++;
            }
            if (pList->flags & PGHDR_WRITEABLE)
            {
#if DEBUG_PRINT_FUNC
                printf("        R: calling pswap: %p <-> %p for pager #%d\n", pData, pPager->dbPtr + (pgno - 1) * (u64)pPager->pageSize, pgno);
                if (pgno == 1)
                {
                    printf("        before: R: page1: %s\n      %s\n", pData, pPager->dbPtr + (pgno - 1) * (u64)pPager->pageSize);
                }
#endif
                ptr1[pswap_count] = pData;
                ptr2[pswap_count] = pPager->dbPtr + (pgno - 1) * (u64)pPager->pageSize;
                pswap_count++;
                if(pswap_count == 1){
#if DEBUG_PRINT_FUNC
                    printf("#########################\n     executing pswap %d\n",pswap_count);
#endif
                    rc = pswap(ptr1, ptr2, pswap_count);
                    pswap_count = 0;
                }
#if DEBUG_PRINT_FUNC
                printf("        R: returned: %d\n", rc);
                if (pgno == 1)
                {
                    printf("        after: R: page1: %s\n      %s\n", pData, pPager->dbPtr + (pgno - 1) * (u64)pPager->pageSize);
                }
#endif
                pdb_addFreeBuffer(pData);
                rc = SQLITE_OK;
            }
            if(pList->pExtra){
                sqlite3_free(pList->pExtra);
            }
            sqlite3_free(pList);
            if (pgc != NULL)
            {
                sqlite3_free(pgc);
            }
        }
        else
        {
            if (store[i].hdr != NULL)
            {
                pgc->next = store[i].next;
                store[i].next = pgc;
            }
            else
            {
                store[i].hdr = pList;
                store[i].pgno = pgno;
                if (pgc)
                    sqlite3_free(pgc);
            }
        }

        if (pgc_next)
        {
            pgc = pgc_next;
            pgc_next = pgc->next;
            pgno = pgc->pgno;
            pList = pgc->hdr;
            goto find_next;
        }
    }
    if(pswap_count){
#if DEBUG_PRINT_FUNC
        printf("#########################\n     executing pswap %d\n",pswap_count);
#endif
        pswap(ptr1, ptr2, pswap_count);
    }
    pPager->dbOff = (pPager->dbSize + 1) * (pPager->pageSize);
    return rc;
}

int pdb_sqlite3PagerCommitPhaseOne(PdbPager *pPager, const char *zMaster, int noSync)
{
    int rc = SQLITE_OK;
    assert(pPager->eState == PAGER_WRITER_LOCKED || pPager->eState == PAGER_WRITER_CACHEMOD || pPager->eState == PAGER_WRITER_DBMOD || pPager->eState == PAGER_ERROR);
    assert(assert_pager_state(pPager));
#if DEBUG_PRINT_FUNC
    printf("calling pdb_sqlite3PagerCommitPhaseOne: %p\n", pPager);
#endif
    if (pPager->errCode)
        return pPager->errCode;
    /* printf("DATABASE SYNC: zMaster=%s, nSize=%d\n", zMaster, pPager->dbSize); */

    // if no database changes have been made, return early;
    /*if(pPager->eState < PAGER_WRITER_CACHEMOD) {
    printf("No database changes have been made! early return.\n");
    return SQLITE_OK;
    }*/

    assert(MEMDB == 0);
    if (0 == pdb_pagerFlushOnCommit(pPager, 1))
    {
        printf("FlushOnCommit: you shouldn't reach here!\n");
        exit(1);
    }
    else
    {
        //vpm-anonymous:

        // bBatch == 0
        rc = pdb_pager_incr_changecounter(pPager, 0);
        if (rc != SQLITE_OK)
        {
            printf("pager_incr_changecounter failed!\n");
            exit(1);
        }
        // sync the journal file and write all dirty pages to the database
        rc = pdb_syncJournal(pPager, 0);
        if (rc != SQLITE_OK)
        {
            printf("Failed to sync journal!\n");
            exit(1);
        }
        // write all pages from the memory to the disk
        rc = pdb_pager_write_pagelist(pPager);
        if (rc != SQLITE_OK)
        {
            assert(rc != SQLITE_IOERR_BLOCKED);
            printf("pdb page write pagelist failed!\n");
            exit(1);
        }
        // no need to clean pcache
        if (pPager->dbSize > pPager->dbPheapSize)
        {
            Pgno nNew = pPager->dbSize - (pPager->dbSize == PAGER_MJ_PGNO(pPager));
            assert(pPager->eState == PAGER_WRITER_DBMOD);
            rc = pdb_pager_truncate(pPager, nNew);
            if (rc != SQLITE_OK)
            {
                printf("pdb page truncate failed!\n");
                exit(1);
            }
        }
        /* Finally - sync the database heap... */
        rc = pdb_sqlite3PagerSync(pPager, zMaster);
        if (rc != SQLITE_OK)
        {
            printf("pdb pager sync failed \n");
            exit(1);
        }
    }

commit_phase_one_exit:
    if (rc == SQLITE_OK)
    {
        pPager->eState = PAGER_WRITER_FINISHED;
    }
    else
    {
        printf("Commit phase one failed!\n");
        exit(1);
    }
    return rc;
}

int pdb_sqlite3PagerCommitPhaseTwo(PdbPager *pPager)
{
#if DEBUG_PRINT_FUNC
    printf("calling pdb_sqlite3PagerCommitPhaseTwo: %p\n", pPager);
#endif
    int rc = SQLITE_OK;
    if (NEVER(pPager->errCode))
    {
        printf("pPager errCode nonzero!\n");
        return pPager->errCode;
    }
    assert(pPager->eState == PAGER_WRITER_LOCKED || pPager->eState == PAGER_WRITER_FINISHED || (pagerUseWal(pPager) && pPager->eState == PAGER_WRITER_CACHEMOD));
    assert(assert_pager_state(pPager));
    if (pPager->eState == PAGER_WRITER_LOCKED && pPager->exclusiveMode && pPager->journalMode == PAGER_JOURNALMODE_PERSIST)
    {
        assert(pPager->journalOff == JOURNAL_HDR_SZ(pPager) || !pPager->journalOff);
        pPager->eState = PAGER_READER;
        return SQLITE_OK;
    }
    pPager->iDataVersion++;
    rc = pdb_pager_end_transaction(pPager, pPager->setMaster, 1);
    // printf("COMMIT PHASE TWO! Writing the journal size and db size! dboff: %d\n", pPager->dbOff);
    // printf("Now I have dboff:%d\n", pPager->dbOff);
    // write dbOff into the pPager header ...
    pheap_write((void *)pPager->pbrk_start, &(pPager->dbOff), sizeof(pPager->dbOff));
    pPager->pbrk_current += sizeof(pPager->dbOff); // increase dbOff ...
    return pdb_pager_error(pPager, rc);
}

int pdb_sqlite3PagerGetJournalMode(PdbPager *pdbPager)
{
    return (int)pdbPager->journalMode;
}

// Pheap DB is not Memdb
int pdb_sqlite3PagerIsMemdb(PdbPager *pPager)
{
    return 0;
}

int pdb_sqlite3PagerExclusiveLock(PdbPager *pPager)
{
    int rc;
    assert(pPager->eLock == SHARED_LOCK || pPager->eLock == EXCLUSIVE_LOCK);
    rc = pdb_pagerLockDb(pPager, EXCLUSIVE_LOCK);
    if (rc != SQLITE_OK)
    {
        pdb_pagerUnlockDb(pPager, SHARED_LOCK);
    }
    return rc;
}
