#include <sys/syscall.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "pheap_flag.h"

#define PBRK_CALLNO 333
#define PATTACH_CALLNO 334
#define PDETACH_CALLNO 335
#define PSWAP_CALLNO 337

#define PHEAP_START 0x2a0002000000
#define PHEAP_SIZE 0x8000000
#define PHEAP_TEMP_SIZE 0x8000000

long pbrk(unsigned long newpbrk) {
  long ret = syscall(PBRK_CALLNO, newpbrk);
  return ret;
}

long pdetach() {
  long ret = syscall(PDETACH_CALLNO);
  return ret;
}

// We allocate 100M for the db
long pattach(const char * name, size_t len, int flag, unsigned long *pbrk_start) {
  long ret;
#ifdef SQLITE_ENABLE_NVDIMM
  // nvdimm enabled, allocate pmem through syscall
  ret = syscall(PATTACH_CALLNO, name, len, flag);
  *pbrk_start = PHEAP_START;
  if(ret < 0) {
    if(flag == PATTACH_CREATE) {
        // If create failed, try attach.
        ret = syscall(PATTACH_CALLNO, name, len, PATTACH_SHARE);
        if(ret < 0) {
	  printf("Failed to attach to the pheap, id conflict?\n");
	  exit(1);
        }
    } else {
      printf("Failed to attach to the pheap, id conflict?\n");
      exit(1);
    }
  }
  ret = pbrk(PHEAP_START + PHEAP_SIZE + PHEAP_TEMP_SIZE);// + PHEAP_TEMP_SIZE);
  if(ret < 0) {
    printf("Failed to increase pbrk to 256M\n");
    exit(1);
  }
#else
  // if nvdimm not enabled, simply malloc
  ret = 0;
  unsigned char* p = malloc(PHEAP_SIZE + PHEAP_TEMP_SIZE);
  memset(p, 0, PHEAP_SIZE + PHEAP_TEMP_SIZE);
  *pbrk_start = (unsigned long)p;
#endif
  printf("pbrk_start: %p\n", (void*)(*pbrk_start));
  return ret;
}

long pswap(u64 const * ptr1, u64 const * ptr2, size_t len){
  return syscall(PSWAP_CALLNO, ptr1, ptr2, len);
}
