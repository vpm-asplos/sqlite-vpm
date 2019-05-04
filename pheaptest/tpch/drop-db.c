#include <sys/syscall.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#define PHEAP_START 0x2a0002000000
#define PHEAP_SIZE 0x8000000
#define PBRK_CALLNO 333
#define PATTACH_CALLNO 334
#define PDETACH_CALLNO 335

int main(void) {
  const char * name="tpch";
  int size = 4;
  long ret = syscall(PATTACH_CALLNO, name, size, 2);
  void* start = (void*)PHEAP_START;
  memset(start, 0, PHEAP_SIZE);
  return 0;
}
