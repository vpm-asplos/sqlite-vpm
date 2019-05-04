// Header: 2M
#define DB_SZ 0x200000
// database: 64M: 2^26 == 0x4000000
#define J_SZ  0x4200000
// journal file: 64M ...
#define J_END 0x8200000

#define PATTACH_CREATE 1
#define PATTACH_SHARE 2
#include <unistd.h>
long pattach(const char * name, size_t len, int flag, unsigned long *pbrk_start);
long pbrk(unsigned long newpbrk) ;
long pdetach(); 
