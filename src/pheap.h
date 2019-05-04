int pdb_sqlite3_opendatabase(const char *pheapname,
			     sqlite3 **ppDb, unsigned int flags, unsigned long pbrk_start); 
void pdb_open_db(ShellState *p, int keepAlive, int newflag);
