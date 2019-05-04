 #!/usr/bin/env bash
if [ $# -eq 0 ]; then
	../../bld/sqlite3  < ./pheapq/pallq.sql
else
	../../bld/sqlite3 < ./pheapq/$1.sql
fi 
