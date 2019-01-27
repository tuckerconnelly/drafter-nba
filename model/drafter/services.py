import os

import sqlite3

print(os.environ.get('SQL_WRITE_URL'))

sql = sqlite3.connect(os.environ['SQL_WRITE_URL'])
sql.row_factory = sqlite3.Row
