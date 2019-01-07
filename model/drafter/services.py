import os

import records

pgr = records.Database(os.environ['PG_READ_URL'])
pgw = records.Database(os.environ['PG_WRITE_URL'])
