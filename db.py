# db.py — manages source and target connections
import psycopg2
import psycopg2.extras
import psycopg2.pool
import logging
from config import SOURCE, TARGET

logger = logging.getLogger(__name__)

# Connection pools — reuse connections instead of opening new ones every cycle
_source_pool = None
_target_pool = None

def get_source_pool():
    global _source_pool
    if _source_pool is None:
        _source_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=3, **SOURCE
        )
        logger.info("Source DB connection pool created")
    return _source_pool

def get_target_pool():
    global _target_pool
    if _target_pool is None:
        _target_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=3, **TARGET
        )
        logger.info("Target DB connection pool created")
    return _target_pool

def source_conn():
    """Get a connection from the source pool (context manager)."""
    return _PooledConnection(get_source_pool())

def target_conn():
    """Get a connection from the target pool (context manager)."""
    return _PooledConnection(get_target_pool())

class _PooledConnection:
    def __init__(self, pool):
        self.pool = pool
        self.conn = None

    def __enter__(self):
        self.conn = self.pool.getconn()
        return self.conn

    def __exit__(self, exc_type, *args):
        if exc_type:
            self.conn.rollback()
        self.pool.putconn(self.conn)

def query_source(sql, params=None):
    """Run a SELECT on source DB, return list of dicts."""
    with source_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or [])
            return cur.fetchall()

def execute_target(sql, params=None):
    """Run a single statement on target DB."""
    with target_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
        conn.commit()

def executemany_target(sql, rows):
    """Bulk insert/upsert on target DB using execute_values."""
    if not rows:
        return 0
    with target_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
        conn.commit()
    return len(rows)
