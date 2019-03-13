from django.conf import settings
from django.db import connections
from django.db.models.sql.compiler import SQLCompiler
from django.db.models.sql.constants import MULTI
from django.db.models.sql.query import Query


def cursor_iter(cursor, fetch_size=1000):
    try:
        while True:
            cursor.execute('FETCH %s FROM seeker_cursor' % fetch_size)
            rows = cursor.fetchall()
            if not rows:
                raise StopIteration()
            yield rows
    finally:
        cursor.execute('ROLLBACK')
        cursor.close()


class CursorCompiler(SQLCompiler):

    def execute_sql(self, result_type=MULTI):
        if result_type != MULTI:
            raise ValueError('CursorCompiler can only be used for MULTI queries.')
        sql, params = self.as_sql()
        cursor = self.connection.cursor()
        cursor.execute('BEGIN')
        cursor.execute('DECLARE seeker_cursor CURSOR FOR ' + sql, params)
        return cursor_iter(cursor, fetch_size=getattr(settings, 'SEEKER_BATCH_SIZE', 1000))


class CursorQuery(Query):

    def get_compiler(self, using=None, connection=None):
        if using:
            connection = connections[using]
        return CursorCompiler(self, connection, using)
