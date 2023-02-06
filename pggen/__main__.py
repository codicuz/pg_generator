import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

class TestRecord():
    @property
    def sername(self):
        return self.__sername
    
    @sername.setter
    def sername(self, sername):
        self.__sername = sername

    @property
    def name(self):
        return self.__name
    
    @property
    def middle_name(self):
        return self.__middle_name

    @property
    def passport(self):
        return self.__passport
    
    @passport.setter
    def passport(self, passport):
        self.__passport = passport

    def get_insert(self):
        return f'''insert into test_tbl values('{self.sername}', '{self.name}', '{self.middle_name}');'''

    def get_insert_with_bolb(self, pg_blob_entity):
        return f'''insert into test_tbl values('{self.sername}', '{self.name}', '{self.middle_name}', {pg_blob_entity});'''
    
    def get_blob(self):
        f = open('blob01.png', 'rb').read()
        
        return f

    def __init__(self, sername, name, middle_name):
        self.__sername = sername
        self.__name = name
        self.__middle_name = middle_name
        self.__passport = None


def main():
    print('Hello')

def create_db():
    conn = psycopg2.connect(host="localhost", database="postgres", password="postgres", user="postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    cursor.execute('DROP DATABASE IF EXISTS test_db;')
    print(cursor.query.decode())
    cursor.execute('CREATE DATABASE test_db;')
    print(cursor.query.decode())
    cursor.close()
    conn.close()

def create_test_table():
    conn = psycopg2.connect(host="localhost", database="test_db", password="postgres", user="postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    cursor.execute('drop table if exists test_tbl;')
    print(cursor.query.decode())
    cursor.execute('create table if not exists test_tbl (sername varchar(40), name varchar(40), middle_name varchar(40), passport bytea);')
    print(cursor.query.decode())
    cursor.close()
    conn.close()

def get_record_by_sequence():
    
    conn = psycopg2.connect(host="localhost", database="test_db", password="postgres", user="postgres")
    cursor = conn.cursor()

    conn.commit()
    
    for i in range(1000):
        rec = TestRecord('Пушкин', 'Александ', 'Сергеевич')
        rec.sername += '_' + str(i)
        rec.passport = psycopg2.Binary(rec.get_blob()) 
        cursor.execute(rec.get_insert_with_bolb(rec.passport))
        # print(cursor.query.decode())

        if i % 10 == 0:
            print(f'Reccord is: {i}. Commit.')
            conn.commit()
    
    conn.commit()

    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
    # create_db()
    create_test_table()
    get_record_by_sequence()
