from berkeleydb import db
import json

class DatabaseInstance:
    def __init__(self) -> None:
        self.mydb = db.DB()
        self.mydb.open('myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)

    def get_cursor(self) :
        return self.mydb.cursor()
    
    def getTableDict(self) -> dict:
        tables = {}
        cursor = self.get_cursor()
        item = cursor.next()
        while item :
            tables[str(item[0], 'utf-8')] = self.bytes_to_dict(item[1])
            item = cursor.next()
        return tables
    
    def bytes_to_dict(self, item: bytes):
        return json.loads(item)
    
    def add_table(self, table_name: str, table_dict: dict) :
        self.mydb.put(bytes(table_name, 'utf-8'), json.dumps(table_dict).encode('utf-8'))
        return
    
    def drop_table(self, table_name:str):
        self.mydb.delete(bytes(table_name, 'utf-8'))
        return