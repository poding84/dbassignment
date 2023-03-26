from lark import Transformer, Lark

from typing import List

PROMPT_CONST = "DB_2018-10371> "

class MyTransformer(Transformer):
    def print_request(self, request):
        print(PROMPT_CONST + request)
    
    def create_table_query(self, items):
        self.print_request("'CREATE TABLE' requested")
        
    def drop_table_query(self, items):
        self.print_request("'DROP TABLE' requested")
        
    def select_query(self, items):
        self.print_request("'SELECT' requested")
        
    def insert_query(self, items):
        self.print_request("'INSERT' requested")
        
    def drop_table_query(self, items):
        self.print_request("'DROP TABLE' requested")
        
    def explain_query(self, items):
        self.print_request("'EXPLAIN' requested")
        
    def describe_query(self, items):
        self.print_request("'DESCRIBE' requested")
        
    def desc_query(self, items):
        self.print_request("'DESC' requested")
        
    def show_tables_query(self, items):
        self.print_request("'SHOW TABLES' requested")
        
    def delete_query(self, items):
        self.print_request("'DELETE' requested")
        
    def update_tables_query(self, items):
        self.print_request("'UPDATE' requested")
    
    def EXIT(self, items):
        return None


class DatabaseManagementSystem :
    def __init__(self) -> None:
        with open('grammar.lark') as file:
            self.sql_parser = Lark(file.read(), start="command", lexer="basic")
    
    def get_input_from_prompt(self):
        return input(PROMPT_CONST)
    
    def print_to_prompt(self, statement):
        return print(PROMPT_CONST + statement)
    
    def get_querys(self):
        query = self.get_input_from_prompt()
        query_list = query.split(";")
        while query_list[-1].strip() != "" :
            next_query = query_list[-1] + "\n" + input()
            query_list.pop()
            query_list.extend(next_query.split(";"))

        return query_list
        
    def parse_querys(self, querys: List[str]):
        for query in querys:
            query = query.replace('\n', ' ')
            try :
                output = self.parse_query(query + ";")
            except :
                self.print_to_prompt("Syntax error")
                output = None
            
            return output
    
    def parse_query(self, query: str):
        return self.sql_parser.parse(query)
    
    def transform_query(self, output):
        if output is None :
            return None

        return MyTransformer().transform(output)
    
    def run_dbms():
        dbms = DatabaseManagementSystem()
        query = dbms.get_querys()
        while dbms.transform_query(dbms.parse_querys(query)) is None :
            query = dbms.get_querys()

DatabaseManagementSystem.run_dbms()