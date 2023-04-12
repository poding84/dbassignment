from lark import Transformer, Lark, Tree, Token
from database import myDatabase
from typing import List, Union

# Declare const for printing the DBMS prompt
PROMPT_CONST = "DB_2018-10371> "

# Execute the actual query
class MyTransformer(Transformer):
    
    # Print function for DBMS
    def print_request(self, request):
        if request is not None and request != "" :
            print(PROMPT_CONST + request)
    
    def create_table_query(self, items: List[Union[Tree, Token]]):
        message = myDatabase.create_table(items)
        self.print_request(message)
        
    def drop_table_query(self, items):
        message = myDatabase.drop_table(items)
        self.print_request(message)
        
    def select_query(self, items):
        message = myDatabase.select(items)
        self.print_request(message)
        
    def insert_query(self, items):
        message = myDatabase.insert(items)
        self.print_request(message)
        
    def drop_table_query(self, items):
        message = myDatabase.drop_table(items)
        self.print_request(message)
        
    def explain_query(self, items):
        message = myDatabase.explain(items)
        self.print_request(message)
        
    def describe_query(self, items):
        message = myDatabase.explain(items)
        self.print_request(message)
        
    def desc_query(self, items):
        message = myDatabase.explain(items)
        self.print_request(message)
        
    def show_tables_query(self, items):
        message = myDatabase.show_tables(items)
        self.print_request(message)
        
    def delete_query(self, items):
        message = myDatabase.delete(items)
        self.print_request(message)
        
    def update_tables_query(self, items):
        message = myDatabase.update_tables(items)
        self.print_request(message)
    
    # This will return True to terminate
    def EXIT(self, items):
        return True


# DBMS class
class DatabaseManagementSystem :
    def __init__(self) -> None:
        with open('grammar.lark') as file:
            self.sql_parser = Lark(file.read(), start="command", lexer="basic")
    
    # get input with prompt const
    def get_input_from_prompt(self):
        return input(PROMPT_CONST)
    
    # print function for DBMS
    def print_to_prompt(self, statement):
        return print(PROMPT_CONST + statement)
    
    # get queries from prompt
    # it will get the inputs until there is nothing after the semicolon at the end of the sentence
    def get_queries(self):
        query = self.get_input_from_prompt()
        query_list = query.split(";")
        while query_list[-1].strip() != "" :
            next_query = query_list[-1] + "\n" + input()
            query_list.pop()
            query_list.extend(next_query.split(";"))
            
        return query_list[:-1] # because of the last element of the query_list has empty string
        
    # parsing the queries and return the list of them
    # if it fails to parse query, it will apend None and stop parsing.
    def parse_queries(self, queries: List[str]):
        outputs = []
        for query in queries:
            query = query.replace('\n', ' ')
            try :
                outputs.append(self.parse_query(query + ";"))
            except :
                outputs.append(None)
                break
            
        return outputs
    
    def parse_query(self, query: str):
        return self.sql_parser.parse(query)
    
    # transforms the query based on the given outputs
    # if the output is None, then print the syntax error and return
    # if the given output is EXIT command, then it will return True
    def transform_query(self, outputs):
        for output in outputs:
            if output is None :
                self.print_to_prompt("Syntax error")
                return False
            ans: Tree = MyTransformer().transform(output)
            if ans.children[0] == True:
                return True
        
        return False
    
    def run_dbms():
        dbms = DatabaseManagementSystem()
        queries = dbms.get_queries()
        
        # It will process the query until meeting the EXIT command
        while not dbms.transform_query(dbms.parse_queries(queries)):
            queries = dbms.get_queries()

# Run the DBMS system
DatabaseManagementSystem.run_dbms()