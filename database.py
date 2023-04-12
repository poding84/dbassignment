from lark import Tree, Token
from typing import List, Union, Optional
from databaseRepository import ColumnDefinition, TableConstraint, dbrepo
from message import Message

class Parser:
    def __init__(self) -> None:
        pass
    
    def parse_table_name(item: Tree) -> Token:
        return item.children[0].lower()
    
    def parse_table_element_list(item: Tree) -> List[Union[ColumnDefinition, TableConstraint]]:
        li = []
        for child in item.children[1:-1] :
            li.append(
                Parser.parse_table_element(child)
            )
        return li

    def parse_table_element(item: Tree) -> Union[TableConstraint, ColumnDefinition]:
        if item.children[0].data == "column_definition" :
            return Parser.parse_column_definition(item.children[0])
        else :
            return Parser.parse_table_constraint_definition(item.children[0])
    
    def parse_table_constraint_definition(item: Tree) -> TableConstraint:
        if item.children[0].data == "primary_key_constraint" :
            return TableConstraint(
                key_type=TableConstraint.PRIMARY_KEY,
                column_list=Parser.parse_column_list(item.children[0].children[2])
            )
        else :
            return TableConstraint(
                key_type=TableConstraint.FOREIGN_KEY,
                column_list=Parser.parse_column_list(item.children[0].children[2]),
                reference_table=item.children[0].children[4].children[0].lower(),
                reference_column_list=Parser.parse_column_list(item.children[0].children[5]),
            )
            
    def parse_column_list(item: Tree) :
        li = []
        for column in item.children[1:-1]:
            li.append(column.children[0].lower())
        return li
    
    def parse_column_definition(item: Tree) -> ColumnDefinition:
        data_type, data_len = Parser.parse_data_type(item.children[1])
        return ColumnDefinition(
            column_name=item.children[0].children[0].lower(),
            data_type=data_type.lower(),
            data_len=data_len,
            not_null=item.children[3]
        )
    
    def parse_data_type(item: Tree) :
        if len(item.children) == 1 :
            return item.children[0], None
        else :
            return item.children[0], item.children[2]

class Database:
    def __init__(self) -> None:
        pass
    
    def pretty_print(self, items: List[Union[Tree, Token]]):
        for i, item in enumerate(items) :
            if type(item) == Tree:
                print("Tree/ " + item.pretty().__str__())
            else :
                print("Token/ " + item.__str__())
    
    def create_table(self, items: List[Union[Tree, Token]]) -> Optional[str]:
        table_name = Parser.parse_table_name(items[2])
        table_element_list = Parser.parse_table_element_list(items[3])
        return dbrepo.create_table(table_name=table_name, table_element_list=table_element_list)
        
    def drop_table(self, items: List[Union[Tree, Token]]) -> Optional[str]:
        self.pretty_print(items)
        
    def select(self, items: List[Union[Tree, Token]]) -> Optional[str]:
        self.pretty_print(items)
        
    def insert(self, items: List[Union[Tree, Token]]) -> Optional[str]:
        self.pretty_print(items)
        
    def drop_table(self, items: List[Union[Tree, Token]]) -> Optional[str]:
        self.pretty_print(items)
        
    def explain(self, items: List[Union[Tree, Token]]) -> Optional[str]:
        table_name = Parser.parse_table_name(items[1])
        return dbrepo.explain(table_name)
        
    def show_tables(self, items: List[Union[Tree, Token]]) -> Optional[str]:
        return dbrepo.show_tables()
        
    def delete(self, items: List[Union[Tree, Token]]) -> Optional[str]:
        self.pretty_print(items)
        
    def update_tables(self, items: List[Union[Tree, Token]]) -> Optional[str]:
        self.pretty_print(items)

myDatabase = Database()