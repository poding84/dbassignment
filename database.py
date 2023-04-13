from lark import Tree, Token
from typing import List, Union, Optional
from databaseRepository import ColumnDefinition, TableConstraint, Query, dbrepo

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
        if item is None:
            return None
        li = []
        for column in item.children[1:-1]:
            li.append(column.children[0].lower())
        return li

    def parse_value_list(item: Tree):
        if item is None:
            return None
        li = []
        for column in item.children[1:-1]:
            if column.children[0].type == "STR":
                li.append(column.children[0][1:-1])
            else :
                li.append(column.children[0])
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

    def parse_select_list(item: Tree):
        li = []
        for child in item.children:
            if child.data == "STR":
                li.append(Query.Select(child[1:-1]))
            else :
                li.append(Query.Select(child))
        return li

    def parse_from_clause(item: Tree):
        li = []
        for child in item.children[1:]:
            table_name = child.children[0].children[0].children[0].lower()
            ref_name = child.children[0].children[2]
            if ref_name is not None:
                ref_name = ref_name.children[0].lower()
            li.append(Query.TableReference(
                table_name=table_name,
                ref_name=ref_name
            ))
        return li

    def parse_where_clause(item):
        pass

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
        table_name = Parser.parse_table_name(items[2])
        return dbrepo.drop_table(table_name)
        
    def select(self, items: List[Union[Tree, Token]]) -> Optional[str]:
        select_list = Parser.parse_select_list(items[1])
        from_clause = Parser.parse_from_clause(items[2].children[0])
        where_clause = Parser.parse_where_clause(items[2].children[1])
        return dbrepo.select(Query(
            select_list=select_list,
            from_clause=from_clause,
            where_clause=where_clause
        ))
        
    def insert(self, items: List[Union[Tree, Token]]) -> Optional[str]:
        table_name = Parser.parse_table_name(items[2])
        column_list = Parser.parse_column_list(items[3])
        row = Parser.parse_value_list(items[5])
        return dbrepo.insert(table_name, row, column_list)
        
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