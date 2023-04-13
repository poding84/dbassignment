from typing import List, Union, Tuple, Dict, Optional
from databaseInstance import DatabaseInstance
from message import Message, BORDER_LINE

class Query:
    class Select :
        def __init__(self, content) -> None:
            pass
    
    class TableReference :
        def __init__(self, table_name, ref_name) -> None:
            self.table_name = table_name
            self.ref_name = ref_name
    
    
    def __init__(self, select_list: List[Select], from_clause: List[TableReference], where_clause) -> None:
        self.select_list = select_list
        self.from_clause = from_clause
        self.where_clause = where_clause


class ColumnDefinition:
    CHAR = "char"
    INT = "int"
    DATE = "date"
    
    def __init__(self, column_name, data_type, data_len, not_null) -> None:
        self.column_name = column_name
        self.data_type = data_type
        self.data_len = int(data_len) if data_len is not None else None
        self.not_null = False if (not_null is None) or (not_null is False) else True

    def to_dict(self) :
        return {
            "column_name" : self.column_name,
            "data_type" : self.data_type,
            "data_len" : self.data_len,
            "not_null" : self.not_null
        }
    
    def __str__(self) -> str:
        return f"{self.column_name} {self.data_type} {self.data_len} {self.not_null}"
    
    def equal_type_with(self, other):
        return self.data_type == other.data_type and self.data_len == other.data_len

class TableConstraint:
    FOREIGN_KEY = "foreign_key"
    PRIMARY_KEY = "primary_key"
    
    def __init__(self, key_type, column_list, reference_table = None, reference_column_list = None) -> None:
        self.key_type = key_type
        self.column_list = column_list
        self.reference_table = reference_table
        self.reference_column_list = reference_column_list
        
    def to_dict(self) :
        return {
            "key_type" : self.key_type,
            "column_list" : self.column_list,
            "reference_table"  : self.reference_table,
            "reference_column_list" : self.reference_column_list
        }

    def is_primary_key(self) -> bool:
        return self.key_type == TableConstraint.PRIMARY_KEY

class Schema:
    def __init__(self, column_definitions = None, table_constraints = None, item: dict = None) -> None:
        if item is None:
            self.column_definitions = column_definitions
            self.table_constraints = table_constraints
        else :
            self.column_definitions = [ColumnDefinition(
                column_name=cd["column_name"],
                data_type=cd["data_type"],
                data_len=cd["data_len"],
                not_null=cd["not_null"]
            ) for cd in item["column_definitions"]]
            self.table_constraints = [TableConstraint(
                key_type=tc["key_type"],
                column_list=tc["column_list"],
                reference_table=tc["reference_table"],
                reference_column_list=tc["reference_column_list"]
            ) for tc in item["table_constraints"]]
            self.setup()
        
        self.column_dict = None
        self.columns = []
        for column in self.column_definitions:
            self.columns.append(column.column_name)
    
    def setup(self) :
                
        self.primary_key_column = None
        for table_constraint in self.table_constraints:
            if table_constraint.is_primary_key():
                self.primary_key_column = table_constraint
                
        self.column_dict = {}
        for column in self.column_definitions:
            self.column_dict[column.column_name] = column
            if self.primary_key_column is not None and column.column_name in self.primary_key_column.column_list:
                column.not_null = True

    def key_check(self) -> Tuple[bool, str]:
        if len(self.columns) != len(set(self.columns)) :
            return False, Message.DuplicateColumnDefError.get_message()
        
        pk_exist = False
        for table_constraint in self.table_constraints:
            
            # Check whether the primary key shows only one time
            if table_constraint.is_primary_key() and not pk_exist:
                pk_exist = True
            elif table_constraint.is_primary_key() and pk_exist :
                return False, Message.DuplicatePrimaryKeyDefError.get_message()
            
                            
            ref_table = dbrepo.get_table_instance(table_constraint.reference_table)
            if ref_table is None and not table_constraint.is_primary_key():
                return False, Message.ReferenceTableExistenceError.get_message()

            if not table_constraint.is_primary_key() :
                if not set(table_constraint.reference_column_list).issubset(set(ref_table.schema.columns)):
                    return False, Message.ReferenceColumnExistenceError.get_message()

                if set(ref_table.schema.primary_key_column.column_list) != set(table_constraint.reference_column_list) :
                    return False, Message.ReferenceNonPrimaryKeyError.get_message()
            
            for index, column_name in enumerate(table_constraint.column_list):
                # Check whether the key column is existing in column list
                if column_name not in self.columns:
                    return False, Message.NonExistingColumnDefError.get_message(column_name)
                
                curr_col = self.get_column(column_name)
                
                if not (curr_col.data_len is None or curr_col.data_len > 0) :
                    return False, Message.CharLengthError.get_message()

                if not table_constraint.is_primary_key():
                    ref_col = ref_table.schema.get_column(table_constraint.reference_column_list[index])
                    if not curr_col.equal_type_with(ref_col):
                        return False, Message.ReferenceTypeError.get_message()


        self.setup()
        return True, None
    
    def get_column(self, column_name) -> ColumnDefinition :
        if self.column_dict is not None :
            if column_name in self.column_dict:
                return self.column_dict[column_name]
        else :
            for column in self.column_definitions :
                if column_name == column.column_name:
                    return column
            
        return None
                
    
    def get_column_key(self, column_name) -> Tuple[bool, bool]:
        is_primary = False
        is_foreign = False
        for table_constraint in self.table_constraints:
            if column_name in table_constraint.column_list:
                if table_constraint.key_type == TableConstraint.FOREIGN_KEY:
                    is_foreign = True
                else :
                    is_primary = True

        return is_primary, is_foreign

    def to_dict(self):
        return {
            "column_definitions" : [item.to_dict() for item in self.column_definitions],
            "table_constraints" : [item.to_dict() for item in self.table_constraints]
        }

class Table:
    def __init__(self, table_name: str = "", schema: Schema = None, item: dict = None) -> None:
        self.table_name = table_name
        if item is None:
            self.schema = schema
            self.rows = []
        else :
            self.schema = Schema(item = item["schema"])
            self.rows = item["rows"]
    
    def to_dict(self):
        return {
            "schema" : self.schema.to_dict(),
            "rows" : self.rows
        }
        
    def references(self, target_table_name: str):
        for table_constraint in self.schema.table_constraints:
            if not table_constraint.is_primary_key():
                if table_constraint.reference_table == target_table_name :
                    return True

        return False
    
    def insert_row(self, row, column_list):
        if column_list is not None:
            row_dict = {}
            for index in range(len(row)):
                row_dict[column_list[index]] = row[index]
            
            new_row = []
            for column in self.schema.columns:
                new_row.append(row_dict[column])
            self.rows.append(new_row)
        else :
            self.rows.append(row)
        
        return True, Message.InsertResult.get_message()

class DatabaseRepository:
    tables: Dict[str, Table]
    def __init__(self) -> None:
        self.tables = {}
        self.dbInstance = DatabaseInstance()
        self.load_from_instance()
    
    def load_from_instance(self) :
        table_dicts = self.dbInstance.getTableDict()
        for table_name, table_dict in table_dicts.items():
            self.tables[table_name] = self.table_dict_to_class(table_name = table_name, item = table_dict)
            
    def table_dict_to_class(self, table_name: str, item: dict):
        return Table(
            table_name=table_name,
            item=item
        )
    
    def create_table(self, table_name: str, table_element_list: List[Union[ColumnDefinition, TableConstraint]]):
        if table_name in self.tables :
            return Message.TableExistenceError.get_message()

        column_definitions = []
        table_constraints = []
        for element in table_element_list :
            if type(element) == ColumnDefinition :
                column_definitions.append(element)
            else :
                table_constraints.append(element)
        schema = Schema(
            column_definitions=column_definitions,
            table_constraints=table_constraints
        )
        
        is_valid_key, message = schema.key_check()
        if not is_valid_key:
            return message
        
        new_table = Table(
            table_name=table_name,
            schema=schema
        )
        
        self._save_table(new_table)
        return Message.CreateTableSuccess.get_message(table_name)
        
    def drop_table(self, table_name: str):
        if table_name not in self.tables :
            return Message.NoSuchTable.get_message()
        
        for t_name, t_inst in self.tables.items():
            if t_inst.references(table_name) :
                return Message.DropReferencedTableError.get_message(t_name)
        
        self._drop_table(table_name)
        return Message.DropSuccess.get_message(table_name)
        
    def select(self, query: Query):
        for table_ref in query.from_clause:
            if table_ref.table_name not in self.tables :
                return Message.NoSuchTable.get_message()
            
        return self._show_query(*self._parse_query(query))
        
    def insert(self, table_name: str, row: List[str], column_list: Optional[List[str]]):
        if table_name not in self.tables:
            return Message.NoSuchTable.get_message()
        
        success, message = self.tables[table_name].insert_row(row, column_list)
        if success:
            self._save_table(self.tables[table_name])
        
        return message
        
        
    def explain(self, table_name):
        if table_name not in self.tables:
            return Message.NoSuchTable.get_message()
        else :
            return self._show_table(table_name)
        
    def show_tables(self):
        line = "\n"
        line += BORDER_LINE + "\n"
        for key in self.tables.keys():
            line += key + "\n"
        line += BORDER_LINE
        return line
        
    def delete(self):
        pass
        
    def update_tables(self):
        pass
    
    def get_table_instance(self, table_name):
        if table_name not in self.tables :
            return None
        
        return self.tables[table_name]
    
    def _parse_query(self, query:Query):
        column_list = []
        rows = []
        widths = []
        for table_ref in query.from_clause: # Currently only one table
            table = self.tables[table_ref.table_name]
            for column in table.schema.columns:
                column_list.append(column)
                widths.append(len(column))
            
            for row in table.rows:
                rows.append(row)
                for index in range(len(row)):
                    widths[index] = max(widths[index], len(row[index]))
                
        return column_list, rows, widths
    
    def _show_query(self, column_list, rows, widths):
        line = "\n"
        col_list = [c.upper() for c in column_list]
        for vars in [None, col_list, None, *rows, None]:
            for index in range(len(widths)):
                if vars is None:
                    line += "+" + "-" * (widths[index]+2)
                else:
                    line += "| " + vars[index] + " " * (widths[index] - len(vars[index]) + 1)
            
            if vars is None:
                line += "+\n"
            else:
                line += "|\n"
                
        return line
    
    def _save_table(self, table: Table):
        self.dbInstance.add_table(table.table_name, table.to_dict())
        self.tables[table.table_name] = table
        return
    
    def _drop_table(self, table_name: str):
        self.dbInstance.drop_table(table_name)
        self.tables.pop(table_name)
        return
    
    def _show_table(self, table_name):

        contents = [("column_name", "type", "null", "key")]
        content_widths = [11, 4, 4, 3]
        for column in self.tables[table_name].schema.column_definitions:
            column_name = column.column_name
            column_type = column.data_type
            if column_type == ColumnDefinition.CHAR :
                column_type += f"({column.data_len})"
            column_null = "N" if column.not_null else "Y"
            is_primary, is_foreign = self.tables[table_name].schema.get_column_key(column_name)
            column_key = ""
            if is_primary:
                column_key += "PRI"
                if is_foreign:
                    column_key += "/FOR"
            elif is_foreign:
                column_key += "FOR"
                
            contents.append((column_name, column_type, column_null, column_key))
            content_widths = [max(content_widths[index], len(contents[-1][index])) for index in range(4)]
        
        line = "\n"
        line += BORDER_LINE + "\n"        
        line += f"table_name [{table_name}]\n"    
        
        for content in contents:
            for index in range (4) :
                line += f"{content[index]}" + (content_widths[index] - len(content[index]) + 3)*" "
            line += "\n"

        line += BORDER_LINE
        return line

dbrepo = DatabaseRepository()