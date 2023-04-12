from typing import List, Union, Tuple, Dict
from databaseInstance import DatabaseInstance
from message import Message, BORDER_LINE

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
            if column.column_name in self.primary_key_column.column_list:
                column.not_null = True

    def key_check(self) -> Tuple[bool, str]:
        if len(self.columns) != len(set(self.columns)) :
            return False, Message.DuplicateColumnDefError.getMessage()
        
        pk_exist = False
        for table_constraint in self.table_constraints:
            
            # Check whether the primary key shows only one time
            if table_constraint.is_primary_key() and not pk_exist:
                pk_exist = True
            elif table_constraint.is_primary_key() and pk_exist :
                return False, Message.DuplicatePrimaryKeyDefError.getMessage()
            
                            
            ref_table = dbrepo.get_table_instance(table_constraint.reference_table)
            if ref_table is None and not table_constraint.is_primary_key():
                return False, Message.ReferenceTableExistenceError.getMessage()

            if not table_constraint.is_primary_key() :
                if not set(table_constraint.reference_column_list).issubset(set(ref_table.schema.columns)):
                    return False, Message.ReferenceColumnExistenceError.getMessage()

                if set(ref_table.schema.primary_key_column.column_list) != set(table_constraint.reference_column_list) :
                    return False, Message.ReferenceNonPrimaryKeyError.getMessage()
            
            for index, column_name in enumerate(table_constraint.column_list):
                # Check whether the key column is existing in column list
                if column_name not in self.columns:
                    return False, Message.NonExistingColumnDefError.getMessage(column_name)
                
                curr_col = self.get_column(column_name)
                
                if not (curr_col.data_len is None or curr_col.data_len > 0) :
                    return False, Message.CharLengthError.getMessage()

                if not table_constraint.is_primary_key():
                    ref_col = ref_table.schema.get_column(table_constraint.reference_column_list[index])
                    if not curr_col.equal_type_with(ref_col):
                        return False, Message.ReferenceTypeError.getMessage()


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

class DatabaseRepository:
    tables: Dict[str, Table]
    def __init__(self) -> None:
        self.tables = {}
        self.dbInstance = DatabaseInstance()
        self.load_from_instance()
    
    def load_from_instance(self) :
        table_dicts = self.dbInstance.getTableDict()
        for table_name, table_dict in table_dicts.items():
            self.tables[table_name] = self.table_dict_to_class(table_dict)
            
    def table_dict_to_class(self, item: dict):
        return Table(item=item)
    
    def create_table(self, table_name: str, table_element_list: List[Union[ColumnDefinition, TableConstraint]]):
        if table_name in self.tables :
            return Message.TableExistenceError.getMessage()

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
        
        self.dbInstance.add_table(table_name, new_table.to_dict())
        self.tables[table_name] = new_table
        return Message.CreateTableSuccess.getMessage(table_name)
        
    def drop_table(self):
        pass
        
    def select(self):
        pass
        
    def insert(self):
        pass
        
    def drop_table(self):
        pass
        
    def explain(self, table_name):
        if table_name not in self.tables:
            return Message.NoSuchTable.getMessage()
        else :
            return self._show_table(table_name, True)
        
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
    
    def _show_table(self, table_name, include_headline = False):

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
                
        if include_headline:
            line += BORDER_LINE + "\n"
                
        line += f"table_name [{table_name}]\n"    
        
        for content in contents:
            for index in range (4) :
                line += f"{content[index]}" + (content_widths[index] - len(content[index]) + 3)*" "
            line += "\n"

        line += BORDER_LINE
        return line

dbrepo = DatabaseRepository()