from enum import Enum

BORDER_LINE = "-----------------------------------------------------------------"

class Message(Enum): 
    SyntaxError = 1
    CreateTableSuccess = 2
    DuplicateColumnDefError = 3
    DuplicatePrimaryKeyDefError = 4
    ReferenceTypeError = 5
    ReferenceNonPrimaryKeyError = 6
    ReferenceColumnExistenceError = 7
    ReferenceTableExistenceError = 8
    NonExistingColumnDefError = 9
    TableExistenceError = 10
    CharLengthError = 11
    DropSuccess = 12
    NoSuchTable = 13
    DropReferencedTableError = 14
    InsertResult = 15
    SelectTableExistenceError = 16
    
    def get_message(self, arg = "") -> str :
        message: str
        need_args = False
        if self == Message.SyntaxError :
            message = "Syntax error"
        elif self == Message.CreateTableSuccess :
            message = f"'{arg}' table is created"
        elif self == Message.DuplicateColumnDefError :
            message = "Create table has failed: column definition is duplicated"
        elif self == Message.DuplicatePrimaryKeyDefError :
            message = "Create table has failed: primary key definition is duplicated"
        elif self == Message.ReferenceTypeError:
            message = "Create table has failed: foreign key references wrong type"
        elif self == Message.ReferenceNonPrimaryKeyError :
            message = "Create table has failed: foreign key references non primary key column"
        elif self == Message.ReferenceColumnExistenceError :
            message = "Create table has failed: foreign key references non existing column"
        elif self == Message.ReferenceTableExistenceError :
            message = "Create table has failed: foreign key references non existing table"
        elif self == Message.NonExistingColumnDefError :
            need_args = True
            message = f"Create table has failed: '{arg}' does not exist in column definition"
        elif self == Message.TableExistenceError:
            message = "Create table has failed: table with the same name already exists"
        elif self == Message.CharLengthError:
            message = "Char length should be over 0"
        elif self == Message.DropSuccess:
            message = f"'{arg}' table is dropped"
        elif self == Message.NoSuchTable:
            message = "No such table"
        elif self == Message.DropReferencedTableError:
            need_args = True
            message = f"Drop table has failed: '{arg}' is referenced by other table"
        elif self == Message.InsertResult:
            message = "The row is inserted"
        elif self == Message.SelectTableExistenceError:
            need_args = True
            message = f"Selection has failed: '{arg}' does not exist"
        
        if need_args and arg == "" :
            raise Exception("Given message needs argument! : ", self)
        return message