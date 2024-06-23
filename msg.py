###########################################
#   Kyu Hyeok Seo         2018-*****      #
###########################################

# __str__ helps the class object to print a specific format

# Success message based on message description using an input
class CreateTableSuccess():
    def __init__(self, tablename):
        self.message = "'" + tablename + "'" + ' table is created'

    def __str__(self):
        return self.message

# Error message based on message description
class DuplicateColumnDefError(Exception):
    def __init__(self, message='Create table has failed: column definition is duplicated'):
        self.message = message

    def __str__(self):
        return self.message

# Error message based on message description
class DuplicatePrimaryKeyDefError(Exception):
    def __init__(self, message='Create table has failed: primary key definition is duplicated'):
        self.message = message

    def __str__(self):
        return self.message

# Error message based on message description
class ReferenceColumnStringError(Exception):
    def __init__(self, message='Create table has failed: referenced column should be string'):
        self.message = message

    def __str__(self):
        return self.message
    
# Error message based on message description
class ReferenceTypeError(Exception):
    def __init__(self, message='Create table has failed: foreign key references wrong type'):
        self.message = message

    def __str__(self):
        return self.message

# Error message based on message description
class ReferenceNonPrimaryKeyError(Exception):
    def __init__(self, message='Create table has failed: foreign key references non primary key column'):
        self.message = message

    def __str__(self):
        return self.message

# Error message based on message description
class ReferenceColumnExistenceError(Exception):
    def __init__(self, message='Create table has failed: foreign key references non existing column'):
        self.message = message

    def __str__(self):
        return self.message

# Error message based on message description
class ReferenceTableExistenceError(Exception):
    def __init__(self, message='Create table has failed: foreign key references non existing table'):
        self.message = message

    def __str__(self):
        return self.message

# Error message based on message description
class NonExistingColumnDefError(Exception):
    def __init__(self, colname, message='Create table has failed: '):
        self.message = message + "'" + colname + "'" + ' does not exist in column definition'

    def __str__(self):
        return self.message

# Error message based on message description
class TableExistenceError(Exception):
    def __init__(self, message='Create table has failed: table with the same name already exists'):
        self.message = message

    def __str__(self):
        return self.message

# Error message based on message description
class CharLengthError(Exception):
    def __init__(self, message='Char length should be over 0'):
        self.message = message

    def __str__(self):
        return self.message

# Success message based on message description using an input
class DropSuccess():
    def __init__(self, tablename, message=' table is dropped'):
        self.message = "'" + tablename + "'" + message

    def __str__(self):
        return self.message

# Error message based on message description
class NoSuchTable(Exception):
    def __init__(self, message='No such table'):
        self.message = message

    def __str__(self):
        return self.message

# Error message based on message description
class DropReferencedTableError(Exception):
    def __init__(self, tablename, message='Drop table has failed: '):
        self.message = message + "'" + tablename + "'" + " is referenced by other table"

    def __str__(self):
        return self.message

# Success message based on message description
class InsertResult():
    def __init__(self, message='1 row inserted'):
        self.message = message

    def __str__(self):
        return self.message

# Error message based on message description
class SelectTableExistenceError(Exception):
    def __init__(self, tablename, message='Selection has failed: '):
        self.message = message + "'" + tablename + "'" + " does not exist"

    def __str__(self):
        return self.message

# Error message based on message description
class InsertTypeMismatchError(Exception):
    def __init__(self, message='Insertion has failed: Types are not matched'):
        self.message = message

    def __str__(self):
        return self.message

# Error message based on message description
class InsertColumnExistenceError(Exception):
    def __init__(self, colname, message='Insertion has failed: '):
        self.message = message + "'" + colname + "'" + " does not exist"

    def __str__(self):
        return self.message
    
# Error message based on message description
class InsertColumnNonNullableError(Exception):
    def __init__(self, colname, message='Insertion has failed: '):
        self.message = message + "'" + colname + "'" + " is not nullable"

    def __str__(self):
        return self.message

# Success message based on message description using an input cnt
class DeleteResult():
    def __init__(self, cnt, message=' row(s) deleted'):
        self.message = "'" + str(cnt) + "'" + message

    def __str__(self):
        return self.message
    
# Error message based on message description
class WhereTableNotSpecified(Exception):
    def __init__(self, message='Where clause trying to reference tables which are not specified'):
        self.message = message

    def __str__(self):
        return self.message
    
# Error message based on message description
class WhereColumnNotExist(Exception):
    def __init__(self, message='Where clause trying to reference non existing column'):
        self.message = message

    def __str__(self):
        return self.message
    
# Error message based on message description
class WhereAmbiguousReference(Exception):
    def __init__(self, message='Where clause contains ambiguous reference'):
        self.message = message

    def __str__(self):
        return self.message
    
# Error message based on message description
class WhereIncomparableError(Exception):
    def __init__(self, message='Where clause trying to compare incomparable values'):
        self.message = message

    def __str__(self):
        return self.message
    
# Error message based on message description
# Customized exception without any document
# This exception occurs when select clause reference a table which is not specified in from clause
#
#
# (EX)
# select B.id from A;
class SelectTableNotSpecified(Exception):
    def __init__(self, message='Select clause trying to reference tables which are not specified in From clause'):
        self.message = message

    def __str__(self):
        return self.message
    
# Error message based on message description
class SelectColumnResolveError(Exception):
    def __init__(self, colname, message='Selection has failed: fail to resolve '):
        self.message = message + "'" + colname + "'"

    def __str__(self):
        return self.message

# Error message based on message description
class InsertDuplicatePrimaryKeyError(Exception):
    def __init__(self, message='Insertion has failed: Primary key duplication'):
        self.message = message

    def __str__(self):
        return self.message