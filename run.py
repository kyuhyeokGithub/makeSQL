###########################################
#   Kyu Hyeok Seo         2018-*****      #
###########################################

from lark import Lark
import sys
from lark import Transformer
from database import *
from msg import *
from argument import *


class SQL_Transformer(Transformer):
    def __init__(self):
        super().__init__()
        self.do = ""    # What do you do
        self.init_table = {           # Only used for a table creation
            "name" : "",              # table name
            "column_list" : [],       # columns name
            "not_null_set" : set(),   # columns name which should not null
            "pk_list" : [],           # a combination of PK
            "fk_dict" : dict()        # dictionary to find which table and columns are referenced
        }
        self.target_table = []    # Target table for DROP, EXPLAIN, DESCRIBE, DESC, INSERT, DELETE, UPDATE, SELECT
        self.target_value = []    # Tuples to be inserted or updated
        self.target_column = []   # Column Names about insertion or update
        self.select_column = []   # Column Names to be selected
        self.where_part = []      # Save where part

    def command(self, items):
        if items[0] == 'exit': # In command, we can find "exit" here
            self.do = 'exit'
        return self.do, self.init_table, self.target_table, self.target_column, self.target_value, self.select_column, self.where_part

    def query_list(self, items):
        # Just return items eliminating '[]'
        return items[0]

    def query(self, items):
        # Just return items eliminating '[]'
        return items[0]
    
    def create_table_query(self, items):
        table_name = items[2].lower() # Get table name and makes it lowercase alphabet
        self.do = "CREATE TABLE"      # Save what type of sql is
        self.init_table["name"] = table_name # Save the table name
        return items
    
    def table_element_list(self, items):
        tmp = []                     # Make items to list
        for element in items:
            tmp.append(element)
        return tmp

    def table_element(self, items):
        return items[0]      # Get rid of "[]"
    
    def column_definition(self, items):
        c_name = items[0]    # Get column name
        c_dtype = items[1]   # Get column data type
        c_notnull = None     # Default value about whether column is null or not
        if len(items) == 4 and items[2] == "not" and items[3] == "null":
            c_notnull = 'not null' # if column should not be null, set c_notnull as 'not null'
        self.init_table["column_list"].append((c_name, c_dtype, c_notnull)) # Add column after grouping column information
        if c_notnull == 'not null':
            self.init_table["not_null_set"].add(c_name)   # Add column name to self.init_table
        return items
    
    def table_constraint_definition(self, items):
        return items[0]      # Get rid of "[]"

    def primary_key_constraint(self, items):
        self.init_table["pk_list"].append(items[2])  # Get pk and save it into self.init_table
        # Fix it during pj2 : if there are primary key more than two, we have to record it through using append
        return items
    

    def referential_constraint(self, items):
        self.init_table["fk_dict"][items[2][0]] = (items[4], items[5][0]) # Get fk and save it into self.init_table
        
        # i.e. self.init_table["fk_dict"][a_id] = (advisor, ID)
        return items
    
    
    def column_name_list(self, items):
        tmp = []
        for i in range(1, len(items)-1):
            tmp.append(items[i])
        # Make items to list except "[]" both sides
        return tmp
    

    def data_type(self, items):
        # Concat data type (i.e. char, (, 15, ) -> char(15))
        tmp = ""
        for x in items:
            tmp += x
        return tmp
    
    def table_name(self, items):
        # get table name value and make it lowercase
        tmp = items[0].lower()
        return tmp

    def column_name(self, items):
        # get column name value and make it lowercase
        tmp = items[0].lower()
        return tmp
    
    def drop_table_query(self, items):
        self.do = "DROP TABLE" # Set self.do as "DROP TABLE"
        table_name = items[2].lower() # Get which table will be dropped
        self.target_table.append(table_name) # Save table_name into self.target_table (list)
        return items
    
    def explain_query(self, items):
        self.do = "EXPLAIN"  # Set self.do as "EXPLAIN TABLE"
        table_name = items[1].lower()  # Get which table will be explained
        self.target_table.append(table_name) # Save table_name into self.target_table (list)
        return items
    
    def describe_query(self, items):
        self.do = "DESCRIBE"  # Set self.do as "DESCRIBE TABLE"
        table_name = items[1].lower() # Get which table will be described
        self.target_table.append(table_name) # Save table_name into self.target_table (list)
        return items

    def desc_query(self, items):
        self.do = "DESC"  # Set self.do as "DESC TABLE"
        table_name = items[1].lower() # Get which table will be desc
        self.target_table.append(table_name) # Save table_name into self.target_table (list)
        return items
    
    def show_tables_query(self, items):
        self.do = "SHOW TABLES" # Set self.do as "SHOW TABLES"
        # We don't need table_name compared to other queries above
        return items

    def select_query(self, items):
        self.do = "SELECT"
        # items[1], items[2] >> select_list, table_expression
        self.select_column = items[1]
        # items[2][0] : from_clause
        # items[2][1] : where_clause
        self.target_table = items[2][0]
        self.where_part = items[2][1]
        return items
    
    def select_list(self, items):
        # items : i.e. [(None, 'customer_name'), ('borrower', 'loan_number')]
        return items

    def selected_column(self, items):
        # tbl : table_name
        # col : column_name in tbl
        tbl, col = items[0], items[1]
        return (tbl, col)

    def table_expression(self, items):
        # just return
        return items
    
    def from_clause(self, items):
        tmp = items[1] # return 2nd element of a list because 1st element is always "FROM"
        return tmp
    
    def table_reference_list(self, items):
        # just return
        return items
    
    def referred_table(self, items):
        tmp = (items[0], items[2]) # return table own name and its alias (if there is NO alias, it is "None")
        return tmp

    def where_clause(self, items):
        tmp = items[1] # return where clause except "where" (=items[0]) 
        return tmp
    
    def boolean_expr(self, items):
        tmp = []
        for i in range(0, len(items), 2): # In order to get rid of "OR" Token between "boolean factor"s
            tmp.append(items[i])
        if len(tmp) == 1:
            return tmp[0] # return w/o "[]"
        else :
            return ('or', tmp)
    
    def boolean_term(self, items):
        tmp = []
        for i in range(0, len(items), 2): # In order to get rid of "AND" Token between "boolean factor"s
            tmp.append(items[i])
        if len(tmp) == 1:
            return tmp[0] # return w/o "[]"
        else :
            return ('and', tmp)
    
    def boolean_factor(self, items):
        if items[0] : # If it is the "NOT" for boolean test
            n = items[0].lower()
        else : # If it is NOT the "NOT" for boolean test
            n = None
        bool_test = items[1]
        tmp = (n, bool_test) # (whether the factor is "NOT" or None, and return boolean test below)
        return tmp

    def boolean_test(self, items):
        tmp = items[0] # return predicate or parenthesized_boolean_exp
        return tmp
    
    def parenthesized_boolean_expr(self, items):
        tmp = items[1] # remove '(' and ')' both sides
        return tmp
    
    def predicate(self, items):
        # items : [comparison_predicate] -> return items[0] in order to reduplicated bracket
        tmp = items[0]
        return tmp
    
    def comparison_predicate(self, items):
        # items : [left_comp_operand, comp_op, right_comp_operand]
        return items

    def comp_op(self, items): # Retrieve token value
        return items[0].value

    def comp_operand(self, items):
        # items's length is 1(comparable_value) or 2(table name, column_name)
        if len(items) == 1: # when items is comparable value
            return items[0]
        elif len(items) == 2: # when items is (table_name, column_name)
            return (items[0], items[1])
        return items
    
    def comparable_value(self, items):
        tmp = items[0].value # tmp is INT, STR, or DATE
        if tmp.endswith("'") or tmp.endswith('"') : # Case : STR
            # return the value with ' or "
            return tmp.lower()
        elif tmp.isdigit(): # Case : INT
            return int(tmp)
        else : # Case : DATE
            return tmp

    def null_predicate(self, items):
        # items -> [table, column, null_operation]
        tmp = [(items[0], items[1]), items[2], None] # To match a predicate's format
        return tmp
    
    def null_operation(self, items):
        # items -> is, not, null OR is, null
        if items[1] == 'not':
            return 'not null'
        else :
            return 'null'

    def insert_query(self, items):
        self.do = "INSERT"
        # items -> INSERT, INTO, table_name, cols, VALUES, value_list
        tbl = items[2]
        cols = items[3]
        value_list = items[5]
        self.target_table.append(tbl)
        self.target_column = cols
        self.target_value = value_list
        return items
    
    def value_list(self, items):
        # Make items to list except LP and RP both sides
        tmp = []
        for i in range(1, len(items)-1):
            tmp.append(items[i])
        return tmp
    
    def dml_value(self, items):
        tmp = items[0].value # tmp is INT, STR, DATE, or NULL
        if tmp.endswith("'") or tmp.endswith('"') : # Case : STR
            # return the value with ' or "
            return tmp.lower()
        elif tmp.isdigit(): # Case : INT
            return int(tmp)
        elif tmp.lower() == 'null': # Case : null
            return None
        else : # Case : DATE
            return tmp
    
    def delete_query(self, items):
        self.do = "DELETE"
        # items : DELETE, FROM, table_name, where_clause
        self.target_table.append(items[2])
        self.where_part = items[3]
        return items

    def update_query(self, items):
        self.do = "UPDATE"
        # items : UPDATE, table_name, SET, column_name, EQUAL, dml_value , where_clause
        self.target_table.append(items[1])
        self.target_column.append(items[3])
        self.target_value.append(items[5])
        self.where_part = items[6]
        return items


################## START MAIN #########################
#
#
# load a sql parser based on grammar defined in grammar.lark
with open('grammar.lark') as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")

mainDB = connect_db(MAIN_DB_NAME)

is_exit = 0    # indicator whether "exit" is appeared
try :
    while is_exit == 0 :    # Parse sqls until "exit" is appeared
        SQL_input = input("DB_2018-10786> ")    # Prompt design
        while True: 
            # Get sql sequences(one-line) until ending with ";"
            SQL_input = SQL_input.strip()
            if SQL_input.endswith(";"): 
                # When input ends with ";" (=after type input and enter)
                break
            else : 
                # When type ENTER, input does not end with ";"
                # It means sql is continued
                SQL_input += " " + sys.stdin.readline().strip()
        # Save sql sequence as a sql_list splitting with ";"
        SQL_input_list = SQL_input.split(";")
        SQL_list = []
        for each_sql in SQL_input_list:
            # Save each sql in sql_list using strip() and add ";" to the right side of each_sql for parsing
            tmp = each_sql.strip()    # eliminate white spaces
            if tmp == "":    # If sql is "" after eliminating white spaces
                continue
            SQL_list.append(tmp+";")    # Add ";" to the right side of each_sql for parsing

            
        for sql in SQL_list :    # Dealing with each_sql in SQL_list
            sql_transformer = SQL_Transformer()     # Loading sql_transformer defined above
            try :
                output = sql_parser.parse(sql)      # Parsing using grammar.lark 
            except :
                # When there is an error executing sql_parser.parse(sql), the error is Syntax Error
                # Deal with Syntax Error, print the message below and break
                print(f'DB_2018-10786> ' + "Syntax error")
                break
            
            try:
                # Get transformer results
                do, init_table, target_table, target_column, target_value, select_column, where_part = sql_transformer.transform(output) 
                # do            : what is a type of sql (i.e. SELECT, CREATE TABLE, DROP TABLE, ...)
                # init_table    : show the schema of a table when CREATE
                # target_table  : show which table is used when SELECT, DROP, UPDATE, INSERT, ...
                # target_column : show which columns are used when INSERT or UPDATE
                # target_value  : show attributes' values when INSERT or UPDATE
                # select_column : show which columns are selected when SELECT ~
                # where_part    : show where clause
                
                # if sql is "exit;", break a loop and change a value of an exit_indicator
                if do == 'exit':
                    is_exit = 1
                    break
                
                # if sql is "create table", create a table using init_table and print a success message
                elif do == 'CREATE TABLE':
                    create_table_success_msg = create_table(init_table)
                    print(f'DB_2018-10786> ' + str(create_table_success_msg))

                # if sql is "drop table", drop the table using target_table and print a success message
                elif do == 'DROP TABLE':
                    drop_table_success_msg = drop_table(target_table)
                    print(f'DB_2018-10786> ' + str(drop_table_success_msg))

                # if sql is "explain", "describe" or "desc", describe a table using target_table
                elif do == 'EXPLAIN' or do == 'DESCRIBE' or do == 'DESC':
                    explain_table(target_table)

                # if sql is "show tables", show existing tables
                elif do == 'SHOW TABLES':
                    show_tables()

                # if sql is "insert", insert a tuple into the table 
                # using target_table, target_value, target_column and where_part and print a success message
                elif do == 'INSERT':
                    insert_success_msg = insert(target_table, target_value, target_column)
                    print(f'DB_2018-10786> ' + str(insert_success_msg))
                
                # if sql is "select", select specific or all tuples and show them
                elif do == 'SELECT':
                    select(target_table, select_column, where_part)

                # if sql is "delete", delete specific or all tuples in the table
                elif do == 'DELETE':
                    delete_success_msg = delete(target_table, where_part)
                    print(f'DB_2018-10786> ' + str(delete_success_msg))
                

            # Register error types
            except (DuplicateColumnDefError, DuplicatePrimaryKeyDefError, NonExistingColumnDefError, 
                    ReferenceError, ReferenceNonPrimaryKeyError, ReferenceColumnExistenceError,
                    ReferenceTableExistenceError, CharLengthError, TableExistenceError, 
                    DropReferencedTableError, NoSuchTable, ReferenceTypeError,
                    InsertTypeMismatchError, InsertColumnExistenceError, InsertColumnNonNullableError,
                    WhereTableNotSpecified, WhereColumnNotExist, WhereIncomparableError, WhereAmbiguousReference,
                    SelectTableNotSpecified, SelectColumnResolveError, SelectTableExistenceError,
                    InsertDuplicatePrimaryKeyError) as error_msg:
                # If 'try' doesn't work well, it means errors
                # Catch an error message and print it
                print(f'DB_2018-10786> ' + str(error_msg))
            
            
except KeyboardInterrupt: # Break the while-loop using KeyboardInterrupt
    print("\nCtrl + C : Exiting...")
    exit()