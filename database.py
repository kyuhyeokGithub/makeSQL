###########################################
#   Kyu Hyeok Seo         2018-*****      #
###########################################

from berkeleydb import db
from pathlib import Path
from msg import *
from argument import *
from table import Table
from tuple import Tuple
import pickle
import os
import re
from itertools import product

# directory for DB files.
PATH = "./DB"

# using for checking DATE pattern
DATE = r"\d{4}-\d{2}-\d{2}"

# serialize, deserialize functions will be used to convert the class Table, Tuple to store in DB and to load it from DB
def serialize(target):
    return pickle.dumps(target)
def deserialize(target):
    return pickle.loads(target)

# make a specific path corresponding to dbname
def where_is_db(dbname):
    DB_FILE_PATH = Path(PATH + "/" + dbname + ".db")
    return DB_FILE_PATH


# return DB with name = dbname
def connect_db(dbname):
    mydb = db.DB()
    DB_FILE_PATH = Path(PATH + "/" + dbname + ".db")
    # if db exists, open it and return it
    if DB_FILE_PATH.exists():
        mydb.open(DB_FILE_PATH, dbname, db.DB_HASH)
    # if db doesn't exist, make it with db.DB_CREATE and open it and return it
    else :
        mydb.open(DB_FILE_PATH, dbname, db.DB_HASH, db.DB_CREATE)
    return mydb


# create a table
def create_table(init_table):
    
    table_name = init_table["name"] # string type
    table_column_list = init_table["column_list"] # list : element = (col_name, dtype, constraint)
    table_not_null_set = init_table["not_null_set"] # set : element = col_name
    table_pk_list = init_table["pk_list"] # list : double list of pk combinations -> will be solved
    table_fk_dict = init_table["fk_dict"] 
    # dictionary : key = referencing column name, value = (referenced table name, referenced column name)

    # List of column names
    table_column_name_list = [i[0] for i in table_column_list]
    # List of referenced information
    table_fk_referencing_list = list(table_fk_dict.keys())

    # if there is duplicated column name, then only one of them will be remained after set operation
    # this means length of set is not equal to length of original table column list
    if len(set(table_column_name_list)) != len(table_column_list) :
        raise DuplicateColumnDefError()

    # When there are two primary key definitions
    if len(table_pk_list) > 1 :
        raise DuplicatePrimaryKeyDefError()

    # Open and load mainDB
    # mainDB deals with table schema of table schema
    mainDB = connect_db(MAIN_DB_NAME)

    # encode table name as a key for mainDB
    target_table_key = table_name.encode('utf-8')

    # Check whether there is already a table with the same table name
    if mainDB.exists(target_table_key):
        raise TableExistenceError()
    
    pk = None
    # if there is primary key in inputs,
    # set it as PK and
    # check whether the columns are in column name list and add them to not null set
    if len(table_pk_list) == 1 :
        pk = table_pk_list[0]
        for pk_each in pk:
            if pk_each not in table_column_name_list :
                raise NonExistingColumnDefError(pk_each)
            else :
                table_not_null_set.add(pk_each)

    # check whether the referencing columns are in column name list
    for fk_each in table_fk_referencing_list :
        if fk_each not in table_column_name_list :
            raise NonExistingColumnDefError(fk_each)
                
    # make column_dict : key = col_name, value = dtype and constraint
    column_dict = {}
    for col_name, col_dtype, col_const in table_column_list :
        column_dict[col_name] = (col_dtype, col_const)
        if col_dtype.startswith("char") :
            # check whether char dtype has a length over 0
            # if it doesn't, raise an error
            if int(col_dtype[5:-1]) < 1 :
                raise CharLengthError
    
    if len(table_fk_referencing_list) != 0:
        # For each foreign key
        for fk_each in table_fk_referencing_list :
            ref_table_name, ref_col = table_fk_dict[fk_each]
            # if referenced column name is not char type
            # have to raise an error
            if not isinstance(ref_col, str):
                raise ReferenceColumnStringError

            # using referenced table name, make a key
            # use the key to check whether the referenced table exists or not
            ref_table_key = ref_table_name.encode('utf-8')
            if not mainDB.exists(ref_table_key):
                raise ReferenceTableExistenceError
            
            # If referenced table exists, load it and deserialize it to utilize it
            ref_table = deserialize(mainDB.get(ref_table_key))
            
            # Check whether referenced column exists or not
            if ref_col not in ref_table.column_dict :
                    raise ReferenceColumnExistenceError

            # Check whether referenced column is PK or not 
            if not set(ref_table.primary_key).issubset(set([ref_col])) :
                raise ReferenceNonPrimaryKeyError

            # Check referencing column and referenced one have the same dtype
            if column_dict[fk_each][0] != ref_table.column_dict[ref_col][0]:
                raise ReferenceTypeError

            # update referenced table to express new table is referencing the table
            ref_table.add_fk_referenced_dict(table_name, ref_col)

            # Save it to mainDB after update
            mainDB.put(ref_table_key, serialize(ref_table))

    # Make the table
    target_table = Table(table_name, table_column_name_list, column_dict, list(table_not_null_set), pk, table_fk_dict, {})
    
    # Make table key to use table name
    table_key = table_name.encode('utf-8')

    # Save it to mainDB
    mainDB.put(table_key, serialize(target_table))
    mainDB.close()

    # make DB file with table_name and close it
    create_DB = connect_db(table_name)
    create_DB.close()
    
    return CreateTableSuccess(table_name)


# Drop table with the name = table_name
def drop_table(table_name):
    
    # drop table query only deals with a table, not multiples
    table_name = table_name[0]

    # load mainDB
    mainDB = connect_db(MAIN_DB_NAME)

    # make table_key and load target_table
    target_table_key = table_name.encode('utf-8')
    target_table = mainDB.get(target_table_key)
    
    # if fail to load target table, it means there is no such table
    if not target_table :
        raise NoSuchTable()

    # Deserialize it to use it
    target_table = deserialize(target_table)
    
    # if there is a table which references target table
    if target_table.fk_referenced_dict :
        raise DropReferencedTableError(table_name)
    
    # If target table references table X,
    # load table X and delete the information that target table references table X
    if target_table.fk_referencing_dict :
        for ref_table_name, _ in target_table.fk_referencing_dict.values():
            ref_table_key = ref_table_name.encode('utf-8')
            ref_table = deserialize(mainDB.get(ref_table_key))
            del ref_table.fk_referenced_dict[table_name]
            # after delete the information, save it to mainDB
            mainDB.put(ref_table_key, serialize(ref_table))
    
    # delete the key in mainDB
    mainDB.delete(target_table_key)

    # get the path of target table and delete target_table.db in the path
    db_path = where_is_db(table_name)
    os.remove(db_path)
    mainDB.close()

    return DropSuccess(table_name)


# For 'explain', 'desc', 'describe' queries
def explain_table(table_name):

    # the function only deals with a table, not multiples
    table_name = table_name[0]

    # load mainDB
    mainDB = connect_db(MAIN_DB_NAME)

    # get the key using table_name and load the target table
    target_table_key = table_name.encode('utf-8')
    target_table = mainDB.get(target_table_key)
    
    # if fail to load target table, it means there is no such table
    if not target_table :
        raise NoSuchTable()

    target_table = deserialize(target_table)

    # print the target_table information
    print(target_table.get_info())
    mainDB.close()


# For 'show tables' query
def show_tables():

    # header
    temp = '-'*65 + "\n"

    # open mainDB and iteratate it to retrieve (key, value)
    mainDB = connect_db(MAIN_DB_NAME)
    cursor = mainDB.cursor()
    while x := cursor.next():
        # Since key is a table name, save it
        temp += x[0].decode('utf-8') + "\n"

    # add the line
    temp += ('-'*65)

    print(temp)
    mainDB.close()


# For 'insert' query
def insert(table_name, target_value, target_column):

    # the function only deals with a table, not multiples
    table_name = table_name[0]

    # open DB
    mainDB = connect_db(MAIN_DB_NAME)

    # get the key using table name and load target table using the key
    target_table_key = table_name.encode('utf-8')
    target_table = mainDB.get(target_table_key)
    
    # if fail to load target table, it means there is no such table
    if not target_table :
        raise NoSuchTable()

    target_table = deserialize(target_table)
    mainDB.close()

    # tuple data dict : key = col_name, value = value of correspoding column attirbute
    # tuple pk : list to save PK combinations
    tuple_data_dict = {}
    tuple_pk = []

    # When target column is explicitly mentioned
    if target_column :
        
        # Check whether the length of target_column equals to the length of target_value
        if len(target_column) != len(target_value) :
            raise InsertTypeMismatchError()

        # Check whether the length of target_column equals to the length of target_value
        for col in target_column :
            if col not in target_table.column_name_list :
                raise InsertColumnExistenceError(col)

        # for each column in inputs
        for idx in range(len(target_column)):
            
            is_char = False # for checking col_value is char type or not
            col_name = target_column[idx]
            col_value = target_value[idx]

            if isinstance(col_value, str) : # this means col_value is char type or date type
                if col_value.endswith('"') or col_value.endswith("'") :

                    # char type start with ' or ", unlike data type
                    is_char = True
                    col_value = col_value[1:-1] # extracting real value of char type

            # if column dtype is char, restrict the length of values with max = dtype number
            if target_table.column_dict[col_name][0].startswith('char') :

                # even though column dtype is char, value is not char type
                if is_char == False :
                    raise InsertTypeMismatchError()
                
                # split input because it has to be save with limited length
                max_len = int(target_table.column_dict[col_name][0][5:-1])
                if len(col_value) > max_len:
                    col_value = col_value[:max_len]
            
            # if column dtype is char, we should save the value with '' to note this is char type
            if is_char :
                tuple_data_dict[col_name] = "'" + col_value + "'"
            else :
                tuple_data_dict[col_name] = col_value
        
        # remaining columns (table schema columns - columns in inputs) will have null values
        for col_name in list(target_table.column_dict.keys()):
            if not (col_name in tuple_data_dict):
                tuple_data_dict[col_name] = None

    # when target_column is None
    else :

        # Check whether the length of target_column equals to the length of target_value
        if len(target_table.column_name_list) != len(target_value) :
            raise InsertTypeMismatchError()

        # for each column in table schema
        for idx in range(len(target_table.column_name_list)) :
            
            is_char = False # for checking col_value is char type or not
            col_name = target_table.column_name_list[idx]
            col_value = target_value[idx]

            if isinstance(col_value, str) : # this means col_value is char type or date type
                
                if col_value.endswith('"') or col_value.endswith("'") :
                    # char type start with ' or ", unlike data type
                    is_char = True
                    col_value = col_value[1:-1]

            # if column dtype is char, restrict the length of values with max = dtype number
            if target_table.column_dict[col_name][0].startswith('char') :

                # even though column dtype is char, value is not char type
                if is_char == False :
                    raise InsertTypeMismatchError()
                
                # split input because it has to be save with limited length
                max_len = int(target_table.column_dict[col_name][0][5:-1])
                if col_value is not None :
                    if len(col_value) > max_len:
                        col_value = col_value[:max_len]

            # save the col_value corresponding col_name
            # if column dtype is char, we should save the value with '' to note this is char type
            if is_char :
                tuple_data_dict[col_name] = "'" + col_value + "'"
            else :
                tuple_data_dict[col_name] = col_value
    


    for col_name, col_value in zip(tuple_data_dict.keys(), tuple_data_dict.values()):

        # Check whether col_value is Null eventhough column is in not_null_list
        if col_value is None and col_name in target_table.not_null_list :
            raise InsertColumnNonNullableError(col_name)
        
        # Check whether col_value is valid for column data type
        if col_value is None :
            pass
        
        # Check inserted value with INT type is valid
        elif target_table.column_dict[col_name][0] == 'int' :
            if not( isinstance(col_value, int) ) :
                raise InsertTypeMismatchError()
        
        # Check inserted value with CHAR type is valid
        elif target_table.column_dict[col_name][0].startswith('char') :
            if not( isinstance(col_value, str) ) :
                raise InsertTypeMismatchError()
        
        # Check inserted value with DATE type is valid
        elif target_table.column_dict[col_name][0] == 'date' :
            if not re.fullmatch(DATE, col_value):
                raise InsertTypeMismatchError()

    # For pks, add them to tuple_pk
    #
    #
    # case for a table with primary key
    if len(target_table.primary_key) != 0 :
        for col_name in target_table.column_name_list :
            if col_name in target_table.primary_key :
                
                # if it is char type, we should eliminate ' both sides
                if target_table.column_dict[col_name][0].startswith('char') :
                    value_with = tuple_data_dict[col_name]
                    tuple_pk.append(value_with[1:-1])
                else :
                    tuple_pk.append(tuple_data_dict[col_name])

    # case for a table without primary key
    # we treat a tuple's all attributes as a key
    else :
        for col_name in target_table.column_name_list :
            # if tuple data has NULL, we will use it as 'null' when we make a key
            if tuple_data_dict[col_name] is None:
                tuple_pk.append('null')

            else :
                # if it is char type, we should eliminate ' both sides
                if target_table.column_dict[col_name][0].startswith('char') :
                    value_with = tuple_data_dict[col_name]
                    tuple_pk.append(value_with[1:-1])
                else :
                    tuple_pk.append(tuple_data_dict[col_name])

    # for data with char type in tuple data dictionary, change its value eliminating ''
    for col_name, col_value in zip(tuple_data_dict.keys(), tuple_data_dict.values()):
        if col_value is not None :
            if target_table.column_dict[col_name][0].startswith('char') :
                tuple_data_dict[col_name] = col_value[1:-1]

    # make new tuple
    new_tuple = Tuple(table_name, tuple_data_dict, tuple_pk)

    # load db file using table name
    target_table_db = connect_db(table_name)

    # load pk values as a list
    string_pk_list = [str(element) for element in tuple_pk]

    # Join the elements of the string list
    pk_string = ', '.join(string_pk_list)

    # make the key using pk_string
    new_tuple_key = pk_string.encode('utf-8')

    # check the tuple key(based on PK)
    if len(target_table.primary_key) > 0 :
        if target_table_db.exists(new_tuple_key) :
            raise InsertDuplicatePrimaryKeyError()
    
    # save the tuple in the table db file
    target_table_db.put(new_tuple_key, serialize(new_tuple))
    target_table_db.close()

    return InsertResult()


# For 'select' query
def select(target_table, select_column, where_part):
    
    # open main DB
    mainDB = connect_db(MAIN_DB_NAME)

    # table name list : only save table_name
    # table list : save table objects corresponding table name
    table_name_list = []
    table_list = []

    # 'FROM' >> get tables and tables' name list
    #
    #
    # for each table name
    for table_name, table_alias in target_table:

        # load the table in mainDB
        target_table_key = table_name.encode('utf-8')
        target_table = mainDB.get(target_table_key)

        # If it doesn't exist, raise an error
        if not target_table :
            raise SelectTableExistenceError(table_name)
        
        table_name_list.append(table_name)
        target_table = deserialize(target_table)
        table_list.append(target_table)
    
    # close mainDB
    mainDB.close()
    #
    # # # # # # # # # # # # # # # # # # # # # # # # #

    # Check columns mentioned in select clause are valid
    #
    #
    without_table_column_name_list = [] # column name list which are mentioned without table name in from clause

    # when select column is specified
    if select_column :
        for idx, (table_name, col_name) in enumerate(select_column) :

            if table_name is not None :
                check = False

                # In table list, find a table with target table name
                for table in table_list :
                    if table.table_name == table_name :
                        my_table = table
                        check = True
                        break

                # Customized Error : Table mentioned in a 'SELECT' part is not existed in a 'FROM' part
                if check == False :
                    raise SelectTableNotSpecified()
                
                # Error : col_name is ambiguous to decide which table contains the column
                if col_name not in my_table.column_name_list :
                    raise SelectColumnResolveError(col_name)

            my_table_candidate = []
            # Case 1 : when we don't know which table include the column -> we have to specify it
            if table_name is None :
                # in table list, find tables which contain the column_name
                for table in table_list :
                    if col_name in table.column_name_list :
                        my_table_candidate.append(table)
                
                # if no table contain the column name
                if len(my_table_candidate) == 0 :
                    raise SelectColumnResolveError(col_name)
                
                # if many tables contain the column name = we can't decide which table is
                elif len(my_table_candidate) > 1 :
                    raise SelectColumnResolveError(col_name)
                
                # if there is an only table with the column name
                else :
                    select_column[idx] = (my_table_candidate[0].table_name, col_name)
                    without_table_column_name_list.append(col_name)
    
    # when select column is not specified = when using *
    else :
        select_column = []
        # Exploring all tables in from clause, get its column name with table name
        for table in table_list :
            for col_name in table.column_name_list :
                select_column.append((table.table_name, col_name))


    # Execute Cartesian products to create all records
    #
    #
    # entire_tuple_dic has table_name as a key, and a list of data dictionary as a value
    entire_tuple_dic = {}
    for table_name in table_name_list : # iterate tables
        entire_tuple_dic[table_name] = []
        
        # connect to table db
        target_table_db = connect_db(table_name)
        cursor = target_table_db.cursor()
        while x := cursor.next():
            # save the tuple's data in the table
            each_tuple_data = (deserialize(x[1])).data
            
            # full_name_tuple_data has (table_name.col_name) as a key and its column value as a value
            # i.e. full_name_tuple_data[student.id] = '2018-10786'
            full_name_tuple_data = {}
            for col_name, value in each_tuple_data.items():
                full_name_tuple_data[(table_name + '.' + col_name)] = value
            entire_tuple_dic[table_name].append(full_name_tuple_data)

    # Make all cartesian product output using tuple_data_dictionary
    #
    all_record = [entire_tuple_dic[table_name] for table_name in table_name_list]
    cartesian_tuple_list = list(product(*all_record))

    for idx, each_pair in enumerate(cartesian_tuple_list) :
        # Case 1 : only one table
        if len(each_pair) == 1 :
            dd = each_pair[0]
            cartesian_tuple_list[idx] = dd

        # Case 2 : there are two tables using for cartesian product
        elif len(each_pair) == 2 :
            d1, d2 = each_pair[0], each_pair[1]
            cartesian_tuple_list[idx] = dict(d1, **d2)

        # Case 3 : there are three tables using for cartesian product
        else :
            d1, d2 = each_pair[0], each_pair[1]
            d0 = dict(d1, **d2)
            # Merge first two data_dictionaries and update it
            for j in range(2, len(each_pair)):
                d0.update(each_pair[j])
            cartesian_tuple_list[idx] = d0
    
    # Get tuple data dictionary list
    # list's element is cartesian product data dictionary
    tuple_data_dic_list = []

    # Case 1 : where part exists
    if where_part : 
        for cartesian_tuple_data in cartesian_tuple_list :
            # Check each tuple satisfies where clause
            flag_where = check_where(where_part, table_list, cartesian_tuple_data)
            # If a tuple satisfies where clause, add it to tuple_data_dic_list
            if flag_where :
                tuple_data_dic_list.append(cartesian_tuple_data)

    # Case 2 : where part does not exist
    elif where_part is None :
        for cartesian_tuple_data in cartesian_tuple_list :
            # Add it to tuple_data_dic_list
            tuple_data_dic_list.append(cartesian_tuple_data)

    # Select tuples with a SELECT format
    select_print(select_column, without_table_column_name_list, tuple_data_dic_list)


def select_print(full_column_list, solitary_column_list, data_dic_list):
    
    # Find the value is None and update it with "NULL"
    for data_dic in data_dic_list:
        for col_name, value in data_dic.items():
            if value is None :
                data_dic[col_name] = "NULL"

    # Determine how many spaces are needed to format
    space_format = []
    print_column_list = []
    for table_name, col_name in full_column_list:

        # col_name is not ambiguous = don't need to print it with its table_name
        if col_name in solitary_column_list :
            print_column_list.append(col_name)
            space_format.append(len(str(col_name)))
        # col_name is ambiguous = have to print it with its table_name
        else :
            print_column_list.append(table_name + '.' + col_name)
            space_format.append(len(table_name + '.' + col_name))
    
    # For each tuple, check the max length of value for each column
    # compare its length to length of col_name and find max one for each column
    for data_dic in data_dic_list:
        for idx, (table_name, col_name) in enumerate(full_column_list):
            value = data_dic[table_name + '.' + col_name]
            space_format[idx] = max(space_format[idx], len(str(value)))

    output = ""
    header = "+"
    mid = "|"
    # make header and mids using space
    for idx, col_name in enumerate(print_column_list):
        space = space_format[idx]
        header += "-" * (2+space) + "+"
        mid += " " + str(col_name) + " "*(space-len(str(col_name))) + " |"
    header += "\n"
    mid += "\n"
    output += header + mid + header

    # for each tuple
    for data_dic in data_dic_list:
        # format for each tuple
        output += "|"
        for idx, (table_name, col_name) in enumerate(full_column_list):
            space = space_format[idx]
            # print value of each column and remaining spaces will be filled with white space
            output += " "+ str(data_dic[table_name + '.' + col_name]) + " " * (space - len(str(data_dic[table_name + '.' + col_name]))) + " |"
        output += "\n"

    # the last line
    for space in space_format:
        output += "+" + "-" * (2+space)
    output += '+'

    print(output)


# For 'delete' query
def delete(table_name, where_part):

    # the function only deals with a table, not multiples
    table_name = table_name[0]

    # open DB
    mainDB = connect_db(MAIN_DB_NAME)

    # get the key using table name and load target table using the key
    target_table_key = table_name.encode('utf-8')
    target_table = mainDB.get(target_table_key)
    
    # if fail to load target table, it means there is no such table
    if not target_table :
        raise NoSuchTable()

    # load target table and close mainDB
    target_table = deserialize(target_table)
    mainDB.close()

    # cnt will be used to count the number of tuples deleted
    cnt = 0

    # First, if there is no where clause
    if where_part is None :

        # load target table db and will delete all tuples in the table
        target_table_db = connect_db(table_name)

        # Exploring all tuples in the table, delete each tuple using cursor and count it
        cursor = target_table_db.cursor()
        while x := cursor.next():
            cnt += 1
            cursor.delete()
    
    # Second, there is where clause
    else :
        # Connect to table db
        target_table_db = connect_db(table_name)
        cursor = target_table_db.cursor()

        while x := cursor.next():
            # Load a tuple in the table using deserialization
            candidate_tuple = deserialize(x[1])
            # Check the tuple satisfies where clause
            flag_where = check_where(where_part, [target_table], candidate_tuple.data)
    
            # count and delete it if the tuple satisfies where clause
            if flag_where :
                cnt += 1
                cursor.delete()
    
    return DeleteResult(cnt)

# check whether data_dic satisfies where clause
#
def check_where(where, table_list, data_dic):

    # extract connection (in [and, or, None(single condition)])
    connection = where[0]
    if connection is None :
        connection = ''

    # make condition as a list
    condition_list = []
    if connection in ['or', 'and'] :
        condition_list = where[1]
    else :
        condition_list.append(where)
    
    # truth list will save each condition's truth value
    truth_list = []

    for condition in condition_list :
        
        # bool flag : 'not' -> NOT condition, '' -> normal condition
        bool_flag = condition[0] if condition[0] is not None else ''

        # save operands and operator
        left_operand = condition[1][0]
        operator = condition[1][1]
        right_operand = condition[1][2]
    
        # get values and their dtypes using data_dictionary
        left_value, left_dtype = get_value(left_operand, table_list, data_dic)
        right_value, right_dtype = get_value(right_operand, table_list, data_dic)

        # check it is valid to compare left_value and right_value with operator considering their data types
        check_comparable(left_value, left_dtype, operator, right_value, right_dtype)

        # get TRUE or FALSE from each condition and save it to truth list
        truth_list.append(do_compare(bool_flag, left_value, operator, right_value))

    # if where clause is a single condition
    if len(truth_list) == 1 :
        return truth_list[0]
    # if where clause is consisted of multiple conditions
    else :
        if connection == 'or' :
            return truth_list[0] or truth_list[1]
        elif connection == 'and' :
            return truth_list[0] and truth_list[1]


def get_value(operand, table_list, data_dic) :

    # There will be 3 cases for operand -> None, value like 'kyuhyeok', 1, 2024-01-01, and (table_name, col_name)
    #
    # Case 1 : operand -> None
    if operand is None :
        return None, None
    #
    # Case 2 : operand -> one of ('abcde', 123, 2024-01-01)
    elif isinstance(operand, int) or isinstance(operand, str):
        if isinstance(operand, int) :
            return operand, 'int'
        elif isinstance(operand, str) :
            if operand.startswith("'") or operand.startswith('"') :
                return operand[1:-1], 'char'
            if bool(re.fullmatch(DATE, operand)):
                return operand, 'date'
    #
    # Case 3 : operand -> (table_name, col_name)
    elif isinstance(operand, tuple) :
        table_name, col_name = operand

        # if table name is specified
        if table_name is not None :
            check = False
            # check the table with table name is in table_list
            for table in table_list :
                if table.table_name == table_name :
                    check = True
                    my_table = table
                    break
            # if there is no table with table name in table list
            if check == False :
                raise WhereTableNotSpecified()
            # if there is no column with col_name in the table
            if col_name not in my_table.column_name_list :
                raise WhereColumnNotExist()

        my_table_candidate = []
        # if table name is not specified
        if table_name is None :
            # find which table contains column with col_name
            for table in table_list :
                if col_name in table.column_name_list :
                    my_table_candidate.append(table)
            
            # if no table contains the column with col_name
            if len(my_table_candidate) == 0 :
                raise WhereColumnNotExist()
            # if there are too much tables having the column with col_name
            elif len(my_table_candidate) > 1 :
                raise WhereAmbiguousReference()
            # if there is an only table having the column with col_name
            else :
                my_table = my_table_candidate[0]
        
        # determine column's data type
        dtype = my_table.column_dict[col_name][0]
        if dtype.startswith('char'):
            dtype = 'char'

        # if table is not specified
        if col_name in data_dic :
            return data_dic[col_name], dtype
        # if table is specified
        elif (my_table.table_name + '.' + col_name) in data_dic :
            return data_dic[(my_table.table_name + '.' + col_name)], dtype


# Check that comparison between left value and right value with operator is valid
#
def check_comparable(left_value, left_dtype, operator, right_value, right_dtype):

    # when operator is comparison operator and both value are not NULL
    if operator in ['>', '<', "=", '!=', '>=', '<='] and left_dtype is not None and right_dtype is not None :
        # if they are 'char' data type, operator should be '=' or '!='
        if left_dtype == 'char' and right_dtype == 'char' and operator in ['>', '<', '>=', '<='] :
            raise WhereIncomparableError()
        # else, their data types should be same
        elif left_dtype != right_dtype :
            raise WhereIncomparableError()
        
# Return truth value of comparison for values and operator
#
def do_compare(bool_flag, left_value, operator, right_value):

    # compare with NULL value, truth value is UNKNOWN
    if left_value is None or right_value is None :
        if operator in ['>', '<', '=', '!=', '>=', '<='] :
            ret = None

    # For comparison operators, it is simple to determine Truth value
    elif operator == '>' :
        ret = (left_value > right_value)

    elif operator == '<' :
        ret = (left_value < right_value)

    elif operator == '=' :
        ret = (left_value == right_value)

    elif operator == '!=' :
        ret = (left_value != right_value)

    elif operator == '>=' :
        ret = (left_value >= right_value)

    elif operator == '<=' :
        ret = (left_value <= right_value)

    # if operator is null operator('null', 'not null')
    if operator == 'null' :
        ret = (left_value is None)

    elif operator == 'not null' :
        ret = (left_value is not None)

    # if condition is a NOT condition, we should return opposite Truth value, but UNKNOWN's oppositie is UNKNOWN
    if bool_flag == 'not' :
        if ret is not None :
            return (not ret)
        elif ret is None :
        # finally, when condition's truth value is UNKNOWN, NOT for the condition acts as UNKNOWN
            return False
    # if condition is not a NOT condition, just return its ret
    else :
        if ret is not None :
            return ret
        else :
            return False