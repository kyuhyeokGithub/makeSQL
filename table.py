###########################################
#   Kyu Hyeok Seo         2018-*****      #
###########################################

class Table():
    def __init__(self, name, col_name_list, col_dict, not_null_list, pk, fk_referencing_dict, fk_referenced_dict):
        self.table_name = name # string

        self.column_name_list = col_name_list # list of column names
        
        self.column_dict = col_dict # dictionary : key = col_name, value = col_dtype, col_constraint
        
        self.not_null_list = not_null_list # list for not null column names
        
        self.primary_key = pk  # list for pk elements
        if self.primary_key is None : # pk can be None when there is no pk declaration while creating a table
            self.primary_key = [] # so we have to consider the case

        # dictionary : key = referencing col_name, value = referenced table, referenced column
        # Show which tables and columns are referenced by this table
        self.fk_referencing_dict = fk_referencing_dict 
        
        # dictionary : key = referencing table_name, value = referenced column
        # Show which tables reference this table
        self.fk_referenced_dict = fk_referenced_dict

    # add new reference relationship to self.fk_referenced_dict
    def add_fk_referenced_dict(self, referencing_table_name, referenced_col):
        self.fk_referenced_dict[referencing_table_name] = referenced_col

    # return a specific format to show table schema
    def get_info(self):

        # first line
        temp = "-" * 65 + "\n"

        # add table name and column names
        temp += "table_name [" + self.table_name + "]" + "\n"
        temp += "column_name" + " " * 11 + "type" + " " * 11 + "null" + " " * 11 + "key" + " "*10 + "\n"

        # For each column, add its information including whether a column is included to fk, pk.
        for col_name in list(self.column_dict.keys()):
            col_dtype, col_const = self.column_dict[col_name]
            temp += col_name + " " * (22-len(col_name)) + col_dtype + " "*(15-len(col_dtype))
            
            # if column is not null
            if col_name in self.not_null_list :
                temp += 'N'
            else :
                temp += 'Y'

            temp += " " * 14

            # if column is one of pk combinations
            if col_name in self.primary_key:
                temp += 'PRI'
                # if column is one of fks
                if col_name in self.fk_referencing_dict :
                    temp += '/FOR'
            # if column is one of fks
            elif col_name in self.fk_referencing_dict :
                temp += 'FOR'
            temp += '\n'
        temp += "-" * 65
        return temp