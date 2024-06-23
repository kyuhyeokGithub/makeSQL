###########################################
#   Kyu Hyeok Seo         2018-*****      #
###########################################

# Tuple class
class Tuple():
    def __init__(self, table, data_dict, pk):
        self.table = table # string; table name
        self.data = data_dict # data dictionary : key = column name, value = value of corresponding attirbute
        self.pk = pk # list : pk combinations
        