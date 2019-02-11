import re

class MultipleQueriesError(Exception):
    pass

class NotAllowedQueriesError(Exception):
    pass

def sql_extract_limit( sql_str):
    pattern = re.compile("limit\\s+(\\d+)\\s*;?$", re.I)
    res = pattern.search(sql_str)
    if res:
        return int(res.group(1))
    else:
        return 0

def sql_incrust_limit( sql_str, default_limit):
    if sql_extract_limit(sql_str) > 0:
        pattern = re.compile("([\\s\\S]*)?(limit\\s*\\d*\\s?;?)+$", re.I)# replace any existing limit with the default limit
        res = pattern.sub("\\1 limit " + str(default_limit), sql_str)
        return res
    else:
        pattern = re.compile("([^;]+)[\\s\\S]*$", re.I)# replace any existing limit with the default limit
        res = pattern.sub("\\1 limit " + str(default_limit), sql_str)
        return res


def sql_rewrite( sql_str, default_limit):
    #sql_req = "select * from ({}) hzykwyxnbv limit {}".format(sql_req, self.params['default_limit'])
    if sql_is_selection(sql_str): #the query is a selection
        if sql_extract_limit(sql_str) > default_limit or sql_extract_limit(sql_str) ==0:# the limit is not set or to high
            return sql_incrust_limit(sql_str, default_limit) # force the default limit
    return sql_str
            
def sql_is_selection(sql_str):
    return re.search(r'^\s*with|select', sql_str, re.I)

def sql_is_create(sql_str):
    return re.search(r'^\s*create\s+table\s+.*stored\s+as\s+orc\s+as', sql_str, re.I)

def sql_is_drop(sql_str):
    return re.search(r'^\s*drop\s+table', sql_str, re.I)

def sql_is_describe(sql_str):
    return re.search(r'^\s*describe\s+', sql_str, re.I)

def sql_is_show(sql_str):
    return re.search(r'^\s*show\s+', sql_str, re.I)
   
def sql_is_use(sql_str):
    return re.search(r'^\s*use\s+', sql_str, re.I)

def sql_validate( sql_str):
    # tolerate ended with ; but not multiple queries
    if sql_str.count(";") > 0 :
        if re.search(r";\s*$", sql_str) and sql_str.count(";") == 1:
            pass
        else:
            raise MultipleQueriesError("only one is possible")
    if sql_is_selection(sql_str) or sql_is_drop(sql_str) or sql_is_create(sql_str) or sql_is_describe(sql_str) or sql_is_show(sql_str) or sql_is_use(sql_str):
        pass
    else:
        raise NotAllowedQueriesError()
