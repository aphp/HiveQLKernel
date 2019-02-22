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
    sql_str = sql_remove_comment(sql_str)
    if sql_is_count(sql_str):# no limit for count
        return sql_str
    if sql_is_selection(sql_str): #the query is a selection
        if sql_extract_limit(sql_str) > default_limit or sql_extract_limit(sql_str) ==0:# the limit is not set or to high
            return sql_incrust_limit(sql_str, default_limit) # force the default limit
    if sql_is_show(sql_str): #the query is a show
        pattern = re.compile("(\\w+)\\s+(\\w+)([\\s\\S]*)$", re.I)# replace any existing limit with the default limit
        res = pattern.sub("\\1 \\2", sql_str)
        return res
    return sql_str

def extract_show_pattern(sql_str):
    pattern = re.compile("(\\w+)\\s+(\\w+)([\\s\\S]*)$", re.I)# replace any existing limit with the default limit
    res = pattern.sub("\\3", sql_str)
    return res.strip()
            
def sql_is_selection(sql_str):
    sql_str = sql_remove_comment(sql_str)
    return sql_is_with(sql_str) or re.search(r'^\s*select', sql_str, re.I)

def sql_is_with(sql_str):
    sql_str = sql_remove_comment(sql_str)
    return re.search(r'^\s*with', sql_str, re.I)

def sql_is_count(sql_str):
    sql_str = sql_remove_comment(sql_str)
    return re.search(r'^\s*select\s+count\(.\)\s+from', sql_str, re.I)

def sql_is_create(sql_str):
    sql_str = sql_remove_comment(sql_str)
    return re.search(r'^\s*create\s+table\s+.*stored\s+as\s+orc\s+as', sql_str, re.I)

def sql_is_drop(sql_str):
    sql_str = sql_remove_comment(sql_str)
    return re.search(r'^\s*drop\s+table', sql_str, re.I)

def sql_is_describe(sql_str):
    sql_str = sql_remove_comment(sql_str)
    return re.search(r'^\s*describe\s+', sql_str, re.I)

def sql_is_show(sql_str):
    sql_str = sql_remove_comment(sql_str)
    return sql_is_show_tables(sql_str) or sql_is_show_databases(sql_str)

def sql_is_show_tables(sql_str):
    sql_str = sql_remove_comment(sql_str)
    return re.search(r'^\s*show\s+tables', sql_str, re.I)

def sql_is_show_databases(sql_str):
    sql_str = sql_remove_comment(sql_str)
    return re.search(r'^\s*show\s+databases', sql_str, re.I)
   
def sql_is_use(sql_str):
    sql_str = sql_remove_comment(sql_str)
    return re.search(r'^\s*use\s+', sql_str, re.I)

def sql_is_set(sql_str):
    sql_str = sql_remove_comment(sql_str)
    return re.search(r'^\s*set\s+', sql_str, re.I)

def sql_is_explain(sql_str):
    sql_str = sql_remove_comment(sql_str)
    return re.search(r'^\s*explain\s+', sql_str, re.I)

def sql_remove_comment(sql_str):
    res = re.sub("--.*\n","", sql_str, re.MULTILINE)
    return res


def sql_validate(sql_str):
    # tolerate ended with ; but not multiple queries
    sql_str = sql_remove_comment(sql_str)
    if sql_str.count(";") > 0 :
        if re.search(r";\s*$", sql_str) and sql_str.count(";") == 1:
            pass
        else:
            raise MultipleQueriesError("only one query per cell")
    if  sql_is_drop(sql_str) or sql_is_create(sql_str) or sql_is_describe(sql_str) or sql_is_show(sql_str) or sql_is_use(sql_str) or sql_is_set(sql_str) or sql_is_selection(sql_str) or sql_is_explain(sql_str):
        pass
    else:
        raise NotAllowedQueriesError()

