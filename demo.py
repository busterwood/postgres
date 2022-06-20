import pg

connection_string = ""
with pg.Connection(connection_string) as conn:
    conn: pg.Connection

    # common case, single result containing a number of rows, varargs for parameters
    table = conn.query("select $1", 1)
    # the result returned is client-side buffered and support __len__ and __getitem__
    for i in range(len(table)):
        # each item is text (by default)
        print(table[i,0])

    # parameterized insert, update, delete - varargs for parameters
    # no results, exception raised if anything but COMMAND_OK status is returned
    conn.execute("INSERT x values ($1, $2)", 1, 2)

    # multi statement script, no results are allowed, no parameters allowed either
    conn.execute_script("CREATE SCHEMA cja; CREATE TABLE cja.one(id int);")
    
    # forward-only cursor, single_row_mode, minimize memory on client
    cursor = conn.start_query("select $1 as first_col", 1)
    while cursor.next_row():
        print(cursor[0])            # __getitem__ support
        print(cursor.first_col)     # __getattr__ support for dynamic attributes
        print(cursor.column_name(0))   # get column name
        print(cursor.is_null(0))    # is the value NULL in the database
        print(cursor.get_bool(0))   # get typed value
        print(cursor.get_int(0))    # get typed value
        print(cursor.get_float(0))  # get typed value
        print(cursor.get_str(0))    # get typed value

    # send a request but don't wait for the result
    conn.start_execute("DELETE FROM very_big_table where id in ($1, $2)", 1, 2)
    # ...some time later....
    # check the result
    conn.end_execute()

    # support copy for fast insertion
    conn.start_copy_in("COPY cja.one FROM STDIN")
    conn.write_copy_data("1\n2\n")
    conn.end_copy()
