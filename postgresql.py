from typing import Any, Iterable


class Result:
    def ok(self) -> bool:
        return True

    def error_message(self) -> str:
        return ""

    def as_exception(self) -> Exception:
        return Exception(self.error_message)

class Row:
    pass


class RowStream:
    def __iter__(self):
        return self

    def __next__(self) -> Row:
        raise NotImplementedError()


class Connection():
    _last_result: Result|None

    @staticmethod
    def open(connection_string:str) -> Connection:
        return Connection()

    def __enter__(self):
        pass

    def __exit__(self):
        pass

    def execute(self, sql:str) -> Result:
        # store the first result
        raise NotImplementedError()
        self._last_result = Result()
        return self

    def execute_params(self, sql:str, params: list[Any]) -> Result:
        raise NotImplementedError()
        return Result()

    def send_query(self, sql:str) -> None:
        raise NotImplementedError()

    def send_query_params(self, sql:str, params: list[Any]) -> None:
        raise NotImplementedError()

    def stream_rows(self) -> Iterable[Row]:
        raise NotImplementedError()

    def next_result(self) -> Result|None:
        raise NotImplementedError()

    def start_copy_in(self, sql:str) -> Result:
        raise NotImplementedError()

    def put_copy_data(self, data:str) -> None:
        raise NotImplementedError()
        
    def end_copy(self) -> None:
        raise NotImplementedError()
        
    def __iter__(self):
        return self

    def __next__(self) -> Result:
        if (res := self._last_result) is None:
            res = None
             # next result from connection
            raise NotImplementedError()
        else:
            self._last_result = None

        if res is None:
            raise StopIteration()
        else:
            return res


connection_string = ""
conn:Connection
with Connection.open(connection_string) as conn:
    # example of executing multiple commands
    if not (res := conn.execute("CREATE SCHEMA cja; CREATE TABLE cja.one(id int);")).ok():
        raise res.as_exception()
    
    for result in conn: # additional results
        if not res.ok():
            raise res.as_exception()

    # execute with no result value - easy to forget to check the result status
    if not (res := conn.execute_params("INSERT INTO cja.one values ($1)", [1])).ok():
        raise res.as_exception()
    
    # async: send a query to the server but don't wait for the result
    conn.send_query("do stuff")
    conn.send_query_params("", [1])

    # returns a stream of rows, one at a time
    for row in conn.stream_rows():
        pass
    
    # blocks until a result is available
    for result in conn:
        if not res.ok():
            raise res.as_exception()

    if not (res := conn.start_copy_in("COPY cja.one FROM STDIN")).ok():
        raise res.as_exception()
    conn.put_copy_data("1\n2\n")
    conn.end_copy()
