from __future__ import annotations # allow __enter__ to return Connection
from types import TracebackType
from typing import Any


class DataTable:
    """A table of values, a number or rows and columns"""
    def __len__(self) -> int:
        raise NotImplementedError()

    def column_count(self) -> int:
        raise NotImplementedError()

    def __getitem__(self, location:tuple[int, int]) -> str:
        raise NotImplementedError()

    def column_name(self, column: int) -> str:
        raise NotImplementedError()

    def get_column_index(self, column_name: str) -> int:
        raise NotImplementedError()


class ForwardCursor:
    """forward only stream of rows.  Saves memory by not buffering all rows"""
    
    def next_row(self) -> bool:
        """moves the cursor to the next row of data.  Returns false when there are no more rows"""
        raise NotImplementedError()
    
    def __getitem__(self, column:int) -> str:
        """Gets the value of a column using the column index"""
        return self.get_str(column)

    def column_count(self) -> int:
        raise NotImplementedError()

    def get_name(self, column: int) -> str:
        raise NotImplementedError()

    def get_column_index(self, column_name: str) -> int:
        raise NotImplementedError()

    def is_null(self, column: int) -> bool:
        raise NotImplementedError()

    def get_bool(self, column: int) -> bool:
        raise NotImplementedError()

    def get_float(self, column: int) -> float:
        raise NotImplementedError()
        
    def get_int(self, column: int) -> int:
        raise NotImplementedError()
    
    def get_str(self, column: int) -> str:
        raise NotImplementedError()

    def __getattr__(self, name:str) -> str|None:
        """dynamic access to a column, accessed via the column name"""
        column = self.get_column_index(name)
        return None if self.is_null(column) else self.get_str(column)
    

class Connection():
    """A connection to PostgreSQL"""

    def __init__(self, connection_string:str):
        """Opens a new connection to PostgreSQL"""
        raise NotImplementedError()

    def query(self, sql:str, *args: Any) -> DataTable:
        """Run a SQL query that returns a table of zero or more rows, e.g. SELECT.  The DataTable is buffered into client memory."""
        raise NotImplementedError()

    def execute(self, sql:str, *args: Any) -> None:
        """Run a SQL statement that does not return any rows, e.g. INSERT, UPDATE or DELETE, and wait for the statement to finish."""
        raise NotImplementedError()

    def start_execute(self, sql:str, *args: Any) -> None:
        """Sends a SQL statement to PostgreSQL but does not wait for the statement to finish.
        The statement must not return any rows, e.g. INSERT, UPDATE or DELETE.  
        You must call end_execute() to check the result."""
        raise NotImplementedError()

    def end_execute(self) -> None:
        """Checks that a statement called via start_execute() was successful"""
        raise NotImplementedError()

    def execute_script(self, script:str) -> None:
        """Run a multiple SQL statements, each one must not return any rows."""
        raise NotImplementedError()

    def start_query(self, sql:str, *args: Any, binary_format:bool=False) -> ForwardCursor:
        """Sends a SQL query to the server but does not wait for it to finish. 
        Results are accessed via the returned forward-only cursor that does NOT buffer the results, 
        useful for reading large number of rows."""
        raise NotImplementedError()

    def start_copy_in(self, sql:str) -> None:
        """Starts a COPY ... FROM STDIN operation"""
        raise NotImplementedError()

    def write_copy_data(self, data:str) -> None:
        """Sends a block of data to PostgreSQL as part of the COPY ... FROM STDIN operation"""
        raise NotImplementedError()
        
    def end_copy(self) -> None:
        """Finishes the COPY operation started by start_copy_in()"""
        raise NotImplementedError()
        
    def close(self) -> None:
        """Closes this connection to PostgreSQL"""
        raise NotImplementedError()

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, traceback: TracebackType | None) -> None:
        self.close()
