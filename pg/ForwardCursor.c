#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <libpq-fe.h>


typedef struct {
    PyObject_HEAD
    /* Type-specific fields go here. */
    PGconn* conn;
    PGresult* res;
} ForwardCursorObject;


static void ForwardCursor_dealloc(ForwardCursorObject *self) {
    // release the result set
    if (self->res != NULL) {
        PQclear(self->res);
        self->res = NULL;
        PQconsumeInput(self->conn);
    }
    Py_TYPE(self)->tp_free(self);
}

static PyObject* ForwardCursor_column_count(ForwardCursorObject *self, PyObject* ignored) {
    int columns = PQnfields(self->res);
    return PyLong_FromLong(columns);
}

static PyObject* ForwardCursor_column_name(ForwardCursorObject *self, PyObject* const* args, Py_ssize_t nargs) {
    if (!nargs || !PyLong_Check(args[0])) {
        PyErr_SetString(PyExc_ValueError, "Expected a single int argument of the column index, starting at zero.");
        return NULL;
    }    
    long index = PyLong_AsLong(args[0]);
    char* name = PQfname(self->res, index);
    if (name == NULL) {
        PyErr_SetString(PyExc_ValueError, "Column index is out of range.");
        return NULL;
    }
    return PyUnicode_FromString(name);
}

static PyObject* ForwardCursor_column_index(ForwardCursorObject *self, PyObject* const* args, Py_ssize_t nargs) {
    if (!nargs || !PyUnicode_Check(args[0])) {
        PyErr_SetString(PyExc_ValueError, "expected a single string argument of the column name.");
        return NULL;
    }    
    const char* name = PyUnicode_AsUTF8(args[0]);
    int index = PQfnumber(self->res, name);
    if (index < 0) {
        PyErr_SetString(PyExc_ValueError, "Column name not found.");
        return NULL;
    }
    return PyLong_FromLong(index);
}

static PyObject* ForwardCursor_next_row(ForwardCursorObject *self, PyObject* ignored) {
    char* error_message;

    if (self->res != NULL) {
        PQclear(self->res);
    }
    self->res = PQgetResult(self->conn);
    
    ExecStatusType status = PQresultStatus(self->res);
    switch (status) {
        case PGRES_SINGLE_TUPLE:        
            return Py_True;
        case PGRES_TUPLES_OK:
        case PGRES_EMPTY_QUERY:        
            PQconsumeInput(self->conn);
            self->res = PQgetResult(self->conn); // read again, NULL expected
            return Py_False;
        default:
            error_message = PQerrorMessage(self->conn);
            PyErr_SetString(PyExc_ConnectionError, error_message);
            return NULL;
    }
}

static PyObject* ForwardCursor_GetItem_sequence(PyObject* obj, Py_ssize_t column) {
    ForwardCursorObject* self = (ForwardCursorObject*)obj;

    int columns = PQnfields(self->res);
    const int row = 0;

    // handle negative columns
    if (column < 0)
        column = columns + column;

    if (column < 0 || column >= columns) {
        PyErr_SetString(PyExc_ValueError, "column is out of range");
        return NULL;
    }

    // return the string at row,column 
    char* value = PQgetvalue(self->res, row, column);
    return value == NULL ? Py_None : PyUnicode_FromString(value);
}

static PyObject* ForwardCursor_iter(PyObject* self) {
    return self;
}

static PyObject* ForwardCursor_iternext(PyObject* obj) {
    ForwardCursorObject* self = (ForwardCursorObject*)obj;
    PyObject* moved_obj = ForwardCursor_next_row(self, NULL);
    if (moved_obj == NULL) {
        return NULL;
    }
    int moved = Py_IsTrue(moved_obj);
    Py_DECREF(moved_obj);
    return moved ? obj : NULL;
}

//
// ForwardCursor type definition
//

static PyMethodDef ForwardCursor_methods[] = {
    {"next_row", (PyCFunction) ForwardCursor_next_row, METH_NOARGS, "Moves the cursor to the next row of data, returns TRUE if there is a next row, FALSE when all rows have been read"},    
    {"column_count", (PyCFunction) ForwardCursor_column_count, METH_NOARGS, "The number of columns in the table."},
    {"column_name", (PyCFunction) ForwardCursor_column_name, METH_FASTCALL, "Returns the name of a column using the supplied column index (zero-based)."},
    {"column_index", (PyCFunction) ForwardCursor_column_index, METH_FASTCALL, "Returns the index of a column using the supplied column name."},    
    {NULL}  /* Sentinel */
};

static PySequenceMethods ForwardCursor_sequence_methods[] = {
    NULL,
    NULL,
    NULL,
    ForwardCursor_GetItem_sequence,
};

PyTypeObject ForwardCursorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pg.ForwardCursor",
    .tp_doc = PyDoc_STR("A single record view of results from PostgreSQL, minimises client memory usage"),
    .tp_basicsize = sizeof(ForwardCursorObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_new = NULL,
    .tp_dealloc = (destructor) ForwardCursor_dealloc,
    .tp_methods = ForwardCursor_methods,
    .tp_as_sequence = ForwardCursor_sequence_methods,
    .tp_iter = ForwardCursor_iter,
    .tp_iternext = ForwardCursor_iternext,
};

// allow the connection to create a forward cursor
PyObject* ForwardCursor_new(PGconn* conn) {
    ForwardCursorObject* obj = PyObject_New(ForwardCursorObject, &ForwardCursorType);
    obj->conn = conn;
    obj->res = NULL;
    return (PyObject*)obj;
}

/*

import libpg as pg
c = pg.Connection("postgresql://flyway@api-dev.db.gfknewron.com:5432/newron")
c.start_query("select * from sales.period where period_seq = $1", 123)
cur = c.end_query()
while cur.next_row():
  print(cur[0])

*/