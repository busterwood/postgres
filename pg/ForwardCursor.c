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
        PQconsumeInput(self->conn);
        PQclear(self->res);
        self->res = NULL;
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

static inline int check_column(PGresult* res, PyObject* arg) {
    int column = 0;
    if (PyLong_Check(arg)) {

        column = PyLong_AsLong(arg);
        int columns = PQnfields(res);
        if (column < 0)
            column = columns + column;
        if (column < 0 || column >= columns) {
            PyErr_SetString(PyExc_ValueError, "column is out of range");
            return -1;
        }
    }
    else if (PyUnicode_Check(arg)) {
        const char* str = PyUnicode_AsUTF8(arg);
        column = PQfnumber(res, str);
        if (column == -1) {
            PyErr_Format(PyExc_ValueError, "column name not found: '%s'", str);
            return -1;
        }
    }
    return column;
}

static PyObject* ForwardCursor_is_null(ForwardCursorObject *self, PyObject* const* args, Py_ssize_t nargs) {
    if (!nargs) {
        PyErr_SetString(PyExc_ValueError, "expected the column index or name.");
        return NULL;
    }

    int column = check_column(self->res, args[0]);
    if (column == -1) {
        return NULL;
    }

    const int row = 0;
    if (PQgetisnull(self->res, row, column)) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

static PyObject* ForwardCursor_get_str(ForwardCursorObject *self, PyObject* const* args, Py_ssize_t nargs) {
    if (!nargs) {
        PyErr_SetString(PyExc_ValueError, "expected the column index.");
        return NULL;
    }

    int column = check_column(self->res, args[0]);
    if (column == -1) {
        return NULL;
    }

    const int row = 0;
    char* value = PQgetvalue(self->res, row, column);
    if (value == NULL) {
        Py_RETURN_NONE;
    }
    return PyUnicode_FromString(value);
}

static PyObject* ForwardCursor_get_float(ForwardCursorObject *self, PyObject* const* args, Py_ssize_t nargs) {
    PyObject* str = ForwardCursor_get_str(self, args, nargs);
    if (!PyUnicode_Check(str)) {
        return str;
    }
    PyObject* value = PyFloat_FromString(str);
    Py_DECREF(str);
    return value;
}

static PyObject* ForwardCursor_get_int(ForwardCursorObject *self, PyObject* const* args, Py_ssize_t nargs) {
    PyObject* str = ForwardCursor_get_str(self, args, nargs);
    if (!PyUnicode_Check(str)) {
        return str;
    }
    PyObject* value = PyLong_FromUnicodeObject(str, 10);
    Py_DECREF(str);
    return value;
}

static PyObject* ForwardCursor_get_bool(ForwardCursorObject *self, PyObject* const* args, Py_ssize_t nargs) {
    PyObject* str = ForwardCursor_get_str(self, args, nargs);
    if (!PyUnicode_Check(str)) {
        return str;
    }
    if (_PyUnicode_EqualToASCIIString(str, "True")) {
        Py_DECREF(str);
        Py_RETURN_TRUE;
    }
    else if (_PyUnicode_EqualToASCIIString(str, "False")) {
        Py_DECREF(str);
        Py_RETURN_FALSE;
    }

    Py_DECREF(str);
    PyErr_Format(PyExc_ValueError, "'%s' is not a valid bool", str);
    return NULL;
}

static PyObject* ForwardCursor_get_value(ForwardCursorObject *self, PyObject* const* args, Py_ssize_t nargs) {
    if (!nargs) {
        PyErr_SetString(PyExc_ValueError, "expected the column index or name.");
        return NULL;
    }

    int column = check_column(self->res, args[0]);
    if (column == -1) {
        return NULL;
    }

    const int row = 0;
    if (PQgetisnull(self->res, row, column)) {
        Py_RETURN_NONE;
    }

    Oid col_type = PQftype(self->res, column);
    switch (col_type) {
        case 16: // BOOL 
            return ForwardCursor_get_bool(self, args, nargs);
        case 21: // INT2 
        case 23: // INT4 
        case 20: // INT8 
            return ForwardCursor_get_int(self, args, nargs);
        case 25: // TEXT 
        case 1043: // VARCHAR 
            return ForwardCursor_get_str(self, args, nargs);
        case 700: // FLOAT4 
        case 701: // FLOAT8 
            return ForwardCursor_get_float(self, args, nargs);
        // case 1082: // DATE 
        // case 1083: // TIME 
        // case 1114: // TIMESTAMP 
        // case 1184: // TIMESTAMP_TZ 
        // case 1186: // INTERVAL 
        // case 1266: // TIME_TZ 
        // case 1560: // BIT 
        default:
            // pretend everything else is a string, enums for example
            return ForwardCursor_get_str(self, args, nargs);
    }
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
            Py_RETURN_TRUE;
        case PGRES_TUPLES_OK:
        case PGRES_EMPTY_QUERY:        
            PQconsumeInput(self->conn);
            self->res = PQgetResult(self->conn); // read again, NULL expected
            Py_RETURN_FALSE;
        default:
            error_message = PQerrorMessage(self->conn);
            PyErr_SetString(PyExc_ConnectionError, error_message);
            return NULL;
    }
}

static PyObject* ForwardCursor_GetItem_sequence(PyObject* obj, Py_ssize_t column) {
    PyObject* arg = PyLong_FromLong(column);
    return ForwardCursor_get_value((ForwardCursorObject*)obj, &arg, 1);
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

static PyObject* ForwardCursor_getattro(ForwardCursorObject* self, PyObject* attr) {
    PyObject* real_attr = PyObject_GenericGetAttr((PyObject*)self, attr);
    if (!PyErr_Occurred()) {
        return real_attr;
    }
    PyErr_Clear();
    return ForwardCursor_get_value(self, &attr, 1);
}

//
// ForwardCursor type definition
//

static PyMethodDef ForwardCursor_methods[] = {
    {"next_row", (PyCFunction) ForwardCursor_next_row, METH_NOARGS, "Moves the cursor to the next row of data, returns TRUE if there is a next row, FALSE when all rows have been read"},    
    {"column_count", (PyCFunction) ForwardCursor_column_count, METH_NOARGS, "The number of columns in the table."},
    {"column_name", (PyCFunction) ForwardCursor_column_name, METH_FASTCALL, "Returns the name of a column using the supplied column index (zero-based)."},
    {"column_index", (PyCFunction) ForwardCursor_column_index, METH_FASTCALL, "Returns the index of a column using the supplied column name."},    
    {"is_null", (PyCFunction) ForwardCursor_is_null, METH_FASTCALL, "Returns TRUE if the value in a column is null."},    
    {"get_str", (PyCFunction) ForwardCursor_get_str, METH_FASTCALL, "Returns the string value of a column, or None if the value is NULL."},    
    {"get_int", (PyCFunction) ForwardCursor_get_int, METH_FASTCALL, "Returns the integer value of a column, or None if the value is NULL."},    
    {"get_float", (PyCFunction) ForwardCursor_get_float, METH_FASTCALL, "Returns the float value of a column, or None if the value is NULL."},    
    {"get_bool", (PyCFunction) ForwardCursor_get_bool, METH_FASTCALL, "Returns the boolean value of a column, or None if the value is NULL."},    
    {"get_value", (PyCFunction) ForwardCursor_get_value, METH_FASTCALL, "Returns the value of a column, or None if the value is NULL."},    
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
    // .tp_getattro = (getattrofunc)ForwardCursor_getattro,
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
c.start_query("select * from sales.item_outlet_sales where country_code = $1 and item_group_code = $2 and periodicity = 'Monthly'", 'DE', 'MOBILE_COMPUTING')
cur = c.end_query()
while cur.next_row():
  print(cur[0], cur.periodicity, cur.year, cur.position_in_year)

*/