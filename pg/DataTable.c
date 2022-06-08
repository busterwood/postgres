#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <libpq-fe.h>


typedef struct {
    PyObject_HEAD
    /* Type-specific fields go here. */
    PGresult* res;
} DataTableObject;


static void DataTable_dealloc(DataTableObject *self) {
    // release the result set
    if (self->res != NULL) {
        PQclear(self->res);
        self->res = NULL;
    }
    Py_TYPE(self)->tp_free(self);
}

static Py_ssize_t DataTable_len(PyObject *obj) {
    DataTableObject* self = (DataTableObject*)obj;
    int tuples = PQntuples(self->res);
    return (Py_ssize_t)tuples;
}

static PyObject* DataTable_column_count(DataTableObject *self, PyObject* const* args, Py_ssize_t nargs) {
    int columns = PQnfields(self->res);
    return PyLong_FromLong(columns);
}

static PyObject* DataTable_column_name(DataTableObject *self, PyObject* const* args, Py_ssize_t nargs) {
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

static PyObject* DataTable_column_index(DataTableObject *self, PyObject* const* args, Py_ssize_t nargs) {
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

static PyObject* DataTable_GetItem(PyObject* obj, PyObject* key) {
    DataTableObject* self = (DataTableObject*)obj;

    int tuples = PQntuples(self->res);
    int columns = PQnfields(self->res);

    if (PyTuple_Check(key)) {
        int row = 0;
        int column = 0;
        if (!PyArg_ParseTuple(key, "ii", &row, &column))
            return NULL;

        // handle negative rows
        if (row < 0)
            row = tuples + row;

        // handle negative columns
        if (column < 0)
            column = columns + column;

        if (row < 0 || row >= tuples) {
            PyErr_SetString(PyExc_ValueError, "row is out of range");
            return NULL;
        }
        if (column < 0 || column >= columns) {
            PyErr_SetString(PyExc_ValueError, "column is out of range");
            return NULL;
        }

        // return the string at row,column 
        char* value = PQgetvalue(self->res, row, column);
        return PyUnicode_FromString(value);
    }

    if (PyLong_Check(key)) {
        long row = PyLong_AsLong(key);

        // handle negative rows
        if (row < 0)
            row = tuples + row;

        if (row < 0 || row >= tuples) {
            PyErr_SetString(PyExc_ValueError, "row is out of range (1)");
            return NULL;
        }

        PyObject* list = PyList_New(columns);
        for (int i = 0; i < columns; i++)
        {
            char* value = PQgetvalue(self->res, row, i);
            PyList_SetItem(list, i, PyUnicode_FromString(value));
        }
        return list;
    }

    PyErr_SetString(PyExc_ValueError, "Expected row index, or (row, column)");
    return NULL;
}

static PyObject* DataTable_GetItem_sequence(PyObject* obj, Py_ssize_t row) {
    DataTableObject* self = (DataTableObject*)obj;

    int tuples = PQntuples(self->res);
    
    // handle negative rows
    if (row < 0)
        row = tuples - row;

    if (row < 0 || row >= tuples)
        return NULL;

    int columns = PQnfields(self->res);
    PyObject* list = PyList_New(columns);
    for (int i = 0; i < columns; i++)
    {
        char* value = PQgetvalue(self->res, row, i);
        PyList_SetItem(list, i, PyUnicode_FromString(value));
    }
    return list;
}

//
// DataTable type definition
//

static PyMethodDef DataTable_methods[] = {
    {"column_count", (PyCFunction) DataTable_column_count, METH_FASTCALL, "The number of columns in the table."},
    {"column_name", (PyCFunction) DataTable_column_name, METH_FASTCALL, "Returns the name of a column using the supplied column index (zero-based)."},
    {"column_index", (PyCFunction) DataTable_column_index, METH_FASTCALL, "Returns the index of a column using the supplied column name."},    
    {NULL}  /* Sentinel */
};

static PyMappingMethods DataTable_mapping_methods = {
    .mp_length = DataTable_len,
    .mp_subscript = DataTable_GetItem,
};

static PySequenceMethods DataTable_sequence_methods = {
    .sq_length = DataTable_len,
    .sq_item = DataTable_GetItem_sequence,
};

PyTypeObject DataTableType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pg.DataTable",
    .tp_doc = PyDoc_STR("A DataTable to PostgreSQL"),
    .tp_basicsize = sizeof(DataTableObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_new = NULL,
    .tp_dealloc = (destructor) DataTable_dealloc,
    .tp_methods = DataTable_methods,
    .tp_as_mapping = &DataTable_mapping_methods,
    .tp_as_sequence = &DataTable_sequence_methods,
};

// allow the connection to create a data table
PyObject* DataTable_new(PGresult* res) {
    DataTableObject* obj = PyObject_New(DataTableObject, &DataTableType);
    obj->res = res;
    return (PyObject*)obj;
}
