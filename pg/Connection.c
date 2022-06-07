#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <libpq-fe.h>

PyObject* DataTable_new(PGresult* res);

typedef struct {
    PyObject_HEAD
    /* Type-specific fields go here. */
    PGconn *conn;
} ConnectionObject;


static void Connection_dealloc(ConnectionObject *self)
{
    // close the connection, if it is open
    if (self->conn != NULL) {
        PQfinish(self->conn);
        self->conn = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

// __init__ method
static int Connection_init(ConnectionObject *self, PyObject *args)
{
    char* error_message = NULL;
    
    const char* connection_string = NULL;
    if (!PyArg_ParseTuple(args, "s", &connection_string))
        return -1;

    self->conn = PQconnectdb(connection_string);

    // check we connected, raise ConnectionError if we failed 
    ConnStatusType status = PQstatus(self->conn);
    if (status != CONNECTION_OK) {
        error_message = PQerrorMessage(self->conn);
        PyErr_SetString(PyExc_ConnectionError, error_message);
        return -1;
    }

    return 0;
}

// cleanup is passed as a double pointer, so dereference it to clear the result
void free_result(PGresult** res) {
    PQclear(*res);
}

static PyObject* Connection_execute_script(ConnectionObject *self, PyObject* const* args, Py_ssize_t nargs) {
    char* error_message = NULL;
        
    if (!nargs || !PyUnicode_Check(args[0])) {
        PyErr_SetString(PyExc_ValueError, "expected the first argument 'sql_script' to be a string");
        return NULL;
    }
    const char* sql_script = PyUnicode_AsUTF8(args[0]);

    // make sure result is cleared (GCC-specific)
    PGresult* res __attribute__((cleanup(free_result))) = PQexec(self->conn, sql_script);

    ExecStatusType status = PQresultStatus(res);
    switch (status) {
        case PGRES_COMMAND_OK:
        case PGRES_EMPTY_QUERY:
        case PGRES_TUPLES_OK: // just ignore any tuples
            break;
        default:
            error_message = PQerrorMessage(self->conn);
            PyErr_SetString(PyExc_ConnectionError, error_message);
            return NULL;;
    }
    return Py_None;
}

static PyObject* Connection_execute(ConnectionObject *self, PyObject* const* args, Py_ssize_t nargs) {
    char* error_message = NULL;
        
    if (!nargs || !PyUnicode_Check(args[0])) {
        PyErr_SetString(PyExc_ValueError, "expected the first argument 'sql_script' to be a string");
        return NULL;
    }
    const char* sql_script = PyUnicode_AsUTF8(args[0]);

    // convert all args to strings
    PyObject** str_args = (PyObject**)malloc(nargs * sizeof(PyObject));
    const char** utf8_args = (const char**)malloc(nargs * sizeof(char*));
    for (size_t i = 0; i < nargs-1; i++)
    {
        str_args[i] = PyObject_Str(args[i+1]);
        utf8_args[i] = PyUnicode_AsUTF8(str_args[i]); // get UTF8 version - no need to free this as it is freed when the str python object is freed
    }
    
    // make sure result is cleared
    PGresult* res __attribute__((cleanup(free_result))) = PQexecParams(self->conn, sql_script, nargs-1, NULL, utf8_args, NULL, NULL, 0);

    // free args (this also frees the utf8 char* at the same time)
    for (size_t i = 0; i < nargs-1; i++) {
        Py_DECREF(str_args[i]);
    }

    ExecStatusType status = PQresultStatus(res);
    switch (status) {
        case PGRES_COMMAND_OK:
        case PGRES_EMPTY_QUERY:
        case PGRES_TUPLES_OK: // just ignore any tuples
            break;
        default:
            error_message = PQerrorMessage(self->conn);
            PyErr_SetString(PyExc_ConnectionError, error_message);
            return NULL;
    }
    return Py_None;
}

static PyObject* Connection_query(ConnectionObject *self, PyObject* const* args, Py_ssize_t nargs) {
    char* error_message = NULL;
        
    if (!nargs || !PyUnicode_Check(args[0])) {
        PyErr_SetString(PyExc_ValueError, "expected the first argument 'sql_script' to be a string");
        return NULL;
    }
    const char* sql_script = PyUnicode_AsUTF8(args[0]);

    // convert all args to strings
    PyObject** str_args = (PyObject**)malloc(nargs * sizeof(PyObject));
    const char** utf8_args = (const char**)malloc(nargs * sizeof(char*));
    for (size_t i = 0; i < nargs-1; i++)
    {
        str_args[i] = PyObject_Str(args[i+1]);
        utf8_args[i] = PyUnicode_AsUTF8(str_args[i]); // get UTF8 version - no need to free this as it is freed when the str python object is freed
    }
    
    // make sure result is cleared
    PGresult* res = PQexecParams(self->conn, sql_script, nargs-1, NULL, utf8_args, NULL, NULL, 0);

    // free args (this also frees the utf8 char* at the same time)
    for (size_t i = 0; i < nargs-1; i++) {
        Py_DECREF(str_args[i]);
    }

    ExecStatusType status = PQresultStatus(res);
    switch (status) {
        case PGRES_COMMAND_OK:
        case PGRES_EMPTY_QUERY:
        case PGRES_TUPLES_OK: // just ignore any tuples
            break;
        default:
            error_message = PQerrorMessage(self->conn);
            PyErr_SetString(PyExc_ConnectionError, error_message);
            PQclear(res);
            return NULL;
    }
    return DataTable_new(res);
}

//
// Connection type definition
//

static PyMethodDef Connection_methods[] = {
    {"execute_script", (PyCFunction) Connection_execute_script, METH_FASTCALL, "Run a multiple SQL statements, each one must not return any rows."},
    {"execute", (PyCFunction) Connection_execute, METH_FASTCALL, "Run a SQL statement that does not return any rows, e.g. INSERT, UPDATE or DELETE, and wait for the statement to finish."},
    {"query", (PyCFunction) Connection_query, METH_FASTCALL, "Run a SQL statement that returns a table of data."},
    {NULL}  /* Sentinel */
};


static PyTypeObject ConnectionType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pg.Connection",
    .tp_doc = PyDoc_STR("A Connection to PostgreSQL"),
    .tp_basicsize = sizeof(ConnectionObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_init = (initproc) Connection_init,
    .tp_dealloc = (destructor) Connection_dealloc,
    .tp_methods = Connection_methods,
};

static PyModuleDef ConnectionModule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "Connection",
    .m_doc = "Example module that creates an extension type.",
    .m_size = -1,
};

// defined in DataTable.c
extern PyTypeObject DataTableType;

PyMODINIT_FUNC PyInit_libpg(void)
{
    PyObject *m;
    if (PyType_Ready(&ConnectionType) < 0 || PyType_Ready(&DataTableType) < 0)
        return NULL;

    m = PyModule_Create(&ConnectionModule);
    if (m == NULL)
        return NULL;

    Py_INCREF(&ConnectionType);
    if (PyModule_AddObject(m, "Connection", (PyObject *) &ConnectionType) < 0) {
        Py_DECREF(&ConnectionType);
        Py_DECREF(m);
        return NULL;
    }

    Py_INCREF(&DataTableType);
    if (PyModule_AddObject(m, "DataTable", (PyObject *) &DataTableType) < 0) {
        Py_DECREF(&DataTableType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}