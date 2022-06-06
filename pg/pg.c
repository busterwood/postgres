#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <libpq-fe.h>


typedef struct {
    PyObject_HEAD
    /* Type-specific fields go here. */
    PGconn *conn;
} ConnectionObject;


static void
Connection_dealloc(ConnectionObject *self)
{
    // close the connection, if it is open
    if (self->conn != NULL) {
        PQfinish(self->conn);
        self->conn = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

// __init__ method
static int
Connection_init(ConnectionObject *self, PyObject *args)
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
void 
free_result(PGresult** res) {
    PQclear(*res);
}

static int
Connection_execute_script(ConnectionObject *self, PyObject *args) {
    char* error_message = NULL;
        
    const char* sql_script = NULL;
    if (!PyArg_ParseTuple(args, "s", &sql_script))
        return -1;

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
            return -1;
    }
    return 0;
}

static int
Connection_execute(ConnectionObject *self, PyObject *args) {
    char* error_message = NULL;
        
    const char* sql_script = NULL;
    if (!PyArg_ParseTuple(args, "s", &sql_script))
        return -1;

    // make sure result is cleared
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
            return -1;
    }
    return 0;
}

//
// Connection type definition
//

static PyMethodDef Connection_methods[] = {
    {"execute_script", (PyCFunction) Connection_execute_script, METH_VARARGS, "Run a multiple SQL statements, each one must not return any rows."},
    {"execute", (PyCFunction) Connection_execute, METH_VARARGS, "Run a SQL statement that does not return any rows, e.g. INSERT, UPDATE or DELETE, and wait for the statement to finish."},
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


PyMODINIT_FUNC
PyInit_libpg(void)
{
    PyObject *m;
    if (PyType_Ready(&ConnectionType) < 0)
        return NULL;

    m = PyModule_Create(&ConnectionModule);
    if (m == NULL)
        return NULL;

    Py_INCREF(& ConnectionType);
    if (PyModule_AddObject(m, "Connection", (PyObject *) &ConnectionType) < 0) {
        Py_DECREF(&ConnectionType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}