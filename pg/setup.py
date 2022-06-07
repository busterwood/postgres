from distutils.ccompiler import new_compiler
from distutils import sysconfig

c = new_compiler()
c.add_include_dir( sysconfig.get_python_inc() )
c.add_include_dir("/usr/include/postgresql")
objects = c.compile(["Connection.c", "DataTable.c", "ForwardCursor.c"], extra_postargs=["-fPIC"])
c.link_shared_lib(objects, "pg", output_dir=".", libraries=["pq"])