from distutils.core import setup, Extension

module1 = Extension(
    'pg', 
    include_dirs=["/usr/include/postgresql"], 
    libraries=["pq"], 
    sources=["Connection.c", "DataTable.c", "ForwardCursor.c"],    
    extra_link_args=["-flto"],
    extra_compile_args=["-march=native"]
    )
setup(name="pg", version="1.0", ext_modules=[module1], )