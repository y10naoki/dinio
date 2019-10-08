#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.61)
AC_INIT(dinio, 0.3.0, y10naoki@gmail.com)
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([src/dinio_server.c])
AC_CONFIG_HEADER([config.h])

# Checks for programs.
AC_PROG_CC
AM_PROG_CC_C_O

# checks for args.
AC_ARG_WITH([nestalib],
    [  --with-nestalib=path location of the nestalib headers (default is /usr/local/include/nestalib))],
    [NESTALIB_HEADERS=${withval}],
    [NESTALIB_HEADERS=/usr/local/include/nestalib])
AC_SUBST([NESTALIB_HEADERS])

# Checks for libraries.
AC_CHECK_LIB([pthread], [pthread_mutex_lock])
AC_CHECK_LIB([rt], [clock_gettime])
AC_CHECK_LIB([z], [deflate])
AC_CHECK_LIB([ssl], [SSL_library_init])
AC_CHECK_LIB([xml2], [xmlReadMemory])

AC_CHECK_LIB([nesta], [nio_initialize],
       [AC_DEFINE([HAVE_NESTALIB], 1, [nestalib is core library])],
       [AC_MSG_ERROR(['nestalib' is not installed.])])
AC_CHECK_LIB([nesta], [nio_initialize])

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST

# Checks for library functions.
AC_FUNC_ALLOCA
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_HEADER_STDC
AC_FUNC_SELECT_ARGTYPES
AC_TYPE_SIGNAL
AC_CHECK_FUNCS([memmove memset realpath select socket strerror])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
