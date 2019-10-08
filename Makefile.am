noinst_PROGRAMS = dinio
dinio_SOURCES = src/main.c \
                src/config.c \
                src/connect.c \
                src/consistent_hash.c \
                src/dataio.c \
                src/dispatch.c \
                src/dinio_cmd.c \
                src/dinio_server.c \
                src/ds_check.c \
                src/ds_server.c \
                src/friend.c \
                src/informed.c \
                src/lock_server.c \
                src/memc_gateway.c \
                src/redistribution.c \
                src/replication.c \
                src/server_cmd.c \
                src/consistent_hash.h \
                src/dinio.h \
                src/ds_server.h

dinio_CFLAGS = -I. -I@NESTALIB_HEADERS@

EXTRA_DIR = conf logs

DISTCLEANFILES = *~

dist-hook:
	for subdir in $(EXTRA_DIR); do \
		cp -rp $$subdir $(distdir); \
		rm -f $(distdir)/$$subdir/*.txt; \
		rm -f $(distdir)/$$subdir/*~; \
		rm -f $(distdir)/$$subdir/.DS*; \
	done
