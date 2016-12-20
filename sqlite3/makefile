

CFLAGS=$(INCLUDES) -I/usr/local/include -L$(LIBPATH) -DEHR_UNIXWARE

.c.o:
	cc -c $(CFLAGS) -o $@ $<

sqlite3.o:
