
# makefile generated from mtutil.prj on 03:42:25 PM Tue 06 Dec 2016 GMT by rog

.SUFFIXES: .c .h
.IGNORE:

BASE=.

SRC=$(BASE)
INCPATH=$(BASE)
LIBPATH=$(BASE)/lib

INCLUDES=-I. -I$(INCPATH) -I../support/include

CFLAGS=$(INCLUDES) -I/usr/local/include -L$(LIBPATH) -DEHR_UNIXWARE

ID=mtwamp
LIB=$(LIBPATH)/lib$(ID).a

.PRECIOUS: $(LIB)

.c.o:
	cc -c $(CFLAGS) -o $@ $<

.c.h:
	./makeh $<

OBJ=mtchannel.o mtjson.o mtwamp.o mtwamputil.o mtwebsocket.o
HEAD=mtchannel.h mtjson.h mtwamp.h mtwamputil.h mtwebsocket.h

$(LIB): $(HEAD) $(OBJ)
	ar cru $(LIB) `lorder $(OBJ) | tsort`
	@echo "LIB $(ID) has been updated '$(LIB)'"

lib: $(ID)
library: $(ID)

clean:
	rm -f  $(OBJ)

spotless: clean
	rm $(HEAD) $(LIB)
