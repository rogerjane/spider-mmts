
# makefile generated from mtutil.prj on 03:42:25 PM Tue 06 Dec 2016 GMT by rog
# Note that this will need to be run with gmake on SCO, not make!

UNAME := $(shell uname)
ifeq ($(UNAME),SCO_SV)
	CC:=CC
	CFLAGS=$(DEBUG) $(INCLUDES) -L$(LIBPATH)

	CXX:=CC
	CXXFLAGS=$(DEBUG) $(INCLUDES) -L$(LIBPATH)
endif

ifeq ($(UNAME),Linux)
	CC:=g++
	CFLAGS=-Wall $(DEBUG) $(INCLUDES) -L$(LIBPATH)

	CXX:=g++
	CXXFLAGS=$(DEBUG) $(INCLUDES) -L$(LIBPATH)
endif

ifeq ($(UNAME),Darwin)
	CC:=g++
	CFLAGS=-Wall $(DEBUG) $(INCLUDES) -L$(LIBPATH)
endif

BASE=.

INCPATH=$(BASE)
LIBPATH=$(BASE)/../lib
SQLITE3=$(BASE)/../sqlite3

INCLUDES=-I. -I$(INCPATH) -I$(SQLITE3)


OBJ=mtchannel.o mtutf8.o mtjson.o mtwamp.o mtwamputil.o mtwebsocket.o hbuf.o mime.o mtsl3.o rogxml.o szz.o guid.o heapstrings.o mtstrings.o smap.o mthttp.o
HEAD=mtchannel.h mtutf8.h mtjson.h mtwamp.h mtwamputil.h mtwebsocket.h hbuf.h mime.h mtsl3.h rogxml.h szz.h guid.h heapstrings.h mtstrings.h smap.h mthttp.h

ID=hydra
LIB=$(LIBPATH)/lib$(ID).a

.cpp.h:
	LANG=C makeh $< pm2

.c.h:
	LANG=C makeh $< pm2

$(LIB): $(HEAD) $(OBJ)
	ar cru $(LIB) `lorder $(OBJ) | tsort`
	@echo "LIB $(ID) has been updated '$(LIB)'"

clean:
	rm -f $(OBJ) *.ti

spotless: clean
	rm -f $(HEAD) $(LIB)
