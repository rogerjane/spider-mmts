
# makefile generated from mtutil.prj on 03:42:25 PM Tue 06 Dec 2016 GMT by rog

.SUFFIXES: .c .C .cpp
.IGNORE:

BASE=.

INCPATH=$(BASE)/include
LIBPATH=$(BASE)/lib
SQLITE3=$(BASE)/../sqlite3

INCLUDES=-I. -I$(INCPATH) -I$(SQLITE3)

CFLAGS=$(INCLUDES) -g -I/usr/local/include -L$(LIBPATH) -DEHR_UNIXWARE
CXXFLAGS=$(INCLUDES) -I/usr/local/include -L$(LIBPATH) -DUSING_SCO_SC -Tlocal -DEHR_UNIXWARE

ID=hydra
LIBRARY=$(LIBPATH)/lib$(ID).a

.PRECIOUS: $(LIBRARY)

.cpp.o:
	CC -c $(CXXFLAGS) $<

.C.o:
	CC -c $(CXXFLAGS) $<

.c.o:
	cc -c $(CFLAGS) -o $@ $<

objects= csv.o guid.o hbuf.o heapstrings.o mime.o mtsl3.o rogxml.o smap.o szz.o mtstrings.o

$(ID): $(objects)
	ar cruv $(LIBRARY) `lorder $(objects) | tsort`
	@echo "LIBRARY $(ID) has been updated '$(LIBRARY)'"

clean:
	rm -f $(objects)

