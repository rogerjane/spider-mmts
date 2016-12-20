
# makefile generated from jtest.prj on 04:48:28 PM Fri 09 Dec 2016 GMT by rog

MAINFILE=jtest.c
MAINOBJ=jtest.o

PM2_INCPATH=$(PM2_BASE)/include
PM2_LIBPATH=$(PM2_BASE)/libs

CTREE_PATH=/microtest/users/faircomm/ctree
RTREE_PATH=/microtest/users/faircomm/rtree
CTREE_INCPATH=-I$(CTREE_PATH)/include -I$(CTREE_PATH)/custom -I$(RTREE_PATH)/include
CTREE_LIBPATH=/microtest/users/faircomm/lib.fpg

INCLUDES=-I. -I$(PM2_INCPATH) $(CTREE_INCPATH)

CFLAGS=$(INCLUDES)  -I/microtest/users/faircomm/ctree/include -I/microtest/users/faircomm/ctree/custom -I/microtest/users/faircomm/rtree/include -I/usr/local/ssl/include -L$(PM2_LIBPATH) -L$(CTREE_LIBPATH)  -L/usr/local/lib -I/usr/include/mysql -L/usr/local/mysql/lib/mysql  -I/usr/local/mysql/include/mysql -DEHR_UNIXWARE
CXXFLAGS=$(INCLUDES) -I/microtest/users/faircomm/ctree/include -I/microtest/users/faircomm/ctree/custom -I/microtest/users/faircomm/rtree/include -I/usr/local/ssl/include -L$(PM2_LIBPATH) -L$(CTREE_LIBPATH)  -L/usr/local/lib -DUSING_SCO_SC -I/usr/include/mysql -L/usr/local/mysql/lib/mysql -I/usr/local/mysql/include/mysql -DEHR_UNIXWARE

ID=jtest

it: $(ID)

.cpp.o:
	CC -c $(CXXFLAGS) $<

.C.o:
	CC -c $(CXXFLAGS) $<


objects= jtest.o mtjson.o



$(ID): $(objects)
	CC $(objects) -o $(PM2_BASE)/bin/./jtest $(CXXFLAGS)  -lmtutil -lmtsl3 -lnpfit -lxstring -lsocket -lnls -lmysqlclient /usr/lib/libz.a -lm
#	CC $(objects) -o $(PM2_BASE)/bin/./jtest $(CXXFLAGS)  -lmtutil -lmtsl3 -lnpfit -lxstring -lsocket -lnls -lmysqlclient -lz -lm
#	CC $(objects) -o $(PM2_BASE)/bin/./jtest $(CXXFLAGS)  -lmtutil -lmtsl3 -lnpfit -lxstring -lsocket -lnls -lz -lm
	@mcs -d $(PM2_BASE)/bin/./jtest
	rm $(MAINOBJ)

clean:
	rm -f  jtest.o mtjson.o

remake:
	make -f jtest.mak clean
	make -f jtest.mak jtest

