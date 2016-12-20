
# makefile generated from rctest.prj on 09:50:43 AM Wed 19 Jun 2013 BST by rogdev

MAINFILE=rctest.c
MAINOBJ=rctest.o

PM2_INCPATH=$(PM2_BASE)/include
PM2_LIBPATH=$(PM2_BASE)/libs

CTREE_PATH=/microtest/users/faircomm/ctree
RTREE_PATH=/microtest/users/faircomm/rtree
CTREE_INCPATH=-I$(CTREE_PATH)/include -I$(CTREE_PATH)/custom -I$(RTREE_PATH)/include
CTREE_LIBPATH=/microtest/users/faircomm/lib.fpg

INCLUDES=-I. -I$(PM2_INCPATH) $(CTREE_INCPATH)

CFLAGS=$(INCLUDES)  -I/microtest/users/faircomm/ctree/include -I/microtest/users/faircomm/ctree/custom -I/microtest/users/faircomm/rtree/include -L$(PM2_LIBPATH) -L$(CTREE_LIBPATH)  -I/usr/include/mysql -L/usr/local/mysql/lib/mysql  -I/usr/local/mysql/include/mysql -DEHR_UNIXWARE
CXXFLAGS=$(INCLUDES) -I/microtest/users/faircomm/ctree/include -I/microtest/users/faircomm/ctree/custom -I/microtest/users/faircomm/rtree/include -L$(PM2_LIBPATH) -L$(CTREE_LIBPATH)  -DUSING_SCO_SC -I/usr/include/mysql -L/usr/local/mysql/lib/mysql -I/usr/local/mysql/include/mysql -DEHR_UNIXWARE

ID=rctest

it: $(ID)

.cpp.o:
	CC -c $(CXXFLAGS) $<

.C.o:
	CC -c $(CXXFLAGS) $<


objects= rctest.o rcache.o



$(ID): $(objects)
	CC $(objects) -o $(PM2_BASE)/bin/./rctest $(CXXFLAGS)  -lmtutil -lxstring -lmtutil -lxstring -ldatetime -lmtfile -lsocket -lnls -lmysqlclient /usr/lib/libz.a -lm
#	CC $(objects) -o $(PM2_BASE)/bin/./rctest $(CXXFLAGS)  -lmtutil -lxstring -lmtutil -lxstring -ldatetime -lmtfile -lsocket -lnls -lmysqlclient -lz -lm
#	CC $(objects) -o $(PM2_BASE)/bin/./rctest $(CXXFLAGS)  -lmtutil -lxstring -lmtutil -lxstring -ldatetime -lmtfile -lsocket -lnls -lz -lm
	@mcs -d $(PM2_BASE)/bin/./rctest
	rm $(MAINOBJ)

clean:
	rm -f  rctest.o rcache.o

remake:
	make -f rctest.mak clean
	make -f rctest.mak rctest

