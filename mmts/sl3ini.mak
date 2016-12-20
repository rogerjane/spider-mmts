
# makefile generated from sl3ini.prj on 03:07:38 PM Sat 22 Feb 2014 GMT by rogdev

MAINFILE=sl3ini.c
MAINOBJ=sl3ini.o

PM2_INCPATH=$(PM2_BASE)/include
PM2_LIBPATH=$(PM2_BASE)/libs

CTREE_PATH=/microtest/users/faircomm/ctree
RTREE_PATH=/microtest/users/faircomm/rtree
CTREE_INCPATH=-I$(CTREE_PATH)/include -I$(CTREE_PATH)/custom -I$(RTREE_PATH)/include
CTREE_LIBPATH=/microtest/users/faircomm/lib.fpg

INCLUDES=-I. -I$(PM2_INCPATH) $(CTREE_INCPATH)

CFLAGS=$(INCLUDES)  -I/microtest/users/faircomm/ctree/include -I/microtest/users/faircomm/ctree/custom -I/microtest/users/faircomm/rtree/include -L$(PM2_LIBPATH) -L$(CTREE_LIBPATH)  -I/usr/include/mysql -L/usr/local/mysql/lib/mysql  -I/usr/local/mysql/include/mysql -DEHR_UNIXWARE
CXXFLAGS=$(INCLUDES) -I/microtest/users/faircomm/ctree/include -I/microtest/users/faircomm/ctree/custom -I/microtest/users/faircomm/rtree/include -L$(PM2_LIBPATH) -L$(CTREE_LIBPATH)  -DUSING_SCO_SC -I/usr/include/mysql -L/usr/local/mysql/lib/mysql -I/usr/local/mysql/include/mysql -DEHR_UNIXWARE

ID=sl3ini

it: $(ID)

.cpp.o:
	CC -c $(CXXFLAGS) $<

.C.o:
	CC -c $(CXXFLAGS) $<


objects= sl3ini.o



$(ID): $(objects)
	CC $(objects) -o $(PM2_BASE)/bin/./sl3ini $(CXXFLAGS)  -lmtsl3 -lmtutil -lsocket -lnls -lmysqlclient /usr/lib/libz.a -lm
#	CC $(objects) -o $(PM2_BASE)/bin/./sl3ini $(CXXFLAGS)  -lmtsl3 -lmtutil -lsocket -lnls -lmysqlclient -lz -lm
#	CC $(objects) -o $(PM2_BASE)/bin/./sl3ini $(CXXFLAGS)  -lmtsl3 -lmtutil -lsocket -lnls -lz -lm
	@mcs -d $(PM2_BASE)/bin/./sl3ini
	rm $(MAINOBJ)

clean:
	rm -f  sl3ini.o

remake:
	make -f sl3ini.mak clean
	make -f sl3ini.mak sl3ini

