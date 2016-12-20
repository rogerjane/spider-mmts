
# makefile generated from mmts.prj on 03:41:10 PM Thu 08 Dec 2016 GMT by rog

MAINFILE=mmts.c
MAINOBJ=mmts.o

PM2_INCPATH=$(PM2_BASE)/include
PM2_LIBPATH=$(PM2_BASE)/libs

CTREE_PATH=/microtest/users/faircomm/ctree
RTREE_PATH=/microtest/users/faircomm/rtree
CTREE_INCPATH=-I$(CTREE_PATH)/include -I$(CTREE_PATH)/custom -I$(RTREE_PATH)/include
CTREE_LIBPATH=/microtest/users/faircomm/lib.fpg

INCLUDES=-I. -I$(PM2_INCPATH) $(CTREE_INCPATH)

CFLAGS=$(INCLUDES)  -I/microtest/users/faircomm/ctree/include -I/microtest/users/faircomm/ctree/custom -I/microtest/users/faircomm/rtree/include -I/usr/local/ssl/include -L$(PM2_LIBPATH) -L$(CTREE_LIBPATH)  -L/usr/local/lib -I/usr/include/mysql -L/usr/local/mysql/lib/mysql  -I/usr/local/mysql/include/mysql -DEHR_UNIXWARE
CXXFLAGS=$(INCLUDES) -I/microtest/users/faircomm/ctree/include -I/microtest/users/faircomm/ctree/custom -I/microtest/users/faircomm/rtree/include -I/usr/local/ssl/include -L$(PM2_LIBPATH) -L$(CTREE_LIBPATH)  -L/usr/local/lib -DUSING_SCO_SC -I/usr/include/mysql -L/usr/local/mysql/lib/mysql -I/usr/local/mysql/include/mysql -DEHR_UNIXWARE

ID=mmts

it: $(ID)

.cpp.o:
	CC -c $(CXXFLAGS) $<

.C.o:
	CC -c $(CXXFLAGS) $<


objects= mmts.o mmts-contract.o mmts-message.o mmts-utils.o varset.o mtjson.o mtchannel.o mtwebsocket.o mtwamp.o mtwamputil.o



$(ID): $(objects)
	CC $(objects) -o $(PM2_BASE)/bin/./mmts $(CXXFLAGS)  -lmtutil -lmtsl3 -lnpfit -lxstring -Bstatic -lz -lssl -lcrypto -Bdynamic -ldbase -lgen -lmtutil -lxstring -ldatetime -lmtfile -lsocket -lnls -lmysqlclient /usr/lib/libz.a -lm
#	CC $(objects) -o $(PM2_BASE)/bin/./mmts $(CXXFLAGS)  -lmtutil -lmtsl3 -lnpfit -lxstring -Bstatic -lz -lssl -lcrypto -Bdynamic -ldbase -lgen -lmtutil -lxstring -ldatetime -lmtfile -lsocket -lnls -lmysqlclient -lz -lm
#	CC $(objects) -o $(PM2_BASE)/bin/./mmts $(CXXFLAGS)  -lmtutil -lmtsl3 -lnpfit -lxstring -Bstatic -lz -lssl -lcrypto -Bdynamic -ldbase -lgen -lmtutil -lxstring -ldatetime -lmtfile -lsocket -lnls -lz -lm
	@mcs -d $(PM2_BASE)/bin/./mmts
	rm $(MAINOBJ)

clean:
	rm -f  mmts.o mmts-contract.o mmts-message.o mmts-utils.o varset.o mtjson.o mtchannel.o mtwebsocket.o mtwamp.o mtwamputil.o

remake:
	make -f mmts.mak clean
	make -f mmts.mak mmts

