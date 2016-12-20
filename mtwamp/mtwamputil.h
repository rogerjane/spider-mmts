#ifndef __MTWAMPUTIL_H
#define __MTWAMPUTIL_H


//#include "mtjson.h"
#include "mtchannel.h"
#include "mtwebsocket.h"
#include "mtwamp.h"

typedef int (*INTWAMPFN)(WAMP *wamp, const char *mesg);


int wamp_Connect(CHANPOOL *pool, const char *url, INTWAMPFN cb_OnDone);
// Makes a connection to another WAMP process on a given URL and drops it into the pool given.
// ... Currently this will always be WebSocket based, but should probably allow for a RawSocket connection as it's simpler
// 1. Decode the address and complain if it's duff
// 2. Make the physical connection (note that this may be TLS)
// 3. Create a channel on this connection
// 4. Put the HTTP header into the outgoing queue for the channel
// 5. Arrange that the channel is handled by our 'connection' responder
// 6. Drop back to the rest can be done asynchronously
// ...  Time passes and we are in the loop, we'll receive some input on this channel (or it might go away):
// 7. Check the response header is good
// 8. Link the 'WAMP' to the web socket and drop back
// 9. Send an initial HELLO packet.
// Returns	1	Failed to figure an address
// ALWAYS calls cb_OnDone in one way or another

void wamp_AcceptBio(CHANPOOL *pool, BIO *bio, INTWAMPFN cb_onDone);
// Accept an incoming connection, attaching it to the pool if we're successful.
// cb_onDone is ALWAYS called when completed, either with the WAMP* or NULL on failure

#endif
