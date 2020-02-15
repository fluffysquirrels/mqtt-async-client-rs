# Notes

## Connect / reconnect behaviour

* Failed connection (no ping response) should disconnect then reconnect
* Reset connection (zero bytes read in read_packet) should disconnect then reconnect
* connect() should connect
* disconnect() should disconnect for good
* Subscribed topics should replay on reconnect
* __TODO__ Publishes should retry for a bit across connections then timeout. [Issue #5](https://github.com/fluffysquirrels/mqtt-async-client-rs/issues/5)
    * __TODO__ Currently pid response map is tied to a connection but it needs to last across connections
