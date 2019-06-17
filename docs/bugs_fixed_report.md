# Bugs we've encountered along the way:

* When comparing string, have to use == instead of equal
* The server will overload when running too many replicas
  * Fixed by removing the server if chosen and shorten the interval and time
    out, until the universe is exhausted.
* Infinite forwarding/multicasting to shards going on because hashes were
  sometimes incorrect. Due to the use of the hash() function.
  * Fixed with hashlib
