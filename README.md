# CS 425 MP3

Code for Alec Benzer and Vignesh Raja's solution to MP3

## Running

Run `./mp3.py --help` to see usage. An example execution is:

    ./mp3.py -p 10000 
    
This reads from the `config.py` file to get certain configurable variables. A sample `config.py` file is show below:

    servers = [
        ("127.0.0.1", 10000),
        ("127.0.0.1", 10001),
        ("127.0.0.1", 10002),
        ("127.0.0.1", 10003)
    ]
    delays = {
        ("127.0.0.1", 10000): 2,
        ("127.0.0.1", 10001): 20,
        ("127.0.0.1", 10002): 2,
        ("127.0.0.1", 10003): 2
    }

With this `config.py`, the preceding `./mp3.py` invocation will launch a key-value server on port 10000 and the delays between
different servers that are also launched are initialized. There must be 4 servers total that are running simultaneously. 

After starting up the servers, one can execute different commands to insert/delete/update/get data from the distributed system. One 
can also specify the preferred consistency level for the operation. Examples for each of these operations are shown below:

Insert key-value pair 5 -> 1 with consistency level "one"

  `insert 5 1 one`

Delete key 5 

  `delete 5`

Update key 5 with value 3 with consistency level "all"

  `update 5 3 all`

Get key 5's current value with consistency level "one"

  `get 5 one`

## Description of Algorithm

To manage the different types of operations that a user may want to execute on the distributed key-value store we have different message
types. In `server.py` we declare these messages for each respective operation as GetRequest, GetResponse, InsertRequest, InsertResponse, etc.
A thread is always running for each server that listens for requests. For most of the operations, the system's behavior
depends on the consistency level. If the level is "all", the server waits until responses are received from all replicas for a key. If the 
consistency level is "one", the server only waits for a single response from any of the key's replicas.

To repair inconsistencies, we implemented read-repairs. Thus, on a get request from a user, we launch a background thread that first collects timestamps
and values for the specified key. Next, the thread sends RepairRequest messages to all of the replicas for the key; these messages contain the most recent
modification timestamp for the key and the most recent value. Each replica eventually receives a RepairRequest message and is updated based on whether the 
replica's local value is up-to-date.
