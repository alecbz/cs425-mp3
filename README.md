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


## Description of Algorithm

