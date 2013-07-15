logbase
=======

This package provides the common library for creating and managing logbases.  Inspired by [Bitcask](http://docs.basho.com/riak/1.2.0/tutorials/choosing-a-backend/Bitcask/), Logbase is just a key-value store that focusses on getting data to disk quickly by simply creating "logs" of new or changed key-value pairs, and providing a backend service to tidy up and minimise disk usage.  The main idea is to keep an index of the entire database within RAM.

I wrote Logbase as a first stab at learning Go from a C/Java background.  I just think large, complex, highly configurable languages and code make creating software too much of a chore.  I love the idea of lightweight, fairly narrowly focussed packages with intuitive APIs that developers can use to customise their own solutions.  I'm using Logbase to build a graph database layer, for example.

## Installation

```bash
$ go get github.com/h00gs/logbase
```

## FinishMe

## License

Package logbase is licensed under the WTFPL.
