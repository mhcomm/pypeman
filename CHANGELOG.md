# [Changelog](https://github.com/mhcomm/pypeman/releases)

## [0.3.3](https://github.com/mhcomm/pypeman/compare/0.3.2...0.3.3)

* fix unit tests
* satisfy new flake version
* can add params for test
* pytest for pypeman tests
* version in setup.py
* fix mllp_endpoint
* switch to pytest coverage

## [0.3.2](https://github.com/mhcomm/pypeman/compare/0.3.1...0.3.2)

* Socket endpoint (use for HTTP and MLLP)
* Reuse port option
* Fix filewatcher, dont silence all exceptions
* Allows remote admin via reverse proxy
* Fix http enpoint setup route
* Redirect / to index.html for web admin
* Pytest
* Testing uses free tcp port
* Freeze version of jsonrpcclient, jsonrpcserver, websocket
* Flake8

## [0.3.1](https://github.com/mhcomm/pypeman/compare/0.3.0...0.3.1)

* Fix HTTPchannel fail without http_args

## [0.3.0](https://github.com/mhcomm/pypeman/compare/0.2.0...0.3.0)

* Add remote admin throught websocket
* Add shell and pyshell remote admin client
* Add alpha version of webclient
* Enhance HTTP request node to allow POST
* Node have now persistent context between executions
* Fix loop cleaning between each test
* Fix error on drop node with generator
* Fix broken HTTP endpoints with more than one channel

## [0.2.0](https://github.com/mhcomm/pypeman/compare/0.1.0...0.2.0)

* Migrate to python 3.5 syntax and stop py34 compatibility
* Test over py35 and py36
* Rewrite FileReader for better naming
* Improved slow loop testing
* Json node has indent parameter

## [0.1.0](https://github.com/mhcomm/pypeman/compare/0.1.0...0.0.1a1)

* Add new "test" command to test your channels
* Add case channel construction
* Add node option to log output
* Add option to report slow tasks
* Add FTP channel/nodes
* Add Email node
* Add Base 64 node
* Add sleep node
* Ensure message order in channel processing
* Code organization refactoring
* Add some action in channel
* Add Message Store
* Better lazyloading of optional contrib
* Better documentation
* Better naming consistency

## [0.0.1a1](https://github.com/mhcomm/pypeman/compare/0.0.1a1...0.0.1a1)

* First version

