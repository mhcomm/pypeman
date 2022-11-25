# [Changelog](https://github.com/mhcomm/pypeman/releases)

## [0.5.0](https://github.com/mhcomm/pypeman/compare/0.4.1...0.5.0)
* remove python 3.6 from supported versions
* Add python 3.9 and python 3.10 compatibility
* Exceptions in subchannels are now brought up in main channel
* New Nodes:
* - FileCleaner
* - FileMover
* - CSV2Python
* - Python2CSVstr
* - CSVstr2Python
* - YielderNode
* Nodes and Channels Improvements:
* - FileWriter Node has a param `create_valid_file` to create acknowledgment file
* - Add cookies in HttpRequestNode
* - Add a `real_extensions` param to FileWatcherChannel to permits to convert filename from acknowledgement file and open associated file
* - Add `binary` param to HttpRequestNode to handle binary requests
* - Add `json` param to HttpRequestNode to convert responses
* RemoteAdmin:
* - Add `view` and `preview` commands 
* - Add date filters in search command
* - Search command can now search a regex

## [0.4.1](https://github.com/mhcomm/pypeman/compare/0.4.0...0.4.1)
* cleanup: rmv bad logs (too verbose combine + file watcher logs)
* change repr string for channels
* fix graph command (sub nodes were missing)
* add print_graph command to cli_mode
* allow None nodes and nested lists for channel.append()
## [0.4.0](https://github.com/mhcomm/pypeman/compare/0.3.5...0.4.0)
* remove python 3.5 from supported versions.
* create a node to combine contexts
* some refactoring for better reuse and a sample project
* some more refactoring and cleanup mv code out of commands, comments, rmv py2 code
* add a first version of a plugin manager. (requires py3.6)

## [0.3.5](https://github.com/mhcomm/pypeman/compare/0.3.4...0.3.5)
* fix regression #133 (pypeman pytest regression for additional args)
* fix issue with aiohttp (with -> async with)
* fix #135 (pypeman pytest has exitcode !=0 on errors)
* first implementation of #129 (printsettings)

## [0.3.4](https://github.com/mhcomm/pypeman/compare/0.3.3...0.3.4)
* http channels have now get params and match info from the urls in their meta
* new settings.PROJECT_MODULE var (allows to override default mod name if desired)
* add signal handlers + minor cleanup
* ensure, that pytest errors result in an exitcode, so that CI aborts
* unit tests pass now also on 12th of each month
* got rid of prints (or converted them to logs)
* signal handlers updated for newer asyncio versions
* helper addeds for asyncio BW compatibility
* fix bug #55 (pytest issues) filewatcher sleeps can be interrupted
* fix bug #72 (node name uniqueness)
* tests running now with newer python versions
* replace begins with click
* improve https client cert errors

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
* Fix filewatcher, don't silence all exceptions
* Allows remote admin via reverse proxy
* Fix http endpoint setup route
* Redirect '/' to 'index.html' for web admin
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
* Nodes have now persistent context between executions
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
* Better lazyloading of optional contribs
* Better documentation
* Better naming consistency

## [0.0.1a1](https://github.com/mhcomm/pypeman/compare/0.0.1a1...0.0.1a1)

* First version

