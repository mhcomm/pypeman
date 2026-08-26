# [Changelog](https://github.com/mhcomm/pypeman/releases)
## unreleased
* CLI reworked: argparse + plugin architecture (`plugin_mgr`, `pypeman.plugins`).
  Commands: `start`, `graph`, `listplugins`, `printsettings`, `shell`.
  Removed: `stop`, `pyshell`, `debug`, `test`, `pytest`, daemon mode, `--reload`,
  `--remote-admin`, the `pypeman_tool` script (stop pypeman with Ctrl-C).
  Dropped dependencies: click, daemonlite, websockets, jsonrpcserver, jsonrpcclient,
  requests, ipython.
* `graph` now builds a structured representation of the channels first and gains
  `--format {ascii,dot,json}`; the JSON output describes channels, nodes, fork/when/case
  (with conditions) and the special end-node paths. `--special final` no longer crashes.
* startproject revived as a standalone `pypeman-startproject` script.
* Plugins can expose web endpoints through a shared web app (`BundledWebappPluginMixin`,
  configured with `settings.WEBAPP_PLUGINS_CONFIG`).
* New `HealthPlugin` (on by default): `GET /health` on the shared web app reports an
  overall ok/degraded status, version, process and event-loop info, and per-channel
  status, in-flight processing time, retry state, store count and last message/error
  times (`GET /health/channels/<name>` for a single channel). Configured with
  `HEALTH_CONFIG` (`url` prefix, `degraded_error_window`).
* New `MetricsPlugin` (on by default): `GET /metrics/channels[/<name>]` serves
  per-channel JSON stats (message/error counts, mean/min/max processing time), with
  optional `start_dt`/`end_dt` time-range stats computed from the message store metas;
  `GET /metrics` serves the Prometheus text format and `GET /metrics/live` the same
  live snapshot as JSON. Configured with `METRICS_CONFIG` (`url` prefix).
* `RetryFileMsgStore` tracks `retry_attempts` and `retry_mode_since`;
  `MessageStore.get_message_metas(start_dt, end_dt)` returns store metas over a time
  span without deserializing the messages;
  `MessageStore.update_message_meta_infos(id, infos)` writes several meta infos in one
  read/write cycle.
* Remote admin is served on the shared web app, under an optional URL prefix
  (`REMOTE_ADMIN_CONFIG["url"]`, empty by default: the API stays at the root);
  host/port come from `WEBAPP_PLUGINS_CONFIG`.
  `REMOTE_ADMIN_WEBSOCKET_CONFIG`/`REMOTE_ADMIN_WEB_CONFIG` are deprecated (only read,
  with a warning, when defined in the project settings). Legacy `pypeman.remoteadmin`
  module removed.
* Channels fire `events.msg_processing_start` / `events.msg_processing_end` around the
  processing of every message. Handlers (typically registered by a plugin from its
  `task_start`) may enrich `msg.meta` before the message store copy, and a raising
  handler is logged instead of breaking the channel (`Event.fire_safely`).
* New opt-in plugin `pypeman.plugins.proctime.ProcTimePlugin`, tagging every message
  with the time its channel took to process it (`msg.meta["process_time"]`, also added
  to the message store entry).
* FIX settings loading no longer fails when the project leaves `RETRY_STORE_PATH = None`.
* FIX remote admin ws RPC rejected every call with parameters; `view_msg` (and the
  `/view` + `/preview` routes) crashed; `shell` host/port arguments were ignored and
  several shell outputs were wrong.

## [0.6.6](https://github.com/mhcomm/pypeman/compare/0.6.5...0.6.6)
* FileWriter node: Don't raise "group not exist" error at startup

## [0.6.5](https://github.com/mhcomm/pypeman/compare/0.6.4...0.6.5)
* HttpRequest node: Add callable headers + FIX add_meta param
* FileWriter node: add grp change + encoding

## [0.6.4](https://github.com/mhcomm/pypeman/compare/0.6.3...0.6.4)
* FIX suchan message store exception

## [0.6.3](https://github.com/mhcomm/pypeman/compare/0.6.2...0.6.3)
* FIX replay and inject in subchannels + lil tests cleanup

## [0.6.2](https://github.com/mhcomm/pypeman/compare/0.6.1...0.6.2)
* Add encoding param to Filewatcher
* HttpRequest: Add possibility to get the url from msg.meta

## [0.6.1](https://github.com/mhcomm/pypeman/compare/0.6.0...0.6.1)
* Add country codes and encoding in hl7 ack
* Use store-related meta, not message meta to create msgstore's message meta key
* Add cookies in Http Request out meta if add_meta param is set
* FIXS:
* - Fix search in filemessagestore that sometimes crash for old messages
* - fix bug : Subchannels always set message state to processed even if it have to be in other state
* - fix bug: If multiples messages are passed to a same subchannel, the subchannel doesn't wait the end of firsts messages before processing the others, it run them all in prallel (as the subchannel doesn't await its process(), but create an asyncio task)
* - FIX: Basenode.__str__ not working if no chan
* - Correcting HTTP channel status return in msg.meta

## [0.6.0](https://github.com/mhcomm/pypeman/compare/0.5.10...0.6.0)
* Add BaseChannel.inject method
* Refactorisation of node calls (node calls are on BaseChannel now instead of being in BaseNode)
* Store and search for meta infos
* Refactorisation of init|join|drop|reject|fail|final nodes call into a single function named call_special_nodes
* Add AutoRetry feature to nodes (+ add RetryFileMessageStore)
* Breaking Changes:
* - Now conditional subchannel raise an EndIteration at the end instead of returning the msg
* Know bugs:
* - The change state doesn't work with conditional subchannels that have a message store (the base message remains in wait_retry state after a successful retry)
* - end nodes are not called for the first message put in the retry store
* - Different behaviour with errors in join nodes between handle and inject (if error occur in join nodes in handle, fail nodes are called, in inject it's not the case)

## [0.5.10](https://github.com/mhcomm/pypeman/compare/0.5.9...0.5.10)
* Fix Filewatcher logging problem
* PypemanTestCase.get_channel now call channels.get_channel

## [0.5.9](https://github.com/mhcomm/pypeman/compare/0.5.8...0.5.9)
* add init nodes that are called before channel subhandle
* Add possibility to extend end nodes
* add possibility to search channels by short_name 
* Add end node search (in chan.get_node)
* msgstore: add start_id param instead of start to have optimized filtering
* msgstore: list channels API now returns only chans with a msgstore
* HttpRequestNode: new add_meta param to add headers in out msg meta
* HttpChannel: Add possibility to return other types than string in the response
* Fix duplicate err logging
* New nodes:
* - MsgFuncNode
* - FuncNode
* - UseMetaFromCtx
* - EmptyMeta
* - Reject

## [0.5.8](https://github.com/mhcomm/pypeman/compare/0.5.7...0.5.8)
* Persistence: add search_ids_by_value and get_num_entries functions
* Http Plugin: list message view default order reversed (-timestamp)

## [0.5.7](https://github.com/mhcomm/pypeman/compare/0.5.6...0.5.7)
* Add MergeChannel
* FileMessageStore: don't use path as id, only basename
* Message.to_dict: add a param to avoid pickling and encoding the payload

## [0.5.6](https://github.com/mhcomm/pypeman/compare/0.5.5...0.5.6)
* Fix backend loop argument for tests
* Robustify CI
* HttpChannel: add headers and msg.content_type meta data
* Add command to view and send from store
* HttpRequest node: Respect of env vars + add url meta info in out msg
* MsgStore: Add infos in msg meta
* Add a verbose name to channels

## [0.5.5](https://github.com/mhcomm/pypeman/compare/0.5.4...0.5.5)
* Fix store_output_as
* Fix remote admin

## [0.5.4](https://github.com/mhcomm/pypeman/compare/0.5.3...0.5.4)
* Change ReadTheDocs config file from v1 to v2
* FIX subchannel endnodes that was launched ever if the msg doesn't enter in the subchan
* HTTPRequest node: allow callable http params + nested dicts in url building
* First Version of the remote Admin Plugin
* Fix endnodes that have a modified msg

## [0.5.3](https://github.com/mhcomm/pypeman/compare/0.5.2...0.5.3)
* Fix HttpRequest: ssl_verify = False not working
* Fix Mllp and http speed responses not working
* Change some logging.exceptions to logging.error to avoid duplicate traceback
* HttpRequest: move parsing outside the handle_request func to simplify overwriting
* add log
* Add HL72Python encoding extension

## [0.5.2](https://github.com/mhcomm/pypeman/compare/0.5.1...0.5.2)
* Fix for nodes.HTTPRequest PATCH method that don't send anything
* rm ssl.PROTOCOL warning

## [0.5.1](https://github.com/mhcomm/pypeman/compare/0.5.0...0.5.1)
* FIX hl7 mllp endpoint + add mllp chan tests
* Add HttpRequestNode Json sending
* add BaseChannel,MLLPChannel, HTTPChannel and BaseNode logs
* FIX ftp channel tests that randomly fails due to don't wait end chann process before assert
* load_graph func can now reload project if already imported
* Fix socket endpoint address already in use + rm code covering check
* dependabot fixes for node dependencies


## [0.5.0](https://github.com/mhcomm/pypeman/compare/0.4.1...0.5.0)
* remove python 3.6 from supported versions
* Add python 3.9 and python 3.10 compatibility
* Add join/drop/reject/final nodes to lauch at end of channels
* Add a wait_subchans param to BaseChannel to wait for subchannels and bring up their Exceptions to main channel
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

