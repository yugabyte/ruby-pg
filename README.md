# ysql

This is a fork of [Ruby interface to the PostgreSQL RDBMS](https://github.com/ged/ruby-pg) to develop a Ruby interface to YugabyteDB.

## Features

This driver has the following features in addition to those that come with the upstream driver:

### Cluster Awareness to eliminate need for a load balancer

This driver requires only an initial _contact point_ for the YugabyteDB cluster, using which it discovers the rest of the nodes. Additionally, it automatically learns about the nodes being started/added or stopped/removed. Internally the driver keeps track of number of connections it has created to each server endpoint and every new connection request is connected to the least loaded server as per the driver's view.

### Topology Awareness to enable geo-distributed apps

This is similar to 'Cluster Awareness' but uses those servers which are part of a given set of geo-locations specified by _topology_keys_.

### Connection Properties added for load balancing

- _load_balance_ - Starting with version 0.6, it expects one of **false, any (same as true), only-primary, only-rr, prefer-primary and prefer-rr** as its possible values. The default value for _load_balance_ property is `false`.
  - _false_ - No connection load balancing. Behaviour is similar to vanilla ruby-pg driver
  - _any_ - Same as value _true_. Distribute connections equally across all nodes in the cluster, irrespective of its type (`primary` or `read-replica`)
  - _only-primary_ - Create connections equally across only the primary nodes of the cluster
  - _only-rr_ - Create connections equally across only the read-replica nodes of the cluster
  - _prefer-primary_ - Create connections equally across primary cluster nodes. If none available, on any available read replica node in the cluster
  - _prefer-rr_ - Create connections equally across read replica nodes of the cluster. If none available, on any available primary cluster node
- _topology_keys_  - It takes a comma separated geo-location values. A single geo-location can be given as 'cloud.region.zone'. Multiple geo-locations too can be specified, separated by comma (`,`). Optionally, you can also register your preference for particular geo-locations by appending the preference value with prefix `:`. For example, `cloud.regionA.zoneA:1,cloud.regionA.zoneB:2`.
- _yb_servers_refresh_interval_ - Minimum time interval, in seconds, between two attempts to refresh the information about cluster nodes. This is checked only when a new connection is requested. Default is 300. Valid values are integers between 0 and 600. Value 0 means refresh for each connection request. Any value outside this range is ignored and the default is used.
- _fallback_to_topology_keys_only_ - When set to true, the driver does not attempt to connect to nodes outside of the geo-locations specified via _topology_keys_. Default value is false.
- _failed_host_reconnect_delay_secs_ - The time interval for which the driver ignores a failed node even if it shows up in refreshed metadata from `yb_servers()` function. Default value is 5 seconds.

Please refer to the [Use the Driver](#Use the Driver) section for examples.

## Install the Driver

```shell
gem install yugabytedb-ysql -- --with-pg-config=<yugabyte-install-dir>/postgres/bin/pg_config
```

## Use the Driver

- Passing new connection properties for load balancing in connection url

  For uniform load balancing across all the server you just need to specify the _load_balance=true_ property in the url.
    ```
    require 'ysql'
    ...
    yburl = "postgresql://yugabyte:yugabyte@127.0.0.1:5433/yugabyte?load_balance=true"
    connection = YSQL.connect(url)
    ...
    ```

  For specifying topology keys you need to set the additional property with a valid comma separated value.

    ```
    require 'ysql'
    ...
    yburl = "postgresql://yugabyte:yugabyte@127.0.0.1:5433/yugabyte?load_balance=true&topology_keys=cloud.regionA.zoneA,cloud.regionA.zoneB"
    connection = YSQL.connect(url)
    ...
    ```

  Alternatively, you could also specify the properties as key, value pairs as shown below.

    ```
    connection = YSQL.connect(host: 'localhost', port: '5433', dbname: 'yugabyte',
                                      user: 'yugabyte', password: 'yugabyte',
                                      load_balance: 'true', yb_servers_refresh_interval: '10')
    ```

### Specifying fallback zones

For topology-aware load balancing, you can specify fallback placements too. This is not applicable for cluster-aware load balancing.
Each placement value can be suffixed with a colon (`:`) followed by a preference value between 1 and 10.
A preference value of `:1` means it is a primary placement. A preference value of `:2` means it is the first fallback placement and so on.
If no preference value is provided, it is considered to be a primary placement (equivalent to one with preference value `:1`). Example given below.

```
yburl = "postgresql://yugabyte:yugabyte@127.0.0.1:5433/yugabyte?load_balance=true&topology_keys=cloud.regionA.zoneA:1,cloud.regionA.zoneB:2"

```

You can also use `*` for specifying all the zones in a given region as shown below. This is not allowed for cloud or region values.

```
yburl = "postgresql://yugabyte:yugabyte@127.0.0.1:5433/yugabyte?load_balance=true&topology_keys=cloud.regionA.*:1,cloud.regionB.*:2";
```

The driver attempts connection to servers in the first fallback placement(s) if it does not find any servers available in the primary placement(s). If no servers are available in the first fallback placement(s),
then it attempts to connect to servers in the second fallback placement(s), if specified. This continues until the driver finds a server to connect to, else an error is returned to the application.
And this repeats for each connection request.

### Using with ActiveRecord

- The load balancing feature of the Ruby Smart driver for YugabyteDB can be used with ActiveRecord - the ORM tool for Ruby apps - via its [adapter for YugabyteDB](https://github.com/yugabyte/activerecord-yugabytedb-adapter).

Rest of the README is from upstream repository.

---

# pg

* home :: https://github.com/ged/ruby-pg
* docs :: http://deveiate.org/code/pg (English) ,
          https://deveiate.org/code/pg/README_ja_md.html (Japanese)
* clog :: link:/History.md

[![Join the chat at https://gitter.im/ged/ruby-pg](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/ged/ruby-pg?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


## Description

Pg is the Ruby interface to the [PostgreSQL RDBMS](http://www.postgresql.org/).
It works with [PostgreSQL 9.3 and later](http://www.postgresql.org/support/versioning/).

A small example usage:

```ruby
  #!/usr/bin/env ruby

require 'pg'

# Output a table of current connections to the DB
conn = YSQL.connect(dbname: 'sales')
conn.exec("SELECT * FROM pg_stat_activity") do |result|
  puts "     PID | User             | Query"
  result.each do |row|
    puts " %7d | %-16s | %s " %
                 row.values_at('pid', 'usename', 'query')
  end
end
```

## Build Status

[![Build Status Github Actions](https://github.com/ged/ruby-pg/actions/workflows/source-gem.yml/badge.svg?branch=master)](https://github.com/ged/ruby-pg/actions/workflows/source-gem.yml)
[![Binary gems](https://github.com/ged/ruby-pg/actions/workflows/binary-gems.yml/badge.svg?branch=master)](https://github.com/ged/ruby-pg/actions/workflows/binary-gems.yml)
[![Build Status Appveyor](https://ci.appveyor.com/api/projects/status/gjx5axouf3b1wicp?svg=true)](https://ci.appveyor.com/project/ged/ruby-pg-9j8l3)


## Requirements

* Ruby 2.5 or newer
* PostgreSQL 9.3.x or later (with headers, -dev packages, etc).

It usually works with earlier versions of Ruby/PostgreSQL as well, but those are
not regularly tested.


## Versioning

We tag and release gems according to the [Semantic Versioning](http://semver.org/) principle.

As a result of this policy, you can (and should) specify a dependency on this gem using the [Pessimistic Version Constraint](http://guides.rubygems.org/patterns/#pessimistic-version-constraint) with two digits of precision.

For example:

```ruby
  spec.add_dependency 'pg', '~> 1.0'
```

## How To Install

Install via RubyGems:

    gem install pg

You may need to specify the path to the 'pg_config' program installed with
Postgres:

    gem install pg -- --with-pg-config=<path to pg_config>

If you're installing via Bundler, you can provide compile hints like so:

    bundle config build.pg --with-pg-config=<path to pg_config>

See README-OS_X.rdoc for more information about installing under MacOS X, and
README-Windows.rdoc for Windows build/installation instructions.

There's also [a Google+ group](http://goo.gl/TFy1U) and a
[mailing list](http://groups.google.com/group/ruby-pg) if you get stuck, or just
want to chat about something.

If you want to install as a signed gem, the public certs of the gem signers
can be found in [the `certs` directory](https://github.com/ged/ruby-pg/tree/master/certs)
of the repository.


## Type Casts

Pg can optionally type cast result values and query parameters in Ruby or
native C code. This can speed up data transfers to and from the database,
because String allocations are reduced and conversions in (slower) Ruby code
can be omitted.

Very basic type casting can be enabled by:

```ruby
    conn.type_map_for_results = YSQL::BasicTypeMapForResults.new conn
# ... this works for result value mapping:
conn.exec("select 1, now(), '{2,3}'::int[]").values
# => [[1, 2014-09-21 20:51:56 +0200, [2, 3]]]

conn.type_map_for_queries = YSQL::BasicTypeMapForQueries.new conn
# ... and this for param value mapping:
conn.exec_params("SELECT $1::text, $2::text, $3::text", [1, 1.23, [2, 3]]).values
# => [["1", "1.2300000000000000E+00", "{2,3}"]]
```

But Pg's type casting is highly customizable. That's why it's divided into
2 layers:

### Encoders / Decoders (ext/pg_*coder.c, lib/pg/*coder.rb)

This is the lower layer, containing encoding classes that convert Ruby
objects for transmission to the DBMS and decoding classes to convert
received data back to Ruby objects. The classes are namespaced according
to their format and direction in PG::TextEncoder, PG::TextDecoder,
PG::BinaryEncoder and PG::BinaryDecoder.

It is possible to assign a type OID, format code (text or binary) and
optionally a name to an encoder or decoder object. It's also possible
to build composite types by assigning an element encoder/decoder.
PG::Coder objects can be used to set up a PG::TypeMap or alternatively
to convert single values to/from their string representation.

The following PostgreSQL column types are supported by ruby-pg (TE = Text Encoder, TD = Text Decoder, BE = Binary Encoder, BD = Binary Decoder):

* Integer: [TE](rdoc-ref:PG::TextEncoder::Integer), [TD](rdoc-ref:PG::TextDecoder::Integer), [BD](rdoc-ref:PG::BinaryDecoder::Integer) 💡 No links? Switch to [here](https://deveiate.org/code/pg/README_md.html#label-Type+Casts) 💡
    * BE: [Int2](rdoc-ref:PG::BinaryEncoder::Int2), [Int4](rdoc-ref:PG::BinaryEncoder::Int4), [Int8](rdoc-ref:PG::BinaryEncoder::Int8)
* Float: [TE](rdoc-ref:PG::TextEncoder::Float), [TD](rdoc-ref:PG::TextDecoder::Float), [BD](rdoc-ref:PG::BinaryDecoder::Float)
    * BE: [Float4](rdoc-ref:PG::BinaryEncoder::Float4), [Float8](rdoc-ref:PG::BinaryEncoder::Float8)
* Numeric: [TE](rdoc-ref:PG::TextEncoder::Numeric), [TD](rdoc-ref:PG::TextDecoder::Numeric)
* Boolean: [TE](rdoc-ref:PG::TextEncoder::Boolean), [TD](rdoc-ref:PG::TextDecoder::Boolean), [BE](rdoc-ref:PG::BinaryEncoder::Boolean), [BD](rdoc-ref:PG::BinaryDecoder::Boolean)
* String: [TE](rdoc-ref:PG::TextEncoder::String), [TD](rdoc-ref:PG::TextDecoder::String), [BE](rdoc-ref:PG::BinaryEncoder::String), [BD](rdoc-ref:PG::BinaryDecoder::String)
* Bytea: [TE](rdoc-ref:PG::TextEncoder::Bytea), [TD](rdoc-ref:PG::TextDecoder::Bytea), [BE](rdoc-ref:PG::BinaryEncoder::Bytea), [BD](rdoc-ref:PG::BinaryDecoder::Bytea)
* Base64: [TE](rdoc-ref:PG::TextEncoder::ToBase64), [TD](rdoc-ref:PG::TextDecoder::FromBase64), [BE](rdoc-ref:PG::BinaryEncoder::FromBase64), [BD](rdoc-ref:PG::BinaryDecoder::ToBase64)
* Timestamp:
    * TE: [local](rdoc-ref:PG::TextEncoder::TimestampWithoutTimeZone), [UTC](rdoc-ref:PG::TextEncoder::TimestampUtc), [with-TZ](rdoc-ref:PG::TextEncoder::TimestampWithTimeZone)
    * TD: [local](rdoc-ref:PG::TextDecoder::TimestampLocal), [UTC](rdoc-ref:PG::TextDecoder::TimestampUtc), [UTC-to-local](rdoc-ref:PG::TextDecoder::TimestampUtcToLocal)
    * BE: [local](rdoc-ref:PG::BinaryEncoder::TimestampLocal), [UTC](rdoc-ref:PG::BinaryEncoder::TimestampUtc)
    * BD: [local](rdoc-ref:PG::BinaryDecoder::TimestampLocal), [UTC](rdoc-ref:PG::BinaryDecoder::TimestampUtc), [UTC-to-local](rdoc-ref:PG::BinaryDecoder::TimestampUtcToLocal)
* Date: [TE](rdoc-ref:PG::TextEncoder::Date), [TD](rdoc-ref:PG::TextDecoder::Date), [BE](rdoc-ref:PG::BinaryEncoder::Date), [BD](rdoc-ref:PG::BinaryDecoder::Date)
* JSON and JSONB: [TE](rdoc-ref:PG::TextEncoder::JSON), [TD](rdoc-ref:PG::TextDecoder::JSON)
* Inet: [TE](rdoc-ref:PG::TextEncoder::Inet), [TD](rdoc-ref:PG::TextDecoder::Inet)
* Array: [TE](rdoc-ref:PG::TextEncoder::Array), [TD](rdoc-ref:PG::TextDecoder::Array)
* Composite Type (also called "Row" or "Record"): [TE](rdoc-ref:PG::TextEncoder::Record), [TD](rdoc-ref:PG::TextDecoder::Record)

The following text and binary formats can also be encoded although they are not used as column type:

* COPY input and output data: [TE](rdoc-ref:PG::TextEncoder::CopyRow), [TD](rdoc-ref:PG::TextDecoder::CopyRow), [BE](rdoc-ref:PG::BinaryEncoder::CopyRow), [BD](rdoc-ref:PG::BinaryDecoder::CopyRow)
* Literal for insertion into SQL string: [TE](rdoc-ref:PG::TextEncoder::QuotedLiteral)
* SQL-Identifier: [TE](rdoc-ref:PG::TextEncoder::Identifier), [TD](rdoc-ref:PG::TextDecoder::Identifier)

### PG::TypeMap and derivations (ext/pg_type_map*.c, lib/pg/type_map*.rb)

A TypeMap defines which value will be converted by which encoder/decoder.
There are different type map strategies, implemented by several derivations
of this class. They can be chosen and configured according to the particular
needs for type casting. The default type map is PG::TypeMapAllStrings.

A type map can be assigned per connection or per query respectively per
result set. Type maps can also be used for COPY in and out data streaming.
See PG::Connection#copy_data .

The following base type maps are available:

* PG::TypeMapAllStrings - encodes and decodes all values to and from strings (default)
* PG::TypeMapByClass - selects encoder based on the class of the value to be sent
* PG::TypeMapByColumn - selects encoder and decoder by column order
* PG::TypeMapByOid - selects decoder by PostgreSQL type OID
* PG::TypeMapInRuby - define a custom type map in ruby

The following type maps are prefilled with type mappings from the PG::BasicTypeRegistry :

* PG::BasicTypeMapForResults - a PG::TypeMapByOid prefilled with decoders for common PostgreSQL column types
* PG::BasicTypeMapBasedOnResult - a PG::TypeMapByOid prefilled with encoders for common PostgreSQL column types
* PG::BasicTypeMapForQueries - a PG::TypeMapByClass prefilled with encoders for common Ruby value classes


## Thread support

PG is thread safe in such a way that different threads can use different PG::Connection objects concurrently.
However it is not safe to access any Pg objects simultaneously from more than one thread.
So make sure to open a new database server connection for every new thread or use a wrapper library like ActiveRecord that manages connections in a thread safe way.

If messages like the following are printed to stderr, you're probably using one connection from several threads:

    message type 0x31 arrived from server while idle
    message type 0x32 arrived from server while idle
    message type 0x54 arrived from server while idle
    message type 0x43 arrived from server while idle
    message type 0x5a arrived from server while idle


## Fiber IO scheduler support

Pg is fully compatible with `Fiber.scheduler` introduced in Ruby-3.0 since pg-1.3.0.
On Windows support for `Fiber.scheduler` is available on Ruby-3.1 or newer.
All possibly blocking IO operations are routed through the `Fiber.scheduler` if one is registered for the running thread.
That is why pg internally uses the asynchronous libpq interface even for synchronous/blocking method calls.
It also uses Ruby's DNS resolution instead of libpq's builtin functions.

Internally Pg always uses the nonblocking connection mode of libpq.
It then behaves like running in blocking mode but ensures, that all blocking IO is handled in Ruby through a possibly registered `Fiber.scheduler`.
When `PG::Connection.setnonblocking(true)` is called then the nonblocking state stays enabled, but the additional handling of blocking states is disabled, so that the calling program has to handle blocking states on its own.

An exception to this rule are the methods for large objects like `PG::Connection#lo_create` and authentication methods using external libraries (like GSSAPI authentication).
They are not compatible with `Fiber.scheduler`, so that blocking states are not passed to the registered IO scheduler.
That means the operation will work properly, but IO waiting states can not be used to switch to another Fiber doing IO.


## Ractor support

Pg is fully compatible with Ractor introduced in Ruby-3.0 since pg-1.5.0.
All type en/decoders and type maps are shareable between ractors if they are made frozen by `Ractor.make_shareable`.
Also frozen PG::Result and PG::Tuple objects can be shared.
All frozen objects (except PG::Connection) can still be used to do communication with the PostgreSQL server or to read retrieved data.

PG::Connection is not shareable and must be created within each Ractor to establish a dedicated connection.


## Contributing

To report bugs, suggest features, or check out the source with Git,
[check out the project page](https://github.com/ged/ruby-pg).

After checking out the source, install all dependencies:

    $ bundle install

Cleanup extension files, packaging files, test databases.
Run this to change between PostgreSQL versions:

    $ rake clean

Compile extension:

    $ rake compile

Run tests/specs on the PostgreSQL version that `pg_config --bindir` points to:

    $ rake test

Or run a specific test per file and line number on a specific PostgreSQL version:

    $ PATH=/usr/lib/postgresql/14/bin:$PATH rspec -Ilib -fd spec/pg/connection_spec.rb:455

Generate the API documentation:

    $ rake docs

Make sure, that all bugs and new features are verified by tests.

The current maintainers are Michael Granger <ged@FaerieMUD.org> and
Lars Kanis <lars@greiz-reinsdorf.de>.


## Copying

Copyright (c) 1997-2022 by the authors.

* Jeff Davis <ruby-pg@j-davis.com>
* Guy Decoux (ts) <decoux@moulon.inra.fr>
* Michael Granger <ged@FaerieMUD.org>
* Lars Kanis <lars@greiz-reinsdorf.de>
* Dave Lee
* Eiji Matsumoto <usagi@ruby.club.or.jp>
* Yukihiro Matsumoto <matz@ruby-lang.org>
* Noboru Saitou <noborus@netlab.jp>

You may redistribute this software under the same terms as Ruby itself; see
https://www.ruby-lang.org/en/about/license.txt or the BSDL file in the source
for details.

Portions of the code are from the PostgreSQL project, and are distributed
under the terms of the PostgreSQL license, included in the file POSTGRES.

Portions copyright LAIKA, Inc.


## Acknowledgments

See Contributors.rdoc for the many additional fine people that have contributed
to this library over the years.

We are thankful to the people at the ruby-list and ruby-dev mailing lists.
And to the people who developed PostgreSQL.
