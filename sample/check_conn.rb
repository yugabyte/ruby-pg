# -*- ruby -*-
# vim: set nosta noet ts=4 sw=4:
# encoding: utf-8

require 'ysql'

# This is a minimal example of a function that can test an existing PG::Connection and
# reset it if necessary.

def check_connection( conn )
	begin
		conn.exec( "SELECT 1" )
	rescue YugabyteYSQL::Error => err
		$stderr.puts "%p while testing connection: %s" % [ err.class, err.message ]
		conn.reset
	end
end

conn = YugabyteYSQL.connect(dbname: 'test' )
check_connection( conn )

