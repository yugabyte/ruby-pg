# -*- ruby -*-

require 'ysql'

conn = YSQL.connect(:dbname => 'test' )
$stderr.puts '---',
             RUBY_DESCRIPTION,
             YSQL.version_string(true ),
             "Server version: #{conn.server_version}",
             "Client version: #{YSQL.library_version}",
             '---'

result = conn.exec( "SELECT * from pg_stat_activity" )

$stderr.puts %Q{Expected this to return: ["select * from pg_stat_activity"]}
p result.field_values( 'current_query' )

