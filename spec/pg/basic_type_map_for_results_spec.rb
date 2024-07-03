# -*- rspec -*-
# encoding: utf-8

require_relative '../helpers'

require 'time'

describe 'Basic type mapping' do
	describe YSQL::BasicTypeMapForResults do
		let!(:basic_type_mapping) do
			YSQL::BasicTypeMapForResults.new(@conn).freeze
		end

		it "can be initialized with a CoderMapsBundle instead of a connection" do
			maps = YSQL::BasicTypeRegistry::CoderMapsBundle.new(@conn).freeze
			tm = YSQL::BasicTypeMapForResults.new(maps)
			expect( tm.rm_coder(0, 16) ).to be_kind_of(YSQL::TextDecoder::Boolean)
		end

		it "can be initialized with a custom type registry" do
			regi = YSQL::BasicTypeRegistry.new
			regi.register_type 1, 'int4', nil, YSQL::BinaryDecoder::Integer
			tm = YSQL::BasicTypeMapForResults.new(@conn, registry: regi).freeze
			res = @conn.exec_params( "SELECT '2021-08-03'::DATE, 234", [], 1 ).map_types!(tm)
			expect{ res.values }.to output(/no type cast defined for type "date" format 1 /).to_stderr
			expect( res.values ).to eq( [["\x00\x00\x1E\xCD".b, 234]] )
		end

		it "should be shareable for Ractor", :ractor do
			Ractor.make_shareable(basic_type_mapping)
		end

		it "should be usable with Ractor and shared type map", :ractor do
			vals = Ractor.new(@conninfo, Ractor.make_shareable(basic_type_mapping)) do |conninfo, btm|
				conn = YSQL.connect(conninfo)
				res = conn.exec( "SELECT 1, 'a', 2.0::FLOAT, TRUE, '2013-06-30'::DATE" )
				res.map_types!(btm).values
			ensure
				conn&.finish
			end.take

			expect( vals ).to eq( [
					[ 1, 'a', 2.0, true, Date.new(2013,6,30) ],
			] )
		end

		it "should be usable with Ractor", :ractor do
			vals = Ractor.new(@conninfo) do |conninfo|
				conn = YSQL.connect(conninfo)
				res = conn.exec( "SELECT 1, 'a', 2.0::FLOAT, TRUE, '2013-06-30'::DATE" )
				btm = YSQL::BasicTypeMapForResults.new(conn)
				res.map_types!(btm).values
			ensure
				conn&.finish
			end.take

			expect( vals ).to eq( [
					[ 1, 'a', 2.0, true, Date.new(2013,6,30) ],
			] )
		end

		#
		# Decoding Examples
		#

		it "should do OID based type conversions" do
			res = @conn.exec( "SELECT 1, 'a', 2.0::FLOAT, TRUE, '2013-06-30'::DATE, generate_series(4,5)" )
			expect( res.map_types!(basic_type_mapping).values ).to eq( [
					[ 1, 'a', 2.0, true, Date.new(2013,6,30), 4 ],
					[ 1, 'a', 2.0, true, Date.new(2013,6,30), 5 ],
			] )
		end

		[1, 0].each do |format|
			it "should warn about undefined types in format #{format}" do
				regi = YSQL::BasicTypeRegistry.new.freeze
				tm = YSQL::BasicTypeMapForResults.new(@conn, registry: regi).freeze
				res = @conn.exec_params( "SELECT 1.23", [], format ).map_types!(tm)
				expect{ res.values }.to output(/type "numeric".*format #{format}.*oid 1700/).to_stderr
			end
		end

		#
		# Decoding Examples text+binary format converters
		#

		describe "connection wide type mapping" do
			before :each do
				@conn.type_map_for_results = basic_type_mapping
			end

			after :each do
				@conn.type_map_for_results = YSQL::TypeMapAllStrings.new.freeze
			end

			it "should do boolean type conversions" do
				[1, 0].each do |format|
					res = @conn.exec_params( "SELECT true::BOOLEAN, false::BOOLEAN, NULL::BOOLEAN", [], format )
					expect( res.values ).to eq( [[true, false, nil]] )
				end
			end

			it "should do binary type conversions" do
				[1, 0].each do |format|
					res = @conn.exec_params( "SELECT E'\\\\000\\\\377'::BYTEA", [], format )
					expect( res.values ).to eq( [[["00ff"].pack("H*")]] )
					expect( res.values[0][0].encoding ).to eq( Encoding::ASCII_8BIT )
				end
			end

			it "should do integer type conversions" do
				[1, 0].each do |format|
					res = @conn.exec_params( "SELECT -8999::INT2, -899999999::INT4, -8999999999999999999::INT8", [], format )
					expect( res.values ).to eq( [[-8999, -899999999, -8999999999999999999]] )
				end
			end

			it "should do string type conversions" do
				@conn.internal_encoding = 'utf-8'
				[1, 0].each do |format|
					res = @conn.exec_params( "SELECT 'abcäöü'::TEXT, 'colname'::name", [], format )
					expect( res.values ).to eq( [['abcäöü', 'colname']] )
					expect( [res.ftype(0), res.ftype(1)] ).to eq( [25, 19] )
					expect( res.values[0].map(&:encoding) ).to eq( [Encoding::UTF_8] * 2 )
				end
			end

			it "should do float type conversions" do
				[1, 0].each do |format|
					res = @conn.exec_params( "SELECT -8.999e3::FLOAT4,
														8.999e10::FLOAT4,
														-8999999999e-99::FLOAT8,
														NULL::FLOAT4,
														'NaN'::FLOAT4,
														'Infinity'::FLOAT4,
														'-Infinity'::FLOAT4
													", [], format )
					expect( res.getvalue(0,0) ).to be_within(1e-2).of(-8.999e3)
					expect( res.getvalue(0,1) ).to be_within(1e5).of(8.999e10)
					expect( res.getvalue(0,2) ).to be_within(1e-109).of(-8999999999e-99)
					expect( res.getvalue(0,3) ).to be_nil
					expect( res.getvalue(0,4) ).to be_nan
					expect( res.getvalue(0,5) ).to eq( Float::INFINITY )
					expect( res.getvalue(0,6) ).to eq( -Float::INFINITY )
				end
			end

			it "should do text datetime without time zone type conversions" do
				# for backward compat text timestamps without time zone are treated as local times
				res = @conn.exec_params( "SELECT CAST('2013-12-31 23:58:59+02' AS TIMESTAMP WITHOUT TIME ZONE),
																	CAST('1913-12-31 23:58:59.1231-03' AS TIMESTAMP WITHOUT TIME ZONE),
																	CAST('4714-11-24 23:58:59.1231-03 BC' AS TIMESTAMP WITHOUT TIME ZONE),
																	CAST('294276-12-31 23:58:59.1231-03' AS TIMESTAMP WITHOUT TIME ZONE),
																	CAST('infinity' AS TIMESTAMP WITHOUT TIME ZONE),
																	CAST('-infinity' AS TIMESTAMP WITHOUT TIME ZONE)", [], 0 )
				expect( res.getvalue(0,0) ).to eq( Time.new(2013, 12, 31, 23, 58, 59) )
				expect( res.getvalue(0,1).iso8601(3) ).to eq( Time.new(1913, 12, 31, 23, 58, 59.1231).iso8601(3) )
				expect( res.getvalue(0,2).iso8601(3) ).to eq( Time.new(-4713, 11, 24, 23, 58, 59.1231).iso8601(3) )
				expect( res.getvalue(0,3).iso8601(3) ).to eq( Time.new(294276, 12, 31, 23, 58, 59.1231).iso8601(3) )
				expect( res.getvalue(0,4) ).to eq( 'infinity' )
				expect( res.getvalue(0,5) ).to eq( '-infinity' )
			end

			[1, 0].each do |format|
				it "should convert format #{format} timestamps per TimestampUtc" do
					regi = YSQL::BasicTypeRegistry.new.register_default_types
					regi.register_type 0, 'timestamp', nil, YSQL::TextDecoder::TimestampUtc
					@conn.type_map_for_results = YSQL::BasicTypeMapForResults.new(@conn, registry: regi)
					res = @conn.exec_params( "SELECT CAST('2013-07-31 23:58:59+02' AS TIMESTAMP WITHOUT TIME ZONE),
																		CAST('1913-12-31 23:58:59.1231-03' AS TIMESTAMP WITHOUT TIME ZONE),
																		CAST('4714-11-24 23:58:59.1231-03 BC' AS TIMESTAMP WITHOUT TIME ZONE),
																		CAST('294276-12-31 23:58:59.1231-03' AS TIMESTAMP WITHOUT TIME ZONE),
																		CAST('infinity' AS TIMESTAMP WITHOUT TIME ZONE),
																		CAST('-infinity' AS TIMESTAMP WITHOUT TIME ZONE)", [], format )
					expect( res.getvalue(0,0).iso8601(3) ).to eq( Time.utc(2013, 7, 31, 23, 58, 59).iso8601(3) )
					expect( res.getvalue(0,1).iso8601(3) ).to eq( Time.utc(1913, 12, 31, 23, 58, 59.1231).iso8601(3) )
					expect( res.getvalue(0,2).iso8601(3) ).to eq( Time.utc(-4713, 11, 24, 23, 58, 59.1231).iso8601(3) )
					expect( res.getvalue(0,3).iso8601(3) ).to eq( Time.utc(294276, 12, 31, 23, 58, 59.1231).iso8601(3) )
					expect( res.getvalue(0,4) ).to eq( 'infinity' )
					expect( res.getvalue(0,5) ).to eq( '-infinity' )
				end
			end

			[1, 0].each do |format|
				it "should convert format #{format} timestamps per TimestampUtcToLocal" do
					regi = YSQL::BasicTypeRegistry.new
					regi.register_type 0, 'timestamp', nil, YSQL::TextDecoder::TimestampUtcToLocal
					regi.register_type 1, 'timestamp', nil, YSQL::BinaryDecoder::TimestampUtcToLocal
					@conn.type_map_for_results = YSQL::BasicTypeMapForResults.new(@conn, registry: regi)
					res = @conn.exec_params( "SELECT CAST('2013-07-31 23:58:59+02' AS TIMESTAMP WITHOUT TIME ZONE),
																		CAST('1913-12-31 23:58:59.1231-03' AS TIMESTAMP WITHOUT TIME ZONE),
																		CAST('4714-11-24 23:58:59.1231-03 BC' AS TIMESTAMP WITHOUT TIME ZONE),
																		CAST('294276-12-31 23:58:59.1231-03' AS TIMESTAMP WITHOUT TIME ZONE),
																		CAST('infinity' AS TIMESTAMP WITHOUT TIME ZONE),
																		CAST('-infinity' AS TIMESTAMP WITHOUT TIME ZONE)", [], format )
					expect( res.getvalue(0,0).iso8601(3) ).to eq( Time.utc(2013, 7, 31, 23, 58, 59).getlocal.iso8601(3) )
					expect( res.getvalue(0,1).iso8601(3) ).to eq( Time.utc(1913, 12, 31, 23, 58, 59.1231).getlocal.iso8601(3) )
					expect( res.getvalue(0,2).iso8601(3) ).to eq( Time.utc(-4713, 11, 24, 23, 58, 59.1231).getlocal.iso8601(3) )
					expect( res.getvalue(0,3).iso8601(3) ).to eq( Time.utc(294276, 12, 31, 23, 58, 59.1231).getlocal.iso8601(3) )
					expect( res.getvalue(0,4) ).to eq( 'infinity' )
					expect( res.getvalue(0,5) ).to eq( '-infinity' )
				end
			end

			[1, 0].each do |format|
				it "should convert format #{format} timestamps per TimestampLocal" do
					regi = YSQL::BasicTypeRegistry.new
					regi.register_type 0, 'timestamp', nil, YSQL::TextDecoder::TimestampLocal
					regi.register_type 1, 'timestamp', nil, YSQL::BinaryDecoder::TimestampLocal
					@conn.type_map_for_results = YSQL::BasicTypeMapForResults.new(@conn, registry: regi)
					res = @conn.exec_params( "SELECT CAST('2013-07-31 23:58:59' AS TIMESTAMP WITHOUT TIME ZONE),
																		CAST('1913-12-31 23:58:59.1231' AS TIMESTAMP WITHOUT TIME ZONE),
																		CAST('4714-11-24 23:58:59.1231-03 BC' AS TIMESTAMP WITHOUT TIME ZONE),
																		CAST('294276-12-31 23:58:59.1231+03' AS TIMESTAMP WITHOUT TIME ZONE),
																		CAST('infinity' AS TIMESTAMP WITHOUT TIME ZONE),
																		CAST('-infinity' AS TIMESTAMP WITHOUT TIME ZONE)", [], format )
					expect( res.getvalue(0,0).iso8601(3) ).to eq( Time.new(2013, 7, 31, 23, 58, 59).iso8601(3) )
					expect( res.getvalue(0,1).iso8601(3) ).to eq( Time.new(1913, 12, 31, 23, 58, 59.1231).iso8601(3) )
					expect( res.getvalue(0,2).iso8601(3) ).to eq( Time.new(-4713, 11, 24, 23, 58, 59.1231).iso8601(3) )
					expect( res.getvalue(0,3).iso8601(3) ).to eq( Time.new(294276, 12, 31, 23, 58, 59.1231).iso8601(3) )
					expect( res.getvalue(0,4) ).to eq( 'infinity' )
					expect( res.getvalue(0,5) ).to eq( '-infinity' )
				end
			end

			[0, 1].each do |format|
				it "should convert format #{format} timestamps with time zone" do
					res = @conn.exec_params( "SELECT CAST('2013-12-31 23:58:59+02' AS TIMESTAMP WITH TIME ZONE),
																		CAST('1913-12-31 23:58:59.1231-03' AS TIMESTAMP WITH TIME ZONE),
																		CAST('4714-11-24 23:58:59.1231-03 BC' AS TIMESTAMP WITH TIME ZONE),
																		CAST('294276-12-31 23:58:59.1231+03' AS TIMESTAMP WITH TIME ZONE),
																		CAST('infinity' AS TIMESTAMP WITH TIME ZONE),
																		CAST('-infinity' AS TIMESTAMP WITH TIME ZONE)", [], format )
					expect( res.getvalue(0,0) ).to be_within(1e-3).of( Time.new(2013, 12, 31, 23, 58, 59, "+02:00").getlocal )
					expect( res.getvalue(0,1) ).to be_within(1e-3).of( Time.new(1913, 12, 31, 23, 58, 59.1231, "-03:00").getlocal )
					expect( res.getvalue(0,2) ).to be_within(1e-3).of( Time.new(-4713, 11, 24, 23, 58, 59.1231, "-03:00").getlocal )
					expect( res.getvalue(0,3) ).to be_within(1e-3).of( Time.new(294276, 12, 31, 23, 58, 59.1231, "+03:00").getlocal )
					expect( res.getvalue(0,4) ).to eq( 'infinity' )
					expect( res.getvalue(0,5) ).to eq( '-infinity' )
				end
			end

			[0, 1].each do |format|
				it "should do format #{format} date type conversions" do
					res = @conn.exec_params( "SELECT CAST('2113-12-31' AS DATE),
																		CAST('1913-12-31' AS DATE),
																		CAST('infinity' AS DATE),
																		CAST('-infinity' AS DATE)", [], format )
					expect( res.getvalue(0,0) ).to eq( Date.new(2113, 12, 31) )
					expect( res.getvalue(0,1) ).to eq( Date.new(1913, 12, 31) )
					expect( res.getvalue(0,2) ).to eq( 'infinity' )
					expect( res.getvalue(0,3) ).to eq( '-infinity' )
				end
			end

			it "should do numeric type conversions", :bigdecimal do
				[0].each do |format|
					small = '123456790123.12'
					large = ('123456790'*10) << '.' << ('012345679')
					numerics = [
						'1',
						'1.0',
						'1.2',
						small,
						large,
					]
					sql_numerics = numerics.map { |v| "CAST(#{v} AS numeric)" }
					res = @conn.exec_params( "SELECT #{sql_numerics.join(',')}", [], format )
					expect( res.getvalue(0,0) ).to eq( BigDecimal('1') )
					expect( res.getvalue(0,1) ).to eq( BigDecimal('1') )
					expect( res.getvalue(0,2) ).to eq( BigDecimal('1.2') )
					expect( res.getvalue(0,3) ).to eq( BigDecimal(small) )
					expect( res.getvalue(0,4) ).to eq( BigDecimal(large) )
				end
			end

			it "should do JSON conversions", :postgresql_94 do
				[0].each do |format|
					['JSON', 'JSONB'].each do |type|
						res = @conn.exec_params( "SELECT CAST('123' AS #{type}),
																			CAST('12.3' AS #{type}),
																			CAST('true' AS #{type}),
																			CAST('false' AS #{type}),
																			CAST('null' AS #{type}),
																			CAST('[1, \"a\", null]' AS #{type}),
																			CAST('{\"b\" : [2,3]}' AS #{type})", [], format )
						expect( res.getvalue(0,0) ).to eq( 123 )
						expect( res.getvalue(0,1) ).to be_within(0.1).of( 12.3 )
						expect( res.getvalue(0,2) ).to eq( true )
						expect( res.getvalue(0,3) ).to eq( false )
						expect( res.getvalue(0,4) ).to eq( nil )
						expect( res.getvalue(0,5) ).to eq( [1, "a", nil] )
						expect( res.getvalue(0,6) ).to eq( {"b" => [2, 3]} )
					end
				end
			end

			it "should do array type conversions" do
				[0].each do |format|
					res = @conn.exec_params( "SELECT CAST('{1,2,3}' AS INT2[]), CAST('{{1,2},{3,4}}' AS INT2[][]),
															CAST('{1,2,3}' AS INT4[]),
															CAST('{1,2,3}' AS INT8[]),
															CAST('{1,2,3}' AS TEXT[]),
															CAST('{1,2,3}' AS VARCHAR[]),
															CAST('{1,2,3}' AS FLOAT4[]),
															CAST('{1,2,3}' AS FLOAT8[])
														", [], format )
					expect( res.getvalue(0,0) ).to eq( [1,2,3] )
					expect( res.getvalue(0,1) ).to eq( [[1,2],[3,4]] )
					expect( res.getvalue(0,2) ).to eq( [1,2,3] )
					expect( res.getvalue(0,3) ).to eq( [1,2,3] )
					expect( res.getvalue(0,4) ).to eq( ['1','2','3'] )
					expect( res.getvalue(0,5) ).to eq( ['1','2','3'] )
					expect( res.getvalue(0,6) ).to eq( [1.0,2.0,3.0] )
					expect( res.getvalue(0,7) ).to eq( [1.0,2.0,3.0] )
				end
			end

			it "should do inet type conversions" do
				[0].each do |format|
					vals = [
						'1.2.3.4',
						'0.0.0.0/0',
						'1.0.0.0/8',
						'1.2.0.0/16',
						'1.2.3.0/24',
						'1.2.3.4/24',
						'1.2.3.4/32',
						'1.2.3.128/25',
						'1234:3456:5678:789a:9abc:bced:edf0:f012',
						'::/0',
						'1234:3456::/32',
						'1234:3456:5678:789a::/64',
						'1234:3456:5678:789a:9abc:bced::/96',
						'1234:3456:5678:789a:9abc:bced:edf0:f012/128',
						'1234:3456:5678:789a:9abc:bced:edf0:f012/0',
						'1234:3456:5678:789a:9abc:bced:edf0:f012/32',
						'1234:3456:5678:789a:9abc:bced:edf0:f012/64',
						'1234:3456:5678:789a:9abc:bced:edf0:f012/96',
					]
					sql_vals = vals.map{|v| "CAST('#{v}' AS inet)"}
					res = @conn.exec_params(("SELECT " + sql_vals.join(', ')), [], format )
					vals.each_with_index do |v, i|
						expect( res.getvalue(0,i) ).to eq( IPAddr.new(v) )
					end
				end
			end

			it "should do cidr type conversions" do
				[0].each do |format|
					vals = [
						'0.0.0.0/0',
						'1.0.0.0/8',
						'1.2.0.0/16',
						'1.2.3.0/24',
						'1.2.3.4/32',
						'1.2.3.128/25',
						'::/0',
						'1234:3456::/32',
						'1234:3456:5678:789a::/64',
						'1234:3456:5678:789a:9abc:bced::/96',
						'1234:3456:5678:789a:9abc:bced:edf0:f012/128',
					]
					sql_vals = vals.map { |v| "CAST('#{v}' AS cidr)" }
					res = @conn.exec_params(("SELECT " + sql_vals.join(', ')), [], format )
					vals.each_with_index do |v, i|
						val = res.getvalue(0,i)
						ip, prefix = v.split('/', 2)
						expect( val.to_s ).to eq( ip )
						if val.respond_to?(:prefix)
							val_prefix = val.prefix
						else
							default_prefix = (val.family == Socket::AF_INET ? 32 : 128)
							range = val.to_range
							val_prefix	= default_prefix - Math.log(((range.end.to_i - range.begin.to_i) + 1), 2).to_i
						end
						if v.include?('/')
							expect( val_prefix ).to eq( prefix.to_i )
						elsif v.include?('.')
							expect( val_prefix ).to eq( 32 )
						else
							expect( val_prefix ).to eq( 128 )
						end
					end
				end
			end
		end

		context "with usage of result oids for copy decoder selection" do
			it "can type cast #copy_data text output with decoder" do
				@conn.exec( "CREATE TEMP TABLE copytable (t TEXT, i INT, ai INT[])" )
				@conn.exec( "INSERT INTO copytable VALUES ('a', 123, '{5,4,3}'), ('b', 234, '{2,3}')" )

				# Retrieve table OIDs per empty result.
				res = @conn.exec( "SELECT * FROM copytable LIMIT 0" )
				tm = basic_type_mapping.build_column_map( res )
				row_decoder = YSQL::TextDecoder::CopyRow.new(type_map: tm).freeze

				rows = []
				@conn.copy_data( "COPY copytable TO STDOUT", row_decoder ) do |res|
					while row=@conn.get_copy_data
						rows << row
					end
				end
				expect( rows ).to eq( [['a', 123, [5,4,3]], ['b', 234, [2,3]]] )
			end

			it "can type cast #copy_data binary output with decoder" do
				@conn.exec( "CREATE TEMP TABLE copytable (b BYTEA, i INT, ts timestamp)" )
				@conn.exec( "INSERT INTO copytable VALUES ('\\xff000a0d27', 1234, '2023-03-17 03:04:05.678912'), ('\\x202078797a2020', '-444', '1990-12-17 15:14:45')" )

				# Retrieve table OIDs per empty result.
				res = @conn.exec_params( "SELECT * FROM copytable LIMIT 0", [], 1 )
				tm = basic_type_mapping.build_column_map( res )
				row_decoder = YSQL::BinaryDecoder::CopyRow.new(type_map: tm).freeze

				rows = []
				@conn.copy_data( "COPY copytable TO STDOUT WITH (FORMAT binary)", row_decoder ) do |res|
					while row=@conn.get_copy_data
						rows << row
					end
				end
				expect( rows.map{|l| l[0,2] } ).to eq( [["\xff\x00\n\r'".b, 1234], ["  xyz  ", -444]] )
				expect( rows[0][2] ).
					to be_within(0.000001).of( Time.utc(2023, 3, 17, 3, 4, 5.678912) )
				expect( rows[1][2] ).
					to be_within(0.000001).of( Time.utc(1990, 12, 17, 15, 14, 45) )
			end
		end
	end
end
