# -*- rspec -*-
# encoding: utf-8

require 'ysql'
require 'time'


describe "PG::Type derivations" do
	let!(:textenc_int) { YSQL::TextEncoder::Integer.new name: 'Integer', oid: 23 }
	let!(:textdec_int) { YSQL::TextDecoder::Integer.new name: 'Integer', oid: 23 }
	let!(:textenc_boolean) { YSQL::TextEncoder::Boolean.new }
	let!(:textdec_boolean) { YSQL::TextDecoder::Boolean.new }
	let!(:textenc_float) { YSQL::TextEncoder::Float.new }
	let!(:textdec_float) { YSQL::TextDecoder::Float.new }
	let!(:textenc_numeric) do
		begin
			YSQL::TextEncoder::Numeric.new
		rescue LoadError
		end
	end
	let!(:textenc_string) { YSQL::TextEncoder::String.new }
	let!(:textdec_string) { YSQL::TextDecoder::String.new }
	let!(:textenc_timestamp) { YSQL::TextEncoder::TimestampWithoutTimeZone.new }
	let!(:textdec_timestamp) { YSQL::TextDecoder::TimestampWithoutTimeZone.new }
	let!(:textenc_timestamputc) { YSQL::TextEncoder::TimestampUtc.new }
	let!(:textdec_timestamputc) { YSQL::TextDecoder::TimestampUtc.new }
	let!(:textdec_timestampul) { YSQL::TextDecoder::TimestampUtcToLocal.new }
	let!(:textenc_timestamptz) { YSQL::TextEncoder::TimestampWithTimeZone.new }
	let!(:textdec_timestamptz) { YSQL::TextDecoder::TimestampWithTimeZone.new }
	let!(:textenc_bytea) { YSQL::TextEncoder::Bytea.new }
	let!(:textdec_bytea) { YSQL::TextDecoder::Bytea.new }
	let!(:binaryenc_int2) { YSQL::BinaryEncoder::Int2.new }
	let!(:binaryenc_int4) { YSQL::BinaryEncoder::Int4.new }
	let!(:binaryenc_int8) { YSQL::BinaryEncoder::Int8.new }
	let!(:binarydec_string) { YSQL::BinaryDecoder::String.new }
	let!(:binarydec_integer) { YSQL::BinaryDecoder::Integer.new }
	let!(:binaryenc_timestamputc) { YSQL::BinaryEncoder::TimestampUtc.new }
	let!(:binaryenc_timestamplocal) { YSQL::BinaryEncoder::TimestampLocal.new }

	let!(:intenc_incrementer) do
		Class.new(YSQL::SimpleEncoder) do
			def encode(value)
				(value.to_i + 1).to_s + " "
			end
		end.new
	end
	let!(:intdec_incrementer) do
		Class.new(YSQL::SimpleDecoder) do
			def decode(string, tuple=nil, field=nil)
				string.to_i+1
			end
		end.new
	end

	let!(:intenc_incrementer_with_encoding) do
		Class.new(YSQL::SimpleEncoder) do
			def encode(value, encoding)
				r = (value.to_i + 1).to_s + " #{encoding}"
				r.encode!(encoding)
			end
		end.new
	end
	let!(:intenc_incrementer_with_int_result) do
		Class.new(YSQL::SimpleEncoder) do
			def encode(value)
				value.to_i+1
			end
		end.new
	end

	it "shouldn't be possible to build a PG::Type directly" do
		expect{ YSQL::Coder.new }.to raise_error(TypeError, /cannot/)
	end

	describe YSQL::SimpleCoder do
		describe '#decode' do
			it "should offer decode method with tuple/field" do
				res = textdec_int.decode("123", 1, 1)
				expect( res ).to eq( 123 )
			end

			it "should offer decode method without tuple/field" do
				res = textdec_int.decode("234")
				expect( res ).to eq( 234 )
			end

			it "should decode with ruby decoder" do
				expect( intdec_incrementer.decode("3") ).to eq( 4 )
			end

			it "should decode integers of different lengths from text format" do
				30.times do |zeros|
					expect( textdec_int.decode("1" + "0"*zeros) ).to eq( 10 ** zeros )
					expect( textdec_int.decode(zeros==0 ? "0" : "9"*zeros) ).to eq( 10 ** zeros - 1 )
					expect( textdec_int.decode("-1" + "0"*zeros) ).to eq( -10 ** zeros )
					expect( textdec_int.decode(zeros==0 ? "0" : "-" + "9"*zeros) ).to eq( -10 ** zeros + 1 )
				end
				66.times do |bits|
					expect( textdec_int.decode((2 ** bits).to_s) ).to eq( 2 ** bits )
					expect( textdec_int.decode((2 ** bits - 1).to_s) ).to eq( 2 ** bits - 1 )
					expect( textdec_int.decode((-2 ** bits).to_s) ).to eq( -2 ** bits )
					expect( textdec_int.decode((-2 ** bits + 1).to_s) ).to eq( -2 ** bits + 1 )
				end
			end

			it 'decodes bytea to a binary string' do
				expect( textdec_bytea.decode("\\x00010203EF") ).to eq( "\x00\x01\x02\x03\xef".b )
				expect( textdec_bytea.decode("\\377\\000") ).to eq( "\xff\0".b )
			end

			context 'timestamps' do
				it 'decodes timestamps without timezone as local time' do
					expect( textdec_timestamp.decode('2016-01-02 23:23:59.123456').iso8601(5) ).
						to eq( Time.new(2016,1,2, 23,23,59.123456).iso8601(5) )
					expect( textdec_timestamp.decode('2016-08-02 23:23:59.123456').iso8601(5) ).
						to eq( Time.new(2016,8,2, 23,23,59.123456).iso8601(5) )
				end
				it 'decodes timestamps with UTC time and returns UTC timezone' do
					expect( textdec_timestamputc.decode('2016-01-02 23:23:59.123456').iso8601(5) ).
						to eq( Time.utc(2016,1,2, 23,23,59.123456).iso8601(5) )
					expect( textdec_timestamputc.decode('2016-08-02 23:23:59.123456').iso8601(5) ).
						to eq( Time.utc(2016,8,2, 23,23,59.123456).iso8601(5) )
				end
				it 'decodes timestamps with UTC time and returns local timezone' do
					expect( textdec_timestampul.decode('2016-01-02 23:23:59.123456').iso8601(5) ).
						to eq( Time.utc(2016,1,2, 23,23,59.123456).getlocal.iso8601(5) )
					expect( textdec_timestampul.decode('2016-08-02 23:23:59.123456').iso8601(5) ).
						to eq( Time.utc(2016,8,2, 23,23,59.123456).getlocal.iso8601(5) )
				end
				it 'decodes timestamps with hour timezone' do
					expect( textdec_timestamptz.decode('2016-01-02 23:23:59.123456-04').iso8601(5) ).
						to eq( Time.new(2016,1,2, 23,23,59.123456, "-04:00").iso8601(5) )
					expect( textdec_timestamptz.decode('2016-08-02 23:23:59.123456+10').iso8601(5) ).
						to eq( Time.new(2016,8,2, 23,23,59.123456, "+10:00").iso8601(5) )
					expect( textdec_timestamptz.decode('1913-12-31 23:58:59.1231-03').iso8601(5) ).
						to eq( Time.new(1913, 12, 31, 23, 58, 59.1231, "-03:00").iso8601(5) )
					expect( textdec_timestamptz.decode('4714-11-24 23:58:59.1231-03 BC').iso8601(5) ).
						to eq( Time.new(-4713, 11, 24, 23, 58, 59.1231, "-03:00").iso8601(5) )
					expect( textdec_timestamptz.decode('294276-12-31 23:58:59.1231+03').iso8601(5) ).
						to eq( Time.new(294276, 12, 31, 23, 58, 59.1231, "+03:00").iso8601(5) )
				end
				it 'decodes timestamps with hour:minute timezone' do
					expect( textdec_timestamptz.decode('2015-01-26 17:26:42.691511-04:15') ).
						to be_within(0.000001).of( Time.new(2015,01,26, 17, 26, 42.691511, "-04:15") )
					expect( textdec_timestamptz.decode('2015-07-26 17:26:42.691511-04:30') ).
						to be_within(0.000001).of( Time.new(2015,07,26, 17, 26, 42.691511, "-04:30") )
					expect( textdec_timestamptz.decode('2015-01-26 17:26:42.691511+10:45') ).
						to be_within(0.000001).of( Time.new(2015,01,26, 17, 26, 42.691511, "+10:45") )
				end
				it 'decodes timestamps with hour:minute:sec timezone' do
					# SET TIME ZONE 'Europe/Dublin'; -- Was UTC−00:25:21 until 1916
					# SELECT '1900-01-01'::timestamptz;
					# -- "1900-01-01 00:00:00-00:25:21"
					expect( textdec_timestamptz.decode('1916-01-01 00:00:00-00:25:21') ).
						to be_within(0.000001).of( Time.new(1916, 1, 1, 0, 0, 0, "-00:25:21") )
				end
				it 'decodes timestamps with date before 1823' do
					expect( textdec_timestamp.decode('1822-01-02 23:23:59.123456').iso8601(5) ).
						to eq( Time.new(1822,01,02, 23, 23, 59.123456).iso8601(5) )
					expect( textdec_timestamputc.decode('1822-01-02 23:23:59.123456').iso8601(5) ).
						to eq( Time.utc(1822,01,02, 23, 23, 59.123456).iso8601(5) )
					expect( textdec_timestampul.decode('1822-01-02 23:23:59.123456').iso8601(5) ).
						to eq( Time.utc(1822,01,02, 23, 23, 59.123456).getlocal.iso8601(5) )
					expect( textdec_timestamptz.decode('1822-01-02 23:23:59.123456+04').iso8601(5) ).
						to eq( Time.new(1822,01,02, 23, 23, 59.123456, "+04:00").iso8601(5) )
				end
				it 'decodes timestamps with date after 2116' do
					expect( textdec_timestamp.decode('2117-01-02 23:23:59.123456').iso8601(5) ).
						to eq( Time.new(2117,01,02, 23, 23, 59.123456).iso8601(5) )
					expect( textdec_timestamputc.decode('2117-01-02 23:23:59.123456').iso8601(5) ).
						to eq( Time.utc(2117,01,02, 23, 23, 59.123456).iso8601(5) )
					expect( textdec_timestampul.decode('2117-01-02 23:23:59.123456').iso8601(5) ).
						to eq( Time.utc(2117,01,02, 23, 23, 59.123456).getlocal.iso8601(5) )
					expect( textdec_timestamptz.decode('2117-01-02 23:23:59.123456+04').iso8601(5) ).
						to eq( Time.new(2117,01,02, 23, 23, 59.123456, "+04:00").iso8601(5) )
				end
				it 'decodes timestamps with variable number of digits for the useconds part' do
					sec = "59.12345678912345"
					(4..sec.length).each do |i|
						expect( textdec_timestamp.decode("2016-01-02 23:23:#{sec[0,i]}") ).
										to be_within(0.000001).of( Time.new(2016,01,02, 23, 23, sec[0,i].to_f) )
					end
				end
				it 'decodes timestamps with leap-second' do
					expect( textdec_timestamp.decode('1998-12-31 23:59:60.1234') ).
						to be_within(0.000001).of( Time.new(1998,12,31, 23, 59, 60.1234) )
				end

				def textdec_timestamptz_decode_should_fail(str)
					expect(textdec_timestamptz.decode(str)).to eq(str)
				end

				it 'fails when the timestamp is an empty string' do
					textdec_timestamptz_decode_should_fail('')
				end
				it 'fails when the timestamp contains values with less digits than expected' do
					textdec_timestamptz_decode_should_fail('2016-0-02 23:23:59.123456+00:25:21')
					textdec_timestamptz_decode_should_fail('2016-01-0 23:23:59.123456+00:25:21')
					textdec_timestamptz_decode_should_fail('2016-01-02 2:23:59.123456+00:25:21')
					textdec_timestamptz_decode_should_fail('2016-01-02 23:2:59.123456+00:25:21')
					textdec_timestamptz_decode_should_fail('2016-01-02 23:23:5.123456+00:25:21')
					textdec_timestamptz_decode_should_fail('2016-01-02 23:23:59.+00:25:21')
					textdec_timestamptz_decode_should_fail('2016-01-02 23:23:59.123456+0:25:21')
					textdec_timestamptz_decode_should_fail('2016-01-02 23:23:59.123456+00:2:21')
					textdec_timestamptz_decode_should_fail('2016-01-02 23:23:59.123456+00:25:2')
				end
				it 'fails when the timestamp contains values with more digits than expected' do
					textdec_timestamptz_decode_should_fail('2016-011-02 23:23:59.123456+00:25:21')
					textdec_timestamptz_decode_should_fail('2016-01-022 23:23:59.123456+00:25:21')
					textdec_timestamptz_decode_should_fail('2016-01-02 233:23:59.123456+00:25:21')
					textdec_timestamptz_decode_should_fail('2016-01-02 23:233:59.123456+00:25:21')
					textdec_timestamptz_decode_should_fail('2016-01-02 23:23:599.123456+00:25:21')
					textdec_timestamptz_decode_should_fail('2016-01-02 23:23:59.123456+000:25:21')
					textdec_timestamptz_decode_should_fail('2016-01-02 23:23:59.123456+00:255:21')
					textdec_timestamptz_decode_should_fail('2016-01-02 23:23:59.123456+00:25:211')
				end
				it 'fails when the timestamp contains values with invalid characters' do
					str = '2013-01-02 23:23:59.123456+00:25:21'
					str.length.times do |i|
						textdec_timestamptz_decode_should_fail(str[0,i] + "x" + str[i+1..-1])
					end
				end
				it 'fails when the timestamp contains leading characters' do
					textdec_timestamptz_decode_should_fail(' 2016-01-02 23:23:59.123456')
				end
				it 'fails when the timestamp contains trailing characters' do
					textdec_timestamptz_decode_should_fail('2016-01-02 23:23:59.123456 ')
				end
				it 'fails when the timestamp contains non ASCII character' do
					textdec_timestamptz_decode_should_fail('2016-01ª02 23:23:59.123456')
				end
			end

			context 'identifier quotation' do
				it 'should build an array out of an quoted identifier string' do
					quoted_type = YSQL::TextDecoder::Identifier.new
					expect( quoted_type.decode(%["A.".".B"]) ).to eq( ["A.", ".B"] )
					expect( quoted_type.decode(%["'A"".""B'"]) ).to eq( ['\'A"."B\''] )
				end

				it 'should split unquoted identifier string' do
					quoted_type = YSQL::TextDecoder::Identifier.new
					expect( quoted_type.decode(%[a.b]) ).to eq( ['a','b'] )
					expect( quoted_type.decode(%[a]) ).to eq( ['a'] )
				end

				it 'should split identifier string with correct character encoding' do
					quoted_type = YSQL::TextDecoder::Identifier.new
					v = quoted_type.decode(%[Héllo].encode("iso-8859-1")).first
					expect( v.encoding ).to eq( Encoding::ISO_8859_1 )
					expect( v ).to eq( %[Héllo].encode(Encoding::ISO_8859_1) )
				end
			end

			it "should raise when decode method is called with wrong args" do
				expect{ textdec_int.decode() }.to raise_error(ArgumentError)
				expect{ textdec_int.decode("123", 2, 3, 4) }.to raise_error(ArgumentError)
				expect{ textdec_int.decode(2, 3, 4) }.to raise_error(TypeError)
				expect( intdec_incrementer.decode(2, 3, 4) ).to eq( 3 )
			end

			it "should pass through nil values" do
				expect( textdec_string.decode( nil )).to be_nil
				expect( textdec_int.decode( nil )).to be_nil
			end

			it "should be defined on an encoder but not on a decoder instance" do
				expect( textdec_int.respond_to?(:decode) ).to be_truthy
				expect( textenc_int.respond_to?(:decode) ).to be_falsey
			end
		end

		describe '#encode' do
			it "should offer encode method for text type" do
				res = textenc_int.encode(123)
				expect( res ).to eq( "123" )
			end

			it "should offer encode method for binary type" do
				res = binaryenc_int8.encode(123)
				expect( res ).to eq( [123].pack("q>") )
			end

			it "should encode integers from string to binary format" do
				expect( binaryenc_int2.encode("  -123  ") ).to eq( [-123].pack("s>") )
				expect( binaryenc_int4.encode("  -123  ") ).to eq( [-123].pack("l>") )
				expect( binaryenc_int8.encode("  -123  ") ).to eq( [-123].pack("q>") )
				expect( binaryenc_int2.encode("  123-xyz  ") ).to eq( [123].pack("s>") )
				expect( binaryenc_int4.encode("  123-xyz  ") ).to eq( [123].pack("l>") )
				expect( binaryenc_int8.encode("  123-xyz  ") ).to eq( [123].pack("q>") )
			end

			it "should encode integers of different lengths to text format" do
				30.times do |zeros|
					expect( textenc_int.encode(10 ** zeros) ).to eq( "1" + "0"*zeros )
					expect( textenc_int.encode(10 ** zeros - 1) ).to eq( zeros==0 ? "0" : "9"*zeros )
					expect( textenc_int.encode(-10 ** zeros) ).to eq( "-1" + "0"*zeros )
					expect( textenc_int.encode(-10 ** zeros + 1) ).to eq( zeros==0 ? "0" : "-" + "9"*zeros )
				end
				66.times do |bits|
					expect( textenc_int.encode(2 ** bits) ).to eq( (2 ** bits).to_s )
					expect( textenc_int.encode(2 ** bits - 1) ).to eq( (2 ** bits - 1).to_s )
					expect( textenc_int.encode(-2 ** bits) ).to eq( (-2 ** bits).to_s )
					expect( textenc_int.encode(-2 ** bits + 1) ).to eq( (-2 ** bits + 1).to_s )
				end
			end

			it "should encode integers from string to text format" do
				expect( textenc_int.encode("  -123  ") ).to eq( "-123" )
				expect( textenc_int.encode("  123-xyz  ") ).to eq( "123" )
			end

			it "should encode boolean values" do
				expect( textenc_boolean.encode(false) ).to eq( "f" )
				expect( textenc_boolean.encode(true) ).to eq( "t" )
				["any", :other, "value", 0, 1, 2].each do |value|
					expect( textenc_boolean.encode(value) ).to eq( value.to_s )
				end
			end

			it "should encode floats" do
				expect( textenc_float.encode(0) ).to eq( "0.0" )
				expect( textenc_float.encode(-1) ).to eq( "-1.0" )
				expect( textenc_float.encode(-1.234567890123456789) ).to eq( "-1.234567890123457" )
				expect( textenc_float.encode(9) ).to eq( "9.0" )
				expect( textenc_float.encode(10) ).to eq( "10.0" )
				expect( textenc_float.encode(-99) ).to eq( "-99.0" )
				expect( textenc_float.encode(-100) ).to eq( "-100.0" )
				expect( textenc_float.encode(999) ).to eq( "999.0" )
				expect( textenc_float.encode(-1000) ).to eq( "-1000.0" )
				expect( textenc_float.encode(1234.567890123456789) ).to eq( "1234.567890123457" )
				expect( textenc_float.encode(-9999) ).to eq( "-9999.0" )
				expect( textenc_float.encode(10000) ).to eq( "10000.0" )
				expect( textenc_float.encode(99999) ).to eq( "99999.0" )
				expect( textenc_float.encode(-100000) ).to eq( "-100000.0" )
				expect( textenc_float.encode(-999999) ).to eq( "-999999.0" )
				expect( textenc_float.encode(1000000) ).to eq( "1000000.0" )
				expect( textenc_float.encode(9999999) ).to eq( "9999999.0" )
				expect( textenc_float.encode(-100000000000000) ).to eq( "-100000000000000.0" )
				expect( textenc_float.encode(123456789012345) ).to eq( "123456789012345.0" )
				expect( textenc_float.encode(-999999999999999) ).to eq( "-999999999999999.0" )
				expect( textenc_float.encode(1000000000000000) ).to eq( "1e15" )
				expect( textenc_float.encode(-1234567890123456) ).to eq( "-1.234567890123456e15" )
				expect( textenc_float.encode(9999999999999999) ).to eq( "1e16" )

				expect( textenc_float.encode(-0.0) ).to eq( "0.0" )
				expect( textenc_float.encode(0.1) ).to eq( "0.1" )
				expect( textenc_float.encode(0.1234567890123456789) ).to eq( "0.1234567890123457" )
				expect( textenc_float.encode(-0.9) ).to eq( "-0.9" )
				expect( textenc_float.encode(-0.01234567890123456789) ).to eq( "-0.01234567890123457" )
				expect( textenc_float.encode(0.09) ).to eq( "0.09" )
				expect( textenc_float.encode(0.001234567890123456789) ).to eq( "0.001234567890123457" )
				expect( textenc_float.encode(-0.009) ).to eq( "-0.009" )
				expect( textenc_float.encode(-0.0001234567890123456789) ).to eq( "-0.0001234567890123457" )
				expect( textenc_float.encode(0.0009) ).to eq( "0.0009" )
				expect( textenc_float.encode(0.00001) ).to eq( "1e-5" )
				expect( textenc_float.encode(0.00001234567890123456789) ).to eq( "1.234567890123457e-5" )
				expect( textenc_float.encode(-0.00009) ).to eq( "-9e-5" )
				expect( textenc_float.encode(-0.11) ).to eq( "-0.11" )
				expect( textenc_float.encode(10.11) ).to eq( "10.11" )
				expect( textenc_float.encode(-1.234567890123456789E-280) ).to eq( "-1.234567890123457e-280" )
				expect( textenc_float.encode(-1.234567890123456789E280) ).to eq( "-1.234567890123457e280" )
				expect( textenc_float.encode(9876543210987654321E280) ).to eq( "9.87654321098765e298" )
				expect( textenc_float.encode(9876543210987654321E-400) ).to eq( "0.0" )
				expect( textenc_float.encode(9876543210987654321E400) ).to eq( "Infinity" )
			end

			it "should encode special floats equally to Float#to_s" do
				expect( textenc_float.encode(Float::INFINITY) ).to eq( Float::INFINITY.to_s )
				expect( textenc_float.encode(-Float::INFINITY) ).to eq( (-Float::INFINITY).to_s )
				expect( textenc_float.encode(-Float::NAN) ).to eq( Float::NAN.to_s )
			end

			it "should encode various inputs to numeric format", :bigdecimal do
				expect( textenc_numeric.encode(0) ).to eq( "0" )
				expect( textenc_numeric.encode(1) ).to eq( "1" )
				expect( textenc_numeric.encode(-12345678901234567890123) ).to eq( "-12345678901234567890123" )
				expect( textenc_numeric.encode(0.0) ).to eq( "0.0" )
				expect( textenc_numeric.encode(1.0) ).to eq( "1.0" )
				expect( textenc_numeric.encode(-1.23456789012e45) ).to eq( "-1.23456789012e45" )
				expect( textenc_numeric.encode(Float::NAN) ).to eq( Float::NAN.to_s )
				expect( textenc_numeric.encode(BigDecimal(0)) ).to eq( "0.0" )
				expect( textenc_numeric.encode(BigDecimal(1)) ).to eq( "1.0" )
				expect( textenc_numeric.encode(BigDecimal("-12345678901234567890.1234567")) ).to eq( "-12345678901234567890.1234567" )
				expect( textenc_numeric.encode(" 123 ") ).to eq( " 123 " )
			end

			it "encodes binary string to bytea" do
				expect( textenc_bytea.encode("\x00\x01\x02\x03\xef".b) ).to eq( "\\x00010203ef" )
			end

			context 'text timestamps' do
				it 'encodes timestamps without timezone' do
					expect( textenc_timestamp.encode(Time.new(2016,1,2, 23, 23, 59.123456, 3*60*60)) ).
						to match( /^2016-01-02 23:23:59.12345\d+$/ )
					expect( textenc_timestamp.encode(Time.new(2016,8,2, 23, 23, 59.123456, 3*60*60)) ).
						to match( /^2016-08-02 23:23:59.12345\d+$/ )
				end
				it 'encodes timestamps with UTC timezone' do
					expect( textenc_timestamputc.encode(Time.new(2016,1,2, 23, 23, 59.123456, 3*60*60)) ).
						to match( /^2016-01-02 20:23:59.12345\d+$/ )
					expect( textenc_timestamputc.encode(Time.new(2016,8,2, 23, 23, 59.123456, 3*60*60)) ).
						to match( /^2016-08-02 20:23:59.12345\d+$/ )
				end
				it 'encodes timestamps with hour timezone' do
					expect( textenc_timestamptz.encode(Time.new(2016,1,02, 23, 23, 59.123456, -4*60*60)) ).
						to match( /^2016-01-02 23:23:59.12345\d+ \-04:00$/ )
					expect( textenc_timestamptz.encode(Time.new(2016,8,02, 23, 23, 59.123456, 10*60*60)) ).
						to match( /^2016-08-02 23:23:59.12345\d+ \+10:00$/ )
				end
			end

			context 'binary timestamps' do
				it 'encodes timestamps as UTC' do
					expect( binaryenc_timestamputc.encode(Time.utc(2000,1,1)) ).
						to eq( "\x00" * 8 )
					expect( binaryenc_timestamputc.encode(Time.utc(2000,1,1).localtime) ).
						to eq( "\x00" * 8 )
				end
				it 'encodes timestamps as local time' do
					expect( binaryenc_timestamplocal.encode(Time.new(2000,1,1)) ).
						to eq( "\x00" * 8 )
					expect( binaryenc_timestamplocal.encode(Time.new(2000,1,1).utc) ).
						to eq( "\x00" * 8 )
				end
			end

			context 'identifier quotation' do
				it 'should quote and escape identifier' do
					quoted_type = YSQL::TextEncoder::Identifier.new
					expect( quoted_type.encode(['schema','table','col']) ).to eq( %["schema"."table"."col"] )
					expect( quoted_type.encode(['A.','.B']) ).to eq( %["A.".".B"] )
					expect( quoted_type.encode(%['A"."B']) ).to eq( %["'A"".""B'"] )
					expect( quoted_type.encode( nil ) ).to be_nil
				end

				it 'should quote identifiers with correct character encoding' do
					quoted_type = YSQL::TextEncoder::Identifier.new
					v = quoted_type.encode(['Héllo'], "iso-8859-1")
					expect( v ).to eq( %["Héllo"].encode(Encoding::ISO_8859_1) )
					expect( v.encoding ).to eq( Encoding::ISO_8859_1 )
				end

				it "will raise a TypeError for invalid arguments to quote_ident" do
					quoted_type = YSQL::TextEncoder::Identifier.new
					expect{ quoted_type.encode( [nil] ) }.to raise_error(TypeError)
					expect{ quoted_type.encode( [['a']] ) }.to raise_error(TypeError)
				end
			end

			it "should encode with ruby encoder" do
				expect( intenc_incrementer.encode(3) ).to eq( "4 " )
			end

			it "should encode with ruby encoder and given character encoding" do
				r = intenc_incrementer_with_encoding.encode(3, Encoding::CP850)
				expect( r ).to eq( "4 CP850" )
				expect( r.encoding ).to eq( Encoding::CP850 )
			end

			it "should return when ruby encoder returns non string values" do
				expect( intenc_incrementer_with_int_result.encode(3) ).to eq( 4 )
			end

			it "should pass through nil values" do
				expect( textenc_string.encode( nil )).to be_nil
				expect( textenc_int.encode( nil )).to be_nil
			end

			it "should be defined on a decoder but not on an encoder instance" do
				expect( textenc_int.respond_to?(:encode) ).to be_truthy
				expect( textdec_int.respond_to?(:encode) ).to be_falsey
			end
		end

		it "should be possible to marshal encoders" do
			mt = Marshal.dump(textenc_int)
			lt = Marshal.load(mt)
			expect( lt.to_h ).to eq( textenc_int.to_h )
		end

		it "should be possible to marshal decoders" do
			mt = Marshal.dump(textdec_int)
			lt = Marshal.load(mt)
			expect( lt.to_h ).to eq( textdec_int.to_h )
		end

		it "should respond to to_h" do
			expect( textenc_int.to_h ).to eq( {
				name: 'Integer', oid: 23, format: 0, flags: 0
			} )
		end

		it "should have reasonable default values" do
			t = YSQL::TextEncoder::String.new
			expect( t.format ).to eq( 0 )
			expect( t.oid ).to eq( 0 )
			expect( t.name ).to be_nil

			t = YSQL::BinaryEncoder::Int4.new
			expect( t.format ).to eq( 1 )
			expect( t.oid ).to eq( 0 )
			expect( t.name ).to be_nil

			t = YSQL::TextDecoder::String.new
			expect( t.format ).to eq( 0 )
			expect( t.oid ).to eq( 0 )
			expect( t.name ).to be_nil

			t = YSQL::BinaryDecoder::String.new
			expect( t.format ).to eq( 1 )
			expect( t.oid ).to eq( 0 )
			expect( t.name ).to be_nil
		end

		it "should overwrite default values as kwargs" do
			t = YSQL::BinaryEncoder::Int4.new(format: 0)
			expect( t.format ).to eq( 0 )
		end

		def expect_deprecated_coder_init
			if RUBY_VERSION >= '3'
				begin
					prev_deprecated = Warning[:deprecated]
					Warning[:deprecated] = true

					expect do
						yield
					end.to output(/deprecated.*type_spec.rb/).to_stderr
				ensure
					Warning[:deprecated] = prev_deprecated
				end
			else
				yield
			end
		end

		it "should overwrite default format" do
			t = nil
			expect_deprecated_coder_init do
				t = YSQL::BinaryEncoder::Int4.new({ format: 0})
			end
			expect( t.format ).to eq( 0 )

			t = YSQL::BinaryEncoder::Int4.new(format: 0)
			expect( t.format ).to eq( 0 )
		end

		it "should take hash argument" do
			t = nil
			expect_deprecated_coder_init { t = YSQL::TextEncoder::Integer.new({ name: "abcä"}) }
			expect( t.name ).to eq( "abcä" )
			expect_deprecated_coder_init { t = YSQL::BinaryEncoder::Int4.new({ name: "abcä"}) }
			expect( t.name ).to eq( "abcä" )
			expect_deprecated_coder_init { t = YSQL::BinaryDecoder::TimestampUtc.new({ name: "abcä"}) }
			expect( t.name ).to eq( "abcä" )
			expect_deprecated_coder_init { t = YSQL::BinaryDecoder::TimestampUtcToLocal.new({ name: "abcä"}) }
			expect( t.name ).to eq( "abcä" )
			expect_deprecated_coder_init { t = YSQL::BinaryDecoder::TimestampLocal.new({ name: "abcä"}) }
			expect( t.name ).to eq( "abcä" )
			expect_deprecated_coder_init { t = YSQL::BinaryEncoder::TimestampUtc.new({ name: "abcä"}) }
			expect( t.name ).to eq( "abcä" )
			expect_deprecated_coder_init { t = YSQL::BinaryEncoder::TimestampLocal.new({ name: "abcä"}) }
			expect( t.name ).to eq( "abcä" )
			expect_deprecated_coder_init { t = YSQL::TextDecoder::TimestampUtc.new({ name: "abcä"}) }
			expect( t.name ).to eq( "abcä" )
			expect_deprecated_coder_init { t = YSQL::TextDecoder::TimestampUtcToLocal.new({ name: "abcä"}) }
			expect( t.name ).to eq( "abcä" )
			expect_deprecated_coder_init { t = YSQL::TextDecoder::TimestampLocal.new({ name: "abcä"}) }
			expect( t.name ).to eq( "abcä" )
			expect_deprecated_coder_init { t = YSQL::TextDecoder::TimestampWithoutTimeZone.new({ name: "abcä"}) }
			expect( t.name ).to eq( "abcä" )
			expect_deprecated_coder_init { t = YSQL::TextDecoder::TimestampWithTimeZone.new({ name: "abcä"}) }
			expect( t.name ).to eq( "abcä" )
		end

		it "shouldn't overwrite timestamp flags" do
			t = YSQL::TextDecoder::TimestampUtc.new({ flags: YSQL::Coder::TIMESTAMP_DB_LOCAL})
			expect( t.flags ).to eq(YSQL::Coder::TIMESTAMP_DB_UTC | YSQL::Coder::TIMESTAMP_APP_UTC )
			t = YSQL::TextDecoder::TimestampUtcToLocal.new({ flags: YSQL::Coder::TIMESTAMP_APP_UTC})
			expect( t.flags ).to eq(YSQL::Coder::TIMESTAMP_DB_UTC | YSQL::Coder::TIMESTAMP_APP_LOCAL )
			t = YSQL::TextDecoder::TimestampLocal.new({ flags: YSQL::Coder::TIMESTAMP_DB_UTC})
			expect( t.flags ).to eq(YSQL::Coder::TIMESTAMP_DB_LOCAL | YSQL::Coder::TIMESTAMP_APP_LOCAL )

			t = YSQL::BinaryDecoder::TimestampUtc.new({ flags: YSQL::Coder::TIMESTAMP_DB_LOCAL})
			expect( t.flags ).to eq(YSQL::Coder::TIMESTAMP_DB_UTC | YSQL::Coder::TIMESTAMP_APP_UTC )
			t = YSQL::BinaryDecoder::TimestampUtcToLocal.new({ flags: YSQL::Coder::TIMESTAMP_APP_UTC})
			expect( t.flags ).to eq(YSQL::Coder::TIMESTAMP_DB_UTC | YSQL::Coder::TIMESTAMP_APP_LOCAL )
			t = YSQL::BinaryDecoder::TimestampLocal.new({ flags: YSQL::Coder::TIMESTAMP_DB_UTC})
			expect( t.flags ).to eq(YSQL::Coder::TIMESTAMP_DB_LOCAL | YSQL::Coder::TIMESTAMP_APP_LOCAL )

			t = YSQL::BinaryEncoder::TimestampUtc.new({ flags: YSQL::Coder::TIMESTAMP_DB_LOCAL})
			expect( t.flags ).to eq(YSQL::Coder::TIMESTAMP_DB_UTC )
			t = YSQL::BinaryEncoder::TimestampLocal.new({ flags: YSQL::Coder::TIMESTAMP_APP_LOCAL})
			expect( t.flags ).to eq(YSQL::Coder::TIMESTAMP_DB_LOCAL )
		end

		it "should deny changes when frozen" do
			t = YSQL::TextEncoder::String.new.freeze
			expect{ t.format = 1 }.to raise_error(FrozenError)
			expect{ t.oid = 0  }.to raise_error(FrozenError)
			expect{ t.name = "x" }.to raise_error(FrozenError)
		end

		it "should be shareable for Ractor", :ractor do
			t = YSQL::TextEncoder::String.new.freeze
			Ractor.make_shareable(t)
		end

		it "should give account about memory usage" do
			expect( ObjectSpace.memsize_of(textenc_int) ).to be > DATA_OBJ_MEMSIZE
			expect( ObjectSpace.memsize_of(binarydec_integer) ).to be > DATA_OBJ_MEMSIZE
		end
	end

	describe YSQL::CompositeCoder do
		describe "Array types" do
			let!(:textenc_string_array) { YSQL::TextEncoder::Array.new elements_type: textenc_string }
			let!(:textdec_string_array) { YSQL::TextDecoder::Array.new elements_type: textdec_string }
			let!(:textdec_string_array_raise) { YSQL::TextDecoder::Array.new elements_type: textdec_string, flags: YSQL::Coder:: FORMAT_ERROR_TO_RAISE }
			let!(:textenc_int_array) { YSQL::TextEncoder::Array.new elements_type: textenc_int, needs_quotation: false }
			let!(:textdec_int_array) { YSQL::TextDecoder::Array.new elements_type: textdec_int, needs_quotation: false }
			let!(:textenc_float_array) { YSQL::TextEncoder::Array.new elements_type: textenc_float, needs_quotation: false }
			let!(:textdec_float_array) { YSQL::TextDecoder::Array.new elements_type: textdec_float, needs_quotation: false }
			let!(:textenc_timestamp_array) { YSQL::TextEncoder::Array.new elements_type: textenc_timestamp, needs_quotation: false }
			let!(:textdec_timestamp_array) { YSQL::TextDecoder::Array.new elements_type: textdec_timestamp, needs_quotation: false }
			let!(:textenc_string_array_with_delimiter) { YSQL::TextEncoder::Array.new elements_type: textenc_string, delimiter: ';' }
			let!(:textdec_string_array_with_delimiter) { YSQL::TextDecoder::Array.new elements_type: textdec_string, delimiter: ';' }
			let!(:textdec_bytea_array) { YSQL::TextDecoder::Array.new elements_type: textdec_bytea }

			#
			# Array parser specs are thankfully borrowed from here:
			# https://github.com/dockyard/pg_array_parser
			#
			describe '#decode' do
				context 'one dimensional arrays' do
					context 'empty' do
						it 'returns an empty array' do
							expect( textdec_string_array.decode(%[{}]) ).to eq( [] )
						end
					end

					context 'no strings' do
						it 'returns an array of strings' do
							expect( textdec_string_array.decode(%[{1,2,3}]) ).to eq( ['1','2','3'] )
						end
					end

					context 'NULL values' do
						it 'returns an array of strings, with nils replacing NULL characters' do
							expect( textdec_string_array.decode(%[{1,NULL,NULL}]) ).to eq( ['1',nil,nil] )
						end
					end

					context 'quoted NULL' do
						it 'returns an array with the word NULL' do
							expect( textdec_string_array.decode(%[{1,"NULL",3}]) ).to eq( ['1','NULL','3'] )
						end
					end

					context 'strings' do
						it 'returns an array of strings when containing commas in a quoted string' do
							expect( textdec_string_array.decode(%[{1,"2,3",4}]) ).to eq( ['1','2,3','4'] )
						end

						it 'returns an array of strings when containing an escaped quote' do
							expect( textdec_string_array.decode(%[{1,"2\\",3",4}]) ).to eq( ['1','2",3','4'] )
						end

						it 'returns an array of strings when containing an escaped backslash' do
							expect( textdec_string_array.decode(%[{1,"2\\\\",3,4}]) ).to eq( ['1','2\\','3','4'] )
							expect( textdec_string_array.decode(%[{1,"2\\\\\\",3",4}]) ).to eq( ['1','2\\",3','4'] )
						end

						it 'returns an array containing empty strings' do
							expect( textdec_string_array.decode(%[{1,"",3,""}]) ).to eq( ['1', '', '3', ''] )
						end

						it 'returns an array containing unicode strings' do
							expect( textdec_string_array.decode(%[{"Paragraph 399(b)(i) – “valid leave” – meaning"}]) ).to eq(['Paragraph 399(b)(i) – “valid leave” – meaning'])
						end

						it 'respects a different delimiter' do
							expect( textdec_string_array_with_delimiter.decode(%[{1;2;3}]) ).to eq( ['1','2','3'] )
						end

						it 'ignores array dimensions' do
							expect( textdec_string_array.decode(%[[2:4]={1,2,3}]) ).to eq( ['1','2','3'] )
							expect( textdec_string_array.decode(%[[]={1,2,3}]) ).to eq( ['1','2','3'] )
							expect( textdec_string_array.decode(%[  [-1:+2]=  {4,3,2,1}]) ).to eq( ['4','3','2','1'] )
						end

						it 'ignores spaces after array' do
							expect( textdec_string_array.decode(%[[2:4]={1,2,3}  ]) ).to eq( ['1','2','3'] )
							expect( textdec_string_array.decode(%[{1,2,3}   ]) ).to eq( ['1','2','3'] )
						end

						describe "with malformed syntax are deprecated" do
							it 'accepts broken array dimensions' do
								expect( textdec_string_array.decode(%([2:4={1,2,3})) ).to eq([['1','2','3']])
								expect( textdec_string_array.decode(%(2:4]={1,2,3})) ).to eq([['1','2','3']])
								expect( textdec_string_array.decode(%(={1,2,3})) ).to eq([['1','2','3']])
								expect( textdec_string_array.decode(%([x]={1,2,3})) ).to eq([['1','2','3']])
								expect( textdec_string_array.decode(%([]{1,2,3})) ).to eq([['1','2','3']])
								expect( textdec_string_array.decode(%(1,2,3)) ).to eq(['','2'])
							end

							it 'accepts malformed arrays' do
								expect( textdec_string_array.decode(%({1,2,3)) ).to eq(['1','2'])
								expect( textdec_string_array.decode(%({1,2,3}})) ).to eq(['1','2','3'])
								expect( textdec_string_array.decode(%({1,2,3}x)) ).to eq(['1','2','3'])
								expect( textdec_string_array.decode(%({{1,2},{2,3})) ).to eq([['1','2'],['2','3']])
								expect( textdec_string_array.decode(%({{1,2},{2,3}}x)) ).to eq([['1','2'],['2','3']])
								expect( textdec_string_array.decode(%({[1,2},{2,3}}})) ).to eq(['[1','2'])
							end
						end

						describe "with malformed syntax are raised with pg-2.0+" do
							it 'complains about broken array dimensions' do
								expect{ textdec_string_array_raise.decode(%([2:4={1,2,3})) }.to raise_error(TypeError)
								expect{ textdec_string_array_raise.decode(%(2:4]={1,2,3})) }.to raise_error(TypeError)
								expect{ textdec_string_array_raise.decode(%(={1,2,3})) }.to raise_error(TypeError)
								expect{ textdec_string_array_raise.decode(%([x]={1,2,3})) }.to raise_error(TypeError)
								expect{ textdec_string_array_raise.decode(%([]{1,2,3})) }.to raise_error(TypeError)
								expect{ textdec_string_array_raise.decode(%(1,2,3)) }.to raise_error(TypeError)
							end

							it 'complains about malformed array' do
								expect{ textdec_string_array_raise.decode(%({1,2,3)) }.to raise_error(TypeError)
								expect{ textdec_string_array_raise.decode(%({1,2,3}})) }.to raise_error(TypeError)
								expect{ textdec_string_array_raise.decode(%({1,2,3}x)) }.to raise_error(TypeError)
								expect{ textdec_string_array_raise.decode(%({{1,2},{2,3})) }.to raise_error(TypeError)
								expect{ textdec_string_array_raise.decode(%({{1,2},{2,3}}x)) }.to raise_error(TypeError)
								expect{ textdec_string_array_raise.decode(%({[1,2},{2,3}}})) }.to raise_error(TypeError)
							end
						end
					end

					context 'bytea' do
						it 'returns an array of binary strings' do
							expect( textdec_bytea_array.decode(%[{"\\\\x00010203EF","2,3",\\377}]) ).to eq( ["\x00\x01\x02\x03\xef".b,"2,3".b,"\xff".b] )
						end
					end

				end

				context 'two dimensional arrays' do
					context 'empty' do
						it 'returns an empty array' do
							expect( textdec_string_array.decode(%[{{}}]) ).to eq( [[]] )
							expect( textdec_string_array.decode(%[{{},{}}]) ).to eq( [[],[]] )
						end
					end
					context 'no strings' do
						it 'returns an array of strings with a sub array' do
							expect( textdec_string_array.decode(%[{1,{2,3},4}]) ).to eq( ['1',['2','3'],'4'] )
						end
					end
					context 'strings' do
						it 'returns an array of strings with a sub array' do
							expect( textdec_string_array.decode(%[{1,{"2,3"},4}]) ).to eq( ['1',['2,3'],'4'] )
						end
						it 'returns an array of strings with a sub array and a quoted }' do
							expect( textdec_string_array.decode(%[{1,{"2,}3",NULL},4}]) ).to eq( ['1',['2,}3',nil],'4'] )
						end
						it 'returns an array of strings with a sub array and a quoted {' do
							expect( textdec_string_array.decode(%[{1,{"2,{3"},4}]) ).to eq( ['1',['2,{3'],'4'] )
						end
						it 'returns an array of strings with a sub array and a quoted { and escaped quote' do
							expect( textdec_string_array.decode(%[{1,{"2\\",{3"},4}]) ).to eq( ['1',['2",{3'],'4'] )
						end
						it 'returns an array of strings with a sub array with empty strings' do
							expect( textdec_string_array.decode(%[{1,{""},4,{""}}]) ).to eq( ['1',[''],'4',['']] )
						end
					end
					context 'timestamps' do
						it 'decodes an array of timestamps with sub arrays' do
							expect( textdec_timestamp_array.decode('{2014-12-31 00:00:00,{NULL,2016-01-02 23:23:59.0000000}}') ).
								to eq( [Time.new(2014,12,31),[nil, Time.new(2016,01,02, 23, 23, 59)]] )
						end
					end
				end
				context 'three dimensional arrays' do
					context 'empty' do
						it 'returns an empty array' do
							expect( textdec_string_array.decode(%[{{{}}}]) ).to eq( [[[]]] )
							expect( textdec_string_array.decode(%[{{{},{}},{{},{}}}]) ).to eq( [[[],[]],[[],[]]] )
						end
					end
					it 'returns an array of strings with sub arrays' do
						expect( textdec_string_array.decode(%[{1,{2,{3,4}},{NULL,6},7}]) ).to eq( ['1',['2',['3','4']],[nil,'6'],'7'] )
					end
				end

				it 'should decode array of types with decoder in ruby space' do
					array_type = YSQL::TextDecoder::Array.new elements_type: intdec_incrementer
					expect( array_type.decode(%[{3,4}]) ).to eq( [4,5] )
				end

				it 'should decode array of nil types' do
					array_type = YSQL::TextDecoder::Array.new elements_type: nil
					expect( array_type.decode(%[{3,4}]) ).to eq( ['3','4'] )
				end
			end

			describe '#encode' do
				context 'three dimensional arrays' do
					it 'encodes an array of strings and numbers with sub arrays' do
						expect( textenc_string_array.encode(['1',['2',['3','4']],[nil,6],7.8]) ).to eq( %[{1,{2,{3,4}},{NULL,6},7.8}] )
					end
					it 'encodes an array of strings with quotes' do
						expect( textenc_string_array.encode(['',[' ',['{','}','\\',',','"','\t']]]) ).to eq( %[{"",{" ",{"{","}","\\\\",",","\\"","\\\\t"}}}] )
					end
					it 'encodes an array of int8 with sub arrays' do
						expect( textenc_int_array.encode([1,[2,[3,4]],[nil,6],7]) ).to eq( %[{1,{2,{3,4}},{NULL,6},7}] )
					end
					it 'encodes an array of int8 with strings' do
						expect( textenc_int_array.encode(['1',['2'],'3']) ).to eq( %[{1,{2},3}] )
					end
					it 'encodes an array of float8 with sub arrays' do
						expect( textenc_float_array.encode([1000.11,[-0.00000221,[3.31,-441]],[nil,6.61],-7.71]) ).to match(Regexp.new(%[^{1000.1*,{-2.2*e-*6,{3.3*,-441.0}},{NULL,6.6*},-7.7*}$].gsub(/([\.\+\{\}\,])/, "\\\\\\1").gsub(/\*/, "\\d*")))
					end
				end
				context 'two dimensional arrays' do
					it 'encodes an array of timestamps with sub arrays' do
						expect( textenc_timestamp_array.encode([Time.new(2014,12,31),[nil, Time.new(2016,01,02, 23, 23, 59.99)]]) ).
								to eq( %[{2014-12-31 00:00:00.000000000,{NULL,2016-01-02 23:23:59.990000000}}] )
					end
				end
				context 'one dimensional array' do
					it 'can encode empty arrays' do
						expect( textenc_int_array.encode([]) ).to eq( '{}' )
						expect( textenc_string_array.encode([]) ).to eq( '{}' )
					end
					it 'encodes an array of NULL strings w/wo quotes' do
						expect( textenc_string_array.encode(['NUL', 'NULL', 'NULLL', 'nul', 'null', 'nulll']) ).to eq( %[{NUL,"NULL",NULLL,nul,"null",nulll}] )
					end
					it 'respects a different delimiter' do
						expect( textenc_string_array_with_delimiter.encode(['a','b,','c']) ).to eq( '{a;b,;c}' )
					end
				end

				context 'array of types with encoder in ruby space' do
					it 'encodes with quotation and default character encoding' do
						array_type = YSQL::TextEncoder::Array.new elements_type: intenc_incrementer, needs_quotation: true
						r = array_type.encode([3,4])
						expect( r ).to eq( %[{"4 ","5 "}] )
						expect( r.encoding ).to eq( Encoding::ASCII_8BIT )
					end

					it 'encodes with quotation and given character encoding' do
						array_type = YSQL::TextEncoder::Array.new elements_type: intenc_incrementer, needs_quotation: true
						r = array_type.encode([3,4], Encoding::CP850)
						expect( r ).to eq( %[{"4 ","5 "}] )
						expect( r.encoding ).to eq( Encoding::CP850 )
					end

					it 'encodes without quotation' do
						array_type = YSQL::TextEncoder::Array.new elements_type: intenc_incrementer, needs_quotation: false
						expect( array_type.encode([3,4]) ).to eq( %[{4 ,5 }] )
					end

					it 'encodes with default character encoding' do
						array_type = YSQL::TextEncoder::Array.new elements_type: intenc_incrementer_with_encoding
						r = array_type.encode([3,4])
						expect( r ).to eq( %[{"4 ASCII-8BIT","5 ASCII-8BIT"}] )
						expect( r.encoding ).to eq( Encoding::ASCII_8BIT )
					end

					it 'encodes with given character encoding' do
						array_type = YSQL::TextEncoder::Array.new elements_type: intenc_incrementer_with_encoding
						r = array_type.encode([3,4], Encoding::CP850)
						expect( r ).to eq( %[{"4 CP850","5 CP850"}] )
						expect( r.encoding ).to eq( Encoding::CP850 )
					end

					it "should raise when ruby encoder returns non string values" do
						array_type = YSQL::TextEncoder::Array.new elements_type: intenc_incrementer_with_int_result, needs_quotation: false
						expect{ array_type.encode([3,4]) }.to raise_error(TypeError)
					end
				end

				it "should pass through non Array inputs" do
					expect( textenc_float_array.encode("text") ).to eq( "text" )
					expect( textenc_float_array.encode(1234) ).to eq( "1234" )
				end

				context 'literal quotation' do
					it 'should quote and escape literals' do
						quoted_type = YSQL::TextEncoder::QuotedLiteral.new elements_type: textenc_string_array
						expect( quoted_type.encode(["'A\",","\\B'"]) ).to eq( %['{"''A\\",","\\\\B''"}'] )
					end

					it 'should quote literals with correct character encoding' do
						quoted_type = YSQL::TextEncoder::QuotedLiteral.new elements_type: textenc_string_array
						v = quoted_type.encode(["Héllo"], "iso-8859-1")
						expect( v.encoding ).to eq( Encoding::ISO_8859_1 )
						expect( v ).to eq( %['{Héllo}'].encode(Encoding::ISO_8859_1) )
					end
				end
			end

			it "should be possible to marshal encoders" do
				mt = Marshal.dump(textenc_int_array)
				lt = Marshal.load(mt)
				expect( lt.to_h ).to eq( textenc_int_array.to_h )
			end

			it "should be possible to marshal decoders" do
				mt = Marshal.dump(textdec_string_array_raise)
				lt = Marshal.load(mt)
				expect( lt.to_h ).to eq( textdec_string_array_raise.to_h )
			end

			it "should respond to to_h" do
				expect( textenc_int_array.to_h ).to eq( {
					name: nil, oid: 0, format: 0, flags: 0,
					elements_type: textenc_int, needs_quotation: false, delimiter: ','
				} )
			end

			it "shouldn't accept invalid elements_types" do
				expect{ YSQL::TextEncoder::Array.new elements_type: false }.to raise_error(TypeError)
			end

			it "should have reasonable default values" do
				t = YSQL::TextEncoder::Array.new
				expect( t.format ).to eq( 0 )
				expect( t.oid ).to eq( 0 )
				expect( t.name ).to be_nil
				expect( t.needs_quotation? ).to eq( true )
				expect( t.delimiter ).to eq( ',' )
				expect( t.elements_type ).to be_nil
			end

		it "should deny changes when frozen" do
			t = YSQL::TextEncoder::Array.new.freeze
			expect{ t.format = 1 }.to raise_error(FrozenError)
			expect{ t.oid = 0  }.to raise_error(FrozenError)
			expect{ t.name = "x" }.to raise_error(FrozenError)
			expect{ t.needs_quotation = true }.to raise_error(FrozenError)
			expect{ t.delimiter = ","  }.to raise_error(FrozenError)
			expect{ t.elements_type = nil }.to raise_error(FrozenError)
		end

		it "should be shareable for Ractor", :ractor do
			t = YSQL::TextEncoder::Array.new.freeze
			Ractor.make_shareable(t)
		end

		it "should give account about memory usage" do
			expect( ObjectSpace.memsize_of(textenc_int_array) ).to be > DATA_OBJ_MEMSIZE
			expect( ObjectSpace.memsize_of(textdec_bytea_array) ).to be > DATA_OBJ_MEMSIZE
		end
	end

		it "should encode Strings as base64 in TextEncoder" do
			e = YSQL::TextEncoder::ToBase64.new
			expect( e.encode("") ).to eq("")
			expect( e.encode("x") ).to eq("eA==")
			expect( e.encode("xx") ).to eq("eHg=")
			expect( e.encode("xxx") ).to eq("eHh4")
			expect( e.encode("xxxx") ).to eq("eHh4eA==")
			expect( e.encode("xxxxx") ).to eq("eHh4eHg=")
			expect( e.encode("\0\n\t") ).to eq("AAoJ")
			expect( e.encode("(\xFBm") ).to eq("KPtt")
		end

		it 'should encode Strings as base64 with correct character encoding' do
			e = YSQL::TextEncoder::ToBase64.new
			v = e.encode("Héllo".encode("utf-16le"), "iso-8859-1")
			expect( v ).to eq("SOlsbG8=")
			expect( v.encoding ).to eq(Encoding::ISO_8859_1)
		end

		it "should encode Strings as base64 in BinaryDecoder" do
			e = YSQL::BinaryDecoder::ToBase64.new
			expect( e.decode("x") ).to eq("eA==")
			v = e.decode("Héllo".encode("utf-16le"))
			expect( v ).to eq("SADpAGwAbABvAA==")
			expect( v.encoding ).to eq(Encoding::ASCII_8BIT)
		end

		it "should encode Integers as base64" do
			# Not really useful, but ensures that two-pass element and composite element encoders work.
			e = YSQL::TextEncoder::ToBase64.new(elements_type: YSQL::TextEncoder::Array.new(elements_type: YSQL::TextEncoder::Integer.new, needs_quotation: false ))
			expect( e.encode([1]) ).to eq(["{1}"].pack("m").chomp)
			expect( e.encode([12]) ).to eq(["{12}"].pack("m").chomp)
			expect( e.encode([123]) ).to eq(["{123}"].pack("m").chomp)
			expect( e.encode([1234]) ).to eq(["{1234}"].pack("m").chomp)
			expect( e.encode([12345]) ).to eq(["{12345}"].pack("m").chomp)
			expect( e.encode([123456]) ).to eq(["{123456}"].pack("m").chomp)
			expect( e.encode([1234567]) ).to eq(["{1234567}"].pack("m").chomp)
		end

		it "should decode base64 to Strings in TextDecoder" do
			e = YSQL::TextDecoder::FromBase64.new
			expect( e.decode("") ).to eq("")
			expect( e.decode("eA==") ).to eq("x")
			expect( e.decode("eHg=") ).to eq("xx")
			expect( e.decode("eHh4") ).to eq("xxx")
			expect( e.decode("eHh4eA==") ).to eq("xxxx")
			expect( e.decode("eHh4eHg=") ).to eq("xxxxx")
			expect( e.decode("AAoJ") ).to eq("\0\n\t")
			expect( e.decode("KPtt") ).to eq("(\xFBm")
		end

		it "should decode base64 in BinaryEncoder" do
			e = YSQL::BinaryEncoder::FromBase64.new
			expect( e.encode("eA==") ).to eq("x")

			e = YSQL::BinaryEncoder::FromBase64.new(elements_type: YSQL::TextEncoder::Integer.new )
			expect( e.encode(124) ).to eq("124=".unpack("m")[0])
		end

		it "should decode base64 to Integers" do
			# Not really useful, but ensures that composite element encoders work.
			e = YSQL::TextDecoder::FromBase64.new(elements_type: YSQL::TextDecoder::Array.new(elements_type: YSQL::TextDecoder::Integer.new ))
			expect( e.decode(["{1}"].pack("m")) ).to eq([1])
			expect( e.decode(["{12}"].pack("m")) ).to eq([12])
			expect( e.decode(["{123}"].pack("m")) ).to eq([123])
			expect( e.decode(["{1234}"].pack("m")) ).to eq([1234])
			expect( e.decode(["{12345}"].pack("m")) ).to eq([12345])
			expect( e.decode(["{123456}"].pack("m")) ).to eq([123456])
			expect( e.decode(["{1234567}"].pack("m")) ).to eq([1234567])
			expect( e.decode(["{12345678}"].pack("m")) ).to eq([12345678])

			e = YSQL::TextDecoder::FromBase64.new(elements_type: YSQL::BinaryDecoder::Integer.new )
			expect( e.decode("ALxhTg==") ).to eq(12345678)
		end

		it "should decode base64 with garbage" do
			e = YSQL::TextDecoder::FromBase64.new format: 1
			expect( e.decode("=") ).to eq("=".unpack("m")[0])
			expect( e.decode("==") ).to eq("==".unpack("m")[0])
			expect( e.decode("===") ).to eq("===".unpack("m")[0])
			expect( e.decode("====") ).to eq("====".unpack("m")[0])
			expect( e.decode("a=") ).to eq("a=".unpack("m")[0])
			expect( e.decode("a==") ).to eq("a==".unpack("m")[0])
			expect( e.decode("a===") ).to eq("a===".unpack("m")[0])
			expect( e.decode("a====") ).to eq("a====".unpack("m")[0])
			expect( e.decode("aa=") ).to eq("aa=".unpack("m")[0])
			expect( e.decode("aa==") ).to eq("aa==".unpack("m")[0])
			expect( e.decode("aa===") ).to eq("aa===".unpack("m")[0])
			expect( e.decode("aa====") ).to eq("aa====".unpack("m")[0])
			expect( e.decode("aaa=") ).to eq("aaa=".unpack("m")[0])
			expect( e.decode("aaa==") ).to eq("aaa==".unpack("m")[0])
			expect( e.decode("aaa===") ).to eq("aaa===".unpack("m")[0])
			expect( e.decode("aaa====") ).to eq("aaa====".unpack("m")[0])
			expect( e.decode("=aa") ).to eq("=aa=".unpack("m")[0])
			expect( e.decode("=aa=") ).to eq("=aa=".unpack("m")[0])
			expect( e.decode("=aa==") ).to eq("=aa==".unpack("m")[0])
			expect( e.decode("=aa===") ).to eq("=aa===".unpack("m")[0])
		end
	end

	describe YSQL::CopyCoder do
		describe YSQL::TextEncoder::CopyRow do
			context "with default typemap" do
				let!(:encoder) do
					YSQL::TextEncoder::CopyRow.new
				end

				it "should deny changes when frozen" do
					t = YSQL::TextEncoder::CopyRow.new.freeze
					expect{ t.format = 1 }.to raise_error(FrozenError)
					expect{ t.oid = 0  }.to raise_error(FrozenError)
					expect{ t.name = "x" }.to raise_error(FrozenError)
					expect{ t.type_map = nil }.to raise_error(FrozenError)
					expect{ t.delimiter = ","  }.to raise_error(FrozenError)
					expect{ t.null_string = "NULL" }.to raise_error(FrozenError)
				end

				it "should be shareable for Ractor", :ractor do
					t = YSQL::TextEncoder::CopyRow.new.freeze
					Ractor.make_shareable(t)
				end

				it "should give account about memory usage" do
					expect( ObjectSpace.memsize_of(encoder) ).to be > DATA_OBJ_MEMSIZE
				end

				it "should encode different types of Ruby objects" do
					expect( encoder.encode([:xyz, 123, 2456, 34567, 456789, 5678901, [1,2,3], 12.1, "abcdefg", nil]) ).
						to eq("xyz\t123\t2456\t34567\t456789\t5678901\t[1, 2, 3]\t12.1\tabcdefg\t\\N\n")
				end

				it 'should output a string with correct character encoding' do
					v = encoder.encode(["Héllo"], "iso-8859-1")
					expect( v.encoding ).to eq( Encoding::ISO_8859_1 )
					expect( v ).to eq( "Héllo\n".encode(Encoding::ISO_8859_1) )
				end
			end

			context "with TypeMapByClass" do
				let!(:tm) do
					tm = YSQL::TypeMapByClass.new
					tm[Integer] = textenc_int
					tm[Float] = intenc_incrementer
					tm[Array] = YSQL::TextEncoder::Array.new elements_type: textenc_string
					tm
				end
				let!(:encoder) do
					YSQL::TextEncoder::CopyRow.new type_map: tm
				end

				it "should have reasonable default values" do
					expect( encoder.name ).to be_nil
					expect( encoder.delimiter ).to eq( "\t" )
					expect( encoder.null_string ).to eq( "\\N" )
				end

				it "copies all attributes with #dup" do
					encoder.name = "test"
					encoder.delimiter = "#"
					encoder.null_string = "NULL"
					encoder.type_map = YSQL::TypeMapByColumn.new []
					encoder2 = encoder.dup
					expect( encoder.object_id ).to_not eq( encoder2.object_id )
					expect( encoder2.name ).to eq( "test" )
					expect( encoder2.delimiter ).to eq( "#" )
					expect( encoder2.null_string ).to eq( "NULL" )
					expect( encoder2.type_map ).to be_a_kind_of(YSQL::TypeMapByColumn )
				end

				describe '#encode' do
					it "should encode different types of Ruby objects" do
						expect( encoder.encode([]) ).to eq("\n")
						expect( encoder.encode(["a"]) ).to eq("a\n")
						expect( encoder.encode([:xyz, 123, 2456, 34567, 456789, 5678901, [1,2,3], 12.1, "abcdefg", nil]) ).
							to eq("xyz\t123\t2456\t34567\t456789\t5678901\t{1,2,3}\t13 \tabcdefg\t\\N\n")
					end

					it "should escape special characters" do
						expect( encoder.encode([" \0\t\n\r\\"]) ).to eq(" \0#\t#\n#\r#\\\n".gsub("#", "\\"))
					end

					it "should escape with different delimiter" do
						encoder.delimiter = " "
						encoder.null_string = "NULL"
						expect( encoder.encode([nil, " ", "\0", "\t", "\n", "\r", "\\"]) ).to eq("NULL #  \0 \t #\n #\r #\\\n".gsub("#", "\\"))
					end
				end
			end
		end

		describe YSQL::BinaryEncoder::CopyRow do
			context "with default typemap" do
				let!(:encoder) do
					YSQL::BinaryEncoder::CopyRow.new
				end

				it "should encode different types of Ruby objects" do
					expect( encoder.encode(["x", "yz"]) ).
						to eq("\x00\x02\x00\x00\x00\x01x\x00\x00\x00\x02yz")
				end

				it 'should output a string with correct character encoding' do
					v = encoder.encode(["Héllo"], "iso-8859-1")
					expect( v.encoding ).to eq( Encoding::ISO_8859_1 )
					expect( v.b ).to eq( "\x00\x01\x00\x00\x00\x05H\xE9llo".b )
				end
			end

			context "with TypeMapByClass" do
				let!(:tm) do
					tm = YSQL::TypeMapByClass.new
					tm[Integer] = binaryenc_int4
					tm[Float] = intenc_incrementer
					tm
				end
				let!(:encoder) do
					YSQL::BinaryEncoder::CopyRow.new type_map: tm
				end

				it "should have reasonable default values" do
					expect( encoder.name ).to be_nil
				end

				it "copies all attributes with #dup" do
					encoder.name = "test"
					encoder.type_map = YSQL::TypeMapByColumn.new []
					encoder2 = encoder.dup
					expect( encoder.object_id ).to_not eq( encoder2.object_id )
					expect( encoder2.name ).to eq( "test" )
					expect( encoder2.type_map ).to be_a_kind_of(YSQL::TypeMapByColumn )
				end

				it "should encode different types of Ruby objects" do
					expect( encoder.encode([]) ).to eq("\x00\x00")
					expect( encoder.encode(["a"]) ).to eq("\x00\x01\x00\x00\x00\x01a")
					expect( encoder.encode([:xyz, 123, 12.1, "abcdefg", nil]) ).
						to eq("\x00\x05\x00\x00\x00\x03xyz\x00\x00\x00\x04\x00\x00\x00{\x00\x00\x00\x0313 \x00\x00\x00\aabcdefg\xFF\xFF\xFF\xFF".b)
				end
			end
		end

		describe YSQL::TextDecoder::CopyRow do
			context "with default typemap" do
				let!(:decoder) do
					YSQL::TextDecoder::CopyRow.new
				end

				describe '#decode' do
					it "should decode COPY text format to array of strings" do
						expect( decoder.decode("123\t \0#\t#\n#\r#\\ \t234\t#\x01#\002\n".gsub("#", "\\"))).to eq( ["123", " \0\t\n\r\\ ", "234", "\x01\x02"] )
					end

					it 'should respect input character encoding' do
						v = decoder.decode("Héllo\n".encode("EUC-JP")).first
						expect( v.encoding ).to eq(Encoding::EUC_JP)
						expect( v ).to eq("Héllo".encode("EUC-JP"))
						expect( v.length ).to eq(5)
					end
				end
			end

			context "with TypeMapByColumn" do
				let!(:tm) do
					YSQL::TypeMapByColumn.new [textdec_int, textdec_string, intdec_incrementer, nil]
				end
				let!(:decoder) do
					YSQL::TextDecoder::CopyRow.new type_map: tm
				end

				it "should give account about memory usage" do
					expect( ObjectSpace.memsize_of(decoder) ).to be > DATA_OBJ_MEMSIZE
				end

				describe '#decode' do
					it "should decode different types of Ruby objects" do
						expect( decoder.decode("123\t \0#\t#\n#\r#\\ \t234\t#\x01#\002\n".gsub("#", "\\"))).to eq( [123, " \0\t\n\r\\ ", 235, "\x01\x02"] )
					end
				end
			end
		end

		describe YSQL::BinaryDecoder::CopyRow do
			context "with default typemap" do
				let!(:decoder) do
					YSQL::BinaryDecoder::CopyRow.new
				end

				describe '#decode' do
					it "should decode COPY binary format to array of strings" do
						expect( decoder.decode([3, -1, 2, "xy", 1, "z"].pack("nNNa*Na*")) )
								.to eq( [nil, "xy", "z"] )
					end

					it "should ignore COPY binary header before data" do
						expect( decoder.decode(["PGCOPY\n\377\r\n\0", 0, 1, "x", 3, 2, "xy", 1, "z", -1].pack("a*NNa*nNa*Na*N")) )
								.to eq( ["xy", "z", nil] )
					end

					it "should decode COPY data trailer to nil" do
						expect( decoder.decode([-1].pack("n")) )
								.to eq( nil )
					end

					it "should raise an error at grabage COPY format" do
						expect{ decoder.decode("123\t \0\\\t\\") }
								.to raise_error(ArgumentError, /premature.*at position: 7$/)
					end

					it "should raise an error at extra data after one row" do
						expect{ decoder.decode([1, -1, 2].pack("nNN")) }
								.to raise_error(ArgumentError, /trailing data.*at position: 7$/)
					end

					it "should raise an error at shortened COPY data" do
						data = [3, -1, 2, "xy", 1, "z"].pack("nNNa*Na*")
						(0 .. data.bytesize-1).each do |len|
							expect{ decoder.decode(data[0, len]) }
								.to raise_error(ArgumentError)
						end
					end
				end
			end

			context "with TypeMapByColumn" do
				let!(:tm) do
					YSQL::TypeMapByColumn.new [binarydec_integer, binarydec_string, intdec_incrementer, nil]
				end
				let!(:decoder) do
					YSQL::BinaryDecoder::CopyRow.new type_map: tm
				end

				describe '#decode' do
					it "should decode different types of Ruby objects" do
						expect( decoder.decode([4, 2, "\x01\x02", 7, " \0\t\n\r\xff ", 0, "", 3, "abc"].pack("nNa*Na*Na*Na*")) )
								.to eq( [258, " \0\t\n\r\xff ".b, 1, "abc"] )
					end
				end
			end
		end
	end

	describe YSQL::RecordCoder do
		describe YSQL::TextEncoder::Record do
			context "with default typemap" do
				let!(:encoder) do
					YSQL::TextEncoder::Record.new
				end

				it "should deny changes when frozen" do
					t = YSQL::TextEncoder::Record.new.freeze
					expect{ t.format = 1 }.to raise_error(FrozenError)
					expect{ t.oid = 0  }.to raise_error(FrozenError)
					expect{ t.name = "x" }.to raise_error(FrozenError)
					expect{ t.type_map = nil }.to raise_error(FrozenError)
				end

				it "should be shareable for Ractor", :ractor do
					t = YSQL::TextEncoder::Record.new.freeze
					Ractor.make_shareable(t)
				end

				it "should give account about memory usage" do
					expect( ObjectSpace.memsize_of(encoder) ).to be > DATA_OBJ_MEMSIZE
				end

				it "should encode different types of Ruby objects" do
					expect( encoder.encode([:xyz, 123, 2456, 34567, 456789, 5678901, [1,2,3], 12.1, "abcdefg", nil]) ).
						to eq('("xyz","123","2456","34567","456789","5678901","[1, 2, 3]","12.1","abcdefg",)')
				end

				it 'should output a string with correct character encoding' do
					v = encoder.encode(["Héllo"], "iso-8859-1")
					expect( v.encoding ).to eq( Encoding::ISO_8859_1 )
					expect( v ).to eq( '("Héllo")'.encode(Encoding::ISO_8859_1) )
				end
			end

			context "with TypeMapByClass" do
				let!(:tm) do
					tm = YSQL::TypeMapByClass.new
					tm[Integer] = textenc_int
					tm[Float] = intenc_incrementer
					tm[Array] = YSQL::TextEncoder::Array.new elements_type: textenc_string
					tm
				end
				let!(:encoder) do
					YSQL::TextEncoder::Record.new type_map: tm
				end

				it "should have reasonable default values" do
					expect( encoder.name ).to be_nil
				end

				it "copies all attributes with #dup" do
					encoder.name = "test"
					encoder.type_map = YSQL::TypeMapByColumn.new []
					encoder2 = encoder.dup
					expect( encoder.object_id ).to_not eq( encoder2.object_id )
					expect( encoder2.name ).to eq( "test" )
					expect( encoder2.type_map ).to be_a_kind_of(YSQL::TypeMapByColumn )
				end

				describe '#encode' do
					it "should encode different types of Ruby objects" do
						expect( encoder.encode([]) ).to eq("()")
						expect( encoder.encode(["a"]) ).to eq('("a")')
						expect( encoder.encode([:xyz, 123, 2456, 34567, 456789, 5678901, [1,2,3], 12.1, "abcdefg", nil]) ).
							to eq('("xyz","123","2456","34567","456789","5678901","{1,2,3}","13 ","abcdefg",)')
					end

					it "should escape special characters" do
						expect( encoder.encode([" \"\t\n\\\r"]) ).to eq("(\" \"\"\t\n##\r\")".gsub("#", "\\"))
					end
				end
			end
		end

		describe YSQL::TextDecoder::Record do
			context "with default typemap" do
				let!(:decoder) do
					YSQL::TextDecoder::Record.new
				end

				it "should give account about memory usage" do
					expect( ObjectSpace.memsize_of(decoder) ).to be > DATA_OBJ_MEMSIZE
				end

				describe '#decode' do
					it "should decode composite text format to array of strings" do
						expect( decoder.decode('("fuzzy dice",,"",42,)') ).to eq( ["fuzzy dice",nil, "", "42", nil] )
					end

					it 'should respect input character encoding' do
						v = decoder.decode("(Héllo)".encode("iso-8859-1")).first
						expect( v.encoding ).to eq(Encoding::ISO_8859_1)
						expect( v ).to eq("Héllo".encode("iso-8859-1"))
					end

					it 'should raise an error on malformed input' do
						expect{ decoder.decode('') }.to raise_error(ArgumentError, /"" - Missing left parenthesis/)
						expect{ decoder.decode('(') }.to raise_error(ArgumentError, /"\(" - Unexpected end of input/)
						expect{ decoder.decode('(\\') }.to raise_error(ArgumentError, /"\(\\" - Unexpected end of input/)
						expect{ decoder.decode('()x') }.to raise_error(ArgumentError, /"\(\)x" - Junk after right parenthesis/)
					end
				end
			end

			context "with TypeMapByColumn" do
				let!(:tm) do
					YSQL::TypeMapByColumn.new [textdec_int, textdec_string, intdec_incrementer, nil]
				end
				let!(:decoder) do
					YSQL::TextDecoder::Record.new type_map: tm
				end

				describe '#decode' do
					it "should decode different types of Ruby objects" do
						expect( decoder.decode("(123,\" #,#\n#\r#\\ \",234,#\x01#\002)".gsub("#", "\\"))).to eq( [123, " ,\n\r\\ ", 235, "\x01\x02"] )
					end
				end
			end
		end
	end
end
