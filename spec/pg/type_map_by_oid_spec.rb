# -*- rspec -*-
# encoding: utf-8

require_relative '../helpers'

require 'ysql'


describe YugabyteYSQL::TypeMapByOid do

	let!(:textdec_int){ YugabyteYSQL::TextDecoder::Integer.new(name: 'INT4', oid: 23).freeze }
	let!(:textdec_float){ YugabyteYSQL::TextDecoder::Float.new(name: 'FLOAT8', oid: 701).freeze }
	let!(:textdec_string){ YugabyteYSQL::TextDecoder::String.new(name: 'TEXT', oid: 25).freeze }
	let!(:textdec_bytea){ YugabyteYSQL::TextDecoder::Bytea.new(name: 'BYTEA', oid: 17).freeze }
	let!(:binarydec_float){ YugabyteYSQL::BinaryDecoder::Float.new(name: 'FLOAT8', oid: 701, format: 1).freeze }
	let!(:pass_through_type) do
		type = Class.new(YugabyteYSQL::SimpleDecoder) do
			def decode(*v)
				v
			end
		end.new
		type.oid = 1082
		type.format = 0
		type.name = 'pass_through'
		type.freeze
	end

	let!(:tm) do
		tm = YugabyteYSQL::TypeMapByOid.new
		tm.add_coder textdec_int
		tm.add_coder textdec_float
		tm.add_coder binarydec_float
		tm.add_coder pass_through_type
		tm.freeze
	end

	let!(:tm_writable) do
		tm_writable = YugabyteYSQL::TypeMapByOid.new
		tm.coders.each do |c|
			tm_writable.add_coder c
		end
		tm_writable
	end

	it "should deny changes when frozen" do
		tm = YugabyteYSQL::TypeMapByOid.new.freeze
		expect{ tm.default_type_map = YugabyteYSQL::TypeMapByClass.new }.to raise_error(FrozenError)
		expect{ tm.with_default_type_map(YugabyteYSQL::TypeMapByClass.new) }.to raise_error(FrozenError)
		expect{ tm.add_coder textdec_int }.to raise_error(FrozenError)
	end

	it "should be shareable for Ractor", :ractor do
		tm = YugabyteYSQL::TypeMapByOid.new.freeze
		Ractor.make_shareable(tm)
	end

	it "should give account about memory usage" do
		tm = YugabyteYSQL::TypeMapByOid.new
		expect( ObjectSpace.memsize_of(tm) ).to be > DATA_OBJ_MEMSIZE
	end

	it "should retrieve it's conversions" do
		expect( tm.coders ).to eq( [
			textdec_int,
			textdec_float,
			pass_through_type,
			binarydec_float,
		] )
	end

	it "should allow deletion of coders" do
		expect( tm_writable.rm_coder 0, 701 ).to eq(textdec_float)
		expect( tm_writable.rm_coder 0, 701 ).to eq(nil)
		expect( tm_writable.rm_coder 1, 701 ).to eq(binarydec_float)
		expect( tm_writable.coders ).to eq( [
			textdec_int,
			pass_through_type,
		] )
	end

	it "should check format when deleting coders" do
		expect{ tm_writable.rm_coder(2, 123) }.to raise_error(ArgumentError)
		expect{ tm_writable.rm_coder(-1, 123) }.to raise_error(ArgumentError)
	end

	it "should check format when adding coders" do
		textdec_int = YugabyteYSQL::TextDecoder::Integer.new(name: 'INT4', oid: 23)
		textdec_int.format = 2
		expect{ tm_writable.add_coder textdec_int }.to raise_error(ArgumentError)
		textdec_int.format = -1
		expect{ tm_writable.add_coder textdec_int }.to raise_error(ArgumentError)
	end

	it "should check coder type when adding coders" do
		expect{ tm_writable.add_coder :dummy }.to raise_error(TypeError)
	end

	it "should allow reading and writing max_rows_for_online_lookup" do
		tm = YugabyteYSQL::TypeMapByOid.new
		expect( tm.max_rows_for_online_lookup ).to eq(10)
		tm.max_rows_for_online_lookup = 5
		expect( tm.max_rows_for_online_lookup ).to eq(5)
	end

	it "should allow building new TypeMapByColumn for a given result" do
		res = @conn.exec( "SELECT 1, 'a', 2.0::FLOAT, '2013-06-30'::DATE" )
		tm2 = tm.build_column_map(res)
		expect( tm2 ).to be_a_kind_of(YugabyteYSQL::TypeMapByColumn)
		expect( tm2.coders ).to eq( [textdec_int, nil, textdec_float, pass_through_type] )
	end

	it "forwards result value conversions to another TypeMapByOid as #default_type_map" do
		# One run with implicit built TypeMapByColumn and another with online lookup
		# for each type map.
		[[0, 0], [0, 10], [10, 0], [10, 10]].each do |max_rows1, max_rows2|
			tm1 = YugabyteYSQL::TypeMapByOid.new
			tm1.add_coder YugabyteYSQL::TextDecoder::Integer.new name: 'INT2', oid: 21
			tm1.max_rows_for_online_lookup = max_rows1

			tm2 = YugabyteYSQL::TypeMapByOid.new
			tm2.add_coder YugabyteYSQL::TextDecoder::Integer.new name: 'INT4', oid: 23
			tm2.max_rows_for_online_lookup = max_rows2
			tm2.default_type_map = tm1

			res = @conn.exec( "SELECT '1'::INT4, '2'::INT2, '3'::INT8" ).map_types!( tm2 )

			expect( res.getvalue(0,0) ).to eq( 1 ) # tm2
			expect( res.getvalue(0,1) ).to eq( 2 ) # tm1
			expect( res.getvalue(0,2) ).to eq( "3" ) # TypeMapAllStrings
		end
	end

	#
	# Decoding Examples text format
	#

	it "should allow mixed type conversions in text format" do
		res = @conn.exec( "SELECT 1, 'a', 2.0::FLOAT, '2013-06-30'::DATE" )
		res.type_map = tm
		expect( res.values ).to eq( [[1, 'a', 2.0, ['2013-06-30', 0, 3] ]] )
	end

	it "should build a TypeMapByColumn when assigned and the number of rows is high enough" do
		res = @conn.exec( "SELECT generate_series(1,20), 'a', 2.0::FLOAT, '2013-06-30'::DATE" )
		res.type_map = tm
		expect( res.type_map ).to be_kind_of(YugabyteYSQL::TypeMapByColumn )
		expect( res.type_map.coders ).to eq( [textdec_int, nil, textdec_float, pass_through_type] )
	end

	it "should use TypeMapByOid for online lookup and the number of rows is low enough" do
		res = @conn.exec( "SELECT 1, 'a', 2.0::FLOAT, '2013-06-30'::DATE" )
		res.type_map = tm
		expect( res.type_map ).to be_kind_of(YugabyteYSQL::TypeMapByOid )
	end

	#
	# Decoding Examples binary format
	#

	it "should allow mixed type conversions in binary format" do
		res = @conn.exec_params( "SELECT 1, 2.0::FLOAT", [], 1 )
		res.type_map = tm
		expect( res.values ).to eq( [["\x00\x00\x00\x01", 2.0 ]] )
	end

	#
	# Encoding Examples
	#

	it "should raise an error used for query params" do
		expect{
			@conn.exec_params( "SELECT $1", [5], 0, tm )
		}.to raise_error(NotImplementedError, /not suitable to map query params/)
	end

end
