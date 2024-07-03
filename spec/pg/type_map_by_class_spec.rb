# -*- rspec -*-
# encoding: utf-8

require_relative '../helpers'

require 'ysql'


describe YSQL::TypeMapByClass do

	let!(:textenc_int){ YSQL::TextEncoder::Integer.new(name: 'INT4', oid: 23).freeze }
	let!(:textenc_float){ YSQL::TextEncoder::Float.new(name: 'FLOAT8', oid: 701).freeze }
	let!(:textenc_string){ YSQL::TextEncoder::String.new(name: 'TEXT', oid: 25).freeze }
	let!(:binaryenc_int){ YSQL::BinaryEncoder::Int8.new(name: 'INT8', oid: 20, format: 1).freeze }
	let!(:pass_through_type) do
		type = Class.new(YSQL::SimpleEncoder) do
			def encode(*v)
				v.inspect
			end
		end.new
		type.oid = 25
		type.format = 0
		type.name = 'pass_through'
		type.freeze
	end

	let!(:tm) do
		tm = YSQL::TypeMapByClass.new
		tm[Integer] = binaryenc_int
		tm[Float] = textenc_float
		tm[Symbol] = pass_through_type
		tm.freeze
	end

	let!(:tm_writable) do
		tm_writable = YSQL::TypeMapByClass.new
		tm.coders.each do |k, v|
			tm_writable[k] = v
		end
		tm_writable
	end

	let!(:raise_class) do
		Class.new
	end

	let!(:derived_tm) do
		tm = Class.new(YSQL::TypeMapByClass) do
			def array_type_map_for(value)
				YSQL::TextEncoder::Array.new name: '_INT4', oid: 1007, elements_type: YSQL::TextEncoder::Integer.new
			end
		end.new
		tm[Integer] = proc{|value| textenc_int }
		tm[raise_class] = proc{|value| /invalid/ }
		tm[Array] = :array_type_map_for
		tm.freeze
	end

	it "should deny changes when frozen" do
		expect{ tm.default_type_map = YSQL::TypeMapByClass.new }.to raise_error(FrozenError)
		expect{ tm[Integer] = nil }.to raise_error(FrozenError)
	end

	it "should be shareable for Ractor", :ractor do
		Ractor.make_shareable(tm)
	end

	it "should give account about memory usage" do
		expect( ObjectSpace.memsize_of(tm) ).to be > DATA_OBJ_MEMSIZE
	end

	it "should retrieve all conversions" do
		expect( tm.coders ).to eq( {
			Integer => binaryenc_int,
			Float => textenc_float,
			Symbol => pass_through_type,
		} )
	end

	it "should retrieve particular conversions" do
		expect( tm[Integer] ).to eq(binaryenc_int)
		expect( tm[Float] ).to eq(textenc_float)
		expect( tm[Range] ).to be_nil
		expect( derived_tm[raise_class] ).to be_kind_of(Proc)
		expect( derived_tm[Array] ).to eq(:array_type_map_for)
	end

	it "should allow deletion of coders" do
		tm_writable[Integer] = nil
		expect( tm_writable[Integer] ).to be_nil
		expect( tm_writable.coders ).to eq( {
			Float => textenc_float,
			Symbol => pass_through_type,
		} )
	end

	it "forwards query param conversions to the #default_type_map" do
		tm1 = YSQL::TypeMapByColumn.new([textenc_int, nil, nil] )

		tm2 = YSQL::TypeMapByClass.new
		tm2[Integer] = YSQL::TextEncoder::Integer.new name: 'INT2', oid: 21
		tm2.default_type_map = tm1

		res = @conn.exec_params( "SELECT $1, $2, $3::TEXT", ['1', 2, 3], 0, tm2 )

		expect( res.ftype(0) ).to eq( 23 ) # tm1
		expect( res.ftype(1) ).to eq( 21 ) # tm2
		expect( res.getvalue(0,2) ).to eq( "3" ) # TypeMapAllStrings
	end

	#
	# Decoding Examples
	#

	it "should raise an error when used for results" do
		res = @conn.exec_params( "SELECT 1", [], 1 )
		expect{ res.type_map = tm }.to raise_error(NotImplementedError, /not suitable to map result values/)
	end

	#
	# Encoding Examples
	#

	it "should allow mixed type conversions" do
		res = @conn.exec_params( "SELECT $1, $2, $3", [5, 1.23, :TestSymbol], 0, tm )
		expect( res.values ).to eq([['5', '1.23', "[:TestSymbol, #{@conn.internal_encoding.inspect}]"]])
		expect( res.ftype(0) ).to eq(20)
	end

	it "should expire the cache after changes to the coders" do
		res = @conn.exec_params( "SELECT $1", [5], 0, tm_writable )
		expect( res.ftype(0) ).to eq(20)

		tm_writable[Integer] = textenc_int

		res = @conn.exec_params( "SELECT $1", [5], 0, tm_writable )
		expect( res.ftype(0) ).to eq(23)
	end

	it "should allow mixed type conversions with derived type map" do
		res = @conn.exec_params( "SELECT $1, $2", [6, [7]], 0, derived_tm )
		expect( res.values ).to eq([['6', '{7}']])
		expect( res.ftype(0) ).to eq(23)
		expect( res.ftype(1) ).to eq(1007)
	end

	it "should raise TypeError with derived type map" do
		expect{
			@conn.exec_params( "SELECT $1", [raise_class.new], 0, derived_tm )
		}.to raise_error(TypeError, /wrong argument type Regexp/)
	end

	it "should raise error on invalid coder object" do
		tm_writable[TrueClass] = "dummy"
		expect{
			@conn.exec_params( "SELECT $1", [true], 0, tm_writable )
		}.to raise_error(NoMethodError, /undefined method.*call/)
	end
end
