# -*- rspec -*-
# encoding: utf-8

require_relative '../helpers'

require 'ysql'


describe YSQL::TypeMap do
	let!(:tm){ YSQL::TypeMap.new.freeze }

	it "should give account about memory usage" do
		expect( ObjectSpace.memsize_of(tm) ).to be > DATA_OBJ_MEMSIZE
	end

	it "should raise an error when used for param type casts" do
		expect{
			@conn.exec_params( "SELECT $1", [5], 0, tm )
		}.to raise_error(NotImplementedError, /not suitable to map query params/)
	end

	it "should raise an error when used for result type casts" do
		res = @conn.exec( "SELECT 1" )
		expect{ res.map_types!(tm) }.to raise_error(NotImplementedError, /not suitable to map result values/)
	end

	it "should be shareable for Ractor", :ractor do
		Ractor.make_shareable(tm)
	end

end
