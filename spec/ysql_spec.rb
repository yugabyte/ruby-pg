# -*- rspec -*-
# encoding: utf-8

require_relative 'helpers'

require 'ysql'

describe YSQL do

	it "knows what version of the libpq library is loaded" do
		expect(YSQL.library_version ).to be_an(Integer )
		expect(YSQL.library_version ).to be >= 90100
	end

	it "can format the pg version" do
		expect(YSQL.version_string ).to be_an(String )
		expect(YSQL.version_string ).to match(/PG \d+\.\d+\.\d+/)
		expect(YSQL.version_string(true) ).to be_an(String )
		expect(YSQL.version_string(true) ).to match(/PG \d+\.\d+\.\d+/)
	end

	it "can select which of both security libraries to initialize" do
		# This setting does nothing here, because there is already a connection
		# to the server, at this point in time.
		YSQL.init_openssl(false, true)
		YSQL.init_openssl(1, 0)
	end

	it "can select whether security libraries to initialize" do
		# This setting does nothing here, because there is already a connection
		# to the server, at this point in time.
		YSQL.init_ssl(false)
		YSQL.init_ssl(1)
	end


	it "knows whether or not the library is threadsafe" do
		expect(YSQL ).to be_threadsafe()
	end

	it "tells about the libpq library path" do
		expect(YSQL::POSTGRESQL_LIB_PATH ).to include("/")
	end

	it "can #connect" do
		c = YSQL.connect(@conninfo)
		expect( c ).to be_a_kind_of(YSQL::Connection )
		c.close
	end

	it "can #connect with block" do
		bres = YSQL.connect(@conninfo) do |c|
			res = c.exec "SELECT 5"
			expect( res.values ).to eq( [["5"]] )
			55
		end

		expect( bres ).to eq( 55 )
	end
end
