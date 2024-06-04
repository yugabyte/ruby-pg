# -*- rspec -*-
# encoding: utf-8

require_relative 'helpers'

require 'yugabyte_ysql'

describe YugabyteYSQL do

	it "knows what version of the libpq library is loaded" do
		expect(YugabyteYSQL.library_version ).to be_an(Integer )
		expect(YugabyteYSQL.library_version ).to be >= 90100
	end

	it "can format the pg version" do
		expect(YugabyteYSQL.version_string ).to be_an(String )
		expect(YugabyteYSQL.version_string ).to match(/PG \d+\.\d+\.\d+/)
		expect(YugabyteYSQL.version_string(true) ).to be_an(String )
		expect(YugabyteYSQL.version_string(true) ).to match(/PG \d+\.\d+\.\d+/)
	end

	it "can select which of both security libraries to initialize" do
		# This setting does nothing here, because there is already a connection
		# to the server, at this point in time.
		YugabyteYSQL.init_openssl(false, true)
		YugabyteYSQL.init_openssl(1, 0)
	end

	it "can select whether security libraries to initialize" do
		# This setting does nothing here, because there is already a connection
		# to the server, at this point in time.
		YugabyteYSQL.init_ssl(false)
		YugabyteYSQL.init_ssl(1)
	end


	it "knows whether or not the library is threadsafe" do
		expect(YugabyteYSQL ).to be_threadsafe()
	end

	it "tells about the libpq library path" do
		expect(YugabyteYSQL::POSTGRESQL_LIB_PATH ).to include("/")
	end

	it "can #connect" do
		c = YugabyteYSQL.connect(@conninfo)
		expect( c ).to be_a_kind_of(YugabyteYSQL::Connection )
		c.close
	end

	it "can #connect with block" do
		bres = YugabyteYSQL.connect(@conninfo) do |c|
			res = c.exec "SELECT 5"
			expect( res.values ).to eq( [["5"]] )
			55
		end

		expect( bres ).to eq( 55 )
	end
end
