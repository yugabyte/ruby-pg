# -*- rspec -*-
# encoding: utf-8

require_relative '../helpers'

require 'yugabyte_ysql'

describe YugabyteYSQL::Error do

	it "does have hierarchical error classes" do
		expect(YugabyteYSQL::UndefinedTable.ancestors[0, 4] ).to eq([
                                                                  YugabyteYSQL::UndefinedTable,
                                                                  YugabyteYSQL::SyntaxErrorOrAccessRuleViolation,
                                                                  YugabyteYSQL::ServerError,
                                                                  YugabyteYSQL::Error
		        ])

		expect(YugabyteYSQL::InvalidSchemaName.ancestors[0, 3] ).to eq([
                                                                     YugabyteYSQL::InvalidSchemaName,
                                                                     YugabyteYSQL::ServerError,
                                                                     YugabyteYSQL::Error
		        ])
	end

	it "can be used to raise errors without text" do
		expect{ raise YugabyteYSQL::InvalidTextRepresentation }.to raise_error(YugabyteYSQL::InvalidTextRepresentation)
	end

	it "should be delivered by Ractor", :ractor do
		r = Ractor.new(@conninfo) do |conninfo|
			conn = YugabyteYSQL.connect(conninfo)
			conn.exec("SELECT 0/0")
		ensure
			conn&.finish
		end

		begin
			r.take
		rescue Exception => err
		end

		expect( err.cause ).to be_kind_of(YugabyteYSQL::Error)
		expect{ raise err.cause }.to raise_error(YugabyteYSQL::DivisionByZero, /division by zero/)
		expect{ raise err }.to raise_error(Ractor::RemoteError)
	end
end
