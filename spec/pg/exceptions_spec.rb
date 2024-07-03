# -*- rspec -*-
# encoding: utf-8

require_relative '../helpers'

require 'ysql'

describe YSQL::Error do

	it "does have hierarchical error classes" do
		expect(YSQL::UndefinedTable.ancestors[0, 4] ).to eq([
                                                          YSQL::UndefinedTable,
                                                          YSQL::SyntaxErrorOrAccessRuleViolation,
                                                          YSQL::ServerError,
                                                          YSQL::Error
		        ])

		expect(YSQL::InvalidSchemaName.ancestors[0, 3] ).to eq([
                                                             YSQL::InvalidSchemaName,
                                                             YSQL::ServerError,
                                                             YSQL::Error
		        ])
	end

	it "can be used to raise errors without text" do
		expect{ raise YSQL::InvalidTextRepresentation }.to raise_error(YSQL::InvalidTextRepresentation)
	end

	it "should be delivered by Ractor", :ractor do
		r = Ractor.new(@conninfo) do |conninfo|
			conn = YSQL.connect(conninfo)
			conn.exec("SELECT 0/0")
		ensure
			conn&.finish
		end

		begin
			r.take
		rescue Exception => err
		end

		expect( err.cause ).to be_kind_of(YSQL::Error)
		expect{ raise err.cause }.to raise_error(YSQL::DivisionByZero, /division by zero/)
		expect{ raise err }.to raise_error(Ractor::RemoteError)
	end
end
