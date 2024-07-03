# -*- rspec -*-
# encoding: utf-8

require_relative '../helpers'

describe 'Basic type mapping' do
	describe YSQL::BasicTypeRegistry do
		it "should be shareable for Ractor", :ractor do
			Ractor.make_shareable(YSQL::BasicTypeRegistry.new.register_default_types)
		end

		it "can register_type" do
			regi = YSQL::BasicTypeRegistry.new
			res = regi.register_type(1, 'int4', YSQL::BinaryEncoder::Int8, YSQL::BinaryDecoder::Integer)

			expect( res ).to be( regi )
			expect( regi.coders_for(1, :encoder)['int4'] ).to be_kind_of(YSQL::BinaryEncoder::Int8)
			expect( regi.coders_for(1, :decoder)['int4'] ).to be_kind_of(YSQL::BinaryDecoder::Integer)
		end

		it "can alias_type" do
			regi = YSQL::BasicTypeRegistry.new
			regi.register_type(1, 'int4', YSQL::BinaryEncoder::Int4, YSQL::BinaryDecoder::Integer)
			res = regi.alias_type(1, 'int8', 'int4')

			expect( res ).to be( regi )
			expect( regi.coders_for(1, :encoder)['int8'] ).to be_kind_of(YSQL::BinaryEncoder::Int4)
			expect( regi.coders_for(1, :decoder)['int8'] ).to be_kind_of(YSQL::BinaryDecoder::Integer)
		end

		it "can register_default_types" do
			regi = YSQL::BasicTypeRegistry.new
			res = regi.register_default_types

			expect( res ).to be( regi )
			expect( regi.coders_for(0, :encoder)['float8'] ).to be_kind_of(YSQL::TextEncoder::Float)
			expect( regi.coders_for(0, :decoder)['float8'] ).to be_kind_of(YSQL::TextDecoder::Float)
		end

		it "can define_default_types (alias to register_default_types)" do
			regi = YSQL::BasicTypeRegistry.new
			res = regi.define_default_types

			expect( res ).to be( regi )
			expect( regi.coders_for(0, :encoder)['float8'] ).to be_kind_of(YSQL::TextEncoder::Float)
			expect( regi.coders_for(0, :decoder)['float8'] ).to be_kind_of(YSQL::TextDecoder::Float)
		end

		it "can register_coder" do
			regi = YSQL::BasicTypeRegistry.new
			enco = YSQL::BinaryEncoder::Int8.new(name: 'test')
			res = regi.register_coder(enco)

			expect( res ).to be( regi )
			expect( regi.coders_for(1, :encoder)['test'] ).to be(enco)
			expect( regi.coders_for(1, :decoder)['test'] ).to be_nil
		end

		it "checks format and direction in coders_for" do
			regi = YSQL::BasicTypeRegistry.new
			expect( regi.coders_for 0, :encoder ).to eq( nil )
			expect{ regi.coders_for 0, :coder }.to raise_error( ArgumentError )
			expect{ regi.coders_for 2, :encoder }.to raise_error( ArgumentError )
		end
	end
end
