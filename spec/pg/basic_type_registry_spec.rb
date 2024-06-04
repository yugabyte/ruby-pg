# -*- rspec -*-
# encoding: utf-8

require_relative '../helpers'

describe 'Basic type mapping' do
	describe YugabyteYSQL::BasicTypeRegistry do
		it "should be shareable for Ractor", :ractor do
			Ractor.make_shareable(YugabyteYSQL::BasicTypeRegistry.new.register_default_types)
		end

		it "can register_type" do
			regi = YugabyteYSQL::BasicTypeRegistry.new
			res = regi.register_type(1, 'int4', YugabyteYSQL::BinaryEncoder::Int8, YugabyteYSQL::BinaryDecoder::Integer)

			expect( res ).to be( regi )
			expect( regi.coders_for(1, :encoder)['int4'] ).to be_kind_of(YugabyteYSQL::BinaryEncoder::Int8)
			expect( regi.coders_for(1, :decoder)['int4'] ).to be_kind_of(YugabyteYSQL::BinaryDecoder::Integer)
		end

		it "can alias_type" do
			regi = YugabyteYSQL::BasicTypeRegistry.new
			regi.register_type(1, 'int4', YugabyteYSQL::BinaryEncoder::Int4, YugabyteYSQL::BinaryDecoder::Integer)
			res = regi.alias_type(1, 'int8', 'int4')

			expect( res ).to be( regi )
			expect( regi.coders_for(1, :encoder)['int8'] ).to be_kind_of(YugabyteYSQL::BinaryEncoder::Int4)
			expect( regi.coders_for(1, :decoder)['int8'] ).to be_kind_of(YugabyteYSQL::BinaryDecoder::Integer)
		end

		it "can register_default_types" do
			regi = YugabyteYSQL::BasicTypeRegistry.new
			res = regi.register_default_types

			expect( res ).to be( regi )
			expect( regi.coders_for(0, :encoder)['float8'] ).to be_kind_of(YugabyteYSQL::TextEncoder::Float)
			expect( regi.coders_for(0, :decoder)['float8'] ).to be_kind_of(YugabyteYSQL::TextDecoder::Float)
		end

		it "can define_default_types (alias to register_default_types)" do
			regi = YugabyteYSQL::BasicTypeRegistry.new
			res = regi.define_default_types

			expect( res ).to be( regi )
			expect( regi.coders_for(0, :encoder)['float8'] ).to be_kind_of(YugabyteYSQL::TextEncoder::Float)
			expect( regi.coders_for(0, :decoder)['float8'] ).to be_kind_of(YugabyteYSQL::TextDecoder::Float)
		end

		it "can register_coder" do
			regi = YugabyteYSQL::BasicTypeRegistry.new
			enco = YugabyteYSQL::BinaryEncoder::Int8.new(name: 'test')
			res = regi.register_coder(enco)

			expect( res ).to be( regi )
			expect( regi.coders_for(1, :encoder)['test'] ).to be(enco)
			expect( regi.coders_for(1, :decoder)['test'] ).to be_nil
		end

		it "checks format and direction in coders_for" do
			regi = YugabyteYSQL::BasicTypeRegistry.new
			expect( regi.coders_for 0, :encoder ).to eq( nil )
			expect{ regi.coders_for 0, :coder }.to raise_error( ArgumentError )
			expect{ regi.coders_for 2, :encoder }.to raise_error( ArgumentError )
		end
	end
end
