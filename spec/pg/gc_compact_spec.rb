# -*- rspec -*-
# encoding: utf-8
#
# Tests to verify correct implementation of compaction callbacks in rb_data_type_t definitions.
#
# Compaction callbacks update moved VALUEs.
# In ruby-2.7 they are invoked only while GC.compact or GC.verify_compaction_references.
# Ruby constants are usually moved, but local variables are not.
#
# Effectiveness of the tests below should be verified by commenting the compact callback out like so:
#
#   static const rb_data_type_t pg_tmbo_type = {
#     "PG::TypeMapByOid",
#     {
#       pg_tmbo_mark,
#       RUBY_TYPED_DEFAULT_FREE,
#       pg_tmbo_memsize,
#   //    pg_compact_callback(pg_tmbo_compact),
#     },
#
# This should result in a segmentation fault aborting the whole process.
# Therefore the effectiveness of only one test can be verified per rspec run.

require_relative '../helpers'

describe "GC.compact", if: GC.respond_to?(:compact) do
	before :all do
		TM1 = Class.new(YSQL::TypeMapByClass) do
			def conv_array(value)
				YSQL::TextEncoder::JSON.new
			end
		end.new
		TM1[Array] = :conv_array

		E1 = YSQL::TextEncoder::JSON.new

		TM2 = YSQL::TypeMapByClass.new
		TM2.default_type_map = YSQL::TypeMapInRuby.new

		TMBC = YSQL::TypeMapByColumn.new([YSQL::TextDecoder::Float.new])


		CONN2 = YSQL.connect(@conninfo)
		CONN2.type_map_for_results = YSQL::BasicTypeMapForResults.new(CONN2)

		RES1 = CONN2.exec("SELECT 234")

		TUP1 = RES1.tuple(0)

		TM3 = YSQL::TypeMapByClass.new
		CPYENC = YSQL::TextEncoder::CopyRow.new type_map: TM3
		RECENC = YSQL::TextEncoder::Record.new type_map: TM3

		begin
			# Use GC.verify_compaction_references instead of GC.compact .
			# This has the advantage that all movable objects are actually moved.
			# The downside is that it doubles the heap space of the Ruby process.
			# Therefore we call it only once and do several tests afterwards.
			GC.verify_compaction_references(toward: :empty, double_heap: true)
		rescue NotImplementedError, NoMethodError => err
			skip("GC.compact skipped: #{err}")
		end
	end

	it "should compact PG::TypeMapByClass #328" do
		res = @conn.exec_params("SELECT $1", [[5]], 0, TM1)
		expect( res.getvalue(0, 0) ).to eq( "[5]" )
	end

	it "should compact PG::CompositeCoder #327" do
		e2 = YSQL::TextEncoder::Array.new elements_type: E1
		expect( e2.encode([5]) ).to eq("{5}")
	end

	it "should compact PG::TypeMap#default_type_map" do
		expect( TM2.default_type_map ).to be_kind_of(YSQL::TypeMapInRuby )
	end

	it "should compact PG::TypeMapByColumn" do
		res = CONN2.exec("SELECT 555")
		expect( res.getvalue(0,0) ).to eq( 555 )
	end

	it "should compact PG::Connection" do
		expect( TMBC.coders[0] ).to be_kind_of(YSQL::TextDecoder::Float)
	end

	it "should compact PG::Result" do
		expect( RES1.getvalue(0,0) ).to eq( 234 )
	end

	it "should compact PG::Tuple" do
		expect( TUP1[0] ).to eq( 234 )
	end

	it "should compact PG::CopyCoder" do
		expect( CPYENC.encode([45]) ).to eq( "45\n" )
	end

	it "should compact PG::RecordCoder" do
		expect( RECENC.encode([34]) ).to eq( '("34")' )
	end

	after :all do
		CONN2.close
	end
end
