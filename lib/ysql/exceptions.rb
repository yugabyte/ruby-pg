# -*- ruby -*-
# frozen_string_literal: true

require 'ysql' unless defined?( YSQL )


module YSQL

	class Error < StandardError
		def initialize(msg=nil, connection: nil, result: nil)
			@connection = connection
			@result = result
			super(msg)
		end
	end

	class NotAllCopyDataRetrieved < YSQL::Error
	end
	class LostCopyState < YSQL::Error
	end
	class NotInBlockingMode < YSQL::Error
	end

end # module PG

