# -*- ruby -*-
# frozen_string_literal: true

require 'yugabyte_ysql' unless defined?( YugabyteYSQL )


module YugabyteYSQL

	class Error < StandardError
		def initialize(msg=nil, connection: nil, result: nil)
			@connection = connection
			@result = result
			super(msg)
		end
	end

	class NotAllCopyDataRetrieved < YugabyteYSQL::Error
	end
	class LostCopyState < YugabyteYSQL::Error
	end
	class NotInBlockingMode < YugabyteYSQL::Error
	end

end # module PG

