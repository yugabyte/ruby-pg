# -*- ruby -*-
# frozen_string_literal: true

module YugabyteYSQL
	module TextDecoder
		# Convenience classes for timezone options
		class TimestampUtc < Timestamp
			def initialize(hash={}, **kwargs)
				warn("PG::Coder.new(hash) is deprecated. Please use keyword arguments instead! Called from #{caller.first}", category: :deprecated) unless hash.empty?
				super(**hash, **kwargs, flags: YugabyteYSQL::Coder::TIMESTAMP_DB_UTC | YugabyteYSQL::Coder::TIMESTAMP_APP_UTC)
			end
		end
		class TimestampUtcToLocal < Timestamp
			def initialize(hash={}, **kwargs)
				warn("PG::Coder.new(hash) is deprecated. Please use keyword arguments instead! Called from #{caller.first}", category: :deprecated) unless hash.empty?
				super(**hash, **kwargs, flags: YugabyteYSQL::Coder::TIMESTAMP_DB_UTC | YugabyteYSQL::Coder::TIMESTAMP_APP_LOCAL)
			end
		end
		class TimestampLocal < Timestamp
			def initialize(hash={}, **kwargs)
				warn("PG::Coder.new(hash) is deprecated. Please use keyword arguments instead! Called from #{caller.first}", category: :deprecated) unless hash.empty?
				super(**hash, **kwargs, flags: YugabyteYSQL::Coder::TIMESTAMP_DB_LOCAL | YugabyteYSQL::Coder::TIMESTAMP_APP_LOCAL)
			end
		end

		# For backward compatibility:
		TimestampWithoutTimeZone = TimestampLocal
		TimestampWithTimeZone = Timestamp
	end
end # module PG
