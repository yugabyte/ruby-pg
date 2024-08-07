# -*- ruby -*-
# frozen_string_literal: true

require 'json'

module YSQL
	module TextEncoder
		class JSON < SimpleEncoder
			def encode(value)
				::JSON.generate(value, quirks_mode: true)
			end
		end
	end
end # module PG
