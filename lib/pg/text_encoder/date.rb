# -*- ruby -*-
# frozen_string_literal: true

module YSQL
	module TextEncoder
		class Date < SimpleEncoder
			def encode(value)
				value.respond_to?(:strftime) ? value.strftime("%Y-%m-%d") : value
			end
		end
	end
end # module PG
