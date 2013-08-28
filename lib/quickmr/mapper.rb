require 'tribe'

class Mapper < Tribe::DedicatedActor
	@@mapper_no = 0
	def self.define(name = "Mapper#{@@mapper_no += 1}", &record_processor)
		mapper = Class.new(Mapper) do
			@@record_processor = record_processor
			def record_processor
				@@record_processor
			end
		end
		Object.const_set(name, mapper)
		mapper
	end

private
	def on_record(record)
		key, value = *record_processor.call(record.data)
		p [key, value]
	end

	def exception_handler(e)
		super
		puts "#{self.class.name}[#{identifier}]: fatal: #{e.exception}"
	end

	def shutdown_handler(e)
		super
		puts "#{self.class.name}[#{identifier}]: done"
	end
end

