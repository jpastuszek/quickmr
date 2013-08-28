require 'quickmr/processor_base'

class Reducer < ProcessorBase
	@@reducer_no = 0
	def self.define(name = "Reducer#{@@reducer_no += 1}", &record_processor)
		reducer = Class.new(Reducer) do
			@@record_processor = record_processor
			def record_processor
				@@record_processor
			end
		end
		Object.const_set(name, reducer)
		reducer
	end

	class Collector
		def initialize
			@processor = nil
		end

		def processor(processor)
			@processor = processor
		end

		def collect(key, value)
			@processor.message! :data, [key, value] if @processor
		end

		def shutdown!
			@processor.message! :data, nil if @processor
		end
	end

	def initialize(options)
		super
		@collector = Collector.new
	end

	def connect(processor)
		@collector.processor(processor)
	end

private
	def on_data(event)
		if not event.data
			shutdown!
			@collector.shutdown!
			return
		end
		@collector.instance_exec(*event.data, &record_processor)
	end
end

