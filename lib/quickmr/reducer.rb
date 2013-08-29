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
		def initialize(parent)
			@parent = parent
		end

		def collect(key, value)
			@parent.output [key, value]
		end

		def flush!
			@parent.output nil
		end
	end

	def initialize(options)
		super
		@collector = Collector.new(self)
	end

private

	def on_data(event)
		if not event.data
			shutdown!
			@collector.flush!
			return
		end
		@collector.instance_exec(*event.data, &record_processor)
	end
end

