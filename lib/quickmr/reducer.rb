require 'quickmr/processor_base'

class Reducer < ProcessorBase
	@@reducer_no = 0
	def self.define(name = "Reducer#{@@reducer_no += 1}", &setup)
		reducer = Class.new(Reducer) do
			@@setup = setup
			def setup
				instance_exec(&@@setup)
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
		setup

		@collector.instance_exec(&@on_start) if @on_start
	end

private

	def start(&block)
		@on_start = block
	end

	def each(&block)
		@on_each = block
	end

	def finish(&block)
		@on_finish = block
	end

	def on_data(event)
		if not event.data
			shutdown!
			@collector.instance_exec(&@on_finish) if @on_finish
			@collector.flush!
			return
		end
		@collector.instance_exec(*event.data, &@on_each) if @on_each
	end
end

