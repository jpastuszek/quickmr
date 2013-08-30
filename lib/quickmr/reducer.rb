require 'quickmr/processor_base'
require 'fiber'

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
			@_total_collections = 0
		end

		def collect(key, value)
			@parent.output [key, value]
			@_total_collections += 1
		end

		def flush!
			@parent.log "collected #{@_total_collections} records"
			@parent.output nil
		end
	end

	class EachKeyControler
		def initialize(collector, &block)
			@fiber = Fiber.new do
				loop do
					@values = Values.new
					collector.instance_exec(Fiber.yield, @values, &block)
				end
			end
		end

		def key(key)
			@fiber.resume :_force_each_stop
			@fiber.resume key
		end

		def value(value)
			@fiber.resume value
		end

		def finish
			@fiber.resume :_force_each_stop
		end
	end

	class Values
		include Enumerable

		def initialize
			@run_once = false
		end

		def each(&block)
			fail 'values is not rewindable' if @run_once
			@run_once = true
			loop do
				value = Fiber.yield
				break if value == :_force_each_stop
				yield value
			end
		end
	end

	def initialize(options)
		super
		@collector = Collector.new(self)
		setup

		@collector.instance_exec(&@on_start) if @on_start

		@last_key = nil
		@total_input_records = 0
	end

private

	def start(&block)
		@on_start = block
	end

	def each(&block)
		@on_each = block
	end

	def each_key(&block)
		@on_each_key = block
	end

	def finish(&block)
		@on_finish = block
	end

	def on_data(event)
		debug{"input: #{event.data}"}
		if not event.data
			log "processed #{@total_input_records}"
			shutdown!
			@collector.instance_exec(&@on_finish) if @on_finish
			@each_key_controler.finish if @each_key_controler
			@collector.flush!
			return
		end
		@total_input_records += 1
		@collector.instance_exec(*event.data, &@on_each) if @on_each

		if @on_each_key 
			@each_key_controler = EachKeyControler.new(@collector, &@on_each_key) if not @each_key_controler

			key = event.data.first or fail 'nil key not allowed'

			if not @last_key or key != @last_key
				debug{"key changed"}
				@each_key_controler.key(key)
				@last_key = key
			end

			@each_key_controler.value(event.data.last)
		end
	end
end

