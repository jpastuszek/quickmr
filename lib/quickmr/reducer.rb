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
		end

		def collect(key, value)
			@parent.output [key, value]
		end

		def flush!
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
			@fiber.resume nil
			@fiber.resume key
		end

		def value(value)
			@fiber.resume value
		end

		def finish
			@fiber.resume nil
		end
	end

	class Values
		include Enumerable

		def each(&block)
			loop do
				value = Fiber.yield
				break unless value
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
			shutdown!
			@collector.instance_exec(&@on_finish) if @on_finish
			@each_key_controler.finish if @each_key_controler
			@collector.flush!
			return
		end
		@collector.instance_exec(*event.data, &@on_each) if @on_each

		if @on_each_key 
			@each_key_controler = EachKeyControler.new(@collector, &@on_each_key) if not @each_key_controler

			key = event.data.first

			if not @last_key or key != @last_key
				debug{"key changed"}
				@each_key_controler.key(key)
				@last_key = key
			end

			@each_key_controler.value(event.data.last)
		end
	end
end

