require 'quickmr/processor_base'
require 'quickmr/mapper'
require 'quickmr/splitter'
require 'quickmr/merger'
require 'quickmr/reducer'
require 'quickmr/output'

class Job < ProcessorBase
	@@job_no = 0
	def self.define(name = "Job#{@@job_no += 1}", &setup)
		job = Class.new(Job) do
			@@setup = setup
			def setup
				instance_exec(&@@setup)
			end
		end
		Object.const_set(name, job)
		job
	end

	def self.start
		Tribe.root.spawn(self)
	end

	def initialize(options)
		@logger = Logger.new(STDERR)
		@logger.level = Logger::WARN
		super options.merge(logger: @logger)

		@mappers = []
		@reducers = []

		setup

		fail 'no mappers defined' if @mappers.empty?
		fail 'no reducers defined' if @reducers.empty?

		@splitters = []
		@mergers = []

		@reducers.each do |reducer|
			merger = spawn(Merger)
			merger.connect(reducer)
			@mergers << merger
		end

		@mappers.each do |mapper|
			splitter = spawn(Splitter, queue_no: @mergers.length)
			mapper.connect(splitter)
			@splitters << splitter
		end

		@mergers.each.with_index do |merger, merger_no|
			queues = @splitters.map do |splitter|
				splitter.queues[merger_no]
			end
			merger.deliver_message! :flush!, queues
		end

		@output = spawn(Output, reducer_no: @reducers.length)
		@reducers.each do |reducer|
			reducer.connect(@output)
		end

		@output.connect(self, :output)

		@mapping_sequence = 0
	end

	def mapper(count, &block)
		count.times do
			@mappers << spawn(Mapper.define(&block))
		end
	end

	def reducer(count, &block)
		count.times do
			@reducers << spawn(Reducer.define(&block))
		end
	end

	def output(&block)
		@on_output = block
	end

	def process(key, value)
		deliver_message!(:data, [key, value])
	end

	def done
		deliver_message! :data, nil # flush
		while alive? do sleep 0.1 end
	end

	def show_info
		@logger.level = Logger::INFO
	end

	def show_debug
		@logger.level = Logger::DEBUG
	end

private
	
	def on_data(event)
		debug{"input: #{event.data}"}
		if not event.data
			@mappers.each do |mapper|
				mapper.deliver_message! :data, nil
			end
			return
		end
		@mappers[@mapping_sequence % @mappers.length].deliver_message! :data, event.data
		@mapping_sequence += 1
	end

	def on_output(event)
		if not event.data
			shutdown!
			return
		end
		@on_output.call(*event.data) if @on_output
	end
end

