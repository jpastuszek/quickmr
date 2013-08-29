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

	def initialize(options)
		super

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

	def connect(processor)
		@output.connect(processor)
	end

	def alive?
		@output.alive?
	end

private
	
	def on_data(event)
		log "input: #{event.data}"
		if not event.data
			@mappers.each do |mapper|
				mapper.deliver_message! :data, nil
			end
			#shutdown!
			return
		end
		@mappers[@mapping_sequence % @mappers.length].deliver_message! :data, event.data
		@mapping_sequence += 1
	end
end

