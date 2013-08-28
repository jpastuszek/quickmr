require 'tribe'
require 'kyotocabinet'
require 'zlib'

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

	class Collector
		def initialize(db)
			@_db = db
			@_seq = 0
		end

		def collect(key, value)
			@_db['%s#%010i' % [key, @_seq += 1]] = value
		end
	end

	def initialize(options)
		super
		
		@db = KyotoCabinet::DB.new
		@db.tune_encoding('utf-8')
		@db.open('%')

		@collector = Collector.new(@db)
	end

private

	def on_map(event)
		@collector.instance_exec(event.data, &record_processor)
	end

	def on_flush!(event)
		puts "#{self.class.name}[#{identifier}]: flusing..."
		queues = event.data
		queue_no = queues.length

		each do |key, value|
			queue = Zlib.crc32(key) % queue_no
			queues[queue].push([key, value])
		end
	end

	def each
		@db.each do |key, value|
			yield key[0..-12], value
		end
	end

	def exception_handler(exception)
		super
		puts "#{self.class.name}[#{identifier}]: fatal: #{exception.exception}: #{exception.backtrace.join("\n")}"
	end

	def shutdown_handler(event)
		super
		@db.close
		puts "#{self.class.name}[#{identifier}]: done"
	end
end

