require 'kyotocabinet'
require 'quickmr/processor_base'

class Mapper < ProcessorBase
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

		def processor(processor)
			@processor = processor
		end

		def collect(key, value)
			@_db['%s#%010i' % [key, @_seq += 1]] = value
		end

		def flush!
			# send sorted data by key
			each do |key, value|
				@processor.message! :data, [key, value]
			end
			@processor.message! :data, nil
		end

		private

		def each
			@_db.each do |key, value|
				yield key[0..-12], value
			end
		end
	end

	def initialize(options)
		super
		
		@db = KyotoCabinet::DB.new
		@db.tune_encoding('utf-8')
		@db.open('%')

		@collector = Collector.new(@db)
	end

	def connect(processor)
		@collector.processor(processor)
	end

private

	def on_data(event)
		if not event.data
			shutdown!
			puts "#{self.class.name}[#{identifier}]: flusing..."
			@collector.flush!
			return
		end
		@collector.instance_exec(*event.data, &record_processor)
	end

	def shutdown_handler(event)
		super
		@db.close
	end
end

