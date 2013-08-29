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
		def initialize(parent, db)
			@parent = parent
			@_db = db
			@_seq = 0
		end

		def collect(key, value)
			@parent.debug{"collecting: #{[key, value]}"}
			@_db['%s#%010i' % [key.to_s, @_seq += 1]] = Marshal.dump([key, value])
		end

		def flush!
			@parent.log 'flusing...'
			# send sorted data by key
			each do |key, value|
				@parent.debug{"flushing: #{[key, value]}"}
				@parent.output [key, value]
			end
			@parent.output nil
		end

		private

		def each
			@_db.each do |key, pair|
				yield *Marshal.load(pair)
			end
		end
	end

	def initialize(options)
		super
		
		@db = KyotoCabinet::DB.new
		@db.tune_encoding('utf-8')
		@db.open('%')

		@collector = Collector.new(self, @db)
	end

private

	def on_data(event)
		if not event.data
			@collector.flush!
			shutdown!
			return
		end
		debug{"data: #{event.data}"}
		@collector.instance_exec(*event.data, &record_processor)
	end

	def shutdown_handler(event)
		super
		@db.close
	end
end

