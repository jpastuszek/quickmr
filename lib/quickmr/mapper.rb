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
		extend Forwardable

		def initialize(parent, db)
			@parent = parent
			@_db = db
			@_seq = 0
			@_total_collections = 0
		end

		def collect(key, value)
			@parent.debug{"collecting: #{[key, value]}"}
			fail 'key cannot be nil' unless key
			@_db['%s#%010i' % [key.to_s, @_seq += 1]] = Marshal.dump([key, value])
			@_total_collections += 1
		end

		def flush!
			@parent.log "collected #{@_total_collections} records; flushing..."
			# send sorted data by key
			reader = @_db.cursor
			reader.jump
			begin
				while record = reader.get(true)
					key, value = *Marshal.load(record[1])
					@parent.debug{"flushing: #{[key, value]}"}
					@parent.output [key, value]
				end
			ensure
				reader.disable
				@parent.output nil
			end
		end

		def_delegators :@parent, :log, :debug, :warn
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

