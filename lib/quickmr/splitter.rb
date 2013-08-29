require 'zlib'
require 'quickmr/processor_base'

class Splitter < ProcessorBase
	def initialize(options)
		super
		@queues = []

		(options[:queue_no] || fail('need queue number')).times do
			@queues << SizedQueue.new(options[:queue_size] || 1000)
		end
		log "splitter initailized with #{@queues.length} queues"
	end

	def queues
		@queues
	end
private

	def on_data(event)
		debug{"data: #{event.data}"}
		if not event.data
			# close the queues
			@queues.each do |queue|
				queue.push nil
			end

			log "done filling queues"
			shutdown!
			return
		end

		queue_no = Zlib.crc32(event.data.first.to_s) % @queues.length
		debug{"enqueuing #{event.data} to queue #{queue_no}"}
		@queues[queue_no].push(event.data)
	end
end

