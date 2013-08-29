require 'zlib'
require 'quickmr/processor_base'

class Splitter < ProcessorBase
	def initialize(options)
		super
		@queues = []

		(options[:queue_no] || fail('need queue number')).times do
			@queues << SizedQueue.new(options[:queue_size] || 1000)
		end
	end

	def queues
		@queues
	end
private

	def on_data(event)
		if not event.data
			shutdown!

			# close the queues
			@queues.each do |queue|
				queue.push nil
			end

			puts "#{self.class.name}[#{identifier}]: done filling queues"
			return
		end

		@queues[Zlib.crc32(event.data.first.to_s) % @queues.length].push(event.data)
	end
end

