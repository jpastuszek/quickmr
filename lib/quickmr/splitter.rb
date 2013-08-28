require 'zlib'
require 'quickmr/processor_base'

class Splitter < ProcessorBase
	def queues(queues)
		@queues = queues
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

