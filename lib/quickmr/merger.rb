require 'quickmr/processor_base'

class Merger < ProcessorBase
private
	def on_flush!(event)
		# note that each queue needs to end with nil!
		queues = event.data

		last = []

		# initialize last
		queues.each do |queue|
			last << queue.pop
		end
		
		# pop from queue with lesser key value
		loop do
			min = nil
			min_key = nil
			no = -1
			last.each do |key, value|
				no += 1
				next if not key
				if not min or min_key > key
					min = no
					min_key = key
				end
			end
			break unless min
			output last[min] if @processor
			last[min] = queues[min].pop
		end

		# end of data
		output nil if @processor
		shutdown!
	end
end

