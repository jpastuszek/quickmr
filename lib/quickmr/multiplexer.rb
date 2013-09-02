require 'quickmr/processor_base'

class Multiplexer < ProcessorBase
	def initialize(options)
		super
		@sequence = 0
		@outputs = []
	end

	def connect(outputs)
		@outputs = outputs
		log "multiplexing to #{@outputs.length} outputs"
	end

private
	def on_data(event)
		if not event.data
			@outputs.each do |output|
				output.deliver_message! :data, nil
				shutdown!
			end
			return
		end
		@outputs[@sequence % @outputs.length].deliver_message! :data, event.data
		@sequence += 1
	end
end


