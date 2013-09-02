require 'quickmr/processor_base'

class Demultiplexer < ProcessorBase
	def initialize(options)
		super
		@inputs_alive = options[:input_no] or fail 'need number of inputs'
		log "waiting for #{@inputs_alive} inputs to finish"
	end

private
	def on_data(event)
		if not event.data
			@inputs_alive -= 1
			if @inputs_alive == 0
				output nil
				log "all inputts done, shutting down"
				shutdown! 
			else
				log "waiting for #{@inputs_alive} inputs to finish"
			end
			return
		end
		output event.data
	end
end

