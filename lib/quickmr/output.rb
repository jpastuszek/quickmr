require 'quickmr/processor_base'

class Output < ProcessorBase
	def initialize(options)
		super
		@reducers_alive = options[:reducer_no] or fail 'need number of reducers'
		log "waiting for #{@reducers_alive} to finish"
	end

private
	def on_data(event)
		if not event.data
			@reducers_alive -= 1
			if @reducers_alive == 0
				output nil
				log "all reducers done, shutting down"
				shutdown! 
			else
				log "waiting for #{@reducers_alive} to finish"
			end
			return
		end
		output event.data
	end
end

