require 'tribe'

class ProcessorBase < Tribe::DedicatedActor
private
	def exception_handler(exception)
		super
		puts "#{self.class.name}[#{identifier}]: fatal: #{exception.exception}: #{exception.backtrace.join("\n")}"
	end

	def shutdown_handler(event)
		super
		puts "#{self.class.name}[#{identifier}]: done"
	end
end

