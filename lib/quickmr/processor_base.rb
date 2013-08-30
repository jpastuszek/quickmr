require 'tribe'
require 'logger'

module Logging
	def root_logger(logger)
		@logger = logger
	end

	def logger
		@logger or fail 'no logger'
	end	
end

class ProcessorBase < Tribe::DedicatedActor
	include Logging

	def initialize(options)
		super
		if options[:parent].respond_to? :logger
			root_logger(options[:parent].logger) 
		else
			root_logger(Logger.new(STDERR))
			warn 'spawning new logger'
		end
	end

	def connect(processor, command = :data)
		@processor = processor
		@command = command
	end

	def output(data)
		fail 'no processor connected' unless @processor and @command
		debug { "output: #{data}" }
		@processor.deliver_message! @command, data
	end

	def debug(&block)
		logger.debug do 
			"#{name}: #{block.call}"
		end
	end

	def log(msg)
		logger.info "#{name}: #{msg.to_s}"
	end

	def warn(msg)
		logger.warn "#{name}: #{msg.to_s}"
	end

	def name
		"#{self.class.name}[#{identifier}]"
	end

	def to_s
		name
	end

private
	def exception_handler(exception)
		super
		log "fatal: #{exception.exception}:\n#{exception.backtrace.join("\n")}"
	end

	def shutdown_handler(event)
		super
		log 'done'
	end
end

