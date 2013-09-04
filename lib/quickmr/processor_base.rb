require 'ffi-rzmq'
require 'logger'

module Logging
	def root_logger(logger)
		@logger = logger
	end

	def logger
		@logger or fail 'no logger'
	end	
end

class InputPort
	def initialize(zmq, &block)
		@socket = zmq.socket ZMQ::PULL
		@address = "ipc:////tmp/quickmr[#{Process.pid}]-InputPort[#{id}]"
		@socket.bind @address
		@on_event = block
	end

	attr_reader :address

	def poll
		event = Marshal.load(@socket.recv_string)
		@on_event.call(event)
	end

	def close!
		@socket.close
	end
end

class RemoteInputPort
	def initialize(address)
		@address = address
	end

	attr_reader :address
end

class OutputPort
	def initialize(zmq)
		@socket = zmq.socket ZMQ::PUSH
	end

	def connect(remote_input_port)
		@socket.connect remote_input_port.address
		self
	end

	def send(event)
		fail 'expecte object' unless event
		@socket.send_string Marshal.dump(event)
	end

	def close!
		@socket.send_string Marshal.dump(nil)
	end
end

module InputPorts
end

module OutportPouts
end

class ProcessorBase
	include Logging

	def initialize(*args)
		@zmq = ZMQ::Context.new

		if options[:logger]
			root_logger(options[:logger]) 
		elsif options[:parent].respond_to? :logger
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
		if not data
			debug { "closing output" }
		else
			debug { "output: #{data}" }
		end
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

	def spawn(klass, *args)
		fork {
			klass.new(*args)
		}
		RemoteProcessor.new()
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

