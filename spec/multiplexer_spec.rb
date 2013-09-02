require_relative 'spec_helper'
require 'quickmr/multiplexer'

describe Multiplexer do
	subject do
		Multiplexer
	end

	class TestProcessor
		def initialize
			@events = []
		end

		attr_accessor :events

		def deliver_message!(*args)
			@events << args
		end
	end

	it 'should output input data to all processors in round-robin fashion' do
		processors = []
		4.times do
			processors << TestProcessor.new
		end

		mux = Tribe.root.spawn(subject)
		mux.connect(processors)

		9.times do |data|
			mux.deliver_message! :data, [0, data]
		end
		mux.deliver_message! :data, nil

		while mux.alive? do sleep 0.1 end

		processors[0].events.should == [[:data, [0, 0]], [:data, [0, 4]], [:data, [0, 8]], [:data, nil]]
		processors[1].events.should == [[:data, [0, 1]], [:data, [0, 5]], [:data, nil]]
		processors[2].events.should == [[:data, [0, 2]], [:data, [0, 6]], [:data, nil]]
		processors[3].events.should == [[:data, [0, 3]], [:data, [0, 7]], [:data, nil]]
	end
end

