require_relative 'spec_helper'
require 'quickmr/demultiplexer'

describe Demultiplexer do
	subject do
		Demultiplexer
	end

	let :processor do
		processor = Class.new
		@processor_messages = []
		processor.stub(:deliver_message!) {|*args| processor_messages << args}
		processor
	end

	let :processor_messages do
		@processor_messages = []
	end

	it 'should collect input from defined number of other porcesses and send nil when all of them finish with nil' do
		demux = Tribe.root.spawn(subject, input_no: 4)
		demux.connect(processor)

		4.times do |input|
			3.times do |data|
				demux.deliver_message! :data, [input, data]
			end
			demux.deliver_message! :data, nil
		end
		while demux.alive? do sleep 0.1 end

		processor_messages.should == [[:data, [0, 0]], [:data, [0, 1]], [:data, [0, 2]], [:data, [1, 0]], [:data, [1, 1]], [:data, [1, 2]], [:data, [2, 0]], [:data, [2, 1]], [:data, [2, 2]], [:data, [3, 0]], [:data, [3, 1]], [:data, [3, 2]], [:data, nil]]
	end
end

