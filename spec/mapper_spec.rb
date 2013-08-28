require_relative 'spec_helper'
require 'quickmr/mapper'

describe Mapper do
	subject do
		Mapper.define do |key, value|
			collect(value % 3, value) if value < 9
		end
	end

	it 'should produce key value records with given block' do
		processor = Class.new
		processor_messages = []
		processor.stub(:message!) {|*args| processor_messages << args}

		mapper = Tribe.root.spawn(subject)
		mapper.connect(processor)

		(0...10).each do |no|
			mapper.deliver_message! :data, [nil, no]
		end
		mapper.deliver_message! :data, nil # flush

		while mapper.alive? do sleep 0.1 end

		processor_messages.should == [[:data, ["0", "0"]], [:data, ["0", "3"]], [:data, ["0", "6"]], [:data, ["1", "1"]], [:data, ["1", "4"]], [:data, ["1", "7"]], [:data, ["2", "2"]], [:data, ["2", "5"]], [:data, ["2", "8"]], [:data, nil]]
	end
end

