require_relative 'spec_helper'
require 'quickmr/reducer'

describe Reducer do
	let :data do
		[[1, 1], [1, 2], [2, 1], [2, 2], [3, 1], [3, 2], [4, 1], [4, 2], [5, 1], [5, 2], [6, 1], [6, 2], [7, 1], [7, 2], nil]
	end

	subject do
		Reducer.define do |key, value|
			@last_key = nil unless defined? @last_key
			@sum = 0 unless defined? @sum

			if key != @last_key
				collect(@last_key, @sum) if @last_key
				@sum = 0
				@last_key = key
			end

			@sum += value
		end
	end

	it 'should provide key value records from sorted input records' do
		processor = Class.new
		processor_messages = []
		processor.stub(:message!) {|*args| processor_messages << args}

		reducer = Tribe.root.spawn(subject)
		reducer.connect(processor)

		data.each do |pair|
			reducer.deliver_message! :data, pair
		end
		
		while reducer.alive? do sleep 0.1 end

		processor_messages.should == [[:data, [1, 3]], [:data, [2, 3]], [:data, [3, 3]], [:data, [4, 3]], [:data, [5, 3]], [:data, [6, 3]], [:data, nil]]
	end
end

