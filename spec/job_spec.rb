require_relative 'spec_helper'
require 'quickmr/job'

describe Job do
	subject do
		Job.define do
			mapper(3) do |key, value|
				collect(value % 10, value) #if value < 19
			end

			reducer(2) do |key, value|
				each_key do |key, values|
					sum = values.inject(0) do |sum, value|
						sum + value
					end
					collect(key, sum)
				end
			end
		end
	end

	it 'should process input into output data according to MR algrithm' do
		processor = Class.new
		processor_messages = []
		processor.stub(:deliver_message!) {|*args| processor_messages << args}

		job = Tribe.root.spawn(subject)
		job.connect(processor)
		(0...20).each do |no|
			job.deliver_message! :data, [nil, no]
		end
		job.deliver_message! :data, nil # flush

		while job.alive? do sleep 0.1 end

		processor_messages.sort_by{|i| i.last and i.last.first or 99}.should == [[:data, [0, 10]], [:data, [1, 12]], [:data, [2, 14]], [:data, [3, 16]], [:data, [4, 18]], [:data, [5, 20]], [:data, [6, 22]], [:data, [7, 24]], [:data, [8, 26]], [:data, [9, 28]], [:data, nil]]
	end
end

