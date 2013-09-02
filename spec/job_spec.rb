require_relative 'spec_helper'
require 'quickmr/job'

describe Job do
	subject do
		Job.define do
			show_debug
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
		processor_messages = []

		job = Tribe.root.spawn(subject)
		job.output do |key, value|
			processor_messages << [key, value]
		end

		(0...20).each do |no|
			job.process nil, no
		end
		job.flush!
		job.wait_done

		processor_messages.sort_by{|i| i.first or 99}.should == [[0, 10], [1, 12], [2, 14], [3, 16], [4, 18], [5, 20], [6, 22], [7, 24], [8, 26], [9, 28]]
	end
end

