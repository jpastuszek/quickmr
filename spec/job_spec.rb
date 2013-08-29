require_relative 'spec_helper'
require 'quickmr/job'

describe Job do
	subject do
		Job.define do
			mapper(3) do |key, value|
				collect(value % 10, value) #if value < 19
			end

			reducer(2) do |key, value|
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

		p processor_messages
	end
end

