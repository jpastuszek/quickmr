require_relative 'spec_helper'
require 'quickmr/reducer'

describe Reducer do
	let :data do
		# modulo and it's base
		(0...20).map do |no|
			[(no % 10), no]
		end.sort_by do |key, value|
			"%s#%10s" % [key, value]
		end | [nil]
	end

	describe 'basic reducer structure' do
		subject do
			# sum base for each modulo
			Reducer.define do |key, value|
				start do
					@last_key = nil
					@sum = 0
				end

				each do |key, value|
					if key != @last_key
						collect(@last_key, @sum) if @last_key
						@sum = 0
						@last_key = key
					end
					@sum += value
				end

				finish do
					collect(@last_key, @sum)
				end
			end
		end

		it 'should provide key value records from sorted input records' do
			processor = Class.new
			processor_messages = []
			processor.stub(:deliver_message!) {|*args| processor_messages << args}

			reducer = Tribe.root.spawn(subject)
			reducer.connect(processor)

			data.each do |pair|
				reducer.deliver_message! :data, pair
			end
			
			while reducer.alive? do sleep 0.1 end

			processor_messages.should == [[:data, [0, 10]], [:data, [1, 12]], [:data, [2, 14]], [:data, [3, 16]], [:data, [4, 18]], [:data, [5, 20]], [:data, [6, 22]], [:data, [7, 24]], [:data, [8, 26]], [:data, [9, 28]], [:data, nil]]
		end
	end
end

