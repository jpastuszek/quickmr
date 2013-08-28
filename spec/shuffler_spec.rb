require_relative 'spec_helper'
require 'quickmr/shuffler'

describe Shuffler do
	let :queue_reducer1 do
		q = Queue.new
		q.push [1, 1]
		q.push [1, 2]
		q.push [4, 1]
		q.push [4, 2]
		q.push [6, 1]
		q.push [6, 2]
		q.push nil
		q
	end

	let :queue_reducer2 do
		q = Queue.new
		q.push [2, 1]
		q.push [2, 2]
		q.push [5, 1]
		q.push [5, 2]
		q.push nil
		q
	end

	let :queue_reducer3 do
		q = Queue.new
		q.push [3, 1]
		q.push [3, 2]
		q.push [7, 1]
		q.push [7, 2]
		q.push nil
		q
	end

	subject do
		Shuffler
	end

	it 'should provide records in key order from many queues' do
		processor = Class.new
		processor_messages = []
		processor.stub(:message!) {|*args| processor_messages << args}

		shuffler = Tribe.root.spawn(subject)
		shuffler.connect(processor)

		shuffler.deliver_message! :flush!, [queue_reducer1, queue_reducer2, queue_reducer3]

		shuffler.shutdown!
		while shuffler.alive? do sleep 0.1 end

		queue_reducer1.should be_empty
		queue_reducer2.should be_empty
		queue_reducer3.should be_empty
		processor_messages.should == [[:reduce, 1, 1], [:reduce, 1, 2], [:reduce, 2, 1], [:reduce, 2, 2], [:reduce, 3, 1], [:reduce, 3, 2], [:reduce, 4, 1], [:reduce, 4, 2], [:reduce, 5, 1], [:reduce, 5, 2], [:reduce, 6, 1], [:reduce, 6, 2], [:reduce, 7, 1], [:reduce, 7, 2]]
	end
end

