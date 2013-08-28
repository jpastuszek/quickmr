require_relative 'spec_helper'
require 'quickmr/mapper'

describe Mapper do
	subject do
		Mapper.define do |record|
			collect(record % 10, record) if record < 90
		end
	end

	let :queue_reducer1 do
		Queue.new
	end

	let :queue_reducer2 do
		Queue.new
	end

	let :queue_reducer3 do
		Queue.new
	end

	it 'should ba a class' do
		subject.should be_a Class
	end

	it 'should produce key value records with given block' do
		mapper = Tribe.root.spawn(subject, name: 'test')
		(0...100).each do |no|
			mapper.deliver_message! :map, no
		end

		mapper.deliver_message! :flush!, [queue_reducer1, queue_reducer2, queue_reducer3]

		mapper.shutdown!
		while mapper.alive? do sleep 0.1 end

		queue_reducer1.length.should == 18
		queue_reducer2.length.should == 45
		queue_reducer3.length.should == 27

		#queue_reducer1.length.times{p queue_reducer1.pop}
		#puts
		#queue_reducer2.length.times{p queue_reducer2.pop}
		#puts
		#queue_reducer3.length.times{p queue_reducer3.pop}
	end
end

