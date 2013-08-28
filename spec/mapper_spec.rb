require_relative 'spec_helper'
require 'quickmr/mapper'

describe Mapper do
	subject do
		Mapper.define do |record|
			next record % 5, record
		end
	end

	it 'should ba a class' do
		subject.should be_a Class
	end

	it 'should produce key value records with given block' do
		mapper = Tribe.root.spawn(subject, name: 'test')
		mapper.deliver_message!(:record, 14)
		mapper.deliver_message!(:record, 23)
		mapper.deliver_message!(:record, 1)
		mapper.shutdown!
		
		while mapper.alive? do sleep 0.1 end
	end
end

