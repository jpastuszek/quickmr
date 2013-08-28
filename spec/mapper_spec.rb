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
		mapper = Tribe.root.spawn(subject)
		(0...100).each do |no|
			mapper.deliver_message! :map, no
		end

		mapper.deliver_message! :flush!, [queue_reducer1, queue_reducer2, queue_reducer3]

		mapper.shutdown!
		while mapper.alive? do sleep 0.1 end

		queue_reducer1.length.should == 19
		queue_reducer2.length.should == 46
		queue_reducer3.length.should == 28

		# 7 and 9 + nil
		(0...queue_reducer1.length).map{queue_reducer1.pop}.should == [["7", "7"], ["7", "17"], ["7", "27"], ["7", "37"], ["7", "47"], ["7", "57"], ["7", "67"], ["7", "77"], ["7", "87"], ["9", "9"], ["9", "19"], ["9", "29"], ["9", "39"], ["9", "49"], ["9", "59"], ["9", "69"], ["9", "79"], ["9", "89"], nil]
		
		# 2, 3, 4, 5 and 6 + nil
		(0...queue_reducer2.length).map{queue_reducer2.pop}.should == [["2", "2"], ["2", "12"], ["2", "22"], ["2", "32"], ["2", "42"], ["2", "52"], ["2", "62"], ["2", "72"], ["2", "82"], ["3", "3"], ["3", "13"], ["3", "23"], ["3", "33"], ["3", "43"], ["3", "53"], ["3", "63"], ["3", "73"], ["3", "83"], ["4", "4"], ["4", "14"], ["4", "24"], ["4", "34"], ["4", "44"], ["4", "54"], ["4", "64"], ["4", "74"], ["4", "84"], ["5", "5"], ["5", "15"], ["5", "25"], ["5", "35"], ["5", "45"], ["5", "55"], ["5", "65"], ["5", "75"], ["5", "85"], ["6", "6"], ["6", "16"], ["6", "26"], ["6", "36"], ["6", "46"], ["6", "56"], ["6", "66"], ["6", "76"], ["6", "86"], nil]

		# 0, 1 and 8 + nil
		(0...queue_reducer3.length).map{queue_reducer3.pop}.should == [["0", "0"], ["0", "10"], ["0", "20"], ["0", "30"], ["0", "40"], ["0", "50"], ["0", "60"], ["0", "70"], ["0", "80"], ["1", "1"], ["1", "11"], ["1", "21"], ["1", "31"], ["1", "41"], ["1", "51"], ["1", "61"], ["1", "71"], ["1", "81"], ["8", "8"], ["8", "18"], ["8", "28"], ["8", "38"], ["8", "48"], ["8", "58"], ["8", "68"], ["8", "78"], ["8", "88"], nil]
	end
end

