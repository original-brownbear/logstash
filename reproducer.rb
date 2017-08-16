require_relative "some.rb"
java_import java.util.concurrent.locks.ReentrantLock

foo = [2, 3, 4, 5, 1, 2, 3, 4]
LITERAL_ARRAY=java.util.ArrayList.new
LITERAL_ARRAY.add_all foo

cls = SomeClass.new
require_relative "other.rb"
other = SomeOtherClass.new
other2 = SomeOtherClass.new
require_relative "yetother.rb"
yet_other = SomeYetOtherClass.new
latch = java.util.concurrent.CountDownLatch.new 1

thread_count = 8
ready = java.util.concurrent.CountDownLatch.new thread_count
threads = {}
(0..thread_count).each do |t|
  threads[t] = Thread.new(t) do |_t|
    ready.count_down
    latch.await
    LITERAL_ARRAY.each do |num2|
      LITERAL_ARRAY.each do |num|
        yet_other.do_blk_stuff(num2, 2, num) {yet_other.do_blk_stuff(1, 2, 3) {yet_other.do_blk_stuff(1, 2, 3) {yet_other.do_blk_stuff(3, 2, 3) {cls.do_block_stuff(_t, other) {other.do_stuff(_t)}}}}}
        yet_other.do_blk_stuff(num, num2, 3) {cls.do_block_stuff(_t * num, other) {other.do_stuff(num * t * _t)}}
        yet_other.do_blk_stuff(num, 2, num2) {cls.do_block_stuff(_t, other) {other.do_stuff(_t)}}
      end
    end
  end
end

ready.await
require_relative "constants.rb"
blub = SomeConstClass.new
latch.count_down

other2.do_stuff(234)
yet_other.do_blk_stuff(444, 2, 3) {
  blub.some_method(cls, other, thread_count, threads, yet_other)
  blub.some_method(cls, other2, thread_count, threads, yet_other)
}


(0..thread_count).each {|t| threads[t].join 2000}
