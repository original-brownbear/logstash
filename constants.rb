class SomeConstClass < SomeClass

  def some_method(cls, other, thread_count, threads, yet_other)
    begin
      yet_other.do_blk_stuff(1, 2, 3) {yet_other.do_blk_stuff(1, 2, 3) {yet_other.do_blk_stuff(1, 2, 3) {yet_other.do_blk_stuff(1, 2, 3) {yet_other.do_blk_stuff(1, 2, 3) {yet_other.do_blk_stuff(1, 2, 3) {cls.do_block_stuff(thread_count, other) {
        LITERAL_ARRAY.each do |onum|
          LITERAL_ARRAY.each do |num|
            other.do_stuff(thread_count * onum * num)
          end
        end
      }}}}}}
      }
      yet_other.do_blk_stuff(1, 2, 3) {cls.do_block_stuff(thread_count, other) {other.do_stuff(thread_count)}}
      yet_other.do_blk_stuff(1, 2, 3) {cls.do_block_stuff(thread_count, other) {other.do_stuff(thread_count)}}
    ensure
      (0..thread_count).each do |t|
        threads[t].join 10
      end
    end
  end
end
