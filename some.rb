class SomeClass

  def initialize
    @state_lock = ReentrantLock.new
  end

  def do_block_stuff(argument, clazz)
    LITERAL_ARRAY.each do |onum|
      begin
        synchronize do
          puts argument.to_s + " foo" + onum.to_s
        end
        yield
      rescue => e
        clazz.print(e)
      ensure
        synchronize do
          puts argument.to_s + " bar"
        end
      end
    end
  end

  def synchronize
    # The JRuby Mutex uses lockInterruptibly which is what we DO NOT want
    @state_lock.lock
    yield
  ensure
    @state_lock.unlock
  end
end
