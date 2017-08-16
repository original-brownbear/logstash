class SomeYetOtherClass

  def initialize
    define_singleton_method :do_blk_stuff do |a, c, d, &b|
      @thread = Thread.current
      b.call
      puts (a+c+d).to_s
      puts @thread
    end
  end

  def print(arg)
    puts arg.to_s
  end
end
