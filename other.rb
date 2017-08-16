class SomeOtherClass

  def initialize
    @action = Proc.new {|match| puts match}
    @other = eval("lambda do |match, &block|\n
  puts match\n
  block.call(match)\n
end\n")
    @yet_other = eval("lambda do |match, match2, &block|\n
  puts match\n
  block.call(match)\n
  block.call(match)\n
end\n")
  end

  def do_stuff(argument)
    (0..8).each do |t|
      puts argument
      @thread = Thread.current
      [/Thread/, /frr/, /foo/, /bla/].each do |reg|
        @thread.to_s.match(reg) {@yet_other.call(t.to_s, t.to_s) {@yet_other.call(t.to_s, t.to_s) {@other.call(t.to_s) {|m| @action.call(m)}}}}
      end
    end
  end

  def print(arg)
    puts arg.to_s
  end
end
