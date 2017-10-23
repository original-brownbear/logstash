
rule ".rb" => ".treetop" do |task, args|
  require "treetop"
  compiler = Treetop::Compiler::GrammarCompiler.new
  compiler.compile(task.source, task.name)
  puts "Compiling #{task.source}"
end

namespace "compile" do
  desc "Compile the config grammar"

  task "grammar" => "logstash-core/lib/logstash/config/grammar.rb"
  
  def safe_system(*args)
    if !system(*args)
      status = $?
      raise "Got exit status #{status.exitstatus} attempting to execute #{args.inspect}!"
    end
  end

  desc "Build everything"
  task "all" => ["grammar"]
end
