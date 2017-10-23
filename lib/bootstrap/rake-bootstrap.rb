require 'rake'

rake = Rake.application
rake.init
ARGV << 'bootstrap'
rake.load_rakefile
puts 'load rakefile'
ARGV.pop
