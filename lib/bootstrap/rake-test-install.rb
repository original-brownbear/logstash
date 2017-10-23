require 'rake'

rake = Rake.application
rake.init
ARGV << 'test:install-core'
rake.load_rakefile
puts 'load rakefile'
