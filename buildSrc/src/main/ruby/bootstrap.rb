#!/usr/bin/env ruby
# Licensed to Elasticsearch under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

def install_gem(name, requirement)
  Gem::Specification.reset
  Gem.clear_paths
  Gem.clear_default_specs

  require 'rubygems/commands/install_command'
  installer = Gem::Commands::InstallCommand.new
  installer.options[:generate_rdoc] = false
  installer.options[:generate_ri] = false
  installer.options[:version] = requirement
  installer.options[:args] = [name]
  installer.options[:install_dir] = ENV['GEM_HOME']
  installer.options[:document] = []
  begin
    installer.execute
  rescue Gem::LoadError => _
    # For some weird reason the rescue from the 'require' task is being brought down here
    # We don't know why placing this solves it, but it does.
  end

  gem name, requirement
end

def ensure_gem(name, requirement)
  gem name, requirement
rescue Gem::LoadError => _
  puts "Failed to load #{name}. Will try to install. "
  install_gem(name, requirement)
end

def bundler(gemfile)
  puts('Invoking bundler install for ' + gemfile)

  require 'bundler/cli'
  Bundler::CLI.start(['install', '--gemfile=' + gemfile])
end

def pack_gem(gemspec)
  require 'rubygems/commands/build_command'
  builder = Gem::Commands::BuildCommand.new
  builder.options[:args] = [gemspec]
  builder.execute
end
