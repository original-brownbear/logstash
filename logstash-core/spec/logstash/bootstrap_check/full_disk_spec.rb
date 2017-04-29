require "spec_helper"
require "tmpdir"
require "logstash/bootstrap_check/full_disk"

describe LogStash::BootstrapCheck::FullDisk do

  context("when persisted queues are enabled") do
    let(:settings) do
      settings = LogStash::SETTINGS.dup
      settings.set_value("queue.type", "persisted")
      settings.set_value("path.queue", ::File.join(Dir.tmpdir, "some/path"))
      settings
    end

    it "should not throw if enough space available" do
      settings.set_value("queue.page_capacity", 1024)
      expect { LogStash::BootstrapCheck::FullDisk.check(settings) }.to_not raise_error
    end

    it "should throw if not enough space available" do
      settings.set_value("queue.page_capacity", 1024 ** 5)
      expect { LogStash::BootstrapCheck::FullDisk.check(settings) }.to raise_error
    end
  end
end
