require "spec_helper"
require "tmpdir"
require "logstash/bootstrap_check/full_disk"

describe LogStash::BootstrapCheck::FullDisk do

  context("when persisted queues are enabled") do
    let(:settings) do
      settings = LogStash::Settings.new
      settings.set("queue.type", "persisted")
      settings.set("path.queue", ::File.join(Dir.tmpdir, "some/path"))
    end

    it "should not throw if enough space available" do
      settings.set("queue.page_capacity", 1024)
      expect(subject.check(settings)).to_not raise_error
    end

    it "should throw if not enough space available" do
      settings.set("queue.page_capacity", 1024 ** 5)
      expect(subject.check(settings)).to raise_error
    end
  end
end
