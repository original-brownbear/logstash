require "spec_helper"
require "tmpdir"
require "logstash/bootstrap_check/full_disk"

describe LogStash::BootstrapCheck::FullDisk do

  context("when persisted queues are enabled") do
    let(:settings) do
      settings = LogStash::SETTINGS.new
      settings.set("queue.type", "persisted")
      settings.set("path.queue", ::File.join(Dir.tmpdir, "some/path"))
      settings.set("queue.page_capacity", "persisted")
    end

    it "should not throw if enough space available" do
      subject.check(settings)
    end
  end
end
