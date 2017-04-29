# encoding: utf-8
require "java"
require "logstash/errors"

module LogStash module BootstrapCheck
    class FullDisk
      def self.check(settings)
        if settings.get("queue.type") == "persisted"
          unless org.logstash.ackedqueue.FsUtil.has_free_space(
            settings.get("path.queue"),
            [settings.get("queue.page_capacity"), settings.get("queue.max_bytes")].max
          )
            raise LogStash::BootstrapCheckError,
                  "Not enough disk space to allocate persisted queue page"
          end
        end
      end
    end
end end
