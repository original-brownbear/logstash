# encoding: utf-8
require "java"
require "logstash/errors"

module LogStash module BootstrapCheck
    class PersistedQueueConfig
      def self.check(settings)
        if settings.get("queue.type") == "persisted"
          if settings.get("queue.page_capacity") > settings.get("queue.max_bytes")
            raise LogStash::BootstrapCheckError,
                  "Invalid configuration, `queue.page_capacity` must be smaller or equal to `queue.max_bytes`"
          end
          unless org.logstash.ackedqueue.FsUtil.has_free_space(
            settings.get("path.queue"), settings.get("queue.max_bytes")
          )
            raise LogStash::BootstrapCheckError,
                  "Not enough disk space to allocate persisted queue page"
          end
        end
      end
    end
end end
