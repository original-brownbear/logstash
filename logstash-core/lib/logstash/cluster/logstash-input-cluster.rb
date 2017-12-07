require "logstash/plugin"
require "logstash/inputs/threadable"
require "logstash/namespace"

class LogStash::Inputs::Cluster < LogStash::Inputs::Threadable
  config_name "cluster"

  def register

  end

  # def register

  def run(queue)

  end

  def close

  end
end
