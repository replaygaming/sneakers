module Sneakers
  module Handlers
    class Oneshot
      def initialize(channel, _opts)
        @channel = channel
      end

      def acknowledge(delivery_info)
        @channel.acknowledge(delivery_info.delivery_tag, false)
      end

      def reject(delivery_info, _headers, _msg, requeue = false)
        @channel.reject(delivery_info.delivery_tag, requeue)
      end

      def error(delivery_info, headers, msg, _err)
        reject(delivery_info, headers, msg)
      end

      def timeout(delivery_info, headers, msg)
        reject(delivery_info, headers, msg)
      end

      def noop(delivery_info); end
    end
  end
end
