module Sneakers
  module Handlers
    class Exponential
      DEFAULT_MAX_RETRY_ATTEMPTS = 25

      ###
      # Exponential uses dead letter policies on RabbitMQ to requeue
      # and retry messages after an exponential timeout
      #
      ###

      def initialize(channel, opts)
        @channel = channel

        @max_retries = [opts.fetch(:max_retries, DEFAULT_MAX_RETRY_ATTEMPTS), 1].max
        exchange = opts[:exchange]

        # If there is no retry exchange specified, use #{exchange}-retry
        retry_name = opts[:retry_exchange] || "#{exchange}-retry"

        # Create the retry exchange as a durable topic so we retain original routing keys but bind the queue using a wildcard
        @retry_exchange = @channel.fanout(retry_name, durable: true)

        # Create the retry queue with the same name as the retry exchange and a dead letter exchange
        # The dead letter exchange is the default exchange and the ttl can be from the opts or defaults to 60 seconds
        @channel.queue(
          retry_name,
          durable: true,
          arguments: { :'x-dead-letter-exchange' => exchange }
        ).bind(retry_exchange)
      end

      def acknowledge(delivery_info)
        @channel.acknowledge(delivery_info.delivery_tag, false)
      end

      def reject(delivery_info, headers, msg, requeue = false)
        if requeue
          requeue(delivery_info, headers, msg)
        else
          @channel.reject(delivery_info.delivery_tag, requeue)
        end
      end

      def requeue(delivery_info, headers, msg)
        reject(delivery_info.delivery_tag)

        retry_count = (headers && headers['retry_count']).to_i + 1

        return if retry_count < @max_retries

        @retry_exchange.publish(
          msg,
          expiration: miliseconds_to_delay(retry_count),
          headers: { retry_count: retry_count }
        )
      end

      def error(delivery_info, headers, msg, _err)
        requeue(delivery_info.delivery_tag, headers, msg)
      end

      def timeout(delivery_info, headers, msg)
        requeue(delivery_info.delivery_tag, headers, msg)
      end

      def noop(_delivery_info); end

      # private

      # # delayed_job uses the same basic formula
      def miliseconds_to_delay(count)
        (count**4) + 15 + (rand(30) * (count + 1)) * 1000
      end
    end
  end
end
