module Sneakers
  module Handlers
    class Maxretry
      DEFAULT_MAX_RETRY_ATTEMPTS = 25

      ###
      # Maxretry uses dead letter policies on Rabbitmq to requeue and retry messages after a timeout
      #
      ###

      def initialize(channel, opts)
        @channel = channel

        @max_retries = opts.fetch(:max_retries, DEFAULT_MAX_RETRY_ATTEMPTS)
        exchange = opts[:exchange]

        # If there is no retry exchange specified, use #{exchange}-retry
        retry_name = opts[:retry_exchange] || "#{exchange}-retry"

        # Create the retry exchange as a durable topic so we retain original routing keys but bind the queue using a wildcard
        retry_exchange = @channel.topic(retry_name, durable: true)

        # Create the retry queue with the same name as the retry exchange and a dead letter exchange
        # The dead letter exchange is the default exchange and the ttl can be from the opts or defaults to 60 seconds
        retry_queue = @channel.queue(retry_name, durable: true, arguments: {
            :'x-dead-letter-exchange' => exchange,
            :'x-message-ttl' => opts[:retry_timeout] || 60_000
        })

        # Bind the retry queue to the retry topic exchange with a wildcard
        retry_queue.bind(retry_exchange, routing_key: '#')

        ## Setup the error queue

        # If there is no error exchange specified, use #{exchange}-error
        error_name = opts[:error_exchange] || "#{exchange}-error"

        # Create the error exchange as a durable topic so we retain original routing keys but bind the queue using a wildcard
        @error_exchange = @channel.topic(error_name, durable: true)

        # Create the error queue with the same name as the error exchange and a dead letter exchange
        # The dead letter exchange is the default exchange and the ttl can be from the opts or defaults to 60 seconds
        error_queue = @channel.queue(error_name, durable: true)

        # Bind the error queue to the error topic exchange with a wildcard
        error_queue.bind(@error_exchange, routing_key: '#')
      end

      def acknowledge(delivery_info)
        @channel.acknowledge(delivery_info.delivery_tag, false)
      end

      def reject(delivery_info, headers, msg, requeue = false)
        # Note to readers, the count of the x-death will increment by 2 for each retry, once for the reject and once for the expiration from the retry queue
        if headers.nil? || headers['x-death'].nil? || headers['x-death'].count < @max_retries
          # We call reject which will route the message to the x-dead-letter-exchange (ie. retry exchange) on the queue
          @channel.reject(delivery_info.delivery_tag, requeue)

        else
          # Retried more than the max times
          # Publish the original message with the routing_key to the error exchange
          @error_exchange.publish(msg, routing_key: delivery_info.routing_key)
          @channel.acknowledge(delivery_info.delivery_tag, false)

        end
      end

      def error(delivery_info, headers, msg, _err)
        reject(delivery_info.delivery_tag, headers, msg)
      end

      def timeout(delivery_info, headers, msg)
        reject(delivery_info.delivery_tag, headers, msg)
      end

      def noop(delivery_info); end

      # private

      # # delayed_job uses the same basic formula
      # def miliseconds_to_delay(count)
      #   (count ** 4) + 15 + (rand(30) * (count + 1)) * 1000
      # end
    end
  end
end
