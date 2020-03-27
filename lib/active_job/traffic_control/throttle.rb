# frozen_string_literal: true

module ActiveJob
  module TrafficControl
    module Throttle
      extend ::ActiveSupport::Concern

      class_methods do
        def throttle(
          threshold:,
          period:,
          drop: false,
          key: nil,
          bucket: nil,
          delay: period,
          min_delay_multiplier: 1,
          max_delay_multiplier: 5
        )
          raise ArgumentError, "Threshold needs to be an integer > 0" if threshold.to_i < 1
          raise ArgumentError, "min_delay_multiplier needs to be a number >= 0" unless min_delay_multiplier.is_a?(Numeric) && min_delay_multiplier >= 0
          raise ArgumentError, "max_delay_multiplier needs to be a number >= max_delay_multiplier" unless max_delay_multiplier.is_a?(Numeric) && max_delay_multiplier >= min_delay_multiplier
          raise ArgumentError, "delay needs to a number > 0 " unless delay.is_a?(Numeric) && delay > 0

          bucket ||= :_default

          self.job_throttling ||= {}

          self.job_throttling[bucket.to_sym] = {
            threshold: threshold,
            period: period,
            drop: drop,
            key: key,
            delay: delay,
            min_delay_multiplier: min_delay_multiplier,
            max_delay_multiplier: max_delay_multiplier
          }
        end

        def throttling_lock_key(job, bucket)
          lock_key("throttle_#{bucket}", job, job_throttling[bucket])
        end
      end

      included do
        include ActiveJob::TrafficControl::Base

        class_attribute :job_throttling, instance_accessor: false

        def job_throttling_bucket
          :_default
        end

        around_perform do |job, block|
          if self.class.job_throttling.blank?
            block.call
            next
          end

          bucket = job_throttling_bucket&.to_sym

          if bucket.blank?
            block.call
            next
          end

          current_job_throttling = self.class.job_throttling[bucket]

          if current_job_throttling.blank?
            block.call
            next
          end

          lock_options = {
            resources: current_job_throttling[:threshold],
            stale_lock_expiration: current_job_throttling[:period]
          }

          with_lock_client(self.class.throttling_lock_key(job, bucket), lock_options) do |client|
            token = client.lock

            if token
              block.call
            elsif current_job_throttling[:drop]
              drop("throttling")
            else
              delay = current_job_throttling[:delay]
              min_delay_multiplier = current_job_throttling[:min_delay_multiplier]
              max_delay_multiplier = current_job_throttling[:max_delay_multiplier]
              reenqueue((delay * min_delay_multiplier)...(delay * max_delay_multiplier), "throttling")
            end
          end
        end
      end
    end
  end
end
