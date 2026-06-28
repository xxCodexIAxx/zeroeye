# frozen_string_literal: true

require 'minitest/autorun'

module Kernel
  alias market_stream_original_require require

  def require(path)
    case path
    when 'eventmachine'
      module ::EM
        class Connection; end
        def self.add_timer(*); end
      end
      true
    when 'em-websocket-client', 'redis'
      true
    when 'sinatra/base'
      module ::Sinatra
        class Base
          def self.set(*); end
          def self.get(*); end
          def self.error(*); end
          def self.not_found(*); end
          def self.run!(*); end
        end
      end
      true
    else
      market_stream_original_require(path)
    end
  end
end

require_relative 'market_stream'

class MarketStreamBackoffTest < Minitest::Test
  class FixedRng
    def initialize(value)
      @value = value
    end

    def rand
      @value
    end
  end

  def test_initial_delay_uses_production_base
    assert_equal Constants::WS_RECONNECT_BASE, ReconnectBackoff.delay_for(0)
  end

  def test_delay_grows_exponentially
    assert_equal 2, ReconnectBackoff.delay_for(1)
    assert_equal 4, ReconnectBackoff.delay_for(2)
    assert_equal 8, ReconnectBackoff.delay_for(3)
  end

  def test_delay_is_capped_at_production_max
    assert_equal Constants::WS_RECONNECT_MAX, ReconnectBackoff.delay_for(20)
  end

  def test_jitter_stays_within_expected_bounds
    lower = ReconnectBackoff.delay_for(3, jitter: 0.25, rng: FixedRng.new(0.0))
    upper = ReconnectBackoff.delay_for(3, jitter: 0.25, rng: FixedRng.new(1.0))

    assert_equal 6.0, lower
    assert_equal 10.0, upper
  end
end
