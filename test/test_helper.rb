$:.unshift(File.dirname(__FILE__) + '/../lib')

require 'test/unit'
require 'rubygems'
require 'active_support'
require 'active_record'
require 'active_record/fixtures'
require 'action_controller'

require 'ruby-debug'
Debugger.settings[:autoeval] = true
Debugger.start

config = YAML::load(IO.read(File.dirname(__FILE__) + '/database.yml'))
ActiveRecord::Base.logger = Logger.new(File.dirname(__FILE__) + '/debug.log')
ActiveRecord::Base.establish_connection(config[ENV['DB'] || 'sqlite3'])

load(File.join(File.dirname(__FILE__), 'schema.rb'))

require 'models'
require File.join(File.dirname(__FILE__), '../init')


def assert_same_elements(a1, a2, msg = nil)
  [:select, :inject, :size].each do |m|
    [a1, a2].each {|a| assert_respond_to(a, m, "Are you sure that #{a.inspect} is an array?  It doesn't respond to #{m}.") }
  end

  assert a1h = a1.inject({}) { |h,e| h[e] = a1.select { |i| i == e }.size; h }
  assert a2h = a2.inject({}) { |h,e| h[e] = a2.select { |i| i == e }.size; h }

  assert_equal(a1h, a2h, msg)
end
