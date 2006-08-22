class Foo < ActiveRecord::Base; end

server do |map_reduce|
  map_reduce.type = Foo
  map_reduce.conditions = ["bar = ?", 1]
end

client do |foo|
  foo.do_some_hard_task
end