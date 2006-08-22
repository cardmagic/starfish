class Foo
  attr_reader :i
  
  def initialize
    @i = 0
  end
  
  def inc
    logger.info "YAY it incremented by 1 up to #{@i}"
    @i += 1
  end
end

server :log => "/tmp/foo.log" do |object|
  object = Foo.new
end

client do |object|
  object.inc
end