require 'drb'

require 'map_reduce/active_record'
require 'map_reduce/file'
require 'map_reduce/array'

class MapReduceError < StandardError; end
class MapReduce
  @@types = {
    ::ActiveRecord::Base => [::Array, String, NilClass],
    ::File => String
  }
  
  attr_accessor :type, :input, :limit
  alias :conditions :input
  alias :conditions= :input=
  
  def initialize
    @lock = false
    @offset = 0
    @limit = 1
    
    @time_began = 0

    @num_objects_grabbed = 0
    @time_spent_grabbing_objects = 0.0
    @time_spent_processing_objects = 0.0
    
    @num_queues_grabbed = 0
    @time_spent_grabbing_queues = 0.0
    
    @queue = []
  end
  
  def add_time_spent_processing_objects(time)
    @time_spent_processing_objects += time
  end
  
  def stats
    {
      :time_began => @time_began,
      :num_queues_grabbed => @num_queues_grabbed,
      :time_spent_grabbing_queues => @time_spent_grabbing_objects,
      :num_objects_grabbed => @num_objects_grabbed,
      :time_spent_grabbing_objects => @time_spent_grabbing_objects,
      :time_spent_processing_objects => @time_spent_processing_objects
    }
  end
  
  def type=(type)
    @type = type
    if valid_type?
      self.class.instance_eval "include MapReduce::#{base_type}"
    end
  end
  
  def base_type_to_s
    base_type.to_s if valid_type?
  end

  def type_to_s
    type.to_s if valid_type?
  end
  
  def spool=(type, input)
    self.type = type
    self.input = input
  end
  
  def valid?
    valid_type? && valid_input?
  end
  
  def valid_type?
    @@types.keys.each {|type| return true if @type && (@type < type || @type == type)}
    return false
  end
  
  def valid_input?
    if not valid_type?
      return false
    else
      Array(@@types[base_type]).each {|input_type| return true if input.is_a?(input_type)}
      raise MapReduceError, "invalid input (#{@input.inspect}) for type: #{base_type}. Try one of the following: #{Array(@@types[base_type]).join(", ")}"
    end
  end
  
  def method_missing(name, *args)
    if name.to_s =~ /(.*)=$/ && args[0].is_a?(Proc)
      self.class.instance_eval do
        define_method($1, args[0])
      end
    else
      super
    end
  end
  
  def raise_if_invalid!
    if not valid_type?
      raise MapReduceError, "invalid type, please make sure you provide one of the following classes or sub-classes thereof: ActiveRecord::Base, File, or Array"
    end
    if not valid_input?
      raise MapReduceError, "invalid input, please make sure you provide one of the following: #{Array(@@types[type]).join(", ")}"
    end
  end
  
  def map_reduce?
    true
  end
  
  def base_type
    check_type = @type
    type_found = false
    while check_type.superclass
      if @@types.include?(check_type)
        return check_type
      else
        check_type = check_type.superclass
      end
    end
  end
end