server :log => "/tmp/mylog" do |map_reduce|
  map_reduce.type = File
  map_reduce.input = "/tmp/big_ass_file"
end

client do |line|
  if line =~ /some_regex/
    logger.info(line)
  end
end