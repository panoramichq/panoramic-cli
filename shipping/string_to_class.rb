#!/usr/bin/env ruby

def class_s(name)
    class_name = name.capitalize
    class_name.gsub!(/[-_.\s]([a-zA-Z0-9])/) { Regexp.last_match(1).upcase }
    class_name.tr!("+", "x")
    class_name.sub!(/(.)@(\d)/, "\\1AT\\2")
    class_name
end

def str_to_class(name)
    mod = Module.new
    class_name = class_s(name)
    class_name
end

if ARGV.length == 1
    puts str_to_class(ARGV[0])
else
    raise("Invalid number of arguments, should only be 1")
end
