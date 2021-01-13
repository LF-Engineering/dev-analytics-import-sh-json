#!/usr/bin/env ruby

require 'json'
require 'pry'

def list_by_org(fn, org)
  data = JSON.parse File.read fn
  data['uidentities'].each do |uuid, row|
    p row if row['enrollments'].select { |rol| rol['organization'] == org }.count > 0
  end
end

if ARGV.length < 2
  puts "Arguments required: filename.json 'org name'"
  exit 1
end

list_by_org(ARGV[0], ARGV[1])
