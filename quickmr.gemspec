# Generated by jeweler
# DO NOT EDIT THIS FILE DIRECTLY
# Instead, edit Jeweler::Tasks in Rakefile, and run 'rake gemspec'
# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = "quickmr"
  s.version = "0.0.1"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Jakub Pastuszek"]
  s.date = "2013-09-04"
  s.description = "In porcess multithreaded map-reduce framework"
  s.email = "jpastuszek@gmail.com"
  s.extra_rdoc_files = [
    "LICENSE.txt",
    "README.rdoc"
  ]
  s.files = [
    ".document",
    ".rspec",
    "Gemfile",
    "Gemfile.lock",
    "LICENSE.txt",
    "README.rdoc",
    "Rakefile",
    "VERSION",
    "features/quickmr.feature",
    "features/step_definitions/quickmr_steps.rb",
    "features/support/env.rb",
    "lib/quickmr.rb",
    "lib/quickmr/demultiplexer.rb",
    "lib/quickmr/job.rb",
    "lib/quickmr/line_reader.rb",
    "lib/quickmr/mapper.rb",
    "lib/quickmr/merger.rb",
    "lib/quickmr/multiplexer.rb",
    "lib/quickmr/processor_base.rb",
    "lib/quickmr/reducer.rb",
    "lib/quickmr/splitter.rb",
    "quickmr.gemspec",
    "spec/demultiplexer_spec.rb",
    "spec/job_spec.rb",
    "spec/mapper_spec.rb",
    "spec/merger_spec.rb",
    "spec/multiplexer_spec.rb",
    "spec/reducer_spec.rb",
    "spec/spec_helper.rb",
    "spec/splitter_spec.rb"
  ]
  s.homepage = "http://github.com/jpastuszek/quickmr"
  s.licenses = ["MIT"]
  s.require_paths = ["lib"]
  s.rubygems_version = "1.8.25"
  s.summary = "Single process map-reduce"

  if s.respond_to? :specification_version then
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<cli>, ["~> 1.1.0"])
      s.add_runtime_dependency(%q<tribe>, ["~> 0.4"])
      s.add_runtime_dependency(%q<kyotocabinet-ruby>, ["~> 1.27"])
      s.add_development_dependency(%q<rspec>, ["~> 2.13"])
      s.add_development_dependency(%q<rspec-mocks>, ["~> 2.13"])
      s.add_development_dependency(%q<cucumber>, [">= 0"])
      s.add_development_dependency(%q<rdoc>, ["~> 3.9"])
      s.add_development_dependency(%q<jeweler>, ["~> 1.8.4"])
    else
      s.add_dependency(%q<cli>, ["~> 1.1.0"])
      s.add_dependency(%q<tribe>, ["~> 0.4"])
      s.add_dependency(%q<kyotocabinet-ruby>, ["~> 1.27"])
      s.add_dependency(%q<rspec>, ["~> 2.13"])
      s.add_dependency(%q<rspec-mocks>, ["~> 2.13"])
      s.add_dependency(%q<cucumber>, [">= 0"])
      s.add_dependency(%q<rdoc>, ["~> 3.9"])
      s.add_dependency(%q<jeweler>, ["~> 1.8.4"])
    end
  else
    s.add_dependency(%q<cli>, ["~> 1.1.0"])
    s.add_dependency(%q<tribe>, ["~> 0.4"])
    s.add_dependency(%q<kyotocabinet-ruby>, ["~> 1.27"])
    s.add_dependency(%q<rspec>, ["~> 2.13"])
    s.add_dependency(%q<rspec-mocks>, ["~> 2.13"])
    s.add_dependency(%q<cucumber>, [">= 0"])
    s.add_dependency(%q<rdoc>, ["~> 3.9"])
    s.add_dependency(%q<jeweler>, ["~> 1.8.4"])
  end
end

