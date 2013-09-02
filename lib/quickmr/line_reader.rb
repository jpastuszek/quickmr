class LineRader < ProcessorBase
	def initialize(options)
		super
		@max_lines = options[:max_lines] || -1
		@total_lines = 0
	end

private

	def on_read_file(event)
		file_name = event.data

		log "reading lines from file: #{file_name}"
		begin
			File.open(file_name, 'r') do |file|
				file.each_line do |line|
					output [file_name, line]
					@total_lines += 1
					@max_lines -= 1
					break if @max_lines == 0
				end
			end
		ensure
			output nil
		end
		log "done reading lines from file: #{file_name}; read #{@total_lines} lines"
	end
end

