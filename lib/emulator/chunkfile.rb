require 'time'
require 'emulator/base'

module OssEmulator

  class ChunkFile < File
    attr_accessor :options

    def self.open(filename, options = {})
      file_handle = super(filename, 'rb')
      file_handle.options = options
      file_handle.options[:cur_pos] = 0
      file_handle.options[:bytes_left_to_read] = options[:read_length]
      file_handle.options[:base_part_filename] = options[:base_part_filename]
      file_handle.options[:part_number] = 1
      file_handle.options[:f_part] = nil
      file_handle.options[:request_id] = options[:request_id]

      Log.debug("ChunkFile.open :#{file_handle.options[:base_part_filename]}#{file_handle.options[:part_number]}, #{file_handle.options[:type]}, #{file_handle.options}", 'blue')
      return file_handle
    end

    def read(args)
      case self.options[:type] 
      when 'single_whole'
        return super(Object::STREAM_CHUNK_SIZE)

      when 'single_range'
        return nil if self.options[:bytes_left_to_read] <= 0
        self.pos = self.options[:start_pos] if self.options[:cur_pos] == 0

        bytes_cur_to_read = (self.options[:bytes_left_to_read] <= Object::STREAM_CHUNK_SIZE) ? self.options[:bytes_left_to_read] : Object::STREAM_CHUNK_SIZE
        self.options[:bytes_left_to_read] -= bytes_cur_to_read
        return super(bytes_cur_to_read)

      when 'multipart_whole' 
        part_filename = "#{self.options[:base_part_filename]}#{self.options[:part_number]}"
        return nil unless File.exist?(part_filename)

        self.options[:f_part] = File.open(part_filename, 'rb') unless self.options[:f_part]

        read_buf = self.options[:f_part].read(Object::STREAM_CHUNK_SIZE)
        if self.options[:f_part].eof?
          self.options[:f_part] = nil
          self.options[:part_number] += 1
        end

        return read_buf

      when 'multipart_range'
        return nil if self.options[:bytes_left_to_read] <= 0    

        if !self.options[:f_part]
          if self.options[:cur_pos] == 0
            part_size = File.size("#{self.options[:base_part_filename]}#{self.options[:part_number]}")
            loop do
              if self.options[:start_pos] >= part_size*self.options[:part_number]
                self.options[:part_number] += 1
                next
              else
                break
              end
            end
            part_filename = "#{self.options[:base_part_filename]}#{self.options[:part_number]}"
            return nil unless File.exist?(part_filename)
            self.options[:f_part] = File.open(part_filename, 'rb') unless self.options[:f_part]
            self.options[:f_part].pos = self.options[:start_pos] - part_size * (self.options[:part_number] - 1)
          else
            part_filename = "#{self.options[:base_part_filename]}#{self.options[:part_number]}"
            return nil unless File.exist?(part_filename)
            self.options[:f_part] = File.open(part_filename, 'rb') unless self.options[:f_part]
            self.options[:f_part].pos  = 0
          end
        else
          self.options[:f_part].pos  = self.options[:cur_pos]
        end

        bytes_cur_to_read = (self.options[:bytes_left_to_read] <= Object::STREAM_CHUNK_SIZE) ? self.options[:bytes_left_to_read] : Object::STREAM_CHUNK_SIZE
        read_buf = self.options[:f_part].read(bytes_cur_to_read)
        self.options[:cur_pos] = self.options[:f_part].pos 
        self.options[:bytes_left_to_read] -= read_buf.length
        if self.options[:f_part].eof?
          self.options[:f_part] = nil
          self.options[:part_number] += 1
        end

        return read_buf
      else
        return nil
      end # when

      return nil
    end # func read

  end # class ChunkFile

end # OssEmulator
